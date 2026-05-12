package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"mr-cgdb/internal/keywords"
	"mr-cgdb/internal/mathvec"
	"mr-cgdb/internal/ollama"
	"mr-cgdb/internal/store"
	rspdf "rsc.io/pdf"
)

func main() {
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL required")
	}
	pdfDir := getenv("PDF_STORAGE_DIR", "/data/pdfs")
	maxBytes := int64(200000000)
	if v := strings.TrimSpace(os.Getenv("PDF_DOWNLOAD_MAX_BYTES")); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed > 0 {
			maxBytes = parsed
		}
	}
	if err := os.MkdirAll(pdfDir, 0o755); err != nil {
		log.Fatal(err)
	}
	ollamaBase := getenv("OLLAMA_BASE_URL", "http://ollama:11434")
	embedModel := getenv("EMBED_MODEL", "nomic-embed-text")
	chatModel := getenv("CHAT_MODEL", "llama3.2:1b")

	db, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	oc := ollama.NewDefault(ollamaBase, embedModel, chatModel)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	cleanupTicker := time.NewTicker(6 * time.Hour)
	defer cleanupTicker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := processOnePDFJob(ctx, db, pdfDir, maxBytes); err != nil {
				log.Printf("worker: %v", err)
			}
			if err := processOneProfileAnalyzeJob(ctx, db, oc); err != nil {
				log.Printf("worker analyze: %v", err)
			}
			if err := processOneProfileLLMVerifyJob(ctx, db, oc, pdfDir, maxBytes); err != nil {
				log.Printf("worker verify: %v", err)
			}
		case <-cleanupTicker.C:
			if err := cleanupUnreferenced(ctx, db); err != nil {
				log.Printf("cleanup: %v", err)
			}
		}
	}
}

func processOneProfileAnalyzeJob(ctx context.Context, db *pgxpool.Pool, oc *ollama.Client) error {
	job, err := store.ClaimNextPendingJob(ctx, db, "profile_analyze")
	if err != nil || job == nil {
		return err
	}
	var payload struct {
		ProfileID int64 `json:"profileId"`
		PaperID   int64 `json:"paperId"`
	}
	if err := json.Unmarshal(job.Payload, &payload); err != nil || payload.ProfileID <= 0 || payload.PaperID <= 0 {
		_ = store.FailJob(ctx, db, job.ID, "invalid payload")
		return nil
	}
	cfg, err := store.GetProfileConfig(ctx, db, payload.ProfileID)
	if err != nil {
		_ = store.FailJob(ctx, db, job.ID, err.Error())
		return nil
	}
	paper, err := store.GetPaperRow(ctx, db, payload.PaperID)
	if err != nil {
		_ = store.FailJob(ctx, db, job.ID, err.Error())
		return nil
	}
	text := strings.ToLower(strings.TrimSpace(paper.Title) + "\n" + strings.TrimSpace(paper.Abstract))
	m := keywords.New(cfg.PositiveKeywords)
	keywordPass := len(cfg.PositiveKeywords) > 0 && m.MatchText(text)
	var hits []string
	for _, kw := range cfg.PositiveKeywords {
		kw = strings.ToLower(strings.TrimSpace(kw))
		if kw != "" && strings.Contains(text, kw) {
			hits = append(hits, kw)
		}
	}
	score := 0.0
	scoreMode := "keyword_ratio"
	// Prefer profile-specific embedding similarity to known relevant papers.
	// Falls back to keyword ratio when no liked embeddings exist yet.
	likedEmb, embErr := store.ListProfileLikeEmbeddings(ctx, db, payload.ProfileID)
	candEmb := []float64(nil)
	if embErr == nil && len(likedEmb) > 0 {
		if e, ge := ensurePaperEmbedding(ctx, db, oc, payload.PaperID, paper.Title, paper.Abstract); ge == nil {
			candEmb = e
		}
	}
	if len(candEmb) > 0 && len(likedEmb) > 0 {
		best := -1.0
		for _, e := range likedEmb {
			if sim := mathvec.Cosine(candEmb, e); sim > best {
				best = sim
			}
		}
		if best < 0 {
			best = 0
		}
		if best > 1 {
			best = 1
		}
		score = best
		scoreMode = "embedding_max_cosine_to_likes"
	} else if len(cfg.PositiveKeywords) > 0 {
		score = float64(len(hits)) / float64(len(cfg.PositiveKeywords))
		if math.IsNaN(score) || math.IsInf(score, 0) {
			score = 0
		}
	}
	var llmRel *bool
	var llmRaw string
	llmSource := strings.ToLower(strings.TrimSpace(getenv("PROFILE_ANALYZE_LLM_SOURCE", "paper")))
	llmCallOK := false
	if llmSource == "profile" {
		prompt := strings.TrimSpace(cfg.LLMPrompt)
		if prompt == "" {
			prompt = `You are a strict classifier for this profile. Given title and abstract, respond with JSON only: {"relevant":true} when clearly relevant, otherwise {"relevant":false}.`
		}
		rel, raw, e := oc.ChatRelevant(ctx, prompt, "Title:\n"+paper.Title+"\n\nAbstract:\n"+paper.Abstract)
		if e == nil {
			llmCallOK = true
			llmRel = &rel
			llmRaw = raw
		} else {
			llmRaw = "llm error: " + e.Error()
		}
	} else {
		// Default: reuse already computed paper-level LLM decision from pipeline.
		// This avoids duplicate /api/chat calls in profile_analyze jobs.
		llmRel = paper.LLMRelevant
		if llmRel != nil {
			llmRaw = "reused from papers.llm_relevant"
		} else {
			// Preserve a previous manual profile-level LLM decision if present.
			prev, pe := store.GetProfilePaperAnalysis(ctx, db, payload.ProfileID, payload.PaperID)
			if pe == nil && prev != nil && prev.LLMRelevant != nil {
				llmRel = prev.LLMRelevant
				llmRaw = prev.LLMRaw
			} else {
				llmRaw = "papers.llm_relevant unavailable"
			}
		}
	}
	wouldAuto := keywordPass
	if llmRel != nil {
		wouldAuto = wouldAuto && *llmRel
	}
	if err := store.UpsertProfilePaperAnalysis(ctx, db, &store.ProfilePaperAnalysis{
		ProfileID:               payload.ProfileID,
		PaperID:                 payload.PaperID,
		KeywordPass:             keywordPass,
		KeywordHits:             hits,
		LLMRelevant:             llmRel,
		LLMRaw:                  llmRaw,
		ShadowWouldAutoDownload: wouldAuto,
		ShadowScore:             score,
	}); err != nil {
		_ = store.FailJob(ctx, db, job.ID, err.Error())
		return nil
	}
	_ = store.CompleteJob(ctx, db, job.ID)

	titlePrev := strings.TrimSpace(paper.Title)
	if len(titlePrev) > 140 {
		titlePrev = titlePrev[:140] + "…"
	}
	rawPrev := strings.TrimSpace(llmRaw)
	if len(rawPrev) > 180 {
		rawPrev = rawPrev[:180] + "…"
	}
	log.Printf("mr-cgdb worker profile_analyze done job_id=%d profile_id=%d paper_id=%d profile_positive_kw=%d keyword_pass=%v keyword_hits=%v llm_source=%s llm_call_ok=%v llm_rel=%v shadow_mode=%s shadow_score=%.4f liked_embedding_count=%d shadow_auto_dl=%v llm_raw_preview=%q title=%q",
		job.ID, payload.ProfileID, payload.PaperID, len(cfg.PositiveKeywords), keywordPass, hits, llmSource, llmCallOK, llmRel, scoreMode, score, len(likedEmb), wouldAuto, rawPrev, titlePrev)
	return nil
}

func ensurePaperEmbedding(ctx context.Context, db *pgxpool.Pool, oc *ollama.Client, paperID int64, title, abstract string) ([]float64, error) {
	if e, err := store.GetPaperEmbedding(ctx, db, paperID); err == nil && len(e) > 0 {
		return e, nil
	}
	text := strings.TrimSpace(title) + "\n" + strings.TrimSpace(abstract)
	emb, err := oc.Embedder(ctx, text)
	if err != nil {
		return nil, err
	}
	if err := store.UpdatePaperEmbedding(ctx, db, paperID, emb); err != nil {
		return nil, err
	}
	return emb, nil
}

func processOneProfileLLMVerifyJob(ctx context.Context, db *pgxpool.Pool, oc *ollama.Client, pdfDir string, maxBytes int64) error {
	job, err := store.ClaimNextPendingJob(ctx, db, "profile_llm_verify")
	if err != nil || job == nil {
		return err
	}
	var payload struct {
		ProfileID int64 `json:"profileId"`
		PaperID   int64 `json:"paperId"`
	}
	if err := json.Unmarshal(job.Payload, &payload); err != nil || payload.ProfileID <= 0 || payload.PaperID <= 0 {
		_ = store.FailJob(ctx, db, job.ID, "invalid payload")
		return nil
	}
	prof, err := store.GetProfileByID(ctx, db, payload.ProfileID)
	if err != nil {
		_ = store.FailJob(ctx, db, job.ID, err.Error())
		return nil
	}
	cfg, err := store.GetProfileConfig(ctx, db, payload.ProfileID)
	if err != nil {
		_ = store.FailJob(ctx, db, job.ID, err.Error())
		return nil
	}
	paper, err := store.GetPaperRow(ctx, db, payload.PaperID)
	if err != nil {
		_ = store.FailJob(ctx, db, job.ID, err.Error())
		return nil
	}

	url := resolvePDFURL("", paper)
	if url == "" {
		_ = store.FailJob(ctx, db, job.ID, "no resolvable pdf url")
		return nil
	}
	dst := filepath.Join(pdfDir, strconv.FormatInt(payload.PaperID, 10)+".verify.pdf")
	if _, err := downloadToFile(url, dst, maxBytes); err != nil {
		_ = store.FailJob(ctx, db, job.ID, "download failed: "+err.Error())
		return nil
	}
	defer os.Remove(dst)
	text, err := extractPDFPlainText(dst)
	if err != nil || strings.TrimSpace(text) == "" {
		text = strings.TrimSpace(paper.Title) + "\n\n" + strings.TrimSpace(paper.Abstract)
	}

	chunks := chunkText(text, 7000, 500)
	if len(chunks) > 12 {
		chunks = chunks[:12]
	}
	summaries := make([]string, 0, len(chunks))
	for i, ch := range chunks {
		sum, se := summarizeChunk(ctx, oc, i+1, len(chunks), ch)
		if se != nil {
			summaries = append(summaries, fmt.Sprintf("Chunk %d: summary failed (%v)", i+1, se))
			continue
		}
		summaries = append(summaries, fmt.Sprintf("Chunk %d: %s", i+1, strings.TrimSpace(sum)))
	}
	likes, _ := store.ListProfileLikes(ctx, db, payload.ProfileID, "", "", 20)
	examples := make([]string, 0, 5)
	for i, lp := range likes {
		if i >= 5 {
			break
		}
		examples = append(examples, fmt.Sprintf("%d) %s\nAbstract: %s",
			i+1, strings.TrimSpace(lp.Title), strings.TrimSpace(lp.Abstract)))
	}
	userPrompt := "This is a paper " + strings.TrimSpace(paper.Title) +
		". The user wants to know if it's relevant to their topic of scientific research - " + strings.TrimSpace(prof.Name) + ".\n" +
		"They have specified keywords: " + strings.Join(cfg.PositiveKeywords, ", ") + ".\n" +
		"Known relevant papers:\n" + strings.Join(examples, "\n\n") + "\n\n" +
		"Paper abstract:\n" + strings.TrimSpace(paper.Abstract) + "\n\n" +
		"PDF chunk summaries:\n" + strings.Join(summaries, "\n")
	systemPrompt := `You are a strict profile-specific verifier. Return JSON only: {"relevant":true} or {"relevant":false}.`
	rel, raw, llmErr := oc.ChatRelevant(ctx, systemPrompt, userPrompt)
	var llmRel *bool
	llmRaw := raw
	if llmErr == nil {
		llmRel = &rel
	} else {
		llmRaw = "llm error: " + llmErr.Error()
	}

	titleAbs := strings.ToLower(strings.TrimSpace(paper.Title) + "\n" + strings.TrimSpace(paper.Abstract))
	m := keywords.New(cfg.PositiveKeywords)
	keywordPass := len(cfg.PositiveKeywords) > 0 && m.MatchText(titleAbs)
	var hits []string
	for _, kw := range cfg.PositiveKeywords {
		kw = strings.ToLower(strings.TrimSpace(kw))
		if kw != "" && strings.Contains(titleAbs, kw) {
			hits = append(hits, kw)
		}
	}
	score := 0.0
	if prev, e := store.GetProfilePaperAnalysis(ctx, db, payload.ProfileID, payload.PaperID); e == nil && prev != nil {
		score = prev.ShadowScore
	} else if len(cfg.PositiveKeywords) > 0 {
		score = float64(len(hits)) / float64(len(cfg.PositiveKeywords))
	}
	wouldAuto := keywordPass
	if llmRel != nil {
		wouldAuto = wouldAuto && *llmRel
	}
	if err := store.UpsertProfilePaperAnalysis(ctx, db, &store.ProfilePaperAnalysis{
		ProfileID:               payload.ProfileID,
		PaperID:                 payload.PaperID,
		KeywordPass:             keywordPass,
		KeywordHits:             hits,
		LLMRelevant:             llmRel,
		LLMRaw:                  llmRaw,
		ShadowWouldAutoDownload: wouldAuto,
		ShadowScore:             score,
	}); err != nil {
		_ = store.FailJob(ctx, db, job.ID, err.Error())
		return nil
	}
	_ = store.CompleteJob(ctx, db, job.ID)
	log.Printf("mr-cgdb worker profile_llm_verify done job_id=%d profile_id=%d paper_id=%d chunks=%d llm_rel=%v",
		job.ID, payload.ProfileID, payload.PaperID, len(chunks), llmRel)
	return nil
}

func extractPDFPlainText(path string) (string, error) {
	r, err := rspdf.Open(path)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for i := 1; i <= r.NumPage(); i++ {
		p := r.Page(i)
		if p.V.IsNull() {
			continue
		}
		c := p.Content()
		for _, t := range c.Text {
			if s := strings.TrimSpace(t.S); s != "" {
				b.WriteString(s)
				b.WriteByte(' ')
			}
		}
	}
	return strings.TrimSpace(b.String()), nil
}

func chunkText(s string, size, overlap int) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	if size < 500 {
		size = 500
	}
	if overlap < 0 || overlap >= size {
		overlap = 0
	}
	rs := []rune(s)
	if len(rs) <= size {
		return []string{s}
	}
	step := size - overlap
	out := make([]string, 0, (len(rs)/step)+1)
	for i := 0; i < len(rs); i += step {
		j := i + size
		if j > len(rs) {
			j = len(rs)
		}
		out = append(out, string(rs[i:j]))
		if j == len(rs) {
			break
		}
	}
	return out
}

func summarizeChunk(ctx context.Context, oc *ollama.Client, idx, total int, chunk string) (string, error) {
	sys := `Summarize the paper chunk for relevance classification. Return JSON only: {"summary":"..."}`
	user := fmt.Sprintf("Chunk %d/%d:\n%s", idx, total, chunk)
	raw, err := oc.ChatJSON(ctx, sys, user)
	if err != nil {
		return "", err
	}
	var out struct {
		Summary string `json:"summary"`
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return "", err
	}
	return out.Summary, nil
}

func processOnePDFJob(ctx context.Context, db *pgxpool.Pool, pdfDir string, maxBytes int64) error {
	job, err := store.ClaimNextPendingJob(ctx, db, "pdf_download")
	if err != nil || job == nil {
		return err
	}
	var payload struct {
		PaperID int64 `json:"paperId"`
	}
	if err := json.Unmarshal(job.Payload, &payload); err != nil || payload.PaperID <= 0 {
		_ = store.FailJob(ctx, db, job.ID, "invalid payload")
		return nil
	}
	local, status, sourceURL, _ := store.GetPaperFile(ctx, db, payload.PaperID)
	if status == "ready" && strings.TrimSpace(local) != "" {
		_ = store.CompleteJob(ctx, db, job.ID)
		return nil
	}
	p, err := store.GetPaperRow(ctx, db, payload.PaperID)
	if err != nil {
		_ = store.FailJob(ctx, db, job.ID, err.Error())
		return nil
	}
	url := strings.TrimSpace(sourceURL)
	if url == "" && p.URL != nil {
		url = strings.TrimSpace(*p.URL)
	}
	url = resolvePDFURL(url, p)
	if url == "" {
		_ = store.SetPaperFileState(ctx, db, payload.PaperID, "", "", 0, "failed", "paper has no resolvable pdf url")
		_ = store.FailJob(ctx, db, job.ID, "paper has no resolvable pdf url")
		return nil
	}
	dst := filepath.Join(pdfDir, strconv.FormatInt(payload.PaperID, 10)+".pdf")
	n, err := downloadToFile(url, dst, maxBytes)
	if err != nil {
		_ = store.SetPaperFileState(ctx, db, payload.PaperID, url, "", 0, "failed", err.Error())
		_ = store.FailJob(ctx, db, job.ID, err.Error())
		return nil
	}
	_ = store.SetPaperFileState(ctx, db, payload.PaperID, url, dst, n, "ready", "")
	_ = store.CompleteJob(ctx, db, job.ID)
	return nil
}

func cleanupUnreferenced(ctx context.Context, db *pgxpool.Pool) error {
	ids, err := store.CleanupUnreferencedPaperFiles(ctx, db, 72*time.Hour)
	if err != nil {
		return err
	}
	for _, id := range ids {
		local, _, source, err := store.GetPaperFile(ctx, db, id)
		if err != nil || strings.TrimSpace(local) == "" {
			continue
		}
		_ = os.Remove(local)
		_ = store.SetPaperFileState(ctx, db, id, source, "", 0, "missing", "")
	}
	return nil
}

func downloadToFile(url, dst string, maxBytes int64) (int64, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "mr-cgdb-worker/1.0")
	cl := &http.Client{Timeout: 3 * time.Minute}
	resp, err := cl.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("download: %s", resp.Status)
	}
	if resp.ContentLength > maxBytes && resp.ContentLength > 0 {
		return 0, fmt.Errorf("pdf too large")
	}
	tmp := dst + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := io.Copy(f, io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	if n > maxBytes {
		_ = os.Remove(tmp)
		return 0, fmt.Errorf("pdf too large")
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	return n, nil
}

func resolvePDFURL(raw string, p *store.PaperRow) string {
	raw = strings.TrimSpace(raw)
	if p != nil && p.PDFURL != nil && strings.TrimSpace(*p.PDFURL) != "" {
		return strings.TrimSpace(*p.PDFURL)
	}
	if strings.HasSuffix(strings.ToLower(raw), ".pdf") {
		return raw
	}
	if p != nil && p.ArxivID != nil && strings.TrimSpace(*p.ArxivID) != "" {
		return "https://arxiv.org/pdf/" + strings.TrimSpace(*p.ArxivID) + ".pdf"
	}
	if strings.Contains(raw, "arxiv.org/abs/") {
		aid := strings.TrimSpace(strings.TrimPrefix(strings.Split(raw, "arxiv.org/abs/")[1], "/"))
		if aid != "" {
			return "https://arxiv.org/pdf/" + strings.TrimSuffix(aid, ".pdf") + ".pdf"
		}
	}
	return ""
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
