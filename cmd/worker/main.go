package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"mr-cgdb/internal/keywords"
	"mr-cgdb/internal/ollama"
	"mr-cgdb/internal/store"
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
	chatModel := getenv("CHAT_MODEL", "llama3.2:1b")

	db, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	oc := ollama.NewDefault(ollamaBase, "", chatModel)

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
	if len(cfg.PositiveKeywords) > 0 {
		score = float64(len(hits)) / float64(len(cfg.PositiveKeywords))
	}
	var llmRel *bool
	var llmRaw string
	prompt := strings.TrimSpace(cfg.LLMPrompt)
	if prompt == "" {
		prompt = `You are a strict classifier for this profile. Given title and abstract, respond with JSON only: {"relevant":true} when clearly relevant, otherwise {"relevant":false}.`
	}
	rel, raw, e := oc.ChatRelevant(ctx, prompt, "Title:\n"+paper.Title+"\n\nAbstract:\n"+paper.Abstract)
	if e == nil {
		llmRel = &rel
		llmRaw = raw
	} else {
		llmRaw = "llm error: " + e.Error()
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
	return nil
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
