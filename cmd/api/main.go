package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"mr-cgdb/internal/arxiv"
	"mr-cgdb/internal/model"
	"mr-cgdb/internal/netx"
	"mr-cgdb/internal/ollama"
	"mr-cgdb/internal/pdfx"
	"mr-cgdb/internal/store"
	"mr-cgdb/internal/wire"
)

func main() {
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL required")
	}
	listen := getenv("LISTEN", ":8080")
	pipelineAddr := os.Getenv("PIPELINE_ADDR")
	if pipelineAddr == "" {
		pipelineAddr = "pipeline:9003"
	}
	dedupAddr := os.Getenv("DEDUP_ADDR")
	if dedupAddr == "" {
		dedupAddr = "dedup:9001"
	}
	arxivCat := getenv("ARXIV_SCAN_CATEGORY", "cs.GR")
	negativeSeedsFile := getenv("SEEDS_NEGATIVE_FILE", "/config/seeds_negative.txt")
	ollamaBase := getenv("OLLAMA_BASE_URL", "http://ollama:11434")
	deepVerifyModel := getenv("DEEP_VERIFY_MODEL", getenv("CHAT_MODEL", "llama3.2:1b"))
	maxDeepChars, _ := strconv.Atoi(getenv("DEEP_VERIFY_MAX_CHARS", "120000"))
	maxDeepBytes, _ := strconv.ParseInt(getenv("DEEP_VERIFY_MAX_BYTES", "100000000"), 10, 64)
	deepChunkChars, _ := strconv.Atoi(getenv("DEEP_VERIFY_CHUNK_CHARS", "4000"))
	deepMaxChunks, _ := strconv.Atoi(getenv("DEEP_VERIFY_MAX_CHUNKS", "12"))

	db, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	oc := ollama.NewDefault(ollamaBase, "", deepVerifyModel)

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	r.Get("/api/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	r.Get("/api/digests", func(w http.ResponseWriter, r *http.Request) {
		lookbackHours, _ := strconv.Atoi(r.URL.Query().Get("lookbackHours"))
		if lookbackHours <= 0 {
			lookbackHours = 72
		}
		gr, err := store.ListDigestGroups(r.Context(), db, time.Duration(lookbackHours)*time.Hour)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"groups": gr})
	})
	r.Get("/api/pending", func(w http.ResponseWriter, r *http.Request) {
		items, err := store.ListPending(r.Context(), db)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	})
	r.Get("/api/rss-feeds", func(w http.ResponseWriter, r *http.Request) {
		feeds, err := store.ListRSSFeeds(r.Context(), db)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": feeds})
	})
	r.Post("/api/rss-feeds", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		raw := strings.TrimSpace(req.URL)
		if raw == "" {
			writeErr(w, http.StatusBadRequest, errBadRequest("url required"))
			return
		}
		u, err := url.Parse(raw)
		if err != nil || u.Scheme == "" || u.Host == "" {
			writeErr(w, http.StatusBadRequest, errBadRequest("invalid URL"))
			return
		}
		id, err := store.AddRSSFeed(r.Context(), db, raw)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "id": id, "url": raw})
	})
	r.Post("/api/labels/main", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			PaperID int64  `json:"paperId"`
			Label   string `json:"label"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		if req.PaperID <= 0 || req.Label == "" {
			writeErr(w, http.StatusBadRequest, errBadRequest("paperId/label required"))
			return
		}
		if err := store.AddHandLabel(r.Context(), db, req.PaperID, "main", req.Label); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if strings.EqualFold(req.Label, "irrelevant") {
			p, err := store.GetPaperRow(r.Context(), db, req.PaperID)
			if err == nil {
				if err := appendNegativeSeedBibTex(negativeSeedsFile, p); err != nil {
					log.Printf("warning: failed appending negative seed for paper %d: %v", req.PaperID, err)
				}
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	r.Post("/api/pending/resolve", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			PaperID   int64 `json:"paperId"`
			Relevant  bool  `json:"relevant"`
			WithLabel bool  `json:"withLabel"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		if req.PaperID <= 0 {
			writeErr(w, http.StatusBadRequest, errBadRequest("paperId required"))
			return
		}
		label := "irrelevant"
		if req.Relevant {
			label = "relevant"
		}
		if err := store.AddHandLabel(r.Context(), db, req.PaperID, "pending", label); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if err := store.ResolvePending(r.Context(), db, req.PaperID, req.Relevant); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	r.Post("/api/pending/retry", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			PaperID int64 `json:"paperId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		if req.PaperID <= 0 {
			writeErr(w, http.StatusBadRequest, errBadRequest("paperId required"))
			return
		}
		if err := store.RequeueLLM(r.Context(), db, req.PaperID); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		conn, err := netx.DialTCP(pipelineAddr)
		if err != nil {
			writeErr(w, http.StatusBadGateway, err)
			return
		}
		defer conn.Close()
		if err := wire.WriteFrame(conn, &model.PipelineWork{PaperID: req.PaperID}); err != nil {
			writeErr(w, http.StatusBadGateway, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	r.Post("/api/scan/arxiv-range", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			From string `json:"from"`
			To   string `json:"to"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		from, err := time.Parse("2006-01-02", strings.TrimSpace(req.From))
		if err != nil {
			writeErr(w, http.StatusBadRequest, errBadRequest("from must be YYYY-MM-DD"))
			return
		}
		to, err := time.Parse("2006-01-02", strings.TrimSpace(req.To))
		if err != nil {
			writeErr(w, http.StatusBadRequest, errBadRequest("to must be YYYY-MM-DD"))
			return
		}
		if to.Before(from) {
			writeErr(w, http.StatusBadRequest, errBadRequest("to must be >= from"))
			return
		}
		if to.Sub(from) > 366*24*time.Hour {
			writeErr(w, http.StatusBadRequest, errBadRequest("range too large; max 366 days per scan"))
			return
		}

		conn, err := netx.DialTCP(dedupAddr)
		if err != nil {
			writeErr(w, http.StatusBadGateway, err)
			return
		}
		defer conn.Close()

		const pageSize = 200
		const maxPages = 50
		fromStr := from.UTC().Format("20060102")
		toStr := to.UTC().Format("20060102")
		totalFetched := 0
		totalSent := 0
		for page := 0; page < maxPages; page++ {
			start := page * pageSize
			q := fmt.Sprintf(
				"search_query=cat:%s+AND+submittedDate:[%s0000+TO+%s2359]&sortBy=submittedDate&sortOrder=ascending&start=%d&max_results=%d",
				arxivCat, fromStr, toStr, start, pageSize,
			)
			entries, err := arxiv.SearchPage(r.Context(), q)
			if err != nil {
				writeErr(w, http.StatusBadGateway, err)
				return
			}
			if len(entries) == 0 {
				break
			}
			totalFetched += len(entries)
			for _, e := range entries {
				aid := e.ArxivID
				it := &model.IngestItem{
					Source:   "arxiv",
					ArxivID:  &aid,
					Title:    e.Title,
					Abstract: e.Summary,
					Authors:  e.Authors,
					URL:      "https://arxiv.org/abs/" + e.ArxivID,
					Year:     e.Year,
				}
				if err := wire.WriteFrame(conn, it); err != nil {
					writeErr(w, http.StatusBadGateway, err)
					return
				}
				totalSent++
			}
			if len(entries) < pageSize {
				break
			}
			time.Sleep(3 * time.Second)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":          true,
			"from":        req.From,
			"to":          req.To,
			"fetched":     totalFetched,
			"forwarded":   totalSent,
			"category":    arxivCat,
			"destination": dedupAddr,
		})
	})
	r.Post("/api/scan/clear", func(w http.ResponseWriter, r *http.Request) {
		if err := store.ClearScanData(r.Context(), db); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	r.Post("/api/papers/{id}/deep-verify", func(w http.ResponseWriter, r *http.Request) {
		idStr := chi.URLParam(r, "id")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil || id <= 0 {
			writeErr(w, http.StatusBadRequest, errBadRequest("invalid paper id"))
			return
		}
		p, err := store.GetPaperRow(r.Context(), db, id)
		if err != nil {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		pdfURL := resolvePaperPDFURL(p)
		if pdfURL == "" {
			writeErr(w, http.StatusBadRequest, errBadRequest("paper has no resolvable PDF URL"))
			return
		}
		dctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
		defer cancel()
		txt, truncated, err := pdfx.FetchAndExtractText(dctx, pdfURL, maxDeepBytes, maxDeepChars)
		tooLarge := false
		if err != nil {
			var tle *pdfx.TooLargeError
			if errors.As(err, &tle) {
				tooLarge = true
			} else {
				writeErr(w, http.StatusBadGateway, err)
				return
			}
		}
		sys := `You are a careful research assistant for computer graphics.
Return JSON only: {"useful":true|false,"reason":"one concise sentence grounded in evidence"}.
Task: decide whether this paper is useful for Cluster/Meshlet LOD research in computer graphics.`
		primaryText := txt
		if strings.TrimSpace(primaryText) == "" {
			primaryText = p.Abstract
		}
		useful, reason, raw, err := deepVerifyByChunks(dctx, oc, sys, p.Title, p.Abstract, primaryText, deepChunkChars, deepMaxChunks)
		if err != nil {
			writeErr(w, http.StatusBadGateway, err)
			return
		}
		if strings.TrimSpace(reason) == "" {
			if useful {
				reason = "Content aligns with Cluster/Meshlet LOD methods and practical rendering tradeoffs."
			} else {
				reason = "Content does not focus on Cluster/Meshlet LOD methods in computer graphics."
			}
		}
		if tooLarge {
			reason = reason + " (PDF exceeded download limit; verification used available summary text.)"
		}
		if err := store.SetDeepVerifyResult(dctx, db, id, useful, reason, raw); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":         true,
			"paperId":    id,
			"pdfUrl":     pdfURL,
			"useful":     useful,
			"reason":     reason,
			"truncated":  truncated,
			"modelRaw":   raw,
			"textLength": len(primaryText),
			"tooLarge":   tooLarge,
		})
	})

	// Static UI
	r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/index.html")
	})

	log.Printf("api listening on %s", listen)
	if err := http.ListenAndServe(listen, r); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]any{"error": err.Error()})
}

type badRequest string

func (e badRequest) Error() string { return string(e) }

func errBadRequest(s string) error { return badRequest(s) }

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func appendNegativeSeedBibTex(path string, p *store.PaperRow) error {
	if p == nil {
		return nil
	}
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	key := fmt.Sprintf("NEG_%d_%d", p.ID, time.Now().Unix())
	author := "Unknown"
	if p.FirstAuthor != nil && strings.TrimSpace(*p.FirstAuthor) != "" {
		author = *p.FirstAuthor
	}
	year := time.Now().Year()
	if p.Year != nil && *p.Year > 0 {
		year = *p.Year
	}
	url := ""
	if p.URL != nil {
		url = strings.TrimSpace(*p.URL)
	}
	if url == "" && p.ArxivID != nil && *p.ArxivID != "" {
		url = "https://arxiv.org/abs/" + *p.ArxivID
	}
	title := escapeBib(strings.TrimSpace(p.Title))
	author = escapeBib(strings.TrimSpace(author))
	abs := escapeBib(strings.TrimSpace(p.Abstract))
	url = escapeBib(url)
	entry := fmt.Sprintf(
		"@Misc{%s,\n  author = {%s},\n  title = {%s},\n  year = {%d},\n  abstract = {%s},\n  url = {%s},\n  keywords = {negative-seed, ui-labeled-irrelevant}\n}\n\n",
		key, author, title, year, abs, url,
	)
	existing, _ := os.ReadFile(path)
	if strings.Contains(string(existing), fmt.Sprintf("title = {%s}", title)) {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(entry)
	return err
}

func escapeBib(s string) string {
	s = strings.ReplaceAll(s, "{", "")
	s = strings.ReplaceAll(s, "}", "")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.TrimSpace(strings.Join(strings.Fields(s), " "))
}

func deepVerifyByChunks(ctx context.Context, oc *ollama.Client, system, title, abs, text string, chunkChars, maxChunks int) (bool, string, string, error) {
	if chunkChars <= 0 {
		chunkChars = 12000
	}
	if maxChunks <= 0 {
		maxChunks = 8
	}
	chunks := splitTextChunks(text, chunkChars, maxChunks)
	if len(chunks) == 0 {
		chunks = []string{strings.TrimSpace(abs)}
	}
	var pos, neg int
	var bestReason string
	var raws []string
	for i, ch := range chunks {
		usr := fmt.Sprintf(
			"Title:\n%s\n\nAbstract:\n%s\n\nDocument chunk %d/%d:\n%s",
			title, abs, i+1, len(chunks), ch,
		)
		useful, reason, raw, err := oc.ChatDeepVerify(ctx, system, usr)
		if err != nil {
			continue
		}
		raws = append(raws, raw)
		if strings.TrimSpace(reason) != "" && bestReason == "" {
			bestReason = strings.TrimSpace(reason)
		}
		if useful {
			pos++
		} else {
			neg++
		}
	}
	if pos == 0 && neg == 0 {
		return false, "", "", fmt.Errorf("deep verify failed on all chunks")
	}
	useful := pos > neg
	if pos == neg {
		useful = pos > 0
	}
	return useful, bestReason, strings.Join(raws, "\n"), nil
}

func splitTextChunks(s string, chunkChars, maxChunks int) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	if len(s) <= chunkChars {
		return []string{s}
	}
	var out []string
	step := chunkChars
	if step < 2000 {
		step = 2000
	}
	for i := 0; i < len(s) && len(out) < maxChunks; i += step {
		j := i + chunkChars
		if j > len(s) {
			j = len(s)
		}
		out = append(out, strings.TrimSpace(s[i:j]))
	}
	return out
}

func resolvePaperPDFURL(p *store.PaperRow) string {
	if p == nil {
		return ""
	}
	if p.ArxivID != nil && strings.TrimSpace(*p.ArxivID) != "" {
		return buildArxivPDFURL(strings.TrimSpace(*p.ArxivID))
	}
	if p.URL == nil {
		return ""
	}
	raw := strings.TrimSpace(*p.URL)
	if raw == "" {
		return ""
	}
	if aid := extractArxivID(raw); aid != "" {
		return buildArxivPDFURL(aid)
	}
	u, err := url.Parse(raw)
	if err == nil {
		u.Fragment = ""
		u.RawQuery = ""
		if strings.HasSuffix(strings.ToLower(u.Path), ".pdf") {
			return u.String()
		}
		// handle common /abs/<id> form safely
		if strings.Contains(u.Path, "/abs/") {
			if aid := extractArxivID(u.Path); aid != "" {
				return buildArxivPDFURL(aid)
			}
			parts := strings.Split(u.Path, "/abs/")
			if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
				return buildArxivPDFURL(strings.TrimSpace(parts[1]))
			}
		}
	}
	if strings.HasSuffix(strings.ToLower(raw), ".pdf") {
		return raw
	}
	return ""
}

var (
	// modern arXiv id, with optional version
	arxivModernIDRe = regexp.MustCompile(`\b(\d{4}\.\d{4,5}(?:v\d+)?)\b`)
	// legacy arXiv id like cs/0112017 with optional version
	arxivLegacyIDRe = regexp.MustCompile(`\b([a-z\-]+\/\d{7}(?:v\d+)?)\b`)
)

func extractArxivID(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return ""
	}
	if m := arxivModernIDRe.FindStringSubmatch(s); len(m) > 1 {
		return m[1]
	}
	if m := arxivLegacyIDRe.FindStringSubmatch(s); len(m) > 1 {
		return m[1]
	}
	return ""
}

func buildArxivPDFURL(arxivID string) string {
	arxivID = strings.TrimSpace(arxivID)
	arxivID = strings.TrimPrefix(arxivID, "abs/")
	arxivID = strings.TrimPrefix(arxivID, "pdf/")
	arxivID = strings.TrimSuffix(arxivID, ".pdf")
	return "https://arxiv.org/pdf/" + arxivID + ".pdf"
}
