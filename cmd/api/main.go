package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"mr-cgdb/internal/arxiv"
	"mr-cgdb/internal/keywords"
	"mr-cgdb/internal/netx"
	"mr-cgdb/internal/ollama"
	"mr-cgdb/internal/store"
	"mr-cgdb/internal/wire"
)

const (
	sessionCookieName = "session_token"
	sessionTTL        = 24 * time.Hour
)

type app struct {
	db              *pgxpool.Pool
	oc              *ollama.Client
	pdfStorageDir   string
	maxServeBytes   int64
	rateMu          sync.Mutex
	rateWindow      time.Time
	rateCounter     map[string]int
	rateLimitPerMin int
}

type authContext struct {
	User *store.User
	CSRF string
}

func main() {
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL required")
	}
	listen := getenv("LISTEN", ":8080")
	pdfDir := getenv("PDF_STORAGE_DIR", "/data/pdfs")
	maxServeBytes, _ := strconv.ParseInt(getenv("PDF_MAX_SERVE_BYTES", "100000000"), 10, 64)
	if maxServeBytes <= 0 {
		maxServeBytes = 100000000
	}
	rateLimitPerMin, _ := strconv.Atoi(getenv("PDF_RATE_LIMIT_PER_MIN", "60"))
	if rateLimitPerMin <= 0 {
		rateLimitPerMin = 60
	}
	ollamaBase := getenv("OLLAMA_BASE_URL", "http://ollama:11434")
	chatModel := getenv("CHAT_MODEL", "llama3.2:1b")

	db, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_ = os.MkdirAll(pdfDir, 0o755)

	a := &app{
		db:              db,
		oc:              ollama.NewDefault(ollamaBase, "", chatModel),
		pdfStorageDir:   pdfDir,
		maxServeBytes:   maxServeBytes,
		rateCounter:     map[string]int{},
		rateWindow:      time.Now().UTC(),
		rateLimitPerMin: rateLimitPerMin,
	}

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	r.Get("/api/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})

	r.Post("/api/auth/register", a.handleRegister)
	r.Post("/api/auth/login", a.handleLogin)
	r.Post("/api/auth/logout", a.withAuth(true, a.handleLogout))
	r.Get("/api/auth/me", a.withAuthOptional(a.handleMe))
	r.Delete("/api/auth/me", a.withAuth(true, a.handleDeleteMyAccount))

	r.Get("/api/profiles/me", a.withAuth(false, a.handleMyProfiles))
	r.Post("/api/profiles", a.withAuth(true, a.handleCreateProfile))
	r.Patch("/api/profiles/{id}", a.withAuth(true, a.handleUpdateProfile))
	r.Delete("/api/profiles/{id}", a.withAuth(true, a.handleDeleteProfile))
	r.Post("/api/profiles/{id}/access", a.withAuth(true, a.handleTouchProfileAccess))
	r.Post("/api/profiles/{id}/analysis/backfill", a.withAuth(true, a.handleBackfillProfileAnalysis))
	r.Get("/api/profiles/{id}/likes", a.withAuthOptional(a.handleProfileLikes))
	r.Get("/api/profiles/{id}/analysis", a.withAuthOptional(a.handleProfileAnalysis))
	r.Get("/api/profiles/{id}/analysis/candidates", a.withAuthOptional(a.handleProfileAnalysisCandidates))
	r.Post("/api/profiles/{id}/likes", a.withAuth(true, a.handleLikePaper))
	r.Post("/api/profiles/{id}/analyze", a.withAuth(true, a.handleAnalyzePaper))
	r.Patch("/api/profiles/{id}/likes/{paperId}", a.withAuth(true, a.handleLikePaper))
	r.Delete("/api/profiles/{id}/likes/{paperId}", a.withAuth(true, a.handleUnlikePaper))
	r.Get("/api/papers/search", a.withAuth(false, a.handleSearchPapers))

	r.Get("/api/public/profiles", a.handlePublicProfiles)
	r.Get("/api/public/u/{username}/{slug}", a.handlePublicProfile)
	r.Get("/api/public/papers/{id}/pdf", a.handleServePublicPDF)

	r.Get("/api/jobs/failures", a.withAuth(false, a.handleJobFailures))
	r.Post("/api/jobs/{id}/retry", a.withAuth(true, a.handleRetryJob))
	r.Post("/api/admin/ingest/arxiv-rescan", a.withAuth(true, a.handleAdminArxivRescan))

	// Legacy endpoints after hard cutover.
	r.Get("/api/digests", legacyGone)
	r.Get("/api/pending", legacyGone)
	r.Post("/api/labels/main", legacyGone)
	r.Post("/api/pending/resolve", legacyGone)
	r.Post("/api/pending/retry", legacyGone)

	r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/index.html")
	})

	log.Printf("api listening on %s", listen)
	if err := http.ListenAndServe(listen, r); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func (a *app) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	username := normalizeUsername(req.Username)
	if username == "" || len(req.Password) < 8 {
		writeErr(w, http.StatusBadRequest, errBadRequest("username and password (min 8 chars) required"))
		return
	}
	count, err := store.CountUsers(r.Context(), a.db)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	u, err := store.CreateUser(r.Context(), a.db, username, string(hash), count == 0)
	if err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	if err := a.issueSession(r.Context(), w, u.ID); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "user": u})
}

func (a *app) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	u, err := store.GetAuthUserByUsername(r.Context(), a.db, normalizeUsername(req.Username))
	if err != nil {
		writeErr(w, http.StatusUnauthorized, errBadRequest("invalid credentials"))
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(req.Password)); err != nil {
		writeErr(w, http.StatusUnauthorized, errBadRequest("invalid credentials"))
		return
	}
	if err := a.issueSession(r.Context(), w, u.ID); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "user": u.User})
}

func (a *app) handleLogout(w http.ResponseWriter, r *http.Request, auth *authContext) {
	token, _ := r.Cookie(sessionCookieName)
	if token != nil {
		_ = store.DeleteSessionByTokenHash(r.Context(), a.db, hashToken(token.Value))
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *app) handleMe(w http.ResponseWriter, r *http.Request, auth *authContext) {
	if auth == nil || auth.User == nil {
		writeJSON(w, http.StatusOK, map[string]any{"authenticated": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"authenticated": true,
		"user":          auth.User,
		"csrfToken":     auth.CSRF,
	})
}

func (a *app) handleDeleteMyAccount(w http.ResponseWriter, r *http.Request, auth *authContext) {
	if err := store.DeleteUserAccount(r.Context(), a.db, auth.User.ID); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookieName, Value: "", Path: "/", HttpOnly: true, MaxAge: -1})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *app) handleMyProfiles(w http.ResponseWriter, r *http.Request, auth *authContext) {
	profiles, err := store.ListMyProfiles(r.Context(), a.db, auth.User.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	type profileOut struct {
		Profile store.Profile        `json:"profile"`
		Config  *store.ProfileConfig `json:"config"`
	}
	var out []profileOut
	for _, p := range profiles {
		cfg, err := store.GetProfileConfig(r.Context(), a.db, p.ID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		out = append(out, profileOut{Profile: p, Config: cfg})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (a *app) handleCreateProfile(w http.ResponseWriter, r *http.Request, auth *authContext) {
	var req struct {
		Slug        string              `json:"slug"`
		Name        string              `json:"name"`
		Description string              `json:"description"`
		Visibility  string              `json:"visibility"`
		Config      store.ProfileConfig `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		writeErr(w, http.StatusBadRequest, errBadRequest("name required"))
		return
	}
	p, err := store.CreateProfile(r.Context(), a.db, auth.User.ID, req.Slug, req.Name, req.Description, req.Visibility, req.Config)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	backfillQueued, _ := store.EnqueueProfileAnalyzeBackfill(r.Context(), a.db, p.ID)
	cfg, _ := store.GetProfileConfig(r.Context(), a.db, p.ID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "profile": p, "config": cfg, "analysisBackfillQueued": backfillQueued})
}

func (a *app) handleUpdateProfile(w http.ResponseWriter, r *http.Request, auth *authContext) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid profile id"))
		return
	}
	var req struct {
		Name        string              `json:"name"`
		Description string              `json:"description"`
		Visibility  string              `json:"visibility"`
		Config      store.ProfileConfig `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := store.UpdateProfile(r.Context(), a.db, id, auth.User.ID, req.Name, req.Description, req.Visibility, req.Config); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, http.StatusNotFound, errBadRequest("profile not found"))
			return
		}
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	backfillQueued, _ := store.EnqueueProfileAnalyzeBackfill(r.Context(), a.db, id)
	p, _ := store.GetProfileByID(r.Context(), a.db, id)
	cfg, _ := store.GetProfileConfig(r.Context(), a.db, id)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "profile": p, "config": cfg, "analysisBackfillQueued": backfillQueued})
}

func (a *app) handleDeleteProfile(w http.ResponseWriter, r *http.Request, auth *authContext) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid profile id"))
		return
	}
	if err := store.DeleteProfile(r.Context(), a.db, id, auth.User.ID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, http.StatusNotFound, errBadRequest("profile not found"))
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *app) handleTouchProfileAccess(w http.ResponseWriter, r *http.Request, auth *authContext) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid profile id"))
		return
	}
	if err := store.TouchProfileAccess(r.Context(), a.db, id, auth.User.ID); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *app) handleSearchPapers(w http.ResponseWriter, r *http.Request, auth *authContext) {
	q := r.URL.Query().Get("q")
	items, err := store.SearchPapers(r.Context(), a.db, q, 50)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *app) handleLikePaper(w http.ResponseWriter, r *http.Request, auth *authContext) {
	profileID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || profileID <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid profile id"))
		return
	}
	paperIDStr := chi.URLParam(r, "paperId")
	var req struct {
		PaperID int64    `json:"paperId"`
		Note    string   `json:"note"`
		Tags    []string `json:"tags"`
	}
	if paperIDStr != "" {
		id, e := strconv.ParseInt(paperIDStr, 10, 64)
		if e == nil {
			req.PaperID = id
		}
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.PaperID <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("paperId required"))
		return
	}
	if err := store.UpsertProfileLike(r.Context(), a.db, profileID, auth.User.ID, req.PaperID, req.Note, req.Tags); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, http.StatusNotFound, errBadRequest("profile not found"))
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *app) handleUnlikePaper(w http.ResponseWriter, r *http.Request, auth *authContext) {
	profileID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || profileID <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid profile id"))
		return
	}
	paperID, err := strconv.ParseInt(chi.URLParam(r, "paperId"), 10, 64)
	if err != nil || paperID <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid paper id"))
		return
	}
	if err := store.RemoveProfileLike(r.Context(), a.db, profileID, auth.User.ID, paperID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, http.StatusNotFound, errBadRequest("profile not found"))
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *app) handleProfileLikes(w http.ResponseWriter, r *http.Request, auth *authContext) {
	profileID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || profileID <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid profile id"))
		return
	}
	prof, err := store.GetProfileByID(r.Context(), a.db, profileID)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if prof.Visibility != "public" {
		if auth == nil || auth.User == nil || auth.User.ID != prof.UserID {
			writeErr(w, http.StatusForbidden, errBadRequest("profile is private"))
			return
		}
	}
	q := r.URL.Query().Get("q")
	tag := r.URL.Query().Get("tag")
	items, err := store.ListProfileLikes(r.Context(), a.db, profileID, q, tag, 200)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"profile": prof, "items": items})
}

func (a *app) handleAnalyzePaper(w http.ResponseWriter, r *http.Request, auth *authContext) {
	profileID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || profileID <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid profile id"))
		return
	}
	prof, err := store.GetProfileByID(r.Context(), a.db, profileID)
	if err != nil || prof.UserID != auth.User.ID {
		writeErr(w, http.StatusForbidden, errBadRequest("profile not found or forbidden"))
		return
	}
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
	paper, err := store.GetPaperRow(r.Context(), a.db, req.PaperID)
	if err != nil {
		writeErr(w, http.StatusNotFound, errBadRequest("paper not found"))
		return
	}
	cfg, err := store.GetProfileConfig(r.Context(), a.db, profileID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	titleAbs := strings.ToLower(strings.TrimSpace(paper.Title) + "\n" + strings.TrimSpace(paper.Abstract))
	m := keywords.New(cfg.PositiveKeywords)
	keywordPass := len(cfg.PositiveKeywords) > 0 && m.MatchText(titleAbs)
	var hits []string
	for _, kw := range cfg.PositiveKeywords {
		kw = strings.TrimSpace(strings.ToLower(kw))
		if kw != "" && strings.Contains(titleAbs, kw) {
			hits = append(hits, kw)
		}
	}
	shadowScore := 0.0
	if len(cfg.PositiveKeywords) > 0 {
		shadowScore = float64(len(hits)) / float64(len(cfg.PositiveKeywords))
	}
	var llmRel *bool
	var llmRaw string
	if strings.TrimSpace(cfg.LLMPrompt) != "" {
		usr := "Title:\n" + paper.Title + "\n\nAbstract:\n" + paper.Abstract
		rel, raw, e := a.oc.ChatRelevant(r.Context(), cfg.LLMPrompt, usr)
		if e == nil {
			llmRel = &rel
			llmRaw = raw
		} else {
			llmRaw = "llm error: " + e.Error()
		}
	}
	wouldAuto := keywordPass
	if llmRel != nil {
		wouldAuto = wouldAuto && *llmRel
	}
	res := &store.ProfilePaperAnalysis{
		ProfileID:               profileID,
		PaperID:                 req.PaperID,
		KeywordPass:             keywordPass,
		KeywordHits:             hits,
		LLMRelevant:             llmRel,
		LLMRaw:                  llmRaw,
		ShadowWouldAutoDownload: wouldAuto,
		ShadowScore:             shadowScore,
	}
	if err := store.UpsertProfilePaperAnalysis(r.Context(), a.db, res); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "analysis": res})
}

func (a *app) handleProfileAnalysis(w http.ResponseWriter, r *http.Request, auth *authContext) {
	profileID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || profileID <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid profile id"))
		return
	}
	prof, err := store.GetProfileByID(r.Context(), a.db, profileID)
	if err != nil {
		writeErr(w, http.StatusNotFound, errBadRequest("profile not found"))
		return
	}
	if prof.Visibility != "public" && (auth == nil || auth.User == nil || auth.User.ID != prof.UserID) {
		writeErr(w, http.StatusForbidden, errBadRequest("profile is private"))
		return
	}
	items, err := store.ListProfileAnalysis(r.Context(), a.db, profileID, 200)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"profile": prof, "items": items})
}

func (a *app) handleProfileAnalysisCandidates(w http.ResponseWriter, r *http.Request, auth *authContext) {
	profileID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || profileID <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid profile id"))
		return
	}
	prof, err := store.GetProfileByID(r.Context(), a.db, profileID)
	if err != nil {
		writeErr(w, http.StatusNotFound, errBadRequest("profile not found"))
		return
	}
	if prof.Visibility != "public" && (auth == nil || auth.User == nil || auth.User.ID != prof.UserID) {
		writeErr(w, http.StatusForbidden, errBadRequest("profile is private"))
		return
	}
	keywordOnly := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("keywordOnly")), "true")
	llmOnly := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("llmRelevantOnly")), "true")
	autoOnly := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("wouldAutoOnly")), "true")
	items, err := store.ListProfileAnalysisCandidates(r.Context(), a.db, profileID, keywordOnly, llmOnly, autoOnly, 200)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	hasRows, hasEn, nPapers, mErr := store.ProfileFeedMeta(r.Context(), a.db, profileID)
	meta := map[string]any{}
	if mErr == nil {
		meta["hasSourceRows"] = hasRows
		meta["hasEnabledSource"] = hasEn
		meta["papersInDatabase"] = nPapers
		switch {
		case nPapers == 0:
			meta["hint"] = "The database has no papers yet. Run an arXiv rescan (admin tools) or wait for the arXiv/RSS watchers."
		case hasRows && !hasEn:
			meta["hint"] = "All sources on this profile are disabled. Enable a subscription in Profile settings or clear sources to use the global ingest feed."
		case !hasRows:
			meta["hint"] = "No subscriptions configured — the feed shows every paper the global pipeline has ingested (matching your configured global keywords file)."
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"profile": prof, "items": items, "meta": meta})
}

func (a *app) handleAdminArxivRescan(w http.ResponseWriter, r *http.Request, auth *authContext) {
	if auth == nil || auth.User == nil || !auth.User.IsAdmin {
		writeErr(w, http.StatusForbidden, errBadRequest("admin only"))
		return
	}
	dedup := strings.TrimSpace(os.Getenv("DEDUP_ADDR"))
	if dedup == "" {
		writeErr(w, http.StatusBadRequest, errBadRequest("DEDUP_ADDR is not set"))
		return
	}
	var req struct {
		Since      string `json:"since"`
		Until      string `json:"until"`
		InnerQuery string `json:"innerQuery"`
		MaxPages   int    `json:"maxPages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	until := time.Now().UTC()
	if strings.TrimSpace(req.Until) != "" {
		t, err := time.Parse(time.RFC3339, strings.TrimSpace(req.Until))
		if err != nil {
			writeErr(w, http.StatusBadRequest, errBadRequest("until must be RFC3339"))
			return
		}
		until = t.UTC()
	}
	since := until.Add(-30 * 24 * time.Hour)
	if strings.TrimSpace(req.Since) != "" {
		t, err := time.Parse(time.RFC3339, strings.TrimSpace(req.Since))
		if err != nil {
			writeErr(w, http.StatusBadRequest, errBadRequest("since must be RFC3339"))
			return
		}
		since = t.UTC()
	}
	if !until.After(since) {
		writeErr(w, http.StatusBadRequest, errBadRequest("until must be after since"))
		return
	}
	if until.Sub(since) > 366*24*time.Hour {
		writeErr(w, http.StatusBadRequest, errBadRequest("window must be at most 366 days"))
		return
	}
	maxPages := req.MaxPages
	if maxPages <= 0 {
		maxPages = 15
	}
	if maxPages > 40 {
		maxPages = 40
	}
	pageSize := 200

	conn, err := netx.DialTCP(dedup)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	defer conn.Close()

	ctx := r.Context()
	sent := 0
	pagesUsed := 0
loop:
	for page := 0; page < maxPages; page++ {
		entries, err := arxiv.SearchPagedInRange(ctx, req.InnerQuery, since, until, page*pageSize, pageSize)
		if err != nil {
			writeErr(w, http.StatusBadGateway, err)
			return
		}
		pagesUsed = page + 1
		if len(entries) == 0 {
			break
		}
		for i := range entries {
			e := &entries[i]
			it := arxiv.IngestItem(e)
			if it == nil {
				continue
			}
			if err := wire.WriteFrame(conn, it); err != nil {
				writeErr(w, http.StatusBadGateway, err)
				return
			}
			sent++
		}
		if len(entries) < pageSize {
			break
		}
		if page == maxPages-1 {
			break
		}
		select {
		case <-ctx.Done():
			break loop
		case <-time.After(3 * time.Second):
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                 true,
		"recordsSentToDedup": sent,
		"since":              since,
		"until":              until,
		"pagesFetched":       pagesUsed,
		"innerQuery":         strings.TrimSpace(req.InnerQuery),
	})
}

func (a *app) handleBackfillProfileAnalysis(w http.ResponseWriter, r *http.Request, auth *authContext) {
	profileID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || profileID <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid profile id"))
		return
	}
	prof, err := store.GetProfileByID(r.Context(), a.db, profileID)
	if err != nil || prof.UserID != auth.User.ID {
		writeErr(w, http.StatusForbidden, errBadRequest("forbidden"))
		return
	}
	n, err := store.EnqueueProfileAnalyzeBackfill(r.Context(), a.db, profileID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "queued": n})
}

func (a *app) handlePublicProfiles(w http.ResponseWriter, r *http.Request) {
	items, err := store.ListPublicProfiles(r.Context(), a.db, r.URL.Query().Get("q"), 100)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]store.PublicExploreProfile, 0, len(items))
	for _, p := range items {
		out = append(out, store.NewPublicExploreSummary(p))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (a *app) handlePublicProfile(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")
	slug := chi.URLParam(r, "slug")
	prof, err := store.GetProfileByUsernameSlug(r.Context(), a.db, username, slug)
	if err != nil {
		writeErr(w, http.StatusNotFound, errBadRequest("profile not found"))
		return
	}
	if prof.Visibility != "public" {
		writeErr(w, http.StatusForbidden, errBadRequest("profile is private"))
		return
	}
	items, err := store.ListProfileLikes(r.Context(), a.db, prof.ID, r.URL.Query().Get("q"), r.URL.Query().Get("tag"), 200)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"profile": prof, "items": items})
}

func (a *app) handleServePublicPDF(w http.ResponseWriter, r *http.Request) {
	paperID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || paperID <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid paper id"))
		return
	}
	if !a.allowRate(remoteIP(r)) {
		writeErr(w, http.StatusTooManyRequests, errBadRequest("rate limit exceeded"))
		return
	}
	ok, err := store.IsPaperPubliclyLiked(r.Context(), a.db, paperID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		writeErr(w, http.StatusForbidden, errBadRequest("paper not public"))
		return
	}
	localPath, status, _, err := store.GetPaperFile(r.Context(), a.db, paperID)
	if err != nil || status != "ready" || localPath == "" {
		writeErr(w, http.StatusNotFound, errBadRequest("pdf not available"))
		return
	}
	clean := filepath.Clean(localPath)
	info, err := os.Stat(clean)
	if err != nil || info.IsDir() {
		writeErr(w, http.StatusNotFound, errBadRequest("file missing"))
		return
	}
	if info.Size() > a.maxServeBytes {
		writeErr(w, http.StatusForbidden, errBadRequest("file too large to serve"))
		return
	}
	http.ServeFile(w, r, clean)
}

func (a *app) handleJobFailures(w http.ResponseWriter, r *http.Request, auth *authContext) {
	items, err := store.ListJobFailures(r.Context(), a.db, auth.User.ID, auth.User.IsAdmin, 200)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *app) handleRetryJob(w http.ResponseWriter, r *http.Request, auth *authContext) {
	jobID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || jobID <= 0 {
		writeErr(w, http.StatusBadRequest, errBadRequest("invalid job id"))
		return
	}
	job, err := store.GetJobByID(r.Context(), a.db, jobID)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	var payload struct {
		ProfileID int64 `json:"profileId"`
	}
	_ = json.Unmarshal(job.Payload, &payload)
	if !auth.User.IsAdmin && payload.ProfileID > 0 {
		prof, err := store.GetProfileByID(r.Context(), a.db, payload.ProfileID)
		if err != nil || prof.UserID != auth.User.ID {
			writeErr(w, http.StatusForbidden, errBadRequest("forbidden"))
			return
		}
	}
	if err := store.RetryJob(r.Context(), a.db, jobID); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *app) withAuth(requireCSRF bool, next func(http.ResponseWriter, *http.Request, *authContext)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth, err := a.getAuth(r)
		if err != nil || auth == nil || auth.User == nil {
			writeErr(w, http.StatusUnauthorized, errBadRequest("auth required"))
			return
		}
		if requireCSRF && isMutating(r.Method) {
			if r.Header.Get("X-CSRF-Token") == "" || r.Header.Get("X-CSRF-Token") != auth.CSRF {
				writeErr(w, http.StatusForbidden, errBadRequest("csrf token invalid"))
				return
			}
		}
		next(w, r, auth)
	}
}

func (a *app) withAuthOptional(next func(http.ResponseWriter, *http.Request, *authContext)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth, _ := a.getAuth(r)
		next(w, r, auth)
	}
}

func (a *app) getAuth(r *http.Request) (*authContext, error) {
	c, err := r.Cookie(sessionCookieName)
	if err != nil || strings.TrimSpace(c.Value) == "" {
		return nil, err
	}
	s, err := store.GetSessionByTokenHash(r.Context(), a.db, hashToken(c.Value))
	if err != nil {
		return nil, err
	}
	if time.Now().After(s.ExpiresAt) {
		return nil, errors.New("session expired")
	}
	u, err := store.GetUserByID(r.Context(), a.db, s.UserID)
	if err != nil {
		return nil, err
	}
	return &authContext{User: u, CSRF: s.CSRFToken}, nil
}

func (a *app) issueSession(ctx context.Context, w http.ResponseWriter, userID int64) error {
	token, err := randomToken(32)
	if err != nil {
		return err
	}
	csrf, err := randomToken(24)
	if err != nil {
		return err
	}
	expires := time.Now().Add(sessionTTL)
	if err := store.CreateSession(ctx, a.db, userID, hashToken(token), csrf, expires); err != nil {
		return err
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   isSecureCookie(),
		Expires:  expires,
	})
	return nil
}

func isSecureCookie() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("COOKIE_SECURE")))
	if v == "0" || v == "false" {
		return false
	}
	return v == "1" || v == "true"
}

func randomToken(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func normalizeUsername(s string) string {
	return strings.TrimSpace(strings.ToLower(s))
}

func (a *app) allowRate(key string) bool {
	a.rateMu.Lock()
	defer a.rateMu.Unlock()
	now := time.Now().UTC()
	if now.Sub(a.rateWindow) >= time.Minute {
		a.rateWindow = now
		a.rateCounter = map[string]int{}
	}
	a.rateCounter[key]++
	return a.rateCounter[key] <= a.rateLimitPerMin
}

func remoteIP(r *http.Request) string {
	if xf := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xf != "" {
		parts := strings.Split(xf, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func isMutating(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func legacyGone(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusGone, map[string]any{
		"error": "legacy relevance endpoints were removed in profile-scoped cutover",
	})
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
