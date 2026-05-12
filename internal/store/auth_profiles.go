package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type User struct {
	ID        int64     `json:"id"`
	Username  string    `json:"username"`
	IsAdmin   bool      `json:"isAdmin"`
	CreatedAt time.Time `json:"createdAt"`
}

type AuthUser struct {
	User
	PasswordHash string
}

type Session struct {
	UserID    int64
	CSRFToken string
	ExpiresAt time.Time
}

type Profile struct {
	ID             int64     `json:"id"`
	UserID         int64     `json:"userId"`
	Username       string    `json:"username,omitempty"`
	Slug           string    `json:"slug"`
	Name           string    `json:"name"`
	Description    string    `json:"description"`
	Visibility     string    `json:"visibility"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
	LastAccessedAt time.Time `json:"lastAccessedAt"`
}

type ProfileConfig struct {
	ProfileID             int64           `json:"profileId"`
	PositiveKeywords      []string        `json:"positiveKeywords"`
	NegativeTitleKeywords []string        `json:"negativeTitleKeywords"`
	LLMPrompt             string          `json:"llmPrompt"`
	Sources               []ProfileSource `json:"sources"`
}

type ProfileSource struct {
	ID          int64     `json:"id"`
	ProfileID   int64     `json:"profileId"`
	SourceType  string    `json:"sourceType"`
	SourceValue string    `json:"sourceValue"`
	Enabled     bool      `json:"enabled"`
	CreatedAt   time.Time `json:"createdAt"`
}

func CountUsers(ctx context.Context, p *pgxpool.Pool) (int64, error) {
	var n int64
	err := p.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&n)
	return n, err
}

func CreateUser(ctx context.Context, p *pgxpool.Pool, username, passwordHash string, isAdmin bool) (*User, error) {
	var u User
	err := p.QueryRow(ctx, `
		INSERT INTO users (username, password_hash, is_admin)
		VALUES ($1, $2, $3)
		RETURNING id, username, is_admin, created_at
	`, strings.TrimSpace(strings.ToLower(username)), passwordHash, isAdmin).Scan(&u.ID, &u.Username, &u.IsAdmin, &u.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func GetAuthUserByUsername(ctx context.Context, p *pgxpool.Pool, username string) (*AuthUser, error) {
	var u AuthUser
	err := p.QueryRow(ctx, `
		SELECT id, username, password_hash, is_admin, created_at
		FROM users
		WHERE username = $1
	`, strings.TrimSpace(strings.ToLower(username))).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.IsAdmin, &u.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func GetUserByID(ctx context.Context, p *pgxpool.Pool, id int64) (*User, error) {
	var u User
	err := p.QueryRow(ctx, `SELECT id, username, is_admin, created_at FROM users WHERE id = $1`, id).
		Scan(&u.ID, &u.Username, &u.IsAdmin, &u.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func CreateSession(ctx context.Context, p *pgxpool.Pool, userID int64, tokenHash, csrf string, expiresAt time.Time) error {
	_, err := p.Exec(ctx, `
		INSERT INTO sessions (user_id, session_token_hash, csrf_token, expires_at)
		VALUES ($1, $2, $3, $4)
	`, userID, tokenHash, csrf, expiresAt.UTC())
	return err
}

func GetSessionByTokenHash(ctx context.Context, p *pgxpool.Pool, tokenHash string) (*Session, error) {
	var s Session
	err := p.QueryRow(ctx, `
		SELECT user_id, csrf_token, expires_at
		FROM sessions
		WHERE session_token_hash = $1
	`, tokenHash).Scan(&s.UserID, &s.CSRFToken, &s.ExpiresAt)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func DeleteSessionByTokenHash(ctx context.Context, p *pgxpool.Pool, tokenHash string) error {
	_, err := p.Exec(ctx, `DELETE FROM sessions WHERE session_token_hash = $1`, tokenHash)
	return err
}

func DeleteExpiredSessions(ctx context.Context, p *pgxpool.Pool) error {
	_, err := p.Exec(ctx, `DELETE FROM sessions WHERE expires_at < now()`)
	return err
}

func CreateProfile(ctx context.Context, p *pgxpool.Pool, userID int64, slug, name, description, visibility string, cfg ProfileConfig) (*Profile, error) {
	tx, err := p.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var prof Profile
	err = tx.QueryRow(ctx, `
		INSERT INTO profiles (user_id, slug, name, description, visibility)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, user_id, slug, name, description, visibility, created_at, updated_at, last_accessed_at
	`, userID, normalizeSlug(slug), strings.TrimSpace(name), strings.TrimSpace(description), normalizeVisibility(visibility)).
		Scan(&prof.ID, &prof.UserID, &prof.Slug, &prof.Name, &prof.Description, &prof.Visibility, &prof.CreatedAt, &prof.UpdatedAt, &prof.LastAccessedAt)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO profile_configs (profile_id, positive_keywords, negative_title_keywords, llm_prompt)
		VALUES ($1, $2::text[], $3::text[], $4)
	`, prof.ID, normalizeTagList(cfg.PositiveKeywords), normalizeTagList(cfg.NegativeTitleKeywords), strings.TrimSpace(cfg.LLMPrompt)); err != nil {
		return nil, err
	}
	for _, s := range cfg.Sources {
		if strings.TrimSpace(s.SourceValue) == "" {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO profile_sources (profile_id, source_type, source_value, enabled)
			VALUES ($1, $2, $3, $4)
		`, prof.ID, normalizeSourceType(s.SourceType), strings.TrimSpace(s.SourceValue), true); err != nil {
			return nil, err
		}
	}
	return &prof, tx.Commit(ctx)
}

func UpdateProfile(ctx context.Context, p *pgxpool.Pool, profileID, userID int64, name, description, visibility string, cfg ProfileConfig) error {
	tx, err := p.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		UPDATE profiles
		SET name = $3, description = $4, visibility = $5, updated_at = now()
		WHERE id = $1 AND user_id = $2
	`, profileID, userID, strings.TrimSpace(name), strings.TrimSpace(description), normalizeVisibility(visibility))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO profile_configs (profile_id, positive_keywords, negative_title_keywords, llm_prompt, updated_at)
		VALUES ($1, $2::text[], $3::text[], $4, now())
		ON CONFLICT (profile_id) DO UPDATE
		SET positive_keywords = EXCLUDED.positive_keywords,
		    negative_title_keywords = EXCLUDED.negative_title_keywords,
		    llm_prompt = EXCLUDED.llm_prompt,
		    updated_at = now()
	`, profileID, normalizeTagList(cfg.PositiveKeywords), normalizeTagList(cfg.NegativeTitleKeywords), strings.TrimSpace(cfg.LLMPrompt))
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM profile_sources WHERE profile_id = $1`, profileID); err != nil {
		return err
	}
	for _, s := range cfg.Sources {
		v := strings.TrimSpace(s.SourceValue)
		if v == "" {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO profile_sources (profile_id, source_type, source_value, enabled)
			VALUES ($1, $2, $3, $4)
		`, profileID, normalizeSourceType(s.SourceType), v, s.Enabled); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func DeleteProfile(ctx context.Context, p *pgxpool.Pool, profileID, userID int64) error {
	tag, err := p.Exec(ctx, `DELETE FROM profiles WHERE id = $1 AND user_id = $2`, profileID, userID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func DeleteUserAccount(ctx context.Context, p *pgxpool.Pool, userID int64) error {
	_, err := p.Exec(ctx, `DELETE FROM users WHERE id = $1`, userID)
	return err
}

func GetProfileByID(ctx context.Context, p *pgxpool.Pool, profileID int64) (*Profile, error) {
	var prof Profile
	err := p.QueryRow(ctx, `
		SELECT p.id, p.user_id, u.username, p.slug, p.name, p.description, p.visibility, p.created_at, p.updated_at, p.last_accessed_at
		FROM profiles p
		JOIN users u ON u.id = p.user_id
		WHERE p.id = $1
	`, profileID).Scan(&prof.ID, &prof.UserID, &prof.Username, &prof.Slug, &prof.Name, &prof.Description, &prof.Visibility, &prof.CreatedAt, &prof.UpdatedAt, &prof.LastAccessedAt)
	if err != nil {
		return nil, err
	}
	return &prof, nil
}

func GetProfileByUsernameSlug(ctx context.Context, p *pgxpool.Pool, username, slug string) (*Profile, error) {
	var prof Profile
	err := p.QueryRow(ctx, `
		SELECT p.id, p.user_id, u.username, p.slug, p.name, p.description, p.visibility, p.created_at, p.updated_at, p.last_accessed_at
		FROM profiles p
		JOIN users u ON u.id = p.user_id
		WHERE u.username = $1 AND p.slug = $2
	`, strings.ToLower(strings.TrimSpace(username)), normalizeSlug(slug)).
		Scan(&prof.ID, &prof.UserID, &prof.Username, &prof.Slug, &prof.Name, &prof.Description, &prof.Visibility, &prof.CreatedAt, &prof.UpdatedAt, &prof.LastAccessedAt)
	if err != nil {
		return nil, err
	}
	return &prof, nil
}

func ListMyProfiles(ctx context.Context, p *pgxpool.Pool, userID int64) ([]Profile, error) {
	rows, err := p.Query(ctx, `
		SELECT id, user_id, slug, name, description, visibility, created_at, updated_at, last_accessed_at
		FROM profiles
		WHERE user_id = $1
		ORDER BY last_accessed_at DESC, updated_at DESC, id DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Profile
	for rows.Next() {
		var prof Profile
		if err := rows.Scan(&prof.ID, &prof.UserID, &prof.Slug, &prof.Name, &prof.Description, &prof.Visibility, &prof.CreatedAt, &prof.UpdatedAt, &prof.LastAccessedAt); err != nil {
			return nil, err
		}
		out = append(out, prof)
	}
	return out, rows.Err()
}

func ListPublicProfiles(ctx context.Context, p *pgxpool.Pool, q string, limit int) ([]Profile, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	needle := "%" + strings.ToLower(strings.TrimSpace(q)) + "%"
	rows, err := p.Query(ctx, `
		SELECT p.id, p.user_id, u.username, p.slug, p.name, p.description, p.visibility, p.created_at, p.updated_at, p.last_accessed_at
		FROM profiles p
		JOIN users u ON u.id = p.user_id
		WHERE p.visibility = 'public'
		  AND ($1 = '%%' OR lower(p.name) LIKE $1 OR lower(p.description) LIKE $1 OR lower(u.username) LIKE $1)
		ORDER BY p.last_accessed_at DESC, p.updated_at DESC, p.id DESC
		LIMIT $2
	`, needle, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Profile
	for rows.Next() {
		var prof Profile
		if err := rows.Scan(&prof.ID, &prof.UserID, &prof.Username, &prof.Slug, &prof.Name, &prof.Description, &prof.Visibility, &prof.CreatedAt, &prof.UpdatedAt, &prof.LastAccessedAt); err != nil {
			return nil, err
		}
		out = append(out, prof)
	}
	return out, rows.Err()
}

func TouchProfileAccess(ctx context.Context, p *pgxpool.Pool, profileID, userID int64) error {
	_, err := p.Exec(ctx, `
		UPDATE profiles
		SET last_accessed_at = now()
		WHERE id = $1 AND user_id = $2
	`, profileID, userID)
	return err
}

func GetProfileConfig(ctx context.Context, p *pgxpool.Pool, profileID int64) (*ProfileConfig, error) {
	var cfg ProfileConfig
	cfg.ProfileID = profileID
	err := p.QueryRow(ctx, `
		SELECT positive_keywords, negative_title_keywords, llm_prompt
		FROM profile_configs
		WHERE profile_id = $1
	`, profileID).Scan(&cfg.PositiveKeywords, &cfg.NegativeTitleKeywords, &cfg.LLMPrompt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &cfg, nil
		}
		return nil, err
	}
	rows, err := p.Query(ctx, `
		SELECT id, profile_id, source_type, source_value, enabled, created_at
		FROM profile_sources
		WHERE profile_id = $1
		ORDER BY id ASC
	`, profileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var s ProfileSource
		if err := rows.Scan(&s.ID, &s.ProfileID, &s.SourceType, &s.SourceValue, &s.Enabled, &s.CreatedAt); err != nil {
			return nil, err
		}
		cfg.Sources = append(cfg.Sources, s)
	}
	cfg.PositiveKeywords = normalizeTagList(cfg.PositiveKeywords)
	cfg.NegativeTitleKeywords = normalizeTagList(cfg.NegativeTitleKeywords)
	cfg.LLMPrompt = strings.TrimSpace(cfg.LLMPrompt)
	return &cfg, rows.Err()
}

func normalizeSlug(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return "profile"
	}
	var out strings.Builder
	lastDash := false
	for _, r := range s {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			out.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				out.WriteByte('-')
				lastDash = true
			}
		}
	}
	res := strings.Trim(out.String(), "-")
	if res == "" {
		return "profile"
	}
	return res
}

func normalizeVisibility(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "public" {
		return "public"
	}
	return "private"
}

func normalizeSourceType(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "arxiv_query" {
		return "arxiv_query"
	}
	return "rss"
}

func normalizeTagList(in []string) []string {
	out := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, t := range in {
		s := strings.TrimSpace(strings.ToLower(t))
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
