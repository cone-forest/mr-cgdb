package store

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type RSSFeed struct {
	ID        int64     `json:"id"`
	URL       string    `json:"url"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"createdAt"`
}

func ListRSSFeeds(ctx context.Context, p *pgxpool.Pool) ([]RSSFeed, error) {
	rows, err := p.Query(ctx, `
		SELECT id, url, enabled, created_at
		FROM rss_feeds
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RSSFeed
	for rows.Next() {
		var f RSSFeed
		if err := rows.Scan(&f.ID, &f.URL, &f.Enabled, &f.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func ListEnabledRSSFeeds(ctx context.Context, p *pgxpool.Pool) ([]RSSFeed, error) {
	rows, err := p.Query(ctx, `
		SELECT id, url, enabled, created_at
		FROM rss_feeds
		WHERE enabled = true
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RSSFeed
	for rows.Next() {
		var f RSSFeed
		if err := rows.Scan(&f.ID, &f.URL, &f.Enabled, &f.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func AddRSSFeed(ctx context.Context, p *pgxpool.Pool, url string) (int64, error) {
	url = strings.TrimSpace(url)
	var id int64
	err := p.QueryRow(ctx, `
		INSERT INTO rss_feeds (url, enabled)
		VALUES ($1, true)
		ON CONFLICT (url) DO UPDATE SET enabled = true
		RETURNING id
	`, url).Scan(&id)
	return id, err
}

func ReplaceRSSFeeds(ctx context.Context, p *pgxpool.Pool, urls []string) error {
	tx, err := p.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM rss_feeds`); err != nil {
		return err
	}
	seen := map[string]struct{}{}
	for _, raw := range urls {
		u := strings.TrimSpace(raw)
		if u == "" {
			continue
		}
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		if _, err := tx.Exec(ctx, `INSERT INTO rss_feeds (url, enabled) VALUES ($1, true)`, u); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
