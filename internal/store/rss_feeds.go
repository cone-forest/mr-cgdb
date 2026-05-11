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
