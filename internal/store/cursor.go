package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ArxivCursor struct {
	LastSubmissionPrefix *string
	LastArxivID          *string
}

func GetArxivCursor(ctx context.Context, p *pgxpool.Pool) (*ArxivCursor, error) {
	var c ArxivCursor
	err := p.QueryRow(ctx, `SELECT last_submission_prefix, last_arxiv_id FROM arxiv_cursor WHERE id = 1`).Scan(
		&c.LastSubmissionPrefix, &c.LastArxivID)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func SetArxivCursor(ctx context.Context, p *pgxpool.Pool, sub *string, arx *string) error {
	_, err := p.Exec(ctx, `UPDATE arxiv_cursor SET last_submission_prefix = $1, last_arxiv_id = $2 WHERE id = 1`, sub, arx)
	return err
}

type RSSCursor struct {
	FeedID  string
	LastTS  *time.Time
	LastKey string
}

// GetRSSCursor returns stored cursor, or a zeroed cursor for a new feed.
func GetRSSCursor(ctx context.Context, p *pgxpool.Pool, feedID string) (*RSSCursor, error) {
	c := &RSSCursor{FeedID: feedID}
	err := p.QueryRow(ctx, `SELECT last_ts, last_key FROM rss_cursors WHERE feed_id = $1`, feedID).Scan(
		&c.LastTS, &c.LastKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, nil
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

func UpsertRSSCursor(ctx context.Context, p *pgxpool.Pool, feedID string, lastTS *time.Time, lastKey string) error {
	_, err := p.Exec(ctx, `
		INSERT INTO rss_cursors (feed_id, last_ts, last_key)
		VALUES ($1, $2, $3)
		ON CONFLICT (feed_id) DO UPDATE
		SET last_ts = $2, last_key = $3
	`, feedID, lastTS, lastKey)
	return err
}
