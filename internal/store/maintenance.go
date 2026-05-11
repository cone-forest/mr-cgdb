package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ClearScanData removes ingested paper/label/cursor state while preserving RSS feed configuration.
func ClearScanData(ctx context.Context, p *pgxpool.Pool) error {
	tx, err := p.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `TRUNCATE TABLE hand_labels, papers, rss_cursors RESTART IDENTITY CASCADE`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE arxiv_cursor SET last_submission_prefix = NULL, last_arxiv_id = NULL WHERE id = 1`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
