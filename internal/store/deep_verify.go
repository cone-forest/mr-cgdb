package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SetDeepVerifyResult persists deep verification output for a paper.
func SetDeepVerifyResult(ctx context.Context, p *pgxpool.Pool, id int64, useful bool, reason string, raw string) error {
	_, err := p.Exec(ctx, `
		UPDATE papers
		SET deep_verify_useful = $2,
		    deep_verify_reason = $3,
		    deep_verify_raw = $4,
		    deep_verify_at = now()
		WHERE id = $1
	`, id, useful, reason, raw)
	return err
}
