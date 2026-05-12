package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

const arxivAdvisoryLockKey int64 = 184221901

// WithArxivRequestLock serializes outbound arXiv API traffic across all services
// sharing the same Postgres database (api + watcher + any other client).
func WithArxivRequestLock(ctx context.Context, p *pgxpool.Pool, fn func(context.Context) error) error {
	if p == nil {
		return fn(ctx)
	}
	conn, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, arxivAdvisoryLockKey); err != nil {
		return err
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, arxivAdvisoryLockKey)
	}()
	return fn(ctx)
}
