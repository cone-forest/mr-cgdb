package store

import (
	"context"
	"embed"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationAdvisoryLock int64 = 0x62726367646d7263 // mr-cgdb (unique session lock id)

//go:embed sql/*.sql
var migrationFS embed.FS

// New creates a connection pool. Caller is responsible for Close().
func New(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	if err := migrate(ctx, p); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func migrate(ctx context.Context, p *pgxpool.Pool) error {
	conn, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationAdvisoryLock); err != nil {
		return fmt.Errorf("migrate lock: %w", err)
	}
	defer func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, `SELECT pg_advisory_unlock($1)`, migrationAdvisoryLock)
	}()

	entries, err := migrationFS.ReadDir("sql")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := migrationFS.ReadFile("sql/" + e.Name())
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(b)); err != nil {
			return fmt.Errorf("migrate %s: %w", e.Name(), err)
		}
	}
	return nil
}
