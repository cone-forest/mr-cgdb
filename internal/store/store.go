package store

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
	entries, err := migrationFS.ReadDir("sql")
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := migrationFS.ReadFile("sql/" + e.Name())
		if err != nil {
			return err
		}
		if _, err := p.Exec(ctx, string(b)); err != nil {
			return fmt.Errorf("migrate %s: %w", e.Name(), err)
		}
	}
	return nil
}
