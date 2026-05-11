package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"mr-cgdb/internal/netx"
	"mr-cgdb/internal/rssx"
	"mr-cgdb/internal/store"
	"mr-cgdb/internal/wire"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL required")
	}
	dedup := os.Getenv("DEDUP_ADDR")
	if dedup == "" {
		log.Fatal("DEDUP_ADDR required (e.g. dedup:9001)")
	}
	bootstrapFeeds := os.Getenv("RSS_FEEDS")
	interval, _ := time.ParseDuration(getenv("RSS_POLL", "3m"))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	var conn net.Conn
	dialDedup := func() error {
		if conn != nil {
			return nil
		}
		c, err := netx.DialTCP(dedup)
		if err != nil {
			return err
		}
		conn = c
		log.Printf("rss-watcher: connected to dedup at %s", dedup)
		return nil
	}

	// Optional bootstrap of feeds from env.
	for _, u := range splitFeeds(bootstrapFeeds) {
		if _, err := store.AddRSSFeed(ctx, pool, u); err != nil {
			log.Printf("bootstrap feed %s: %v", u, err)
		}
	}

	runOnce := func() {
		if err := dialDedup(); err != nil {
			log.Printf("dial dedup: %v", err)
			return
		}
		feeds, err := store.ListEnabledRSSFeeds(ctx, pool)
		if err != nil {
			log.Printf("list feeds: %v", err)
			return
		}
		for _, f := range feeds {
			feedID := "feed-" + strconv.FormatInt(f.ID, 10)
			if err := processFeed(ctx, pool, conn, f.URL, feedID); err != nil {
				log.Printf("process feed %s: %v", f.URL, err)
				if conn != nil {
					_ = conn.Close()
				}
				conn = nil
			}
		}
	}

	runOnce()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

func processFeed(ctx context.Context, pool *pgxpool.Pool, conn net.Conn, feedURL, feedID string) error {
	feed, err := rssx.Fetch(ctx, feedURL)
	if err != nil {
		return err
	}
	cur, err := store.GetRSSCursor(ctx, pool, feedID)
	if err != nil {
		return err
	}
	bestTS := cur.LastTS
	bestKey := cur.LastKey

	// process old -> new so cursor ends at newest successful item
	for i := len(feed.Items) - 1; i >= 0; i-- {
		item := feed.Items[i]
		key := rssx.ItemKeyForFeed(item)
		pub := rssx.PublishedTime(item)
		if !rssx.AfterCursor(pub, key, bestTS, bestKey) {
			continue
		}
		ing, err := rssx.IngestItem(feedID, feedURL, item)
		if err != nil {
			log.Printf("rss ingest %s: %v", feedURL, err)
			continue
		}
		if err := wire.WriteFrame(conn, ing); err != nil {
			return err
		}
		bestTS = pub
		bestKey = key
	}
	if bestKey != cur.LastKey || (bestTS != nil && (cur.LastTS == nil || !bestTS.Equal(*cur.LastTS))) {
		if err := store.UpsertRSSCursor(ctx, pool, feedID, bestTS, bestKey); err != nil {
			return err
		}
	}
	return nil
}

func splitFeeds(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
