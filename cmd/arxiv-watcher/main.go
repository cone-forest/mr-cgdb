package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mr-cgdb/internal/arxiv"
	"mr-cgdb/internal/model"
	"mr-cgdb/internal/netx"
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
	q := getenv("ARXIV_QUERY", "search_query=cat:cs.GR&sortBy=submittedDate&sortOrder=descending&start=0&max_results=200")
	interval, _ := time.ParseDuration(getenv("ARXIV_POLL", "2m"))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	var c net.Conn
	dialD := func() error {
		if c != nil {
			return nil
		}
		nc, err := netx.DialTCP(dedup)
		if err != nil {
			return err
		}
		c = nc
		log.Printf("arxiv-watcher: connected to dedup at %s", dedup)
		return nil
	}

	tick := time.NewTicker(interval)
	defer tick.Stop()

	run := func() {
		if err := dialD(); err != nil {
			log.Printf("dial dedup: %v", err)
			return
		}
		ac, err := store.GetArxivCursor(ctx, pool)
		if err != nil {
			log.Printf("cursor: %v", err)
			return
		}
		var lastT *time.Time
		var lastID *string
		if ac.LastSubmissionPrefix != nil && *ac.LastSubmissionPrefix != "" {
			for _, f := range []string{time.RFC3339Nano, time.RFC3339} {
				t, e := time.Parse(f, *ac.LastSubmissionPrefix)
				if e == nil {
					lastT = &t
					break
				}
			}
		}
		lastID = ac.LastArxivID
		empty := ""
		if lastID == nil {
			lastID = &empty
		}

		entries, err := arxiv.SearchPage(ctx, q)
		if err != nil {
			log.Printf("arxiv query: %v", err)
			return
		}
		if len(entries) == 0 {
			return
		}
		tm, im := arxiv.MaxComposite(entries)
		tStr := tm.UTC().Format(time.RFC3339Nano)

		for i := range entries {
			e := &entries[i]
			if !e.After(lastT, lastID) {
				continue
			}
			aid := e.ArxivID
			it := &model.IngestItem{
				Source:   "arxiv",
				ArxivID:  &aid,
				Title:    e.Title,
				Abstract: e.Summary,
				Authors:  e.Authors,
				URL:      "https://arxiv.org/abs/" + e.ArxivID,
			}
			if e.Year != nil {
				it.Year = e.Year
			}
			if err := wire.WriteFrame(c, it); err != nil {
				log.Printf("write dedup: %v", err)
				_ = c.Close()
				c = nil
				return
			}
		}
		if err := store.SetArxivCursor(ctx, pool, &tStr, &im); err != nil {
			log.Printf("set arxiv cursor: %v", err)
		}
	}

	run() // one immediate run
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			run()
		}
	}
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
