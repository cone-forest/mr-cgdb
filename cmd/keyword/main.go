package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"mr-cgdb/internal/identity"
	"mr-cgdb/internal/keywords"
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
	kw := getenv("LISTEN", ":9002")
	pipeAddr := os.Getenv("PIPELINE_ADDR")
	if pipeAddr == "" {
		log.Fatal("PIPELINE_ADDR required (e.g. pipeline:9003)")
	}
	kf := getenv("KEYWORDS_FILE", "/config/keywords.txt")
	negTitleKeywords := splitCSV(getenv("NEGATIVE_TITLE_KEYWORDS", "gaussian,splatt"))

	defaultMatcher, err := keywords.Load(kf)
	if err != nil {
		log.Fatalf("keywords: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	var pMu sync.Mutex
	var pconn net.Conn
	dialP := func() error {
		pMu.Lock()
		defer pMu.Unlock()
		if pconn != nil {
			return nil
		}
		c, err := netx.DialTCP(pipeAddr)
		if err != nil {
			return err
		}
		pconn = c
		log.Printf("keyword: connected to pipeline at %s", pipeAddr)
		return nil
	}

	ln, err := net.Listen("tcp", kw)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("keyword listening on %s", kw)

	reader := func(c net.Conn) {
		defer c.Close()
		for {
			var it model.IngestItem
			if err := wire.ReadFrame(c, &it); err != nil {
				return
			}
			if titleHasAny(it.Title, negTitleKeywords) {
				continue
			}
			blob := strings.ToLower(it.Title) + " " + strings.ToLower(it.Abstract)
			if !defaultMatcher.MatchText(blob) {
				continue
			}
			wkstr := identity.WeakKey(it.Title, it.Year, it.Authors)
			var wk *string
			if wkstr != "" {
				wk = &wkstr
			}
			id, err := store.InsertAfterKeyword(ctx, pool, &it, wk)
			if err != nil {
				log.Printf("insert: %v", err)
				continue
			}
			// Phase 2: automatically fan out profile analysis jobs by source subscription.
			switch {
			case it.Source == "arxiv" || it.ArxivID != nil:
				profileIDs, e := store.ListProfileIDsForArxivRouting(ctx, pool)
				if e != nil {
					log.Printf("list arxiv-routed profiles: %v", e)
				} else {
					for _, pid := range profileIDs {
						if e := store.EnqueueProfileAnalyzeJob(ctx, pool, pid, id); e != nil {
							log.Printf("enqueue profile analyze pid=%d paper=%d: %v", pid, id, e)
						}
					}
				}
			case it.FeedID != "":
				feedURL, e := store.ResolveRSSFeedURL(ctx, pool, it.FeedID)
				if e != nil {
					log.Printf("resolve rss feed url %s: %v", it.FeedID, e)
				}
				if feedURL != "" {
					profileIDs, e := store.ListProfileIDsForRSSRouting(ctx, pool, feedURL)
					if e != nil {
						log.Printf("list rss-routed profiles: %v", e)
					} else {
						for _, pid := range profileIDs {
							if e := store.EnqueueProfileAnalyzeJob(ctx, pool, pid, id); e != nil {
								log.Printf("enqueue profile analyze pid=%d paper=%d: %v", pid, id, e)
							}
						}
					}
				}
			}
			bare, e := store.ListBareProfileIDs(ctx, pool)
			if e != nil {
				log.Printf("list bare profiles: %v", e)
			} else {
				for _, pid := range bare {
					if e := store.EnqueueProfileAnalyzeJob(ctx, pool, pid, id); e != nil {
						log.Printf("enqueue bare profile analyze pid=%d paper=%d: %v", pid, id, e)
					}
				}
			}
			job := model.PipelineWork{PaperID: id}
			if err := dialP(); err != nil {
				log.Printf("dial pipeline: %v", err)
				continue
			}
			pMu.Lock()
			pc := pconn
			if err := wire.WriteFrame(pc, &job); err != nil {
				pconn = nil
				pc.Close()
				pMu.Unlock()
				log.Printf("write pipeline: %v", err)
				continue
			}
			pMu.Unlock()
		}
	}

	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("accept: %v", err)
			continue
		}
		go reader(c)
	}
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if v := strings.ToLower(strings.TrimSpace(p)); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func titleHasAny(title string, kws []string) bool {
	if len(kws) == 0 {
		return false
	}
	t := strings.ToLower(title)
	for _, kw := range kws {
		if strings.Contains(t, kw) {
			return true
		}
	}
	return false
}
