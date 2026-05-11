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
	"time"

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

	var cfgMu sync.RWMutex
	activeMatcher := defaultMatcher
	activeNegTitleKeywords := negTitleKeywords
	refreshConfig := func() {
		cfg, err := store.GetSystemConfig(ctx, pool)
		if err != nil {
			log.Printf("keyword config refresh failed: %v", err)
			return
		}
		cfgMu.Lock()
		if len(cfg.PositiveKeywords) > 0 {
			activeMatcher = keywords.New(cfg.PositiveKeywords)
		} else {
			activeMatcher = defaultMatcher
		}
		if len(cfg.NegativeTitleKeywords) > 0 {
			activeNegTitleKeywords = cfg.NegativeTitleKeywords
		} else {
			activeNegTitleKeywords = negTitleKeywords
		}
		cfgMu.Unlock()
	}
	refreshConfig()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refreshConfig()
			}
		}
	}()

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
			cfgMu.RLock()
			m := activeMatcher
			neg := append([]string(nil), activeNegTitleKeywords...)
			cfgMu.RUnlock()
			if titleHasAny(it.Title, neg) {
				continue
			}
			blob := strings.ToLower(it.Title) + " " + strings.ToLower(it.Abstract)
			if !m.MatchText(blob) {
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
