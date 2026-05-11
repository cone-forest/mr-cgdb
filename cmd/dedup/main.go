package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"mr-cgdb/internal/identity"
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
	listen := getenv("LISTEN", ":9001")
	keyword := os.Getenv("KEYWORD_ADDR")
	if keyword == "" {
		log.Fatal("KEYWORD_ADDR required (e.g. keyword:9002)")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("dedup listening on %s, forwarding to %s", listen, keyword)

	var kwMu sync.Mutex
	var kw net.Conn
	dialKW := func() error {
		kwMu.Lock()
		defer kwMu.Unlock()
		if kw != nil {
			return nil
		}
		c, err := netx.DialTCP(keyword)
		if err != nil {
			return err
		}
		kw = c
		log.Printf("dedup: connected to keyword at %s", keyword)
		return nil
	}

	acceptLoop := func(c net.Conn) {
		defer c.Close()
		for {
			var it model.IngestItem
			if err := wire.ReadFrame(c, &it); err != nil {
				return
			}
			wkstr := identity.WeakKey(it.Title, it.Year, it.Authors)
			var wk *string
			if wkstr != "" {
				wk = &wkstr
			}
			id, err := store.FindIDByIdentity(ctx, pool, it.ArxivID, it.DOI, wk)
			if err != nil {
				log.Printf("find identity: %v", err)
				continue
			}
			src := it.Source
			if it.FeedID != "" {
				src = "rss:" + it.FeedID
			}
			if id > 0 {
				if err := store.MergeSource(ctx, pool, id, src); err != nil {
					log.Printf("merge: %v", err)
				}
				continue
			}
			if err := dialKW(); err != nil {
				log.Printf("dial keyword: %v", err)
				continue
			}
			kwMu.Lock()
			kc := kw
			if err := wire.WriteFrame(kc, &it); err != nil {
				kw = nil
				kc.Close()
				kwMu.Unlock()
				log.Printf("write keyword: %v", err)
				continue
			}
			kwMu.Unlock()
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
		go acceptLoop(c)
	}
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
