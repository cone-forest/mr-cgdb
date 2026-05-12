package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"mr-cgdb/internal/identity"
	"mr-cgdb/internal/keywords"
	"mr-cgdb/internal/model"
	"mr-cgdb/internal/netx"
	"mr-cgdb/internal/store"
	"mr-cgdb/internal/wire"
)

var (
	kwFrames       atomic.Uint64
	kwSkipNegTitle atomic.Uint64
	kwSkipGlobalKW atomic.Uint64
	kwInserted     atomic.Uint64
	kwMissMu       sync.Mutex
	kwMissSamples  []string
)

func kwRecordGlobalKeywordMiss(title string) {
	kwSkipGlobalKW.Add(1)
	t := strings.TrimSpace(title)
	if len(t) > 140 {
		t = t[:140] + "…"
	}
	kwMissMu.Lock()
	defer kwMissMu.Unlock()
	if len(kwMissSamples) < 16 {
		kwMissSamples = append(kwMissSamples, t)
	}
}

func kwAfterIngestFrame() {
	n := kwFrames.Add(1)
	if n != 1 && n%250 != 0 {
		return
	}
	kwMissMu.Lock()
	samples := append([]string(nil), kwMissSamples...)
	kwMissMu.Unlock()
	log.Printf("mr-cgdb keyword ingest frames=%d inserted_total=%d skip_negative_title_gate=%d skip_global_keyword_file_gate=%d sample_titles_that_failed_global_kw=%q",
		n, kwInserted.Load(), kwSkipNegTitle.Load(), kwSkipGlobalKW.Load(), samples)
}

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
	// Only KEYWORD_GLOBAL_GATE=strict applies keywords.txt before DB insert. Default leaves relevance to per-profile settings.
	globalGateStrict := strings.EqualFold(strings.TrimSpace(os.Getenv("KEYWORD_GLOBAL_GATE")), "strict")

	defaultMatcher, err := keywords.Load(kf)
	if err != nil {
		log.Fatalf("keywords: %v", err)
	}
	nPhrases := defaultMatcher.PhraseCount()
	applyGlobalPhraseGate := globalGateStrict && nPhrases > 0
	switch {
	case globalGateStrict && nPhrases == 0:
		log.Printf("mr-cgdb keyword KEYWORD_GLOBAL_GATE=strict but %s has 0 phrases — no global substring gate", kf)
	case globalGateStrict:
		log.Printf("mr-cgdb keyword KEYWORD_GLOBAL_GATE=strict — applying global substring allowlist from %s (phrase_count=%d)", kf, nPhrases)
	default:
		log.Printf("mr-cgdb keyword KEYWORD_GLOBAL_GATE not strict (default) — phrases file %s not used at ingest; relevance uses each profile's keywords / LLM", kf)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	var profileKWCacheMu sync.Mutex
	profileKWCache := map[int64]*keywords.Matcher{}
	profileKWHasKeywords := map[int64]bool{}

	profileMatchesByKeywords := func(profileID int64, text string) (bool, error) {
		profileKWCacheMu.Lock()
		m, okM := profileKWCache[profileID]
		hasKW, okHas := profileKWHasKeywords[profileID]
		profileKWCacheMu.Unlock()
		if okM && okHas {
			if !hasKW {
				return false, nil
			}
			return m.MatchText(text), nil
		}
		cfg, e := store.GetProfileConfig(ctx, pool, profileID)
		if e != nil {
			return false, e
		}
		hasKW = len(cfg.PositiveKeywords) > 0
		m = keywords.New(cfg.PositiveKeywords)
		profileKWCacheMu.Lock()
		profileKWCache[profileID] = m
		profileKWHasKeywords[profileID] = hasKW
		profileKWCacheMu.Unlock()
		if !hasKW {
			return false, nil
		}
		return m.MatchText(text), nil
	}

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
	log.Printf("mr-cgdb keyword listening addr=%s keywords_file=%s global_phrase_count=%d apply_global_phrase_gate=%v negative_title_terms=%v pipeline_addr=%s",
		ln.Addr().String(), kf, nPhrases, applyGlobalPhraseGate, negTitleKeywords, pipeAddr)

	reader := func(c net.Conn) {
		defer c.Close()
		for {
			var it model.IngestItem
			if err := wire.ReadFrame(c, &it); err != nil {
				return
			}
			kwAfterIngestFrame()
			if titleHasAny(it.Title, negTitleKeywords) {
				kwSkipNegTitle.Add(1)
				continue
			}
			blob := strings.ToLower(it.Title) + " " + strings.ToLower(it.Abstract)
			if applyGlobalPhraseGate && !defaultMatcher.MatchText(blob) {
				kwRecordGlobalKeywordMiss(it.Title)
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
			kwInserted.Add(1)
			nIns := kwInserted.Load()
			if nIns <= 5 || nIns%100 == 0 {
				log.Printf("mr-cgdb keyword inserted_into_corpus paper_id=%d source=%s title=%q",
					id, it.Source, truncateKeywordTitle(it.Title))
			}
			// Phase 2: fan out profile analysis jobs by source subscription first,
			// then keep only profiles whose own positive keywords match this paper.
			candidateProfileIDs := map[int64]struct{}{}
			switch {
			case it.Source == "arxiv" || it.ArxivID != nil:
				profileIDs, e := store.ListProfileIDsForArxivRouting(ctx, pool)
				if e != nil {
					log.Printf("list arxiv-routed profiles: %v", e)
				} else {
					for _, pid := range profileIDs {
						candidateProfileIDs[pid] = struct{}{}
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
							candidateProfileIDs[pid] = struct{}{}
						}
					}
				}
			}
			bare, e := store.ListBareProfileIDs(ctx, pool)
			if e != nil {
				log.Printf("list bare profiles: %v", e)
			} else {
				for _, pid := range bare {
					candidateProfileIDs[pid] = struct{}{}
				}
			}
			matchedProfiles := 0
			for pid := range candidateProfileIDs {
				ok, me := profileMatchesByKeywords(pid, blob)
				if me != nil {
					log.Printf("profile keyword match profile_id=%d paper_id=%d: %v", pid, id, me)
					continue
				}
				if !ok {
					continue
				}
				if e := store.EnqueueProfileAnalyzeJob(ctx, pool, pid, id); e != nil {
					log.Printf("enqueue profile analyze pid=%d paper=%d: %v", pid, id, e)
					continue
				}
				matchedProfiles++
			}
			if matchedProfiles == 0 {
				log.Printf("mr-cgdb keyword globally_irrelevant paper_id=%d source=%s reason=no_profile_keyword_match title=%q",
					id, it.Source, truncateKeywordTitle(it.Title))
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

func truncateKeywordTitle(title string) string {
	t := strings.TrimSpace(title)
	if len(t) > 160 {
		return t[:160] + "…"
	}
	return t
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
