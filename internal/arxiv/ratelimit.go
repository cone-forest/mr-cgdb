package arxiv

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// arXiv API terms: ~1 request per 3 seconds. We default slightly above that and
// serialize all export.arxiv.org calls in this process so watcher + admin rescan cannot overlap.

var (
	rateMu             sync.Mutex
	rateEarliestNext   time.Time
)

func apiMinInterval() time.Duration {
	d := 3200 * time.Millisecond
	if s := strings.TrimSpace(os.Getenv("ARXIV_API_MIN_INTERVAL")); s != "" {
		if x, err := time.ParseDuration(s); err == nil && x >= 3*time.Second {
			d = x
		}
	}
	return d
}

func parseRetryAfter(h string) time.Duration {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0
	}
	if sec, err := strconv.Atoi(h); err == nil && sec > 0 {
		return time.Duration(sec) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		d := time.Until(t)
		if d < 0 {
			return 0
		}
		return d
	}
	return 0
}

func rateWait(ctx context.Context) error {
	for {
		rateMu.Lock()
		wait := time.Duration(0)
		now := time.Now()
		if rateEarliestNext.After(now) {
			wait = rateEarliestNext.Sub(now)
		}
		rateMu.Unlock()
		if wait <= 0 {
			return nil
		}
		if wait >= 250*time.Millisecond {
			log.Printf("mr-cgdb arxiv pacing sleep %s before next HTTP request (terms-of-use spacing)", wait.Round(time.Millisecond))
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
			t.Stop()
		}
	}
}

func rateNoteResponse(statusCode int, retryAfterHeader string) {
	gap := apiMinInterval()
	rateMu.Lock()
	defer rateMu.Unlock()
	when := time.Now().Add(gap)
	switch statusCode {
	case http.StatusTooManyRequests:
		extra := parseRetryAfter(retryAfterHeader)
		if extra < 10*time.Second {
			extra = 10 * time.Second
		}
		until429 := time.Now().Add(extra)
		if until429.After(when) {
			when = until429
		}
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		extra := parseRetryAfter(retryAfterHeader)
		if extra < 8*time.Second {
			extra = 8 * time.Second
		}
		until5 := time.Now().Add(extra)
		if until5.After(when) {
			when = until5
		}
	}
	delay := time.Until(when)
	if statusCode != http.StatusOK || delay > gap+50*time.Millisecond {
		log.Printf("mr-cgdb arxiv rate_limiter status=%d next_request_earliest_in=%s min_interval=%s retry_after_hdr=%q",
			statusCode, delay.Round(time.Millisecond), gap.Round(time.Millisecond), retryAfterHeader)
	}
	rateEarliestNext = when
}
