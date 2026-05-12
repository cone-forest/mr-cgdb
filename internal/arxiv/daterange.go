package arxiv

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func normalizeInner(innerSearch string) string {
	s := strings.TrimSpace(innerSearch)
	if s == "" {
		return "cat:cs.GR"
	}
	return s
}

func arxivTimeToken(t time.Time) string {
	return t.UTC().Format("200601021504")
}

// SearchPagedInRange lists papers with submittedDate in [since, until] UTC (inclusive by API semantics),
// paginated via start/maxResults. Caller should respect arXiv etiquette (~3s between pages).
func SearchPagedInRange(ctx context.Context, innerSearch string, since, until time.Time, start, maxResults int) ([]Entry, error) {
	since, until = since.UTC(), until.UTC()
	if until.Before(since) {
		return nil, nil
	}
	bracket := "[" + arxivTimeToken(since) + " TO " + arxivTimeToken(until) + "]"
	sq := "(" + normalizeInner(innerSearch) + ") AND submittedDate:" + bracket
	v := url.Values{}
	v.Set("search_query", sq)
	v.Set("sortBy", "submittedDate")
	v.Set("sortOrder", "ascending")
	v.Set("start", strconv.Itoa(start))
	v.Set("max_results", strconv.Itoa(maxResults))
	return SearchPage(ctx, v.Encode())
}
