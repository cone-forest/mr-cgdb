package rssx

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/mmcdole/gofeed"
	"mr-cgdb/internal/model"
)

// ItemKeyForFeed returns a stable (feedID, key) for dedup/ordering.
// Prefer GUID; else canonical link without tracking query.
func ItemKeyForFeed(item *gofeed.Item) string {
	if g := strings.TrimSpace(item.GUID); g != "" {
		return g
	}
	if item.Link != "" {
		return canonicalURLStr(item.Link)
	}
	if item.Title != "" {
		return "title:" + strings.TrimSpace(item.Title)
	}
	return ""
}

func canonicalURLStr(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.Fragment = ""
	q := u.Query()
	for k := range q {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "utm_") {
			q.Del(k)
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// PublishedTime returns best-effort time for a feed item.
func PublishedTime(item *gofeed.Item) *time.Time {
	if item == nil {
		return nil
	}
	if item.PublishedParsed != nil {
		return item.PublishedParsed
	}
	if item.UpdatedParsed != nil {
		return item.UpdatedParsed
	}
	if item.Published != "" {
		if t, err := time.Parse(time.RFC1123Z, item.Published); err == nil {
			return &t
		}
	}
	return nil
}

// AfterCursor is true if (pub, key) is strictly after (lastTS, lastKey).
func AfterCursor(pub *time.Time, key string, lastTS *time.Time, lastKey string) bool {
	if key == "" {
		return false
	}
	if lastKey == "" {
		return true
	}
	if pub == nil {
		// if no time, allow forward progress by string compare
		return key > lastKey
	}
	if lastTS == nil {
		return true
	}
	if pub.After(*lastTS) {
		return true
	}
	if pub.Before(*lastTS) {
		return false
	}
	return key > lastKey
}

// IngestItem builds a model for TCP from a feed item.
func IngestItem(feedID string, furl string, item *gofeed.Item) (*model.IngestItem, error) {
	if item == nil {
		return nil, fmt.Errorf("nil item")
	}
	title := strings.TrimSpace(item.Title)
	desc := item.Description
	if desc == "" {
		desc = item.Content
	}
	desc = stripHTML(desc)
	key := ItemKeyForFeed(item)
	if key == "" {
		return nil, fmt.Errorf("no guid/link")
	}
	var y *int
	if t := PublishedTime(item); t != nil {
		yy := t.Year()
		y = &yy
	}
	authors := authorNames(item)
	it := &model.IngestItem{
		Source:   "rss",
		URL:      firstNonEmpty(item.Link, furl),
		Title:    title,
		Year:     y,
		Authors:  authors,
		Abstract: desc,
		FeedID:   feedID,
		ItemKey:  key,
	}
	if t := PublishedTime(item); t != nil {
		it.Published = t.UTC().Format(time.RFC3339)
	}
	return it, nil
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

func authorNames(item *gofeed.Item) []string {
	var out []string
	for _, p := range item.Authors {
		if strings.TrimSpace(p.Name) != "" {
			out = append(out, p.Name)
		}
	}
	if len(out) == 0 && item.Author != nil {
		if strings.TrimSpace(item.Author.Name) != "" {
			return []string{item.Author.Name}
		}
	}
	return out
}

// stripHTML is a very small tag stripper; good enough for RSS summaries.
func stripHTML(s string) string {
	// simple: remove tags
	var b strings.Builder
	in := false
	for _, r := range s {
		switch {
		case r == '<':
			in = true
		case r == '>':
			in = false
		case !in:
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

// Fetch is a test helper: parse a feed URL.
func Fetch(ctx context.Context, u string) (*gofeed.Feed, error) {
	fp := gofeed.NewParser()
	fp.UserAgent = "mr-cgdb/1.0"
	return fp.ParseURLWithContext(u, ctx)
}
