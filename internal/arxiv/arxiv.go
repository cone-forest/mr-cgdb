package arxiv

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var arxivIDFromURL = regexp.MustCompile(`(\d{4}\.\d{4,5})(?:v\d+)?`)

// Entry is a single arXiv result from the query API.
type Entry struct {
	ArxivID string
	Title   string
	Summary string
	Authors []string
	Updated time.Time
	Year    *int
}

type atomFeed struct {
	XMLName xml.Name `xml:"http://www.w3.org/2005/Atom feed"`
	Entry   []struct {
		ID      string `xml:"id"`
		Title   string `xml:"title"`
		Summary string `xml:"summary"`
		Author  []struct {
			Name string `xml:"name"`
		} `xml:"author"`
		Updated string `xml:"updated"`
		Year    *int   // not in atom, derived
	} `xml:"entry"`
}

// decodeAtom parses an arXiv Atom API response payload.
func decodeAtom(b []byte) ([]Entry, error) {
	var f atomFeed
	if err := xml.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	var out []Entry
	for _, e := range f.Entry {
		aid := extractArxivID(e.ID)
		if aid == "" {
			continue
		}
		title := strings.Join(strings.Fields(strings.TrimSpace(e.Title)), " ")
		summary := strings.TrimSpace(e.Summary)
		var authors []string
		for _, a := range e.Author {
			authors = append(authors, strings.TrimSpace(a.Name))
		}
		updated, _ := time.Parse(time.RFC3339, e.Updated)
		y := updated.Year()
		out = append(out, Entry{
			ArxivID: aid,
			Title:   title,
			Summary: summary,
			Authors: authors,
			Updated: updated,
			Year:    &y,
		})
	}
	return out, nil
}

// SearchPage calls export.arxiv.org with the full query string (without base URL) and decodes the result.
// Requests are globally rate-limited for this process (see ratelimit.go). Transient 429 and 5xx responses are retried.
func SearchPage(ctx context.Context, q string) ([]Entry, error) {
	baseURL := "http://export.arxiv.org/api/query?"
	u := baseURL + q
	cl := &http.Client{Timeout: 120 * time.Second}
	var lastErr error
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := rateWait(ctx); err != nil {
			log.Printf("mr-cgdb arxiv cancelled_or_deadline attempt=%d err=%v", attempt+1, err)
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "mr-cgdb/1.0 (local paper ingest; polite batching)")
		log.Printf("mr-cgdb arxiv http_request attempt=%d/%d url_byte_len=%d query_preview=%q",
			attempt+1, maxAttempts, len(u), previewEncodedQuery(q, 220))
		t0 := time.Now()
		resp, err := cl.Do(req)
		if err != nil {
			log.Printf("mr-cgdb arxiv http_transport_error attempt=%d elapsed=%s err=%v",
				attempt+1, time.Since(t0).Round(time.Millisecond), err)
			rateNoteResponse(0, "")
			return nil, err
		}
		body, rerr := io.ReadAll(resp.Body)
		resp.Body.Close()
		elapsed := time.Since(t0)
		if rerr != nil {
			log.Printf("mr-cgdb arxiv read_body_error attempt=%d status=%s bytes_read_failed elapsed=%s err=%v",
				attempt+1, resp.Status, elapsed.Round(time.Millisecond), rerr)
			rateNoteResponse(resp.StatusCode, resp.Header.Get("Retry-After"))
			return nil, rerr
		}
		log.Printf("mr-cgdb arxiv http_response attempt=%d status=%s body_bytes=%d elapsed=%s content_type=%q",
			attempt+1, resp.Status, len(body), elapsed.Round(time.Millisecond), resp.Header.Get("Content-Type"))
		switch resp.StatusCode {
		case http.StatusOK:
			rateNoteResponse(http.StatusOK, "")
			out, decErr := decodeAtom(body)
			if decErr != nil {
				log.Printf("mr-cgdb arxiv atom_decode_error attempt=%d err=%v body_prefix=%q",
					attempt+1, decErr, previewBytes(body, 180))
				return nil, decErr
			}
			log.Printf("mr-cgdb arxiv success attempt=%d entries_parsed=%d", attempt+1, len(out))
			return out, nil
		case http.StatusTooManyRequests:
			lastErr = fmt.Errorf("arxiv http 429 Too Many Requests")
			log.Printf("mr-cgdb arxiv will_retry attempt=%d reason=429 Retry-After=%q body_prefix=%q",
				attempt+1, resp.Header.Get("Retry-After"), previewBytes(body, 120))
			rateNoteResponse(resp.StatusCode, resp.Header.Get("Retry-After"))
			continue
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			lastErr = fmt.Errorf("arxiv http %s", resp.Status)
			log.Printf("mr-cgdb arxiv will_retry attempt=%d reason=transient_5xx status=%s Retry-After=%q body_prefix=%q",
				attempt+1, resp.Status, resp.Header.Get("Retry-After"), previewBytes(body, 120))
			rateNoteResponse(resp.StatusCode, resp.Header.Get("Retry-After"))
			continue
		default:
			log.Printf("mr-cgdb arxiv non_retryable_status attempt=%d status=%s body_prefix=%q",
				attempt+1, resp.Status, previewBytes(body, 160))
			rateNoteResponse(resp.StatusCode, "")
			return nil, fmt.Errorf("arxiv http %s", resp.Status)
		}
	}
	log.Printf("mr-cgdb arxiv exhausted_retries attempts=%d last_err=%v", maxAttempts, lastErr)
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("arxiv: too many retries")
}

func previewEncodedQuery(q string, max int) string {
	if max <= 3 {
		return "…"
	}
	q = strings.TrimSpace(q)
	if len(q) <= max {
		return q
	}
	return q[:max] + "…"
}

func previewBytes(b []byte, max int) string {
	if max <= 3 {
		return "…"
	}
	s := strings.TrimSpace(string(b))
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", "")
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

func extractArxivID(arxivIDURL string) string {
	m := arxivIDFromURL.FindStringSubmatch(arxivIDURL)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}
