package arxiv

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
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

// SearchPage calls export.arxiv.org with the full query string (without base URL) and decodes the result.
func SearchPage(ctx context.Context, q string) ([]Entry, error) {
	u := "http://export.arxiv.org/api/query?" + q
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("arxiv http %s", resp.Status)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
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

func extractArxivID(arxivIDURL string) string {
	m := arxivIDFromURL.FindStringSubmatch(arxivIDURL)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}
