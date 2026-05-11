package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Window is [Start, End) in UTC.
type Window struct {
	Start time.Time
	End   time.Time
}

// DigestItem is a paper row in a digest response.
type DigestItem struct {
	ID            int64   `json:"id"`
	Title         string  `json:"title"`
	URL           string  `json:"url"`
	Year          *int    `json:"year,omitempty"`
	FirstAuthor   *string `json:"firstAuthor,omitempty"`
	Abstract      string  `json:"abstract"`
	HandLabelMain *string `json:"handLabelMain,omitempty"`
}

// PendingItem is an LLM-failed or pending review row.
type PendingItem struct {
	ID          int64   `json:"id"`
	Title       string  `json:"title"`
	URL         string  `json:"url"`
	Year        *int    `json:"year,omitempty"`
	FirstAuthor *string `json:"firstAuthor,omitempty"`
	Abstract    string  `json:"abstract"`
	LastError   *string `json:"lastError,omitempty"`
}

// DigestGroup is one panel in the UI.
type DigestGroup struct {
	Window Window       `json:"window"`
	Items  []DigestItem `json:"items"`
}

// rolling12hWindows returns the last ceil(lookback/12h) 12h slices ending at now: [t-12h,t), [t-24h,t-12h), ... (UTC, newest first).
func rolling12hWindows(now time.Time, lookback time.Duration) []Window {
	now = now.UTC()
	if lookback < 12*time.Hour {
		lookback = 12 * time.Hour
	}
	n := int(lookback / (12 * time.Hour))
	if lookback%(12*time.Hour) != 0 {
		n++
	}
	if n < 1 {
		n = 1
	}
	var out []Window
	end := now
	for i := 0; i < n; i++ {
		st := end.Add(-12 * time.Hour)
		out = append(out, Window{Start: st, End: end})
		end = st
	}
	return out
}

// ListDigestGroups returns per-window items where relevant_at falls in the window and is set.
func ListDigestGroups(ctx context.Context, p *pgxpool.Pool, lookback time.Duration) ([]DigestGroup, error) {
	now := time.Now()
	ws := rolling12hWindows(now, lookback)
	var res []DigestGroup
	for _, w := range ws {
		rows, err := p.Query(ctx, `
			SELECT id, title,
			       COALESCE(url, CASE WHEN arxiv_id IS NOT NULL THEN 'https://arxiv.org/abs/' || arxiv_id ELSE '' END) AS paper_url,
			       year, first_author, abstract, hand_label_main
			FROM papers
			WHERE relevant_at IS NOT NULL
			  AND relevant_at >= $1
			  AND relevant_at < $2
			ORDER BY relevant_at DESC, id DESC
		`, w.Start.UTC(), w.End.UTC())
		if err != nil {
			return nil, err
		}
		var items []DigestItem
		for rows.Next() {
			var d DigestItem
			if err := rows.Scan(&d.ID, &d.Title, &d.URL, &d.Year, &d.FirstAuthor, &d.Abstract, &d.HandLabelMain); err != nil {
				rows.Close()
				return nil, err
			}
			items = append(items, d)
		}
		rows.Close()
		res = append(res, DigestGroup{Window: w, Items: items})
	}
	return res, nil
}

// ListPending returns pending/failed LLM jobs not human-resolved, or with pending status.
func ListPending(ctx context.Context, p *pgxpool.Pool) ([]PendingItem, error) {
	rows, err := p.Query(ctx, `
		SELECT id, title,
		       COALESCE(url, CASE WHEN arxiv_id IS NOT NULL THEN 'https://arxiv.org/abs/' || arxiv_id ELSE '' END) AS paper_url,
		       year, first_author, abstract, last_llm_error
		FROM papers
		WHERE (llm_status = 'pending' OR llm_status = 'failed')
		  AND (human_resolved = false)
		ORDER BY id DESC
		LIMIT 500
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingItem
	for rows.Next() {
		var pitem PendingItem
		if err := rows.Scan(&pitem.ID, &pitem.Title, &pitem.URL, &pitem.Year, &pitem.FirstAuthor, &pitem.Abstract, &pitem.LastError); err != nil {
			return nil, err
		}
		out = append(out, pitem)
	}
	return out, rows.Err()
}
