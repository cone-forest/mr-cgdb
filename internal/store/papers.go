package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"mr-cgdb/internal/model"
)

// FindIDByIdentity returns an existing row id, or 0.
func FindIDByIdentity(ctx context.Context, p *pgxpool.Pool, arx, doi, wk *string) (int64, error) {
	var id int64
	err := p.QueryRow(ctx, `
		SELECT id FROM papers WHERE
			($1::text IS NOT NULL AND $1::text <> '' AND arxiv_id = $1) OR
			($2::text IS NOT NULL AND $2::text <> '' AND doi = $2) OR
			($3::text IS NOT NULL AND $3::text <> '' AND weak_key = $3)
		LIMIT 1
	`, deref(arx), deref(doi), deref(wk)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return id, nil
}

func deref(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

// MergeSource appends a source label if not already present in sources.
func MergeSource(ctx context.Context, p *pgxpool.Pool, id int64, source string) error {
	return mergeSource(ctx, p, id, source)
}

func mergeSource(ctx context.Context, p *pgxpool.Pool, id int64, source string) error {
	var raw []byte
	err := p.QueryRow(ctx, `SELECT sources::text FROM papers WHERE id = $1`, id).Scan(&raw)
	if err != nil {
		return err
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err != nil {
		return err
	}
	for _, s := range list {
		if s == source {
			return nil
		}
	}
	list = append(list, source)
	b, _ := json.Marshal(list)
	_, err = p.Exec(ctx, `UPDATE papers SET sources = $1::jsonb WHERE id = $2`, b, id)
	return err
}

// InsertAfterKeyword stores a new paper; weakKey may be nil if incomplete.
func InsertAfterKeyword(ctx context.Context, p *pgxpool.Pool, it *model.IngestItem, weakKey *string) (int64, error) {
	var arx, d, wk, fa *string
	if it.ArxivID != nil && *it.ArxivID != "" {
		arx = it.ArxivID
	}
	if it.DOI != nil && *it.DOI != "" {
		d = it.DOI
	}
	if weakKey != nil && *weakKey != "" {
		wk = weakKey
	}
	if len(it.Authors) > 0 {
		s := it.Authors[0]
		fa = &s
	}
	src := it.Source
	if it.FeedID != "" {
		src = fmt.Sprintf("rss:%s", it.FeedID)
	}
	initial, _ := json.Marshal([]string{src})
	var y *int
	if it.Year != nil {
		y = it.Year
	}
	var id int64
	err := p.QueryRow(ctx, `
		INSERT INTO papers (arxiv_id, doi, weak_key, url, title, year, first_author, abstract, source, sources, llm_status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,'pending')
		RETURNING id
	`, arx, d, wk, it.URL, it.Title, y, fa, it.Abstract, src, string(initial)).Scan(&id)
	return id, err
}

// PaperRow is used by the API and pipeline.
type PaperRow struct {
	ID            int64
	ArxivID       *string
	DOI           *string
	URL           *string
	Title         string
	Year          *int
	FirstAuthor   *string
	Abstract      string
	Sources       []string
	ShadowMax     *float64
	ShadowPass    *bool
	ShadowArgmax  *string
	LLMRelevant   *bool
	LLMStatus     string
	HumanResolved bool
	HumanRelevant *bool
	HandLabelMain *string
	RelevantAt    *time.Time
	CreatedAt     time.Time
}

// GetPaperRow for pipeline updates.
func GetPaperRow(ctx context.Context, p *pgxpool.Pool, id int64) (*PaperRow, error) {
	var r PaperRow
	var arx, doi, url, first, sseed, hlm, llmraw, llmerr *string
	var y *int
	var shmax *float64
	var shp *bool
	var llmrel *bool
	var hr *bool
	var ra *time.Time
	var sources []byte
	err := p.QueryRow(ctx, `
		SELECT id, arxiv_id, doi, url, title, year, first_author, abstract, sources,
		       shadow_max_sim, shadow_would_pass, shadow_argmax_seed,
		       llm_relevant, llm_status, llm_raw, last_llm_error,
		       human_resolved, human_relevant, hand_label_main, relevant_at, created_at
		FROM papers WHERE id = $1
	`, id).Scan(
		&r.ID, &arx, &doi, &url, &r.Title, &y, &first, &r.Abstract, &sources,
		&shmax, &shp, &sseed, &llmrel, &r.LLMStatus, &llmraw, &llmerr,
		&r.HumanResolved, &hr, &hlm, &ra, &r.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	_ = llmraw
	_ = llmerr
	if err := json.Unmarshal(sources, &r.Sources); err != nil {
		return nil, err
	}
	r.ArxivID, r.DOI, r.URL = arx, doi, url
	r.Year = y
	r.FirstAuthor = first
	r.ShadowMax, r.ShadowPass, r.ShadowArgmax = shmax, shp, sseed
	r.LLMRelevant, r.HumanRelevant, r.HandLabelMain = llmrel, hr, hlm
	r.RelevantAt = ra
	return &r, nil
}
