package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ProfilePaperAnalysis struct {
	ProfileID               int64     `json:"profileId"`
	PaperID                 int64     `json:"paperId"`
	KeywordPass             bool      `json:"keywordPass"`
	KeywordHits             []string  `json:"keywordHits"`
	LLMRelevant             *bool     `json:"llmRelevant,omitempty"`
	LLMRaw                  string    `json:"llmRaw,omitempty"`
	ShadowWouldAutoDownload bool      `json:"shadowWouldAutoDownload"`
	ShadowScore             float64   `json:"shadowScore"`
	UpdatedAt               time.Time `json:"updatedAt"`
}

type ProfileAnalysisCandidate struct {
	ProfilePaperAnalysis
	Title       string  `json:"title"`
	URL         string  `json:"url"`
	Year        *int    `json:"year,omitempty"`
	FirstAuthor *string `json:"firstAuthor,omitempty"`
	Abstract    string  `json:"abstract"`
}

func UpsertProfilePaperAnalysis(ctx context.Context, p *pgxpool.Pool, a *ProfilePaperAnalysis) error {
	if a == nil {
		return nil
	}
	_, err := p.Exec(ctx, `
		INSERT INTO profile_paper_analysis (
			profile_id, paper_id, keyword_pass, keyword_hits, llm_relevant, llm_raw,
			shadow_would_auto_download, shadow_score, updated_at
		)
		VALUES ($1,$2,$3,$4::text[],$5,$6,$7,$8,now())
		ON CONFLICT (profile_id, paper_id) DO UPDATE
		SET keyword_pass = EXCLUDED.keyword_pass,
		    keyword_hits = EXCLUDED.keyword_hits,
		    llm_relevant = EXCLUDED.llm_relevant,
		    llm_raw = EXCLUDED.llm_raw,
		    shadow_would_auto_download = EXCLUDED.shadow_would_auto_download,
		    shadow_score = EXCLUDED.shadow_score,
		    updated_at = now()
	`, a.ProfileID, a.PaperID, a.KeywordPass, normalizeTagList(a.KeywordHits), a.LLMRelevant, a.LLMRaw, a.ShadowWouldAutoDownload, a.ShadowScore)
	return err
}

func GetProfilePaperAnalysis(ctx context.Context, p *pgxpool.Pool, profileID, paperID int64) (*ProfilePaperAnalysis, error) {
	var a ProfilePaperAnalysis
	err := p.QueryRow(ctx, `
		SELECT profile_id, paper_id, keyword_pass, keyword_hits, llm_relevant, llm_raw,
		       shadow_would_auto_download, shadow_score, updated_at
		FROM profile_paper_analysis
		WHERE profile_id = $1 AND paper_id = $2
	`, profileID, paperID).Scan(
		&a.ProfileID, &a.PaperID, &a.KeywordPass, &a.KeywordHits, &a.LLMRelevant, &a.LLMRaw,
		&a.ShadowWouldAutoDownload, &a.ShadowScore, &a.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func ListProfileAnalysis(ctx context.Context, p *pgxpool.Pool, profileID int64, limit int) ([]ProfilePaperAnalysis, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	rows, err := p.Query(ctx, `
		SELECT profile_id, paper_id, keyword_pass, keyword_hits, llm_relevant, llm_raw,
		       shadow_would_auto_download, shadow_score, updated_at
		FROM profile_paper_analysis
		WHERE profile_id = $1
		ORDER BY updated_at DESC
		LIMIT $2
	`, profileID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProfilePaperAnalysis
	for rows.Next() {
		var a ProfilePaperAnalysis
		if err := rows.Scan(
			&a.ProfileID, &a.PaperID, &a.KeywordPass, &a.KeywordHits, &a.LLMRelevant, &a.LLMRaw,
			&a.ShadowWouldAutoDownload, &a.ShadowScore, &a.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func ListProfileAnalysisCandidates(ctx context.Context, p *pgxpool.Pool, profileID int64, keywordOnly, llmRelevantOnly, wouldAutoOnly bool, limit int) ([]ProfileAnalysisCandidate, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	rows, err := p.Query(ctx, `
		SELECT
			COALESCE(a.profile_id, $1),
			pa.id,
			COALESCE(a.keyword_pass, false),
			COALESCE(a.keyword_hits, ARRAY[]::TEXT[]),
			a.llm_relevant,
			COALESCE(a.llm_raw, ''),
			COALESCE(a.shadow_would_auto_download, false),
			COALESCE(a.shadow_score, 0)::double precision,
			COALESCE(a.updated_at, pa.created_at),
			pa.title,
			COALESCE(pa.url, CASE WHEN pa.arxiv_id IS NOT NULL THEN 'https://arxiv.org/abs/' || pa.arxiv_id ELSE '' END) AS paper_url,
			pa.year, pa.first_author, pa.abstract
		FROM papers pa
		LEFT JOIN profile_paper_analysis a ON a.profile_id = $1 AND a.paper_id = pa.id
		WHERE (
		  (
			pa.source = 'arxiv'
			AND EXISTS (
				SELECT 1 FROM profile_sources ps
				WHERE ps.profile_id = $1
				  AND ps.enabled = true
				  AND ps.source_type = 'arxiv_query'
			)
		  )
		  OR EXISTS (
			SELECT 1
			FROM rss_feeds rf
			INNER JOIN profile_sources ps
				ON ps.profile_id = $1
			   AND ps.enabled = true
			   AND ps.source_type = 'rss'
			   AND lower(ps.source_value) = lower(rf.url)
			WHERE pa.source = 'rss:feed-' || rf.id::text
		  )
		  OR (
			NOT EXISTS (SELECT 1 FROM profile_sources ps WHERE ps.profile_id = $1)
			AND (pa.source = 'arxiv' OR pa.source LIKE 'rss:feed-%')
		  )
		)
		  AND ($2::bool = false OR COALESCE(a.keyword_pass, false) = true)
		  AND ($3::bool = false OR COALESCE(a.llm_relevant, false) = true)
		  AND ($4::bool = false OR COALESCE(a.shadow_would_auto_download, false) = true)
		ORDER BY COALESCE(a.updated_at, pa.created_at) DESC
		LIMIT $5
	`, profileID, keywordOnly, llmRelevantOnly, wouldAutoOnly, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ProfileAnalysisCandidate, 0)
	for rows.Next() {
		var c ProfileAnalysisCandidate
		if err := rows.Scan(
			&c.ProfileID, &c.PaperID, &c.KeywordPass, &c.KeywordHits, &c.LLMRelevant, &c.LLMRaw,
			&c.ShadowWouldAutoDownload, &c.ShadowScore, &c.UpdatedAt,
			&c.Title, &c.URL, &c.Year, &c.FirstAuthor, &c.Abstract,
		); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
