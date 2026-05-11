package store

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SystemConfig struct {
	DomainName            string   `json:"domainName"`
	PositiveKeywords      []string `json:"positiveKeywords"`
	NegativeTitleKeywords []string `json:"negativeTitleKeywords"`
	LLMSystemPrompt       string   `json:"llmSystemPrompt"`
	ArxivQuery            string   `json:"arxivQuery"`
}

func GetSystemConfig(ctx context.Context, p *pgxpool.Pool) (*SystemConfig, error) {
	var cfg SystemConfig
	if err := p.QueryRow(ctx, `
		SELECT domain_name, positive_keywords, negative_title_keywords, llm_system_prompt, arxiv_query
		FROM system_config
		WHERE id = 1
	`).Scan(
		&cfg.DomainName,
		&cfg.PositiveKeywords,
		&cfg.NegativeTitleKeywords,
		&cfg.LLMSystemPrompt,
		&cfg.ArxivQuery,
	); err != nil {
		return nil, err
	}
	cfg.PositiveKeywords = normalizeKeywordList(cfg.PositiveKeywords)
	cfg.NegativeTitleKeywords = normalizeKeywordList(cfg.NegativeTitleKeywords)
	cfg.DomainName = strings.TrimSpace(cfg.DomainName)
	cfg.LLMSystemPrompt = strings.TrimSpace(cfg.LLMSystemPrompt)
	cfg.ArxivQuery = strings.TrimSpace(cfg.ArxivQuery)
	return &cfg, nil
}

func UpsertSystemConfig(ctx context.Context, p *pgxpool.Pool, cfg *SystemConfig) error {
	if cfg == nil {
		return nil
	}
	_, err := p.Exec(ctx, `
		INSERT INTO system_config (
			id, domain_name, positive_keywords, negative_title_keywords, llm_system_prompt, arxiv_query, updated_at
		)
		VALUES (1, $1, $2::text[], $3::text[], $4, $5, now())
		ON CONFLICT (id) DO UPDATE
		SET domain_name = EXCLUDED.domain_name,
		    positive_keywords = EXCLUDED.positive_keywords,
		    negative_title_keywords = EXCLUDED.negative_title_keywords,
		    llm_system_prompt = EXCLUDED.llm_system_prompt,
		    arxiv_query = EXCLUDED.arxiv_query,
		    updated_at = now()
	`,
		strings.TrimSpace(cfg.DomainName),
		normalizeKeywordList(cfg.PositiveKeywords),
		normalizeKeywordList(cfg.NegativeTitleKeywords),
		strings.TrimSpace(cfg.LLMSystemPrompt),
		strings.TrimSpace(cfg.ArxivQuery),
	)
	return err
}

func normalizeKeywordList(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, v := range in {
		s := strings.ToLower(strings.TrimSpace(v))
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
