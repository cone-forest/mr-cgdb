package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// UpdateShadowResult writes shadow metrics and the candidate embedding.
// maxSim/wouldPass can be nil when seed set is empty.
func UpdateShadowResult(ctx context.Context, p *pgxpool.Pool, id int64, emb []float64, maxSim *float64, wouldPass *bool, argmax *string) error {
	_, err := p.Exec(ctx, `
		UPDATE papers SET
			embedding = $2::float8[],
			shadow_max_sim = $3,
			shadow_would_pass = $4,
			shadow_argmax_seed = $5
		WHERE id = $1
	`, id, emb, maxSim, wouldPass, argmax)
	return err
}

// SetLLMOK records a successful LLM result and stamps relevant_at the first time the row becomes digest-eligible.
func SetLLMOK(ctx context.Context, p *pgxpool.Pool, id int64, relevant bool, raw string) error {
	_, err := p.Exec(ctx, `
		UPDATE papers SET
			llm_status = 'ok',
			llm_relevant = $2,
			llm_raw = $3,
			last_llm_error = NULL,
			relevant_at = CASE
				WHEN $2 = true AND relevant_at IS NULL THEN now()
				ELSE relevant_at
			END
		WHERE id = $1
	`, id, relevant, raw)
	return err
}

// SetLLMPending marks a row as needing manual or retry; no final bool.
func SetLLMPending(ctx context.Context, p *pgxpool.Pool, id int64, errMsg string) error {
	_, err := p.Exec(ctx, `
		UPDATE papers SET
			llm_status = 'pending',
			llm_relevant = NULL,
			last_llm_error = $2
		WHERE id = $1
	`, id, errMsg)
	return err
}

// ResolvePending applies an authoritative label from the pending tab.
func ResolvePending(ctx context.Context, p *pgxpool.Pool, id int64, relevant bool) error {
	_, err := p.Exec(ctx, `
		UPDATE papers SET
			human_resolved = true,
			human_relevant = $2,
			llm_status = 'ok',
			llm_relevant = $2,
			relevant_at = COALESCE(
				relevant_at,
				CASE WHEN $2 THEN now() END
			)
		WHERE id = $1
	`, id, relevant)
	return err
}

// RequeueLLM marks a paper for another LLM attempt.
func RequeueLLM(ctx context.Context, p *pgxpool.Pool, id int64) error {
	_, err := p.Exec(ctx, `
		UPDATE papers
		SET llm_status = 'pending',
		    llm_relevant = NULL,
		    last_llm_error = NULL
		WHERE id = $1
	`, id)
	return err
}

// AddHandLabel logs a hand label; for main, updates hand_label_main for convenience.
func AddHandLabel(ctx context.Context, p *pgxpool.Pool, paperID int64, ctxl, label string) error {
	if _, err := p.Exec(ctx, `INSERT INTO hand_labels (paper_id, context, label) VALUES ($1, $2, $3)`,
		paperID, ctxl, label); err != nil {
		return err
	}
	if ctxl == "main" {
		_, err := p.Exec(ctx, `UPDATE papers SET hand_label_main = $2 WHERE id = $1`, paperID, label)
		return err
	}
	return nil
}
