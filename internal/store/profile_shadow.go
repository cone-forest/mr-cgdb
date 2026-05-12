package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GetPaperEmbedding returns the stored paper embedding from papers.embedding.
func GetPaperEmbedding(ctx context.Context, p *pgxpool.Pool, paperID int64) ([]float64, error) {
	var emb []float64
	if err := p.QueryRow(ctx, `SELECT embedding FROM papers WHERE id = $1`, paperID).Scan(&emb); err != nil {
		return nil, err
	}
	return emb, nil
}

// ListProfileLikeEmbeddings returns embeddings for papers marked relevant (liked) in a profile.
func ListProfileLikeEmbeddings(ctx context.Context, p *pgxpool.Pool, profileID int64) ([][]float64, error) {
	rows, err := p.Query(ctx, `
		SELECT pa.embedding
		FROM profile_paper_likes ppl
		INNER JOIN papers pa ON pa.id = ppl.paper_id
		WHERE ppl.profile_id = $1
		  AND pa.embedding IS NOT NULL
		ORDER BY ppl.liked_at DESC
	`, profileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([][]float64, 0)
	for rows.Next() {
		var emb []float64
		if err := rows.Scan(&emb); err != nil {
			return nil, err
		}
		if len(emb) == 0 {
			continue
		}
		out = append(out, emb)
	}
	return out, rows.Err()
}
