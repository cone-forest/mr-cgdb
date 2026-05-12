package store

import (
	"context"
	"log"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ListProfileIDsForArxivRouting returns profile IDs with enabled arxiv_query sources.
func ListProfileIDsForArxivRouting(ctx context.Context, p *pgxpool.Pool) ([]int64, error) {
	rows, err := p.Query(ctx, `
		SELECT DISTINCT profile_id
		FROM profile_sources
		WHERE enabled = true
		  AND source_type = 'arxiv_query'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ListBareProfileIDs returns profiles with no profile_sources rows (global / unspecialized feed).
func ListBareProfileIDs(ctx context.Context, p *pgxpool.Pool) ([]int64, error) {
	rows, err := p.Query(ctx, `
		SELECT p.id
		FROM profiles p
		WHERE NOT EXISTS (SELECT 1 FROM profile_sources ps WHERE ps.profile_id = p.id)
		ORDER BY p.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ResolveRSSFeedURL resolves feed ID in the form "feed-<id>" to rss_feeds.url.
func ResolveRSSFeedURL(ctx context.Context, p *pgxpool.Pool, feedID string) (string, error) {
	feedID = strings.TrimSpace(feedID)
	if feedID == "" {
		return "", nil
	}
	if !strings.HasPrefix(feedID, "feed-") {
		return "", nil
	}
	idPart := strings.TrimPrefix(feedID, "feed-")
	n, err := strconv.ParseInt(idPart, 10, 64)
	if err != nil || n <= 0 {
		return "", nil
	}
	var url string
	if err := p.QueryRow(ctx, `SELECT url FROM rss_feeds WHERE id = $1`, n).Scan(&url); err != nil {
		return "", err
	}
	return strings.TrimSpace(url), nil
}

// ListProfileIDsForRSSRouting returns profile IDs with enabled rss sources matching sourceValue.
func ListProfileIDsForRSSRouting(ctx context.Context, p *pgxpool.Pool, sourceValue string) ([]int64, error) {
	sourceValue = normalizeFeedURLForMatch(sourceValue)
	if sourceValue == "" {
		return nil, nil
	}
	rows, err := p.Query(ctx, `
		SELECT DISTINCT profile_id
		FROM profile_sources
		WHERE enabled = true
		  AND source_type = 'rss'
		  AND trim(trailing '/' from regexp_replace(lower(trim(source_value)), '^(https?:)?//', '')) = $1
	`, sourceValue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func normalizeFeedURLForMatch(v string) string {
	v = strings.TrimSpace(strings.ToLower(v))
	v = strings.TrimPrefix(v, "https://")
	v = strings.TrimPrefix(v, "http://")
	v = strings.TrimPrefix(v, "//")
	v = strings.TrimSuffix(v, "/")
	return v
}

// ProfileFeedMeta summarizes subscription + global paper counts for candidate UI hints.
func ProfileFeedMeta(ctx context.Context, p *pgxpool.Pool, profileID int64) (hasSourceRows, hasEnabledSource bool, papersTotal int64, err error) {
	err = p.QueryRow(ctx, `
		SELECT
			EXISTS (SELECT 1 FROM profile_sources ps WHERE ps.profile_id = $1),
			EXISTS (SELECT 1 FROM profile_sources ps WHERE ps.profile_id = $1 AND ps.enabled = true),
			(SELECT COUNT(*)::bigint FROM papers)
	`, profileID).Scan(&hasSourceRows, &hasEnabledSource, &papersTotal)
	return hasSourceRows, hasEnabledSource, papersTotal, err
}

func EnqueueProfileAnalyzeJob(ctx context.Context, p *pgxpool.Pool, profileID, paperID int64) error {
	_, err := p.Exec(ctx, `
		INSERT INTO jobs (kind, status, payload)
		VALUES ('profile_analyze', 'pending', jsonb_build_object('profileId', $1, 'paperId', $2))
	`, profileID, paperID)
	return err
}

// EnqueueProfileAnalyzeBackfill queues analysis jobs for existing papers matching profile sources.
func EnqueueProfileAnalyzeBackfill(ctx context.Context, p *pgxpool.Pool, profileID int64) (int64, error) {
	var eligibleArxiv int64
	err := p.QueryRow(ctx, `
		SELECT COUNT(*)::bigint FROM papers pa
		WHERE pa.source = 'arxiv'
		  AND (
			EXISTS (
				SELECT 1 FROM profile_sources ps
				WHERE ps.profile_id = $1::bigint
				  AND ps.enabled = true
				  AND (
					ps.source_type = 'arxiv_query'
					OR (
						ps.source_type = 'rss'
						AND (
							trim(trailing '/' from regexp_replace(lower(trim(ps.source_value)), '^(https?:)?//', '')) LIKE 'rss.arxiv.org/rss/%'
							OR trim(trailing '/' from regexp_replace(lower(trim(ps.source_value)), '^(https?:)?//', '')) LIKE 'export.arxiv.org/rss/%'
						)
					)
				  )
			)
			OR NOT EXISTS (
				SELECT 1 FROM profile_sources ps
				WHERE ps.profile_id = $1::bigint
			)
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM profile_paper_analysis a
			WHERE a.profile_id = $1::bigint AND a.paper_id = pa.id
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM jobs j
			WHERE j.kind = 'profile_analyze'
			  AND j.status IN ('pending', 'running')
			  AND COALESCE(j.payload->>'profileId', '') <> ''
			  AND COALESCE(j.payload->>'paperId', '') <> ''
			  AND (j.payload->>'profileId')::bigint = $1::bigint
			  AND (j.payload->>'paperId')::bigint = pa.id
		  )
	`, profileID).Scan(&eligibleArxiv)
	if err != nil {
		return 0, err
	}

	var eligibleRSS int64
	err = p.QueryRow(ctx, `
		SELECT COUNT(*)::bigint FROM papers pa
		LEFT JOIN rss_feeds rf ON ('rss:feed-' || rf.id::text) = pa.source
		WHERE pa.source LIKE 'rss:feed-%'
		  AND (
			(
				rf.id IS NOT NULL
				AND EXISTS (
					SELECT 1 FROM profile_sources ps
					WHERE ps.profile_id = $1::bigint
					  AND ps.enabled = true
					  AND ps.source_type = 'rss'
					  AND trim(trailing '/' from regexp_replace(lower(trim(ps.source_value)), '^(https?:)?//', '')) =
					      trim(trailing '/' from regexp_replace(lower(trim(rf.url)), '^(https?:)?//', ''))
				)
			)
			OR NOT EXISTS (
				SELECT 1 FROM profile_sources ps
				WHERE ps.profile_id = $1::bigint
			)
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM profile_paper_analysis a
			WHERE a.profile_id = $1::bigint AND a.paper_id = pa.id
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM jobs j
			WHERE j.kind = 'profile_analyze'
			  AND j.status IN ('pending', 'running')
			  AND COALESCE(j.payload->>'profileId', '') <> ''
			  AND COALESCE(j.payload->>'paperId', '') <> ''
			  AND (j.payload->>'profileId')::bigint = $1::bigint
			  AND (j.payload->>'paperId')::bigint = pa.id
		  )
	`, profileID).Scan(&eligibleRSS)
	if err != nil {
		return 0, err
	}

	tag1, err := p.Exec(ctx, `
		INSERT INTO jobs (kind, status, payload)
		SELECT 'profile_analyze', 'pending', jsonb_build_object('profileId', $1::bigint, 'paperId', pa.id)
		FROM papers pa
		WHERE pa.source = 'arxiv'
		  AND (
			EXISTS (
				SELECT 1 FROM profile_sources ps
				WHERE ps.profile_id = $1::bigint
				  AND ps.enabled = true
				  AND (
					ps.source_type = 'arxiv_query'
					OR (
						ps.source_type = 'rss'
						AND (
							trim(trailing '/' from regexp_replace(lower(trim(ps.source_value)), '^(https?:)?//', '')) LIKE 'rss.arxiv.org/rss/%'
							OR trim(trailing '/' from regexp_replace(lower(trim(ps.source_value)), '^(https?:)?//', '')) LIKE 'export.arxiv.org/rss/%'
						)
					)
				  )
			)
			OR NOT EXISTS (
				SELECT 1 FROM profile_sources ps
				WHERE ps.profile_id = $1::bigint
			)
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM profile_paper_analysis a
			WHERE a.profile_id = $1::bigint AND a.paper_id = pa.id
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM jobs j
			WHERE j.kind = 'profile_analyze'
			  AND j.status IN ('pending', 'running')
			  AND COALESCE(j.payload->>'profileId', '') <> ''
			  AND COALESCE(j.payload->>'paperId', '') <> ''
			  AND (j.payload->>'profileId')::bigint = $1::bigint
			  AND (j.payload->>'paperId')::bigint = pa.id
		  )
	`, profileID)
	if err != nil {
		return 0, err
	}
	tag2, err := p.Exec(ctx, `
		INSERT INTO jobs (kind, status, payload)
		SELECT 'profile_analyze', 'pending', jsonb_build_object('profileId', $1::bigint, 'paperId', pa.id)
		FROM papers pa
		LEFT JOIN rss_feeds rf ON ('rss:feed-' || rf.id::text) = pa.source
		WHERE pa.source LIKE 'rss:feed-%'
		  AND (
			(
				rf.id IS NOT NULL
				AND EXISTS (
					SELECT 1 FROM profile_sources ps
					WHERE ps.profile_id = $1::bigint
					  AND ps.enabled = true
					  AND ps.source_type = 'rss'
					  AND trim(trailing '/' from regexp_replace(lower(trim(ps.source_value)), '^(https?:)?//', '')) =
					      trim(trailing '/' from regexp_replace(lower(trim(rf.url)), '^(https?:)?//', ''))
				)
			)
			OR NOT EXISTS (
				SELECT 1 FROM profile_sources ps
				WHERE ps.profile_id = $1::bigint
			)
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM profile_paper_analysis a
			WHERE a.profile_id = $1::bigint AND a.paper_id = pa.id
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM jobs j
			WHERE j.kind = 'profile_analyze'
			  AND j.status IN ('pending', 'running')
			  AND COALESCE(j.payload->>'profileId', '') <> ''
			  AND COALESCE(j.payload->>'paperId', '') <> ''
			  AND (j.payload->>'profileId')::bigint = $1::bigint
			  AND (j.payload->>'paperId')::bigint = pa.id
		  )
	`, profileID)
	if err != nil {
		return 0, err
	}
	insertArxiv := tag1.RowsAffected()
	insertRSS := tag2.RowsAffected()
	total := insertArxiv + insertRSS
	log.Printf("mr-cgdb store ProfileAnalyzeBackfill profile_id=%d eligible_arxiv_slots=%d eligible_rss_slots=%d inserted_jobs_arxiv=%d inserted_jobs_rss=%d inserted_total=%d",
		profileID, eligibleArxiv, eligibleRSS, insertArxiv, insertRSS, total)
	return total, nil
}
