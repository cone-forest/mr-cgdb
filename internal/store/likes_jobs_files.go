package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PaperBrief struct {
	ID          int64   `json:"id"`
	Title       string  `json:"title"`
	URL         string  `json:"url"`
	Year        *int    `json:"year,omitempty"`
	FirstAuthor *string `json:"firstAuthor,omitempty"`
	Abstract    string  `json:"abstract"`
}

type ProfileLikedPaper struct {
	PaperBrief
	Note      string    `json:"note"`
	Tags      []string  `json:"tags"`
	LikedAt   time.Time `json:"likedAt"`
	PDFStatus string    `json:"pdfStatus"`
}

type Job struct {
	ID          int64           `json:"id"`
	Kind        string          `json:"kind"`
	Status      string          `json:"status"`
	Payload     json.RawMessage `json:"payload"`
	ErrorReason *string         `json:"errorReason,omitempty"`
	CreatedAt   time.Time       `json:"createdAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
}

type JobFailure struct {
	Job
	ProfileID int64 `json:"profileId"`
}

func SearchPapers(ctx context.Context, p *pgxpool.Pool, q string, limit int) ([]PaperBrief, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	needle := "%" + strings.ToLower(strings.TrimSpace(q)) + "%"
	rows, err := p.Query(ctx, `
		SELECT id, title,
		       COALESCE(url, CASE WHEN arxiv_id IS NOT NULL THEN 'https://arxiv.org/abs/' || arxiv_id ELSE '' END) AS paper_url,
		       year, first_author, abstract
		FROM papers
		WHERE ($1 = '%%' OR lower(title) LIKE $1 OR lower(abstract) LIKE $1)
		ORDER BY id DESC
		LIMIT $2
	`, needle, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PaperBrief
	for rows.Next() {
		var pb PaperBrief
		if err := rows.Scan(&pb.ID, &pb.Title, &pb.URL, &pb.Year, &pb.FirstAuthor, &pb.Abstract); err != nil {
			return nil, err
		}
		out = append(out, pb)
	}
	return out, rows.Err()
}

func UpsertProfileLike(ctx context.Context, p *pgxpool.Pool, profileID, ownerUserID, paperID int64, note string, tags []string) error {
	ok, err := isProfileOwner(ctx, p, profileID, ownerUserID)
	if err != nil {
		return err
	}
	if !ok {
		return pgx.ErrNoRows
	}
	_, err = p.Exec(ctx, `
		INSERT INTO profile_paper_likes (profile_id, paper_id, note, tags, liked_at)
		VALUES ($1, $2, $3, $4::text[], now())
		ON CONFLICT (profile_id, paper_id) DO UPDATE
		SET note = EXCLUDED.note,
		    tags = EXCLUDED.tags,
		    liked_at = now()
	`, profileID, paperID, strings.TrimSpace(note), normalizeTagList(tags))
	if err != nil {
		return err
	}
	_, err = p.Exec(ctx, `
		INSERT INTO paper_files (paper_id, source_url, status, updated_at)
		SELECT id, COALESCE(pdf_url, url, CASE WHEN arxiv_id IS NOT NULL THEN 'https://arxiv.org/pdf/' || arxiv_id || '.pdf' ELSE '' END), 'queued', now()
		FROM papers WHERE id = $1
		ON CONFLICT (paper_id) DO UPDATE
		SET status = CASE WHEN paper_files.status = 'ready' THEN 'ready' ELSE 'queued' END,
		    source_url = EXCLUDED.source_url,
		    updated_at = now(),
		    last_error = NULL
	`, paperID)
	if err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{
		"profileId": profileID,
		"paperId":   paperID,
	})
	_, err = p.Exec(ctx, `
		INSERT INTO jobs (kind, status, payload)
		VALUES ('pdf_download', 'pending', $1::jsonb)
	`, string(payload))
	if err != nil {
		return err
	}
	_, err = p.Exec(ctx, `
		INSERT INTO jobs (kind, status, payload)
		VALUES ('profile_analyze', 'pending', $1::jsonb)
	`, string(payload))
	return err
}

func RemoveProfileLike(ctx context.Context, p *pgxpool.Pool, profileID, ownerUserID, paperID int64) error {
	ok, err := isProfileOwner(ctx, p, profileID, ownerUserID)
	if err != nil {
		return err
	}
	if !ok {
		return pgx.ErrNoRows
	}
	_, err = p.Exec(ctx, `DELETE FROM profile_paper_likes WHERE profile_id = $1 AND paper_id = $2`, profileID, paperID)
	return err
}

func ListProfileLikes(ctx context.Context, p *pgxpool.Pool, profileID int64, q, tag string, limit int) ([]ProfileLikedPaper, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	needle := "%" + strings.ToLower(strings.TrimSpace(q)) + "%"
	tag = strings.TrimSpace(strings.ToLower(tag))
	rows, err := p.Query(ctx, `
		SELECT pp.paper_id, pa.title,
		       COALESCE(pa.url, CASE WHEN pa.arxiv_id IS NOT NULL THEN 'https://arxiv.org/abs/' || pa.arxiv_id ELSE '' END) AS paper_url,
		       pa.year, pa.first_author, pa.abstract, pp.note, pp.tags, pp.liked_at, COALESCE(pf.status, 'missing')
		FROM profile_paper_likes pp
		JOIN papers pa ON pa.id = pp.paper_id
		LEFT JOIN paper_files pf ON pf.paper_id = pp.paper_id
		WHERE pp.profile_id = $1
		  AND ($2 = '%%' OR lower(pa.title) LIKE $2 OR lower(pa.abstract) LIKE $2)
		  AND ($3 = '' OR $3 = ANY(pp.tags))
		ORDER BY pp.liked_at DESC, pp.paper_id DESC
		LIMIT $4
	`, profileID, needle, tag, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProfileLikedPaper
	for rows.Next() {
		var item ProfileLikedPaper
		if err := rows.Scan(&item.ID, &item.Title, &item.URL, &item.Year, &item.FirstAuthor, &item.Abstract, &item.Note, &item.Tags, &item.LikedAt, &item.PDFStatus); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func IsProfilePublic(ctx context.Context, p *pgxpool.Pool, profileID int64) (bool, error) {
	var visibility string
	if err := p.QueryRow(ctx, `SELECT visibility FROM profiles WHERE id = $1`, profileID).Scan(&visibility); err != nil {
		return false, err
	}
	return visibility == "public", nil
}

func IsPaperPubliclyLiked(ctx context.Context, p *pgxpool.Pool, paperID int64) (bool, error) {
	var n int
	if err := p.QueryRow(ctx, `
		SELECT COUNT(*)::int
		FROM profile_paper_likes ppl
		JOIN profiles pr ON pr.id = ppl.profile_id
		WHERE ppl.paper_id = $1 AND pr.visibility = 'public'
	`, paperID).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

func ClaimNextPendingJob(ctx context.Context, p *pgxpool.Pool, kind string) (*Job, error) {
	tx, err := p.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var j Job
	err = tx.QueryRow(ctx, `
		SELECT id, kind, status, payload, error_reason, created_at, updated_at
		FROM jobs
		WHERE status = 'pending' AND kind = $1
		ORDER BY created_at ASC, id ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, kind).Scan(&j.ID, &j.Kind, &j.Status, &j.Payload, &j.ErrorReason, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE jobs SET status='running', updated_at=now(), error_reason=NULL WHERE id=$1`, j.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	j.Status = "running"
	return &j, nil
}

func CompleteJob(ctx context.Context, p *pgxpool.Pool, jobID int64) error {
	_, err := p.Exec(ctx, `UPDATE jobs SET status='done', updated_at=now(), finished_at=now(), error_reason=NULL WHERE id=$1`, jobID)
	return err
}

func FailJob(ctx context.Context, p *pgxpool.Pool, jobID int64, reason string) error {
	_, err := p.Exec(ctx, `UPDATE jobs SET status='failed', updated_at=now(), finished_at=now(), error_reason=$2 WHERE id=$1`, jobID, strings.TrimSpace(reason))
	return err
}

func RetryJob(ctx context.Context, p *pgxpool.Pool, jobID int64) error {
	_, err := p.Exec(ctx, `UPDATE jobs SET status='pending', updated_at=now(), finished_at=NULL WHERE id=$1`, jobID)
	return err
}

func EnqueueProfileLLMVerifyJob(ctx context.Context, p *pgxpool.Pool, profileID, paperID int64) (bool, error) {
	tag, err := p.Exec(ctx, `
		INSERT INTO jobs (kind, status, payload)
		SELECT 'profile_llm_verify', 'pending', jsonb_build_object('profileId', $1::bigint, 'paperId', $2::bigint)
		WHERE NOT EXISTS (
			SELECT 1
			FROM jobs j
			WHERE j.kind = 'profile_llm_verify'
			  AND j.status IN ('pending', 'running')
			  AND COALESCE(j.payload->>'profileId', '') <> ''
			  AND COALESCE(j.payload->>'paperId', '') <> ''
			  AND (j.payload->>'profileId')::bigint = $1::bigint
			  AND (j.payload->>'paperId')::bigint = $2::bigint
		)
	`, profileID, paperID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func GetJobByID(ctx context.Context, p *pgxpool.Pool, jobID int64) (*Job, error) {
	var j Job
	if err := p.QueryRow(ctx, `
		SELECT id, kind, status, payload, error_reason, created_at, updated_at
		FROM jobs
		WHERE id = $1
	`, jobID).Scan(&j.ID, &j.Kind, &j.Status, &j.Payload, &j.ErrorReason, &j.CreatedAt, &j.UpdatedAt); err != nil {
		return nil, err
	}
	return &j, nil
}

func SetPaperFileState(ctx context.Context, p *pgxpool.Pool, paperID int64, sourceURL, localPath string, bytes int64, status, lastErr string) error {
	_, err := p.Exec(ctx, `
		INSERT INTO paper_files (paper_id, source_url, local_path, bytes, status, last_error, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		ON CONFLICT (paper_id) DO UPDATE
		SET source_url = EXCLUDED.source_url,
		    local_path = EXCLUDED.local_path,
		    bytes = EXCLUDED.bytes,
		    status = EXCLUDED.status,
		    last_error = EXCLUDED.last_error,
		    updated_at = now()
	`, paperID, strings.TrimSpace(sourceURL), nullIfEmpty(localPath), nullIfZero(bytes), status, nullIfEmpty(lastErr))
	return err
}

func GetPaperFile(ctx context.Context, p *pgxpool.Pool, paperID int64) (localPath, status, sourceURL string, err error) {
	var lp, st, su string
	err = p.QueryRow(ctx, `SELECT COALESCE(local_path,''), status, COALESCE(source_url,'') FROM paper_files WHERE paper_id = $1`, paperID).
		Scan(&lp, &st, &su)
	return lp, st, su, err
}

func ListJobFailures(ctx context.Context, p *pgxpool.Pool, userID int64, isAdmin bool, limit int) ([]JobFailure, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	sql := `
		SELECT j.id, j.kind, j.status, j.payload, j.error_reason, j.created_at, j.updated_at, COALESCE((j.payload->>'profileId')::bigint,0)
		FROM jobs j
		WHERE j.status = 'failed'
	`
	args := []any{}
	if !isAdmin {
		sql += `
		  AND EXISTS (
			SELECT 1 FROM profiles p
			WHERE p.id = COALESCE((j.payload->>'profileId')::bigint,0)
			  AND p.user_id = $1
		  )
		`
		args = append(args, userID)
		sql += ` ORDER BY j.updated_at DESC LIMIT $2`
		args = append(args, limit)
	} else {
		sql += ` ORDER BY j.updated_at DESC LIMIT $1`
		args = append(args, limit)
	}
	rows, err := p.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []JobFailure
	for rows.Next() {
		var jf JobFailure
		if err := rows.Scan(&jf.ID, &jf.Kind, &jf.Status, &jf.Payload, &jf.ErrorReason, &jf.CreatedAt, &jf.UpdatedAt, &jf.ProfileID); err != nil {
			return nil, err
		}
		out = append(out, jf)
	}
	return out, rows.Err()
}

func CleanupUnreferencedPaperFiles(ctx context.Context, p *pgxpool.Pool, olderThan time.Duration) ([]int64, error) {
	if olderThan <= 0 {
		olderThan = 72 * time.Hour
	}
	rows, err := p.Query(ctx, `
		SELECT pf.paper_id
		FROM paper_files pf
		WHERE pf.status = 'ready'
		  AND pf.updated_at < now() - ($1 * interval '1 second')
		  AND NOT EXISTS (SELECT 1 FROM profile_paper_likes ppl WHERE ppl.paper_id = pf.paper_id)
	`, int64(olderThan.Seconds()))
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

func isProfileOwner(ctx context.Context, p *pgxpool.Pool, profileID, userID int64) (bool, error) {
	var n int
	if err := p.QueryRow(ctx, `SELECT COUNT(*)::int FROM profiles WHERE id = $1 AND user_id = $2`, profileID, userID).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

func nullIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return strings.TrimSpace(s)
}

func nullIfZero(v int64) any {
	if v <= 0 {
		return nil
	}
	return v
}
