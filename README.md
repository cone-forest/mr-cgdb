# mr-cgdb

Local-first scientific paper platform with multi-user auth and profile-based curation.

## Current model (Phase 1)

- Users register/login with `username + password`.
- Each user can create multiple research profiles (`public` or `private`).
- Profiles hold manual paper relevance selections with optional notes/tags.
- Public profiles are discoverable and browsable by other users.
- Liking a paper enqueues a PDF download job into local cache.
- Cached PDFs are publicly accessible only when paper is present in at least one public profile.
- Failed jobs are visible with error reason and manually retryable.

Legacy global relevance endpoints (`/api/digests`, `/api/pending`, label/resolve/retry) now return `410 Gone`.

## Run

```bash
docker compose up --build
```

Open UI at <http://localhost:8080>.

## Services

- `postgres`
- `dedup`, `keyword`, `pipeline`, `arxiv-watcher`, `rss-watcher` (ingestion path)
- `api` (auth/profile/public API + UI hosting)
- `worker` (PDF download jobs + cache cleanup)
- `ollama` (local model service for pipeline/deep verify paths)

## Auth/Profile API (core)

- `POST /api/auth/register`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/auth/me`
- `GET /api/profiles/me`
- `POST /api/profiles`
- `PATCH /api/profiles/{id}`
- `DELETE /api/profiles/{id}`
- `POST /api/profiles/{id}/access`
- `POST /api/profiles/{id}/analysis/backfill`
- `GET /api/profiles/{id}/analysis/candidates`
- `GET /api/public/profiles`
- `GET /api/public/u/{username}/{slug}`
- `POST /api/profiles/{id}/likes`
- `PATCH /api/profiles/{id}/likes/{paperId}`
- `DELETE /api/profiles/{id}/likes/{paperId}`
- `GET /api/public/papers/{id}/pdf`
- `GET /api/jobs/failures`
- `POST /api/jobs/{id}/retry`

## Notes

- CSRF token is required on mutating authenticated endpoints (`X-CSRF-Token` header).
- First registered account is automatically marked admin.
- PDF cache cleanup removes files not referenced by any profile like for 3+ days.
- Profile configuration is profile-local; there is no global inherited config hierarchy.
