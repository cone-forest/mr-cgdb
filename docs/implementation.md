# Implementation Notes

## New migration layer

Phase 1 adds `003_users_profiles_phase1.sql` with:

- auth tables (`users`, `sessions`)
- profile tables (`profiles`, `profile_configs`, `profile_sources`)
- curation table (`profile_paper_likes`)
- local file metadata (`paper_files`)
- worker queue (`jobs`)

It also performs a hard cutover reset of legacy global relevance state and drops `hand_labels`/`system_config`.

## API implementation

`cmd/api/main.go` is now profile-centric:

- Auth/session endpoints:
  - register/login/logout/me/delete-account
- Profile CRUD:
  - create/update/delete/list-own
  - profile access touch for last-accessed ordering
  - analysis backfill trigger
- Curation:
  - like/unlike/list likes
  - paper search for manual curation
  - candidate analysis feed for profile-scoped relevance suggestions
- Public endpoints:
  - profile directory
  - public profile by `/u/{username}/{slug}`
  - public cached PDF serving
- Job operations:
  - list failures
  - manual retry
- Legacy relevance endpoints return `410 Gone`.

CSRF checks use `X-CSRF-Token` for mutating authenticated operations.

## Store layer additions

New files:

- `internal/store/auth_profiles.go`
  - user/session/profile/config/source CRUD
- `internal/store/likes_jobs_files.go`
  - paper search
  - like/unlike operations
  - job claim/complete/fail/retry
  - `paper_files` state updates
  - failed job listing and cleanup candidate listing

## Worker service

`cmd/worker/main.go`:

- polls pending `pdf_download` jobs
- polls pending `profile_analyze` jobs
- resolves and downloads PDFs into local storage
- updates `paper_files` and job status
- computes/stores profile analysis signals in `profile_paper_analysis`
- runs periodic cleanup for unreferenced files older than 72h

Failures are persisted and intentionally not auto-retried.

## UI implementation

`web/index.html` was rewritten to:

- handle register/login/logout
- manage own profiles and profile config fields (manual setup only; no auto-generation)
- search global papers and manually like with note/tags
- inspect own likes and unlike
- browse public profile directory
- open public profile paper lists
- open cached public PDFs
- inspect/retry failed jobs
- review analysis candidate feed with filters and one-click like from analysis
- **Reload** on the candidate feed POSTs `/api/profiles/{id}/analysis/backfill`, which queues `profile_analyze` jobs for corpus papers matching the profile’s sources (see worker); then the UI refetches candidates. Metrics filters only change the GET query—they do not enqueue work

The old digest/pending interface is removed from the main UI flow.

## Operational notes

- First registered user becomes admin.
- Cookie security can be toggled via `COOKIE_SECURE`.
- Public PDF serving uses in-memory per-IP rate limiting and max-byte checks.
- Docker compose now includes a dedicated `worker` and shared `pdfdata` volume.
