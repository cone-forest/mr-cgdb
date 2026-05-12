# Architecture

## Purpose

`mr-cgdb` is now a multi-user research curation platform with profile-based public/private paper lists.

Phase 1 introduces user auth, profile ownership, public profile discovery, and manual like-driven PDF caching while preserving existing global ingestion services.

## Runtime Topology

- `postgres`: source of truth.
- `arxiv-watcher`, `rss-watcher`, `dedup`, `keyword`, `pipeline`: legacy ingestion stack (kept running).
- `api`: auth/profile/public endpoints + UI host.
- `worker`: background job consumer for PDF downloads and cache cleanup.
- `ollama`: model service (legacy/optional model paths).

## Core Data Domains

### Global corpus domain

- `papers`
- `arxiv_cursor`
- `rss_cursors`
- `rss_feeds`

These remain global and shared by all users/profiles.

### User/profile domain

- `users`
- `sessions`
- `profiles`
- `profile_configs`
- `profile_sources`
- `profile_paper_likes`
- `paper_files`
- `jobs`

This domain defines visibility, curation, and local file cache lifecycle.

## Access Model

- Users authenticate via username/password + session cookie.
- Profiles are single-owner and immutable slug.
- Profile visibility is `public` or `private`.
- Public endpoint shape: `/u/{username}/{profileSlug}`.
- Public PDF access is allowed only when paper is present in at least one public profile like.

## Background Work Model

- Liking a paper enqueues a `pdf_download` job.
- Worker claims pending jobs, downloads file, writes `paper_files`.
- Job failures are persisted with reason; retries are manual.
- Cleanup pass removes `ready` cached files not referenced by any like for 3+ days.

## Legacy Cutover

Global relevance APIs (`/api/digests`, `/api/pending`, old label/resolve/retry) were hard-cut and now return `410 Gone`.

This keeps old ingestion available without exposing deprecated global relevance UX.
