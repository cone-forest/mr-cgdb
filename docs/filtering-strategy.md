# Filtering Strategy and Hierarchy

## Current state after cutover

Global relevance and digest/pending UX were removed from the primary product surface.

Phase 1 relevance is explicit and manual:

- A user chooses a profile.
- User manually likes papers in that profile.
- Public visibility depends on profile visibility.

This means publication/discovery is curation-first, not model-first.

## Effective hierarchy now

1. **Global ingest gates** (watchers -> dedup -> keyword -> pipeline) still produce corpus rows in `papers`.
   - The **keyword** step always applies the negative-title blocklist (`NEGATIVE_TITLE_KEYWORDS`).
   - **Profile-scoped relevance** (positive keywords, negative title keywords, LLM) lives in **`profile_configs`** and is applied later by **`profile_analyze`** jobs — that is the primary keyword story.
   - **Optional legacy gate:** set **`KEYWORD_GLOBAL_GATE=strict`** on the keyword service to also require a substring match from **`KEYWORDS_FILE`** before insert (shrinks shared corpus early). Default is **no global phrase file gate** so ingest breadth follows your ArXiv/RSS queries and profiles filter downstream.
2. **Profile ownership gate** decides who can curate a profile.
3. **Profile visibility gate** (`public/private`) decides who can read the profile.
4. **Manual like gate** (`profile_paper_likes`) determines what appears in that profile feed.
5. **PDF access gate** allows public cached PDF serving only when paper is liked in at least one public profile.

## Manual curation semantics

Each `profile + paper` relationship stores:

- `liked_at`
- `note`
- `tags[]`

This is the canonical relevance signal for public discovery in Phase 1.

## PDF policy

- Download is triggered only by manual likes.
- Failures are persisted in `jobs.error_reason`.
- Retry is manual.
- Cleanup removes cached files not referenced by any likes for >= 72h.

## Phase 2 status

Phase 2 analysis signals are now active:

- Source-matched papers enqueue `profile_analyze` jobs automatically.
- Likes also enqueue analysis jobs.
- Worker computes profile-scoped:
  - `keyword_pass`
  - `keyword_hits`
  - `llm_relevant`
  - `shadow_score`
  - `shadow_would_auto_download`
- Results are persisted in `profile_paper_analysis`.

Manual likes remain the authoritative publish action for public profile output.
