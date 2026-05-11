# Architecture

## Purpose

`mr-cgdb` is a local-first paper routing system for computer graphics literature, centered on Cluster/Meshlet/LOD relevance.

At runtime it behaves like a staged streaming pipeline:

1. Source collectors (`arxiv-watcher`, `rss-watcher`) emit candidate papers.
2. `dedup` removes already-known identities and merges source provenance.
3. `keyword` performs cheap lexical filtering and persists candidates.
4. `pipeline` runs embedding shadow scoring and LLM relevance classification.
5. `api` + `web/index.html` expose digest, pending review, and operator actions.
6. PostgreSQL is the system of record; Ollama provides embeddings/chat inference.

## Topology

Deployed via `docker-compose.yml`:

- `postgres`: relational state and cursors.
- `ollama`: model host (`nomic-embed-text`, `llama3.2:1b` by default).
- `arxiv-watcher`: polls arXiv API.
- `rss-watcher`: polls configured RSS feeds.
- `dedup`: identity gate and source merger.
- `keyword`: lexical gate + insert + pipeline enqueue.
- `pipeline`: embedding + shadow metrics + LLM verdict.
- `api`: HTTP control plane and static UI host.

## Service Boundaries

- **Ingestion services**
  - `cmd/arxiv-watcher/main.go`
  - `cmd/rss-watcher/main.go`
  - Emit `model.IngestItem` over TCP.
- **Stream processors**
  - `cmd/dedup/main.go`
  - `cmd/keyword/main.go`
  - `cmd/pipeline/main.go`
- **Control/UI plane**
  - `cmd/api/main.go`
  - `web/index.html`
- **Shared internals**
  - Protocol: `internal/wire` (length-prefixed JSON frames)
  - Networking: `internal/netx` (retrying TCP dial)
  - Storage: `internal/store/*`
  - Domain parsing: `internal/arxiv`, `internal/rssx`, `internal/identity`
  - Filtering primitives: `internal/keywords`, `internal/pipeline`, `internal/seed`
  - Model IO: `internal/ollama`
  - Deep document verification: `internal/pdfx`

## Dataflow Contracts

Two core message types (`internal/model/ingest.go`):

- `IngestItem`: full candidate payload from watchers to dedup/keyword.
- `PipelineWork`: `paper_id` work item from keyword to pipeline.

Transport (`internal/wire/wire.go`):

- Big-endian `uint32` length prefix + JSON body.
- `MaxMessageSize` hard cap: 16 MiB.

## Persistence Model

Main table: `papers` (`migrations/001_init.sql` and embedded `internal/store/sql/001_init.sql`).

Key properties:

- Identity columns: `arxiv_id`, `doi`, `weak_key` with partial unique indexes.
- Classification state: `llm_status`, `llm_relevant`, `last_llm_error`.
- Shadow telemetry: `embedding`, `shadow_max_sim`, `shadow_would_pass`, `shadow_argmax_seed`.
- Human review state: `human_resolved`, `human_relevant`, `hand_label_main`.
- Digest eligibility timestamp: `relevant_at`.
- Optional deep verify evidence: `deep_verify_*`.
- Provenance: `source` + `sources` JSON list.

Supporting tables:

- `arxiv_cursor`: composite cursor for polling progress.
- `rss_cursors`: per-feed cursor.
- `rss_feeds`: dynamic feed config.
- `hand_labels`: append-only label log.

## Runtime Control Flow

### 1) Ingest

- `arxiv-watcher` polls `export.arxiv.org` query pages, compares against `(last_submission_prefix, last_arxiv_id)` cursor, forwards only new entries, then advances cursor.
- `rss-watcher` fetches each enabled feed, computes item key + published time ordering, forwards new items, then upserts feed cursor.

### 2) Dedup + Provenance Merge

- Computes weak identity hash from normalized title + year + first author surname.
- Looks up existing row by arXiv ID, DOI, or weak key.
- Existing row: append source tag to `sources`.
- New row: forward to keyword stage only.

### 3) Keyword Gate + Insert

- Title-only negative substring filter (`NEGATIVE_TITLE_KEYWORDS`).
- Positive keyword phrase match against lowercased title+abstract (`config/keywords.txt`).
- On pass: insert into `papers` and enqueue `PipelineWork`.

### 4) ML Classification

- Builds embedding on `title + "\n" + abstract`.
- Computes cosine similarity against positive and negative seed embeddings.
- Stores shadow metrics (telemetry in v1; not authoritative gate).
- Runs strict-JSON LLM classifier with retries.
- On success: writes `llm_status='ok'`, `llm_relevant`.
- First positive verdict stamps `relevant_at` for digest inclusion.
- On parse/timeouts/errors: keeps row pending for manual resolution.

### 5) Operator/API Loop

- `GET /api/digests`: grouped by rolling 12-hour windows over `relevant_at`.
- `GET /api/pending`: unresolved pending/failed rows.
- `POST /api/pending/resolve`: authoritative human decision.
- `POST /api/pending/retry`: requeue a row to `pipeline`.
- `POST /api/labels/main`: telemetry label; "irrelevant" also appends a negative seed BibTeX entry.
- Deep verify endpoint performs optional full-text PDF chunk voting via LLM.

## Design Characteristics

- **Local-first**: all services run in one compose stack.
- **Streaming decoupling**: stage boundaries are TCP message queues, not in-process calls.
- **Fail-soft classification**: uncertain model outputs route to human pending queue instead of discard.
- **Stateful observability**: shadow similarity is persisted even when not gating.
- **Feedback loop**: UI irrelevant labels can expand negative seed corpus automatically.
