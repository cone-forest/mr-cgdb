# Implementation Notes

## Repository Layout

- `cmd/*`: entrypoints for independent long-running services.
- `internal/*`: reusable domain/infrastructure modules.
- `migrations/001_init.sql`: top-level migration reference.
- `internal/store/sql/001_init.sql`: embedded migration used at startup.
- `config/*`: lexical keywords and seed corpora.
- `web/index.html`: single-page UI served by API process.

## Process Model

Each command process is intentionally simple:

- Parse env configuration.
- Create PG pool via `store.New()`.
- Open TCP listener or outbound connection(s).
- Loop forever until signal cancellation.

There is no orchestrator process inside Go; orchestration is externalized to Compose.

## Storage Layer (`internal/store`)

`store.New` performs:

1. `pgxpool` creation.
2. health `Ping`.
3. embedded SQL migration execution (`go:embed sql/*.sql`).

Core data operations:

- Identity lookup: `FindIDByIdentity`.
- Source provenance merge: `MergeSource`.
- Candidate insertion after keyword pass: `InsertAfterKeyword`.
- LLM/shadow updates: `UpdateShadowResult`, `SetLLMOK`, `SetLLMPending`.
- Human resolution and retry: `ResolvePending`, `RequeueLLM`.
- Label logging: `AddHandLabel`.
- Digest/pending queries: `ListDigestGroups`, `ListPending`.
- Cursor management: `Get/SetArxivCursor`, `Get/UpsertRSSCursor`.
- Maintenance reset: `ClearScanData`.
- Deep verify persistence: `SetDeepVerifyResult`.

## Identity + Dedup

`internal/identity` implements weak keys:

- Normalize title using NFKC + lowercase + whitespace collapse.
- Extract first author surname.
- Build hash of `normalized_title|year|surname` using SHA-256.

Dedup semantics:

- Strong IDs (`arxiv_id`, `doi`) are preferred.
- Weak key fills gaps when strong IDs are unavailable.
- Duplicate sightings enrich provenance (`sources`) rather than create rows.

## Watchers

### arXiv watcher

- Uses `internal/arxiv.SearchPage`.
- Parses Atom feed.
- Keeps a composite "after" cursor `(updated_time, arxiv_id)` for strict progression.
- Forwards as `IngestItem` to dedup.

### RSS watcher

- Uses `gofeed` parser via `internal/rssx.Fetch`.
- Stable item ordering key:
  - GUID first,
  - else canonicalized link,
  - else title fallback.
- Cursor comparison (`AfterCursor`) handles missing timestamps.
- Feed list comes from `rss_feeds` table (optionally bootstrapped from env).

## Keyword Stage

Implemented in `cmd/keyword/main.go` + `internal/keywords`.

Behavior:

- Negative title blocklist first (`NEGATIVE_TITLE_KEYWORDS`).
- Positive keyword phrase match second (`config/keywords.txt`).
- Match type: lowercase substring (no stemming/tokenization).
- Only passing items are persisted and queued for pipeline processing.

This stage intentionally avoids expensive inference for obvious misses.

## Pipeline Stage (Embedding + LLM)

Implemented in `internal/pipeline/processor.go`.

### Seed loading

- Reads positive and negative BibTeX files.
- `internal/seed` parses entries and builds text payload from title/abstract (+ fallback metadata).
- Embedding model is called per seed at process start.

### Shadow scoring

- Computes cosine similarity between candidate embedding and each seed embedding.
- Persists:
  - `shadow_max_sim` over positive seeds,
  - `shadow_argmax_seed`,
  - optional `shadow_would_pass` based on threshold and negative-bias margin.
- Shadow output is persisted telemetry in current v1, not an exclusion gate.

### LLM relevance

- Calls Ollama chat with strict JSON schema: `{"relevant": true|false}`.
- Uses low-temperature deterministic setting.
- Retries parse/model failures (`LLM_MAX_ATTEMPTS`).
- Auto-pulls model once on first failure path.
- Timeout path marks row pending with error text.

## API Layer

`cmd/api/main.go` exposes:

- Health: `/api/health`
- Output queues:
  - `/api/digests`
  - `/api/pending`
- Operator actions:
  - `/api/labels/main`
  - `/api/pending/resolve`
  - `/api/pending/retry`
  - `/api/scan/arxiv-range`
  - `/api/scan/clear`
  - `/api/papers/{id}/deep-verify`
- RSS feed management:
  - `GET/POST /api/rss-feeds`

Notable implementation details:

- Main-list "irrelevant" labels append BibTeX to negative seeds file (deduped by title).
- Pending resolve is authoritative: it writes human + LLM fields and can set `relevant_at`.
- Retry endpoint resets LLM fields then sends a `PipelineWork` message.

## Deep Verify Path

Optional manual action (`/api/papers/{id}/deep-verify`):

1. Resolve PDF URL from arXiv ID or paper URL.
2. Download with hard byte cap.
3. Extract text via `rsc.io/pdf`.
4. Chunk text and run per-chunk LLM verdict (`useful/reason`).
5. Majority vote across chunks; store result in `deep_verify_*`.

If PDF is missing/too large, the route falls back to abstract-driven verification with explicit note.

## Reliability and Failure Semantics

- TCP dial retry loops in `internal/netx`.
- Stage restarts are safe due to idempotent DB checks + unique identity indexes.
- Failures in model parsing/inference do not drop rows; they become pending work.
- Cursor-based watchers avoid repeated ingestion while still allowing backfill scans via API.

## Configuration Surface

Important env vars by concern:

- Networking:
  - `LISTEN`, `DEDUP_ADDR`, `KEYWORD_ADDR`, `PIPELINE_ADDR`
- Classification:
  - `KEYWORDS_FILE`, `NEGATIVE_TITLE_KEYWORDS`
  - `SEEDS_POSITIVE_FILE`, `SEEDS_NEGATIVE_FILE`
  - `SHADOW_THRESHOLD`, `SHADOW_NEGATIVE_BIAS`
  - `LLM_MAX_ATTEMPTS`
- Models:
  - `OLLAMA_BASE_URL`, `EMBED_MODEL`, `CHAT_MODEL`, `DEEP_VERIFY_MODEL`
- Polling:
  - `ARXIV_QUERY`, `ARXIV_POLL`, `RSS_POLL`

## Current Architectural Trade-offs

- **Pros**
  - Simple stage isolation and restartability.
  - Fast early-reject gates reduce expensive inference.
  - Human-in-the-loop path is explicit and auditable.
- **Cons**
  - In-memory TCP fanout means no durable queue between stages.
  - Keyword matcher may overmatch generic terms like "lod".
  - Seed embeddings load at startup only; runtime updates require restart.
