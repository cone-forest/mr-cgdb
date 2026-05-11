# Filtering Strategy and Hierarchy

## Goal

Route high-signal papers about Cluster/Meshlet/LOD in computer graphics into digest views, while keeping uncertain cases reviewable instead of silently dropping them.

The system uses a layered filter stack from cheapest to most expensive.

## Decision Hierarchy (Top to Bottom)

## Stage 0: Source Cursor Gate (Watcher-level)

Before semantic filtering, watchers suppress already-processed source records:

- arXiv: keep only entries strictly after stored `(submission_time, arxiv_id)` cursor.
- RSS: keep only items after `(published_time, item_key)` per feed.

This is not semantic relevance filtering; it is incremental ingestion control.

## Stage 1: Identity Dedup Gate (`dedup`)

Input: raw `IngestItem` from watchers.

Identity precedence:

1. `arxiv_id`
2. `doi`
3. weak hash(`normalized_title`, `year`, `first_author_surname`)

Outcomes:

- **Known paper**: no semantic reclassification; merge source into `sources`.
- **Unknown paper**: forward to keyword stage.

This gate avoids duplicate model calls and keeps provenance merged.

## Stage 2: Lexical Filter Gate (`keyword`)

This is the first semantic gate and the only hard pre-insert filter.

Order matters:

1. **Negative title keyword denylist**
   - Config: `NEGATIVE_TITLE_KEYWORDS`
   - Current default: `gaussian,splatt`
   - Match: lowercase substring on title only.
2. **Positive keyword passlist**
   - Config file: `config/keywords.txt`
   - Match: any phrase as lowercase substring in `title + abstract`.

Outcome matrix:

- denylist hit -> reject immediately.
- no denylist hit and no positive phrase match -> reject.
- positive phrase match -> insert into `papers` and enqueue pipeline work.

Consequence: only keyword-passing papers exist in `papers` (v1 design intent).

## Stage 3: Embedding Shadow Scoring (`pipeline`, non-authoritative)

For each inserted paper:

- Build embedding for `title + abstract`.
- Compare against positive and negative seed embedding sets.

Persisted metrics:

- `shadow_max_sim` (best positive similarity)
- `shadow_argmax_seed` (best matching positive seed ID)
- `shadow_would_pass`:
  - true when `shadow_max_sim >= SHADOW_THRESHOLD`
  - and (if negative seeds exist) `(shadow_max_sim - neg_max_sim) >= SHADOW_NEGATIVE_BIAS`

Important: in current v1 behavior this stage is telemetry only; it does **not** remove papers.

## Stage 4: LLM Boolean Classifier (`pipeline`, authoritative machine gate)

Prompt intent: strict relevance to cluster/hierarchical/LOD computer graphics research.

Contract:

- Expected JSON response: `{"relevant": true|false}`
- `llm_status='ok'` when parse succeeds.
- `llm_status='pending'` on timeout/parse/model failures.

Outcomes:

- `relevant=true`: set `llm_relevant=true`; stamp `relevant_at` if first eligibility.
- `relevant=false`: set `llm_relevant=false`; not digest-eligible.
- failure/pending: send to pending review queue.

This is the primary automatic relevance decision in production flow.

## Stage 5: Human Review Layer (Operator Overrides)

Two distinct label contexts:

- `main` labels (telemetry/training signal)
  - stored in `hand_labels`
  - latest copied to `hand_label_main`
  - does not directly alter digest eligibility
- `pending` resolve labels (authoritative)
  - marks `human_resolved=true`
  - writes `human_relevant` and aligned `llm_relevant`
  - can set `relevant_at` when resolved relevant

So unresolved model ambiguity is converted into explicit human decisions.

## Stage 6: Optional Deep Verify Layer (On-demand evidence check)

Triggered manually per paper:

- Download and parse PDF text (or fallback abstract if unavailable).
- Chunked LLM voting returns `useful` + reason.
- Store evidence in `deep_verify_*` columns.

Deep verify currently supports human judgment and future tuning; it does not automatically rewire base digest gating.

## End-State Buckets

A paper eventually lands in one of these operational buckets:

1. **Dropped early** (cursor/dedup/keyword) -> not in `papers`.
2. **Machine classified irrelevant** (`llm_status='ok'`, `llm_relevant=false`) -> kept for audit, not digested.
3. **Machine classified relevant** (`llm_relevant=true`) -> digest-eligible via `relevant_at`.
4. **Pending review** (`llm_status='pending'` and unresolved) -> shown in pending UI.
5. **Human-resolved** (`human_resolved=true`) -> authoritative final state for pending cases.

## Feedback Loops and Adaptive Behavior

## Negative seed expansion from user labels

When user labels a main-list paper as `"irrelevant"`:

- API appends a BibTeX entry to `seeds_negative.txt` (unless same title already exists).
- On next pipeline restart, this seed contributes to negative embedding comparisons.

Effect: shadow telemetry becomes better aligned with observed false positives over time.

## Manual retry loop

Pending item retry:

- clears LLM error fields
- requeues `paper_id` into pipeline

Effect: transient model/infra failures can be resolved without reinsertion.

## Why This Hierarchy Works

- **Cost-aware**: cheap string filters run before embeddings/LLM.
- **Safety-aware**: uncertain ML outcomes are deferred to human review.
- **Traceable**: each gate writes explicit state into DB.
- **Adaptable**: human labels feed negative seed corpus and future tuning.

## Tuning Levers (By Layer)

- Stage 2 lexical precision/recall:
  - edit `config/keywords.txt`
  - adjust `NEGATIVE_TITLE_KEYWORDS`
- Stage 3 shadow aggressiveness:
  - `SHADOW_THRESHOLD`
  - `SHADOW_NEGATIVE_BIAS`
  - seed corpus quality (`seeds_positive.txt`, `seeds_negative.txt`)
- Stage 4 LLM robustness:
  - `CHAT_MODEL`
  - `LLM_MAX_ATTEMPTS`
  - system prompt content in `internal/pipeline/processor.go`
- Stage 6 deep verify depth/cost:
  - `DEEP_VERIFY_MAX_CHARS`
  - `DEEP_VERIFY_CHUNK_CHARS`
  - `DEEP_VERIFY_MAX_CHUNKS`
