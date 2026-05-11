# mr-cgdb

Localhost scientific paper router for computer graphics.

Pipeline: `arxiv/rss watchers -> dedup -> keyword -> pipeline(embed+shadow+llm) -> postgres -> ui/api`.

Keyword stage supports title-only negative filters via `NEGATIVE_TITLE_KEYWORDS` (default: `gaussian,splatt`).

## Run

1. Ensure Docker + Compose are installed.
2. Optionally edit:
   - `config/keywords.txt`
   - `config/seeds_positive.txt` (BibTeX entries, one or more `@...{...}` blocks)
   - `config/seeds_negative.txt` (BibTeX entries, auto-appended from UI irrelevant labels)
   - `RSS_FEEDS` in `docker-compose.yml`
3. Start:

```bash
docker compose up --build
```

4. Open:
   - UI: <http://localhost:8080>
   - (Optional) Ollama API is internal to Compose by default.

If models are not present yet, pull once after startup:

```bash
docker compose exec ollama ollama pull nomic-embed-text
docker compose exec ollama ollama pull llama3.2:1b
```

NVIDIA GPU forwarding for Ollama is enabled in `docker-compose.yml` (`gpus: all`).

## API

- `GET /api/digests?lookbackHours=72`
- `GET /api/pending`
- `POST /api/labels/main` body: `{ "paperId": 123, "label": "irrelevant" }`
- `POST /api/pending/resolve` body: `{ "paperId": 123, "relevant": true }`
- `POST /api/pending/retry` body: `{ "paperId": 123 }`
- `POST /api/scan/arxiv-range` body: `{ "from": "2026-01-01", "to": "2026-01-31" }`
- `POST /api/scan/clear` body: `{}`
- `POST /api/papers/{id}/deep-verify` body: `{}`
  - Runs chunked full-text verification (when PDF text is available) and returns concise reasoning.

## Notes

- Digest uses rolling 12-hour groups.
- Main-list labels are telemetry only.
- Pending labels are authoritative and move paper out of pending.
- Embedding shadow metrics are logged but do not gate in v1.
- Positive/negative seed sets are separate; UI irrelevant labels append to negative seed file.
