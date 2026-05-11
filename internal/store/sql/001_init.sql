-- Papers exist only after passing the global keyword check (v1 design).
CREATE TABLE IF NOT EXISTS papers (
  id            BIGSERIAL PRIMARY KEY,
  arxiv_id      TEXT,
  doi           TEXT,
  weak_key      TEXT,
  url           TEXT,
  title         TEXT NOT NULL,
  year          INT,
  first_author  TEXT,
  abstract      TEXT,
  source        TEXT NOT NULL, -- "arxiv" | "rss:feed_id" etc.
  sources       JSONB NOT NULL DEFAULT '[]',

  -- embedding + shadow (v1: shadow is log only, not a gate)
  embedding     DOUBLE PRECISION[],
  shadow_max_sim   DOUBLE PRECISION,
  shadow_would_pass BOOLEAN,
  shadow_argmax_seed TEXT,

  -- LLM: single bool when ok; pending/failed keep row for manual review
  llm_relevant  BOOLEAN,
  llm_status    TEXT NOT NULL DEFAULT 'pending' CHECK (llm_status IN ('ok', 'pending', 'failed')),
  llm_raw       TEXT,
  last_llm_error TEXT,
  deep_verify_useful BOOLEAN,
  deep_verify_reason TEXT,
  deep_verify_raw TEXT,
  deep_verify_at TIMESTAMPTZ,

  -- Human: main-list labels are telemetry; pending resolve is authoritative
  human_resolved      BOOLEAN NOT NULL DEFAULT FALSE,
  human_relevant      BOOLEAN,
  -- training label on main (disagree with LLM), does not change digest eligibility
  hand_label_main     TEXT, -- "relevant" | "irrelevant" | "unsure" or null

  relevant_at  TIMESTAMPTZ, -- set when item becomes eligible for 12h digest

  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS papers_arxiv_id_key ON papers (arxiv_id) WHERE arxiv_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS papers_doi_key ON papers (doi) WHERE doi IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS papers_weak_key_key ON papers (weak_key) WHERE weak_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS papers_relevant_at ON papers (relevant_at);
CREATE INDEX IF NOT EXISTS papers_llm_status ON papers (llm_status);
ALTER TABLE papers ADD COLUMN IF NOT EXISTS url TEXT;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS deep_verify_useful BOOLEAN;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS deep_verify_reason TEXT;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS deep_verify_raw TEXT;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS deep_verify_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS papers_deep_verify_at ON papers (deep_verify_at);

CREATE TABLE IF NOT EXISTS arxiv_cursor (
  id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  last_submission_prefix TEXT,
  last_arxiv_id TEXT
);

INSERT INTO arxiv_cursor (id) VALUES (1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS rss_cursors (
  feed_id   TEXT NOT NULL,
  last_ts   TIMESTAMPTZ,
  last_key  TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (feed_id)
);

CREATE TABLE IF NOT EXISTS rss_feeds (
  id BIGSERIAL PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rss_feeds_enabled ON rss_feeds (enabled);
INSERT INTO rss_feeds (url) VALUES ('https://export.arxiv.org/rss/cs.GR') ON CONFLICT (url) DO NOTHING;

CREATE TABLE IF NOT EXISTS hand_labels (
  id BIGSERIAL PRIMARY KEY,
  paper_id BIGINT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
  context TEXT NOT NULL CHECK (context IN ('main', 'pending')),
  label  TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS hand_labels_paper_id ON hand_labels (paper_id);
