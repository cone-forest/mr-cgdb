-- Phase 1 hard cutover: remove legacy relevance/config state while keeping papers and source cursors.
DROP TABLE IF EXISTS hand_labels;
DROP TABLE IF EXISTS system_config;

UPDATE papers
SET llm_relevant = NULL,
    llm_status = 'pending',
    llm_raw = NULL,
    last_llm_error = NULL,
    human_resolved = FALSE,
    human_relevant = NULL,
    hand_label_main = NULL,
    relevant_at = NULL;

CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  is_admin BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sessions (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  session_token_hash TEXT NOT NULL UNIQUE,
  csrf_token TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sessions_user_id ON sessions (user_id);
CREATE INDEX IF NOT EXISTS sessions_expires_at ON sessions (expires_at);

CREATE TABLE IF NOT EXISTS profiles (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  slug TEXT NOT NULL,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  visibility TEXT NOT NULL CHECK (visibility IN ('public','private')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, slug)
);
CREATE INDEX IF NOT EXISTS profiles_visibility_updated_at ON profiles (visibility, updated_at DESC);

CREATE TABLE IF NOT EXISTS profile_configs (
  profile_id BIGINT PRIMARY KEY REFERENCES profiles(id) ON DELETE CASCADE,
  positive_keywords TEXT[] NOT NULL DEFAULT '{}',
  negative_title_keywords TEXT[] NOT NULL DEFAULT '{}',
  llm_prompt TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS profile_sources (
  id BIGSERIAL PRIMARY KEY,
  profile_id BIGINT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  source_type TEXT NOT NULL CHECK (source_type IN ('rss', 'arxiv_query')),
  source_value TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS profile_sources_profile_id ON profile_sources (profile_id);

CREATE TABLE IF NOT EXISTS profile_paper_likes (
  profile_id BIGINT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  paper_id BIGINT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
  note TEXT NOT NULL DEFAULT '',
  tags TEXT[] NOT NULL DEFAULT '{}',
  liked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (profile_id, paper_id)
);
CREATE INDEX IF NOT EXISTS profile_paper_likes_liked_at ON profile_paper_likes (liked_at DESC);
CREATE INDEX IF NOT EXISTS profile_paper_likes_paper_id ON profile_paper_likes (paper_id);

CREATE TABLE IF NOT EXISTS paper_files (
  paper_id BIGINT PRIMARY KEY REFERENCES papers(id) ON DELETE CASCADE,
  source_url TEXT NOT NULL DEFAULT '',
  local_path TEXT,
  bytes BIGINT,
  status TEXT NOT NULL DEFAULT 'missing' CHECK (status IN ('missing','queued','ready','failed')),
  last_error TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS paper_files_status ON paper_files (status);

CREATE TABLE IF NOT EXISTS jobs (
  id BIGSERIAL PRIMARY KEY,
  kind TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending','running','failed','done')),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS jobs_status_kind ON jobs (status, kind, created_at);
