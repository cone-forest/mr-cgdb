CREATE TABLE IF NOT EXISTS profile_paper_analysis (
  profile_id BIGINT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  paper_id BIGINT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
  keyword_pass BOOLEAN NOT NULL DEFAULT FALSE,
  keyword_hits TEXT[] NOT NULL DEFAULT '{}',
  llm_relevant BOOLEAN,
  llm_raw TEXT,
  shadow_would_auto_download BOOLEAN NOT NULL DEFAULT FALSE,
  shadow_score DOUBLE PRECISION NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (profile_id, paper_id)
);
CREATE INDEX IF NOT EXISTS profile_paper_analysis_profile_updated ON profile_paper_analysis (profile_id, updated_at DESC);
