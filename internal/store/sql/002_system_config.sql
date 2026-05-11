CREATE TABLE IF NOT EXISTS system_config (
  id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  domain_name TEXT NOT NULL DEFAULT '',
  positive_keywords TEXT[] NOT NULL DEFAULT '{}',
  negative_title_keywords TEXT[] NOT NULL DEFAULT '{}',
  llm_system_prompt TEXT NOT NULL DEFAULT '',
  arxiv_query TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO system_config (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
