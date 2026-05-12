ALTER TABLE profiles
  ADD COLUMN IF NOT EXISTS last_accessed_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS profiles_user_last_accessed ON profiles (user_id, last_accessed_at DESC);
