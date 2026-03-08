ALTER TABLE capture_attempt_log ADD COLUMN IF NOT EXISTS rows_activity INT NOT NULL DEFAULT 0;
ALTER TABLE capture_attempt_log ADD COLUMN IF NOT EXISTS rows_blocking INT NOT NULL DEFAULT 0;
ALTER TABLE capture_attempt_log ADD COLUMN IF NOT EXISTS rows_query_stats INT NOT NULL DEFAULT 0;
ALTER TABLE capture_attempt_log ADD COLUMN IF NOT EXISTS rows_health_counters INT NOT NULL DEFAULT 0;
ALTER TABLE capture_attempt_log ADD COLUMN IF NOT EXISTS rows_xevents INT NOT NULL DEFAULT 0;
