CREATE TABLE IF NOT EXISTS capture_attempt_log (
    id SERIAL PRIMARY KEY,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows_captured INT NOT NULL,
    rows_inserted INT NOT NULL,
    rows_activity INT NOT NULL DEFAULT 0,
    rows_blocking INT NOT NULL DEFAULT 0,
    rows_query_stats INT NOT NULL DEFAULT 0,
    rows_health_counters INT NOT NULL DEFAULT 0,
    rows_xevents INT NOT NULL DEFAULT 0,
    status VARCHAR(30) NOT NULL,
    error_message TEXT
);
