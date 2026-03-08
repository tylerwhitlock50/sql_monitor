CREATE TABLE IF NOT EXISTS sqlserver_query_stats_log (
    id BIGSERIAL PRIMARY KEY,
    capture_time TIMESTAMPTZ,
    database_name VARCHAR(128),
    object_schema VARCHAR(128),
    object_name VARCHAR(256),
    execution_count BIGINT,
    total_elapsed_ms BIGINT,
    max_elapsed_ms BIGINT,
    last_elapsed_ms BIGINT,
    total_worker_ms BIGINT,
    total_logical_reads BIGINT,
    total_physical_reads BIGINT,
    last_execution_time TIMESTAMPTZ,
    query_hash VARCHAR(34),
    query_plan_hash VARCHAR(34),
    full_sql_text TEXT,
    statement_text TEXT
);
