INSERT INTO sqlserver_query_stats_log (
    capture_time,
    database_name,
    object_schema,
    object_name,
    execution_count,
    total_elapsed_ms,
    max_elapsed_ms,
    last_elapsed_ms,
    total_worker_ms,
    total_logical_reads,
    total_physical_reads,
    last_execution_time,
    query_hash,
    query_plan_hash,
    full_sql_text,
    statement_text
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
