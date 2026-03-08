INSERT INTO sqlserver_activity_log (
    capture_time,
    session_id,
    request_id,
    blocking_session_id,
    status,
    command,
    database_name,
    login_name,
    host_name,
    program_name,
    start_time,
    total_elapsed_ms,
    cpu_time_ms,
    logical_reads,
    reads,
    writes,
    wait_type,
    wait_time_ms,
    wait_resource,
    open_transaction_count,
    object_schema,
    object_name,
    full_sql_text,
    statement_text,
    input_buffer
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
