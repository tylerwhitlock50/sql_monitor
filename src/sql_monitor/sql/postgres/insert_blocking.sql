INSERT INTO sqlserver_blocking_log (
    capture_time,
    waiting_session_id,
    waiting_request_id,
    blocking_session_id,
    blocking_request_id,
    wait_type,
    wait_duration_ms,
    resource_description,
    waiting_status,
    waiting_command,
    waiting_database_name,
    waiting_login_name,
    waiting_host_name,
    waiting_program_name,
    waiting_statement_text,
    blocking_status,
    blocking_command,
    blocking_database_name,
    blocking_login_name,
    blocking_host_name,
    blocking_program_name,
    blocking_statement_text
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
