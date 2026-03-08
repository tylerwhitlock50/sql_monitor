INSERT INTO sqlserver_xevent_log (
    capture_time,
    event_time,
    event_name,
    session_id,
    database_name,
    client_app_name,
    client_hostname,
    username,
    error_number,
    severity,
    state,
    duration_ms,
    message,
    sql_text,
    event_xml,
    event_hash
)
VALUES %s
ON CONFLICT (event_hash) DO NOTHING
