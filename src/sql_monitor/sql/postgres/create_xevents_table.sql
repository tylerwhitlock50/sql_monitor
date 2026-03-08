CREATE TABLE IF NOT EXISTS sqlserver_xevent_log (
    id BIGSERIAL PRIMARY KEY,
    capture_time TIMESTAMPTZ,
    event_time TIMESTAMPTZ,
    event_name VARCHAR(128),
    session_id INT,
    database_name VARCHAR(128),
    client_app_name VARCHAR(512),
    client_hostname VARCHAR(256),
    username VARCHAR(256),
    error_number INT,
    severity INT,
    state INT,
    duration_ms BIGINT,
    message TEXT,
    sql_text TEXT,
    event_xml TEXT,
    event_hash VARCHAR(64) UNIQUE NOT NULL
);
