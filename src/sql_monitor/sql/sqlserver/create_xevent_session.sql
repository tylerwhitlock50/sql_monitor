CREATE EVENT SESSION [{session_name}]
ON SERVER
ADD EVENT sqlserver.error_reported(
    ACTION(
        sqlserver.client_app_name,
        sqlserver.client_hostname,
        sqlserver.database_name,
        sqlserver.session_id,
        sqlserver.sql_text,
        sqlserver.username
    )
    WHERE ([severity] >= (11) OR [error_number] = (1205) OR [error_number] = (1222))
),
ADD EVENT sqlserver.lock_timeout_greater_than_0(
    ACTION(
        sqlserver.client_app_name,
        sqlserver.client_hostname,
        sqlserver.database_name,
        sqlserver.session_id,
        sqlserver.sql_text,
        sqlserver.username
    )
),
ADD EVENT sqlserver.xml_deadlock_report(
    ACTION(
        sqlserver.client_app_name,
        sqlserver.client_hostname,
        sqlserver.database_name,
        sqlserver.session_id,
        sqlserver.sql_text,
        sqlserver.username
    )
)
ADD TARGET package0.ring_buffer(SET max_memory = (10240), max_events_limit = (5000))
WITH (
    MAX_MEMORY = 4096 KB,
    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
    MAX_DISPATCH_LATENCY = 5 SECONDS,
    TRACK_CAUSALITY = OFF,
    STARTUP_STATE = ON
);
