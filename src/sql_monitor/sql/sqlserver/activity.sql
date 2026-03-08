WITH running AS
(
    SELECT
        SYSUTCDATETIME() AS capture_time,
        r.session_id,
        r.request_id,
        r.blocking_session_id,
        r.status,
        r.command,
        DB_NAME(r.database_id) AS database_name,
        s.login_name,
        s.host_name,
        s.program_name,
        r.start_time,
        r.total_elapsed_time AS total_elapsed_ms,
        r.cpu_time AS cpu_time_ms,
        r.logical_reads,
        r.reads,
        r.writes,
        r.wait_type,
        r.wait_time AS wait_time_ms,
        r.wait_resource,
        r.open_transaction_count,
        r.sql_handle,
        r.statement_start_offset,
        r.statement_end_offset
    FROM sys.dm_exec_requests r
    INNER JOIN sys.dm_exec_sessions s
        ON r.session_id = s.session_id
    WHERE r.session_id <> @@SPID
      AND s.is_user_process = 1
)
SELECT
    r.capture_time,
    r.session_id,
    r.request_id,
    r.blocking_session_id,
    r.status,
    r.command,
    r.database_name,
    r.login_name,
    r.host_name,
    r.program_name,
    r.start_time,
    r.total_elapsed_ms,
    r.cpu_time_ms,
    r.logical_reads,
    r.reads,
    r.writes,
    r.wait_type,
    r.wait_time_ms,
    r.wait_resource,
    r.open_transaction_count,
    OBJECT_SCHEMA_NAME(st.objectid, st.dbid) AS object_schema,
    OBJECT_NAME(st.objectid, st.dbid) AS object_name,
    CAST(st.text AS NVARCHAR(MAX)) AS full_sql_text,
    CAST(SUBSTRING(
        st.text,
        (r.statement_start_offset / 2) + 1,
        (
            (
                CASE
                    WHEN r.statement_end_offset = -1 THEN DATALENGTH(st.text)
                    ELSE r.statement_end_offset
                END - r.statement_start_offset
            ) / 2
        ) + 1
    ) AS NVARCHAR(MAX)) AS statement_text,
    CAST(ib.event_info AS NVARCHAR(MAX)) AS input_buffer
FROM running r
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
OUTER APPLY sys.dm_exec_input_buffer(r.session_id, r.request_id) ib
ORDER BY r.total_elapsed_ms DESC;
