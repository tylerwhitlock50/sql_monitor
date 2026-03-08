SELECT
    SYSUTCDATETIME() AS capture_time,
    wt.session_id AS waiting_session_id,
    wr.request_id AS waiting_request_id,
    wt.blocking_session_id,
    br.request_id AS blocking_request_id,
    wt.wait_type,
    wt.wait_duration_ms,
    wt.resource_description,
    wr.status AS waiting_status,
    wr.command AS waiting_command,
    DB_NAME(wr.database_id) AS waiting_database_name,
    ws.login_name AS waiting_login_name,
    ws.host_name AS waiting_host_name,
    ws.program_name AS waiting_program_name,
    CAST(
        CASE
            WHEN wr.session_id IS NULL THEN NULL
            ELSE SUBSTRING(
                wst.text,
                (wr.statement_start_offset / 2) + 1,
                (
                    (
                        CASE
                            WHEN wr.statement_end_offset = -1 THEN DATALENGTH(wst.text)
                            ELSE wr.statement_end_offset
                        END - wr.statement_start_offset
                    ) / 2
                ) + 1
            )
        END
    AS NVARCHAR(MAX)) AS waiting_statement_text,
    br.status AS blocking_status,
    br.command AS blocking_command,
    DB_NAME(br.database_id) AS blocking_database_name,
    bs.login_name AS blocking_login_name,
    bs.host_name AS blocking_host_name,
    bs.program_name AS blocking_program_name,
    CAST(
        CASE
            WHEN br.session_id IS NULL THEN NULL
            ELSE SUBSTRING(
                bst.text,
                (br.statement_start_offset / 2) + 1,
                (
                    (
                        CASE
                            WHEN br.statement_end_offset = -1 THEN DATALENGTH(bst.text)
                            ELSE br.statement_end_offset
                        END - br.statement_start_offset
                    ) / 2
                ) + 1
            )
        END
    AS NVARCHAR(MAX)) AS blocking_statement_text
FROM sys.dm_os_waiting_tasks wt
LEFT JOIN sys.dm_exec_requests wr
    ON wt.session_id = wr.session_id
LEFT JOIN sys.dm_exec_sessions ws
    ON wt.session_id = ws.session_id
LEFT JOIN sys.dm_exec_requests br
    ON wt.blocking_session_id = br.session_id
LEFT JOIN sys.dm_exec_sessions bs
    ON wt.blocking_session_id = bs.session_id
OUTER APPLY sys.dm_exec_sql_text(wr.sql_handle) wst
OUTER APPLY sys.dm_exec_sql_text(br.sql_handle) bst
WHERE wt.blocking_session_id > 0
  AND ws.is_user_process = 1
ORDER BY wt.wait_duration_ms DESC;
