SELECT TOP ({top_n})
    SYSUTCDATETIME() AS capture_time,
    DB_NAME(st.dbid) AS database_name,
    OBJECT_SCHEMA_NAME(st.objectid, st.dbid) AS object_schema,
    OBJECT_NAME(st.objectid, st.dbid) AS object_name,
    qs.execution_count,
    CAST(qs.total_elapsed_time / 1000 AS BIGINT) AS total_elapsed_ms,
    CAST(qs.max_elapsed_time / 1000 AS BIGINT) AS max_elapsed_ms,
    CAST(qs.last_elapsed_time / 1000 AS BIGINT) AS last_elapsed_ms,
    CAST(qs.total_worker_time / 1000 AS BIGINT) AS total_worker_ms,
    qs.total_logical_reads,
    qs.total_physical_reads,
    qs.last_execution_time,
    CONVERT(VARCHAR(34), qs.query_hash, 1) AS query_hash_hex,
    CONVERT(VARCHAR(34), qs.query_plan_hash, 1) AS query_plan_hash_hex,
    CAST(st.text AS NVARCHAR(MAX)) AS full_sql_text,
    CAST(SUBSTRING(
        st.text,
        (qs.statement_start_offset / 2) + 1,
        (
            (
                CASE
                    WHEN qs.statement_end_offset = -1 THEN DATALENGTH(st.text)
                    ELSE qs.statement_end_offset
                END - qs.statement_start_offset
            ) / 2
        ) + 1
    ) AS NVARCHAR(MAX)) AS statement_text
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
WHERE st.dbid IS NOT NULL
ORDER BY qs.max_elapsed_time DESC;
