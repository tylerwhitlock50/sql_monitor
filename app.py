import logging
import os
import re
import time
from hashlib import sha256
from datetime import datetime, timezone
from urllib.parse import parse_qs, unquote, urlparse

import psycopg2
from psycopg2.extras import execute_batch, execute_values
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

SQLSERVER_ACTIVITY_QUERY = """
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
"""

SQLSERVER_BLOCKING_QUERY = """
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
"""

SQLSERVER_QUERY_STATS_QUERY_TEMPLATE = """
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
"""

SQLSERVER_HEALTH_COUNTERS_QUERY = """
SELECT
    SYSUTCDATETIME() AS capture_time,
    object_name,
    counter_name,
    instance_name,
    cntr_type,
    cntr_value
FROM sys.dm_os_performance_counters
WHERE counter_name IN (
    'Number of Deadlocks/sec',
    'Lock Timeouts/sec',
    'Lock Waits/sec'
)
ORDER BY counter_name, instance_name;
"""

SQLSERVER_XEVENTS_QUERY = """
WITH session_targets AS (
    SELECT CAST(st.target_data AS XML) AS target_data
    FROM sys.dm_xe_sessions s
    INNER JOIN sys.dm_xe_session_targets st
        ON s.address = st.event_session_address
    WHERE s.name = :session_name
      AND st.target_name = 'ring_buffer'
)
SELECT
    SYSUTCDATETIME() AS capture_time,
    evt.value('@timestamp', 'datetime2') AS event_time,
    evt.value('@name', 'nvarchar(128)') AS event_name,
    evt.value('(action[@name="session_id"]/value)[1]', 'int') AS session_id,
    evt.value('(action[@name="database_name"]/value)[1]', 'nvarchar(128)') AS database_name,
    evt.value('(action[@name="client_app_name"]/value)[1]', 'nvarchar(512)') AS client_app_name,
    evt.value('(action[@name="client_hostname"]/value)[1]', 'nvarchar(256)') AS client_hostname,
    evt.value('(action[@name="username"]/value)[1]', 'nvarchar(256)') AS username,
    evt.value('(data[@name="error_number"]/value)[1]', 'int') AS error_number,
    evt.value('(data[@name="severity"]/value)[1]', 'int') AS severity,
    evt.value('(data[@name="state"]/value)[1]', 'int') AS state,
    evt.value('(data[@name="duration"]/value)[1]', 'bigint') AS duration_ms,
    evt.value('(data[@name="message"]/value)[1]', 'nvarchar(4000)') AS message,
    evt.value('(action[@name="sql_text"]/value)[1]', 'nvarchar(4000)') AS sql_text,
    CAST(evt.query('.') AS NVARCHAR(MAX)) AS event_xml
FROM session_targets st
CROSS APPLY st.target_data.nodes('/RingBufferTarget/event') AS x(evt)
ORDER BY event_time DESC;
"""

SQLSERVER_XEVENT_SESSION_EXISTS_QUERY = """
SELECT 1
FROM sys.server_event_sessions
WHERE name = :session_name;
"""

SQLSERVER_XEVENT_SESSION_RUNNING_QUERY = """
SELECT 1
FROM sys.dm_xe_sessions
WHERE name = :session_name;
"""

INSERT_ACTIVITY_SQL = """
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
"""

INSERT_BLOCKING_SQL = """
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
"""

INSERT_QUERY_STATS_SQL = """
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
"""

INSERT_HEALTH_COUNTERS_SQL = """
INSERT INTO sqlserver_health_counters_log (
    capture_time,
    object_name,
    counter_name,
    instance_name,
    cntr_type,
    cntr_value
) VALUES (
    %s, %s, %s, %s, %s, %s
)
"""

INSERT_XEVENTS_SQL = """
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
"""

CREATE_ACTIVITY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sqlserver_activity_log (
    id BIGSERIAL PRIMARY KEY,
    capture_time TIMESTAMPTZ,
    session_id INT,
    request_id INT,
    blocking_session_id INT,
    status VARCHAR(60),
    command VARCHAR(60),
    database_name VARCHAR(128),
    login_name VARCHAR(128),
    host_name VARCHAR(128),
    program_name VARCHAR(512),
    start_time TIMESTAMPTZ,
    total_elapsed_ms BIGINT,
    cpu_time_ms BIGINT,
    logical_reads BIGINT,
    reads BIGINT,
    writes BIGINT,
    wait_type VARCHAR(60),
    wait_time_ms BIGINT,
    wait_resource VARCHAR(256),
    open_transaction_count INT,
    object_schema VARCHAR(128),
    object_name VARCHAR(256),
    full_sql_text TEXT,
    statement_text TEXT,
    input_buffer TEXT
);
"""

CREATE_BLOCKING_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sqlserver_blocking_log (
    id BIGSERIAL PRIMARY KEY,
    capture_time TIMESTAMPTZ,
    waiting_session_id INT,
    waiting_request_id INT,
    blocking_session_id INT,
    blocking_request_id INT,
    wait_type VARCHAR(120),
    wait_duration_ms BIGINT,
    resource_description TEXT,
    waiting_status VARCHAR(60),
    waiting_command VARCHAR(60),
    waiting_database_name VARCHAR(128),
    waiting_login_name VARCHAR(128),
    waiting_host_name VARCHAR(128),
    waiting_program_name VARCHAR(512),
    waiting_statement_text TEXT,
    blocking_status VARCHAR(60),
    blocking_command VARCHAR(60),
    blocking_database_name VARCHAR(128),
    blocking_login_name VARCHAR(128),
    blocking_host_name VARCHAR(128),
    blocking_program_name VARCHAR(512),
    blocking_statement_text TEXT
);
"""

CREATE_QUERY_STATS_TABLE_SQL = """
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
"""

CREATE_HEALTH_COUNTERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sqlserver_health_counters_log (
    id BIGSERIAL PRIMARY KEY,
    capture_time TIMESTAMPTZ,
    object_name VARCHAR(128),
    counter_name VARCHAR(128),
    instance_name VARCHAR(128),
    cntr_type INT,
    cntr_value BIGINT
);
"""

CREATE_XEVENTS_TABLE_SQL = """
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
"""

CREATE_ATTEMPT_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS capture_attempt_log (
    id SERIAL PRIMARY KEY,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows_captured INT NOT NULL,
    rows_inserted INT NOT NULL,
    rows_activity INT NOT NULL DEFAULT 0,
    rows_blocking INT NOT NULL DEFAULT 0,
    rows_query_stats INT NOT NULL DEFAULT 0,
    rows_health_counters INT NOT NULL DEFAULT 0,
    rows_xevents INT NOT NULL DEFAULT 0,
    status VARCHAR(30) NOT NULL,
    error_message TEXT
);
"""

ALTER_ATTEMPT_LOG_SQL = [
    "ALTER TABLE capture_attempt_log ADD COLUMN IF NOT EXISTS rows_activity INT NOT NULL DEFAULT 0;",
    "ALTER TABLE capture_attempt_log ADD COLUMN IF NOT EXISTS rows_blocking INT NOT NULL DEFAULT 0;",
    "ALTER TABLE capture_attempt_log ADD COLUMN IF NOT EXISTS rows_query_stats INT NOT NULL DEFAULT 0;",
    "ALTER TABLE capture_attempt_log ADD COLUMN IF NOT EXISTS rows_health_counters INT NOT NULL DEFAULT 0;",
    "ALTER TABLE capture_attempt_log ADD COLUMN IF NOT EXISTS rows_xevents INT NOT NULL DEFAULT 0;",
]

INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_activity_capture_time ON sqlserver_activity_log (capture_time DESC);",
    "CREATE INDEX IF NOT EXISTS idx_activity_blocking_session ON sqlserver_activity_log (blocking_session_id);",
    "CREATE INDEX IF NOT EXISTS idx_activity_elapsed ON sqlserver_activity_log (total_elapsed_ms DESC);",
    "CREATE INDEX IF NOT EXISTS idx_blocking_capture_time ON sqlserver_blocking_log (capture_time DESC);",
    "CREATE INDEX IF NOT EXISTS idx_blocking_blocker ON sqlserver_blocking_log (blocking_session_id);",
    "CREATE INDEX IF NOT EXISTS idx_query_stats_capture_time ON sqlserver_query_stats_log (capture_time DESC);",
    "CREATE INDEX IF NOT EXISTS idx_query_stats_max_elapsed ON sqlserver_query_stats_log (max_elapsed_ms DESC);",
    "CREATE INDEX IF NOT EXISTS idx_health_capture_time ON sqlserver_health_counters_log (capture_time DESC);",
    "CREATE INDEX IF NOT EXISTS idx_xevents_event_time ON sqlserver_xevent_log (event_time DESC);",
    "CREATE INDEX IF NOT EXISTS idx_xevents_event_name ON sqlserver_xevent_log (event_name, event_time DESC);",
    "CREATE INDEX IF NOT EXISTS idx_xevents_error_number ON sqlserver_xevent_log (error_number, event_time DESC);",
    "CREATE INDEX IF NOT EXISTS idx_attempt_attempted_at ON capture_attempt_log (attempted_at DESC);",
]

INSERT_ATTEMPT_LOG_SQL = """
INSERT INTO capture_attempt_log (
    attempted_at,
    rows_captured,
    rows_inserted,
    rows_activity,
    rows_blocking,
    rows_query_stats,
    rows_health_counters,
    rows_xevents,
    status,
    error_message
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def _normalize_env_url(key: str) -> str:
    value = os.environ.get(key, "").strip()
    if "\n" in value:
        value = value.split("\n", 1)[0].strip()
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        value = value[1:-1].strip()
    return value


def _is_enabled(env_key: str, default: str = "1") -> bool:
    return os.getenv(env_key, default).strip().lower() not in {"0", "false", "no", "off"}


def _to_utc_if_naive(value):
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _safe_sqlserver_identifier(value: str) -> str:
    identifier = (value or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", identifier):
        raise ValueError(
            "XEVENT_SESSION_NAME must match [A-Za-z_][A-Za-z0-9_]{0,127}"
        )
    return identifier


def _is_sqlserver_xevent_session_running(sql_conn, session_name: str) -> bool:
    row = sql_conn.execute(
        text(SQLSERVER_XEVENT_SESSION_RUNNING_QUERY), {"session_name": session_name}
    ).first()
    return row is not None


def _create_sqlserver_xevent_session(sql_conn, session_name: str):
    quoted_name = f"[{session_name}]"
    create_session_sql = f"""
CREATE EVENT SESSION {quoted_name}
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
"""
    with sql_conn.engine.connect() as ddl_conn:
        ddl_conn = ddl_conn.execution_options(isolation_level="AUTOCOMMIT")
        ddl_conn.execute(text(create_session_sql))


def ensure_sqlserver_xevent_session(sql_conn, session_name: str, auto_create: bool) -> bool:
    safe_name = _safe_sqlserver_identifier(session_name)

    if _is_sqlserver_xevent_session_running(sql_conn, safe_name):
        return True

    if not auto_create:
        return False

    session_defined = sql_conn.execute(
        text(SQLSERVER_XEVENT_SESSION_EXISTS_QUERY), {"session_name": safe_name}
    ).first()
    if session_defined is None:
        _create_sqlserver_xevent_session(sql_conn, safe_name)

    start_sql = f"ALTER EVENT SESSION [{safe_name}] ON SERVER STATE = START;"
    with sql_conn.engine.connect() as ddl_conn:
        ddl_conn = ddl_conn.execution_options(isolation_level="AUTOCOMMIT")
        ddl_conn.execute(text(start_sql))
    return _is_sqlserver_xevent_session_running(sql_conn, safe_name)


def _hash_xevent_record(record: dict) -> str:
    event_time = _to_utc_if_naive(record.get("event_time"))
    event_xml = record.get("event_xml")
    event_xml_text = "" if event_xml is None else str(event_xml)

    parts = [
        str(event_time or ""),
        str(record.get("event_name") or ""),
        str(record.get("session_id") or ""),
        str(record.get("error_number") or ""),
        str(record.get("severity") or ""),
        str(record.get("state") or ""),
        str(record.get("duration_ms") or ""),
        str(record.get("message") or ""),
        event_xml_text,
    ]
    return sha256("|".join(parts).encode("utf-8")).hexdigest()


def get_sqlserver_engine():
    url = _normalize_env_url("SQLSERVER_CONN_STR")
    if not url:
        raise ValueError("SQLSERVER_CONN_STR is not set")
    return create_engine(url, connect_args={"timeout": 15})


def get_sqlserver_connection():
    return get_sqlserver_engine().connect()


def _parse_postgres_url(url: str) -> dict:
    if "+" in url.split("://")[0]:
        url = "postgresql://" + url.split("://", 1)[1]
    elif not url.startswith("postgresql://"):
        url = "postgresql://" + url

    parsed = urlparse(url)
    kwargs = {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 5432,
        "dbname": (parsed.path or "/").lstrip("/") or None,
        "user": unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else None,
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}

    if parsed.query:
        for key, values in parse_qs(parsed.query).items():
            if key in {"sslmode", "sslrootcert"} and values and values[0]:
                kwargs[key] = values[0]

    return kwargs


def get_postgres_connection():
    conn_str = _normalize_env_url("POSTGRES_CONN_STR")
    if not conn_str:
        raise ValueError("POSTGRES_CONN_STR is not set")
    if "://" in conn_str:
        kwargs = _parse_postgres_url(conn_str)
        return psycopg2.connect(**kwargs)
    return psycopg2.connect(conn_str)


def fetch_rows(sql_conn, query: str, params: dict | None = None):
    result = sql_conn.execute(text(query), params or {})
    columns = list(result.keys())
    return [dict(zip(columns, row)) for row in result.fetchall()]


def fetch_sqlserver_activity(sql_conn):
    return fetch_rows(sql_conn, SQLSERVER_ACTIVITY_QUERY)


def fetch_sqlserver_blocking(sql_conn):
    return fetch_rows(sql_conn, SQLSERVER_BLOCKING_QUERY)


def fetch_sqlserver_query_stats(sql_conn, top_n: int):
    safe_top_n = max(int(top_n), 1)
    query = SQLSERVER_QUERY_STATS_QUERY_TEMPLATE.format(top_n=safe_top_n)
    return fetch_rows(sql_conn, query)


def fetch_sqlserver_health_counters(sql_conn):
    return fetch_rows(sql_conn, SQLSERVER_HEALTH_COUNTERS_QUERY)


def fetch_sqlserver_xevents(sql_conn, session_name: str):
    safe_name = _safe_sqlserver_identifier(session_name)
    return fetch_rows(
        sql_conn,
        SQLSERVER_XEVENTS_QUERY,
        {"session_name": safe_name},
    )


def insert_sqlserver_activity(pg_conn, rows):
    if not rows:
        return 0

    values = []
    for row in rows:
        values.append(
            (
                _to_utc_if_naive(row.get("capture_time")),
                row.get("session_id"),
                row.get("request_id"),
                row.get("blocking_session_id"),
                row.get("status"),
                row.get("command"),
                row.get("database_name"),
                row.get("login_name"),
                row.get("host_name"),
                row.get("program_name"),
                _to_utc_if_naive(row.get("start_time")),
                row.get("total_elapsed_ms"),
                row.get("cpu_time_ms"),
                row.get("logical_reads"),
                row.get("reads"),
                row.get("writes"),
                row.get("wait_type"),
                row.get("wait_time_ms"),
                row.get("wait_resource"),
                row.get("open_transaction_count"),
                row.get("object_schema"),
                row.get("object_name"),
                row.get("full_sql_text"),
                row.get("statement_text"),
                row.get("input_buffer"),
            )
        )

    with pg_conn.cursor() as cur:
        execute_batch(cur, INSERT_ACTIVITY_SQL, values, page_size=100)
    pg_conn.commit()
    return len(values)


def insert_sqlserver_blocking(pg_conn, rows):
    if not rows:
        return 0

    values = []
    for row in rows:
        values.append(
            (
                _to_utc_if_naive(row.get("capture_time")),
                row.get("waiting_session_id"),
                row.get("waiting_request_id"),
                row.get("blocking_session_id"),
                row.get("blocking_request_id"),
                row.get("wait_type"),
                row.get("wait_duration_ms"),
                row.get("resource_description"),
                row.get("waiting_status"),
                row.get("waiting_command"),
                row.get("waiting_database_name"),
                row.get("waiting_login_name"),
                row.get("waiting_host_name"),
                row.get("waiting_program_name"),
                row.get("waiting_statement_text"),
                row.get("blocking_status"),
                row.get("blocking_command"),
                row.get("blocking_database_name"),
                row.get("blocking_login_name"),
                row.get("blocking_host_name"),
                row.get("blocking_program_name"),
                row.get("blocking_statement_text"),
            )
        )

    with pg_conn.cursor() as cur:
        execute_batch(cur, INSERT_BLOCKING_SQL, values, page_size=100)
    pg_conn.commit()
    return len(values)


def insert_sqlserver_query_stats(pg_conn, rows):
    if not rows:
        return 0

    values = []
    for row in rows:
        values.append(
            (
                _to_utc_if_naive(row.get("capture_time")),
                row.get("database_name"),
                row.get("object_schema"),
                row.get("object_name"),
                row.get("execution_count"),
                row.get("total_elapsed_ms"),
                row.get("max_elapsed_ms"),
                row.get("last_elapsed_ms"),
                row.get("total_worker_ms"),
                row.get("total_logical_reads"),
                row.get("total_physical_reads"),
                _to_utc_if_naive(row.get("last_execution_time")),
                row.get("query_hash_hex"),
                row.get("query_plan_hash_hex"),
                row.get("full_sql_text"),
                row.get("statement_text"),
            )
        )

    with pg_conn.cursor() as cur:
        execute_batch(cur, INSERT_QUERY_STATS_SQL, values, page_size=100)
    pg_conn.commit()
    return len(values)


def insert_sqlserver_health_counters(pg_conn, rows):
    if not rows:
        return 0

    values = []
    for row in rows:
        values.append(
            (
                _to_utc_if_naive(row.get("capture_time")),
                row.get("object_name"),
                row.get("counter_name"),
                row.get("instance_name"),
                row.get("cntr_type"),
                row.get("cntr_value"),
            )
        )

    with pg_conn.cursor() as cur:
        execute_batch(cur, INSERT_HEALTH_COUNTERS_SQL, values, page_size=100)
    pg_conn.commit()
    return len(values)


def insert_sqlserver_xevents(pg_conn, rows):
    if not rows:
        return 0

    values = []
    for row in rows:
        event_xml = row.get("event_xml")
        event_xml_text = "" if event_xml is None else str(event_xml)
        values.append(
            (
                _to_utc_if_naive(row.get("capture_time")),
                _to_utc_if_naive(row.get("event_time")),
                row.get("event_name"),
                row.get("session_id"),
                row.get("database_name"),
                row.get("client_app_name"),
                row.get("client_hostname"),
                row.get("username"),
                row.get("error_number"),
                row.get("severity"),
                row.get("state"),
                row.get("duration_ms"),
                row.get("message"),
                row.get("sql_text"),
                event_xml_text,
                _hash_xevent_record(row),
            )
        )

    with pg_conn.cursor() as cur:
        returned_rows = execute_values(
            cur,
            INSERT_XEVENTS_SQL + " RETURNING event_hash",
            values,
            page_size=100,
            fetch=True,
        )
        inserted = len(returned_rows)
    pg_conn.commit()
    return inserted


def ensure_tables(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(CREATE_ACTIVITY_TABLE_SQL)
        cur.execute(CREATE_BLOCKING_TABLE_SQL)
        cur.execute(CREATE_QUERY_STATS_TABLE_SQL)
        cur.execute(CREATE_HEALTH_COUNTERS_TABLE_SQL)
        cur.execute(CREATE_XEVENTS_TABLE_SQL)
        cur.execute(CREATE_ATTEMPT_LOG_TABLE_SQL)

        for alter_sql in ALTER_ATTEMPT_LOG_SQL:
            cur.execute(alter_sql)

        for index_sql in INDEX_SQL:
            cur.execute(index_sql)

    pg_conn.commit()


def log_capture_attempt(
    pg_conn,
    attempted_at,
    rows_captured,
    rows_inserted,
    rows_activity,
    rows_blocking,
    rows_query_stats,
    rows_health_counters,
    rows_xevents,
    status,
    error_message=None,
):
    with pg_conn.cursor() as cur:
        cur.execute(
            INSERT_ATTEMPT_LOG_SQL,
            (
                attempted_at,
                rows_captured,
                rows_inserted,
                rows_activity,
                rows_blocking,
                rows_query_stats,
                rows_health_counters,
                rows_xevents,
                status,
                error_message,
            ),
        )
    pg_conn.commit()


def main():
    interval_seconds = max(int(os.getenv("POLL_INTERVAL_SECONDS", "15")), 1)
    query_stats_top_n = max(int(os.getenv("QUERY_STATS_TOP_N", "50")), 1)
    capture_blocking = _is_enabled("CAPTURE_BLOCKING", "1")
    capture_query_stats = _is_enabled("CAPTURE_QUERY_STATS", "1")
    capture_health_counters = _is_enabled("CAPTURE_HEALTH_COUNTERS", "1")
    capture_xevents = _is_enabled("CAPTURE_XEVENTS", "1")
    xevent_session_name = (
        os.getenv("XEVENT_SESSION_NAME", "sql_monitor_diag").strip() or "sql_monitor_diag"
    )
    xevent_auto_create_session = _is_enabled("XEVENT_AUTO_CREATE_SESSION", "1")

    logging.info("Starting SQL Server activity logger")

    try:
        pg_conn_init = get_postgres_connection()
        ensure_tables(pg_conn_init)
        pg_conn_init.close()
        logging.info("Ensured tables exist")
    except Exception as exc:
        logging.exception("Failed to create tables: %s", exc)

    if capture_xevents:
        sql_conn_init = None
        try:
            sql_conn_init = get_sqlserver_connection()
            xevents_ready = ensure_sqlserver_xevent_session(
                sql_conn_init, xevent_session_name, xevent_auto_create_session
            )
            if xevents_ready:
                logging.info("Extended Events session is ready: %s", xevent_session_name)
            else:
                logging.warning(
                    "Extended Events session is not running: %s", xevent_session_name
                )
        except Exception as exc:
            logging.exception("Failed to ensure Extended Events session: %s", exc)
        finally:
            if sql_conn_init is not None:
                try:
                    sql_conn_init.close()
                except Exception:
                    pass

    while True:
        started = time.time()
        sql_conn = None
        pg_conn = None

        attempted_at = datetime.now(timezone.utc)
        rows_activity = 0
        rows_blocking = 0
        rows_query_stats = 0
        rows_health_counters = 0
        rows_xevents = 0
        rows_captured = 0
        rows_inserted = 0
        collector_errors = []

        try:
            sql_conn = get_sqlserver_connection()
            pg_conn = get_postgres_connection()

            activity_rows = fetch_sqlserver_activity(sql_conn)
            rows_captured = len(activity_rows)
            rows_activity = insert_sqlserver_activity(pg_conn, activity_rows)

            if capture_blocking:
                try:
                    blocking_rows = fetch_sqlserver_blocking(sql_conn)
                    rows_blocking = insert_sqlserver_blocking(pg_conn, blocking_rows)
                except Exception as exc:
                    collector_errors.append(f"blocking_capture_failed: {exc}")
                    logging.exception("Blocking capture failed: %s", exc)

            if capture_query_stats:
                try:
                    query_stats_rows = fetch_sqlserver_query_stats(sql_conn, query_stats_top_n)
                    rows_query_stats = insert_sqlserver_query_stats(pg_conn, query_stats_rows)
                except Exception as exc:
                    collector_errors.append(f"query_stats_capture_failed: {exc}")
                    logging.exception("Query stats capture failed: %s", exc)

            if capture_health_counters:
                try:
                    health_rows = fetch_sqlserver_health_counters(sql_conn)
                    rows_health_counters = insert_sqlserver_health_counters(pg_conn, health_rows)
                except Exception as exc:
                    collector_errors.append(f"health_counter_capture_failed: {exc}")
                    logging.exception("Health counter capture failed: %s", exc)

            if capture_xevents:
                try:
                    xevents_ready = _is_sqlserver_xevent_session_running(
                        sql_conn, _safe_sqlserver_identifier(xevent_session_name)
                    )
                    if xevents_ready:
                        xevent_rows = fetch_sqlserver_xevents(sql_conn, xevent_session_name)
                        rows_xevents = insert_sqlserver_xevents(pg_conn, xevent_rows)
                    else:
                        collector_errors.append(
                            "xevent_session_not_running_or_not_accessible"
                        )
                except Exception as exc:
                    collector_errors.append(f"xevent_capture_failed: {exc}")
                    logging.exception("Extended Events capture failed: %s", exc)

            rows_inserted = (
                rows_activity
                + rows_blocking
                + rows_query_stats
                + rows_health_counters
                + rows_xevents
            )
            status = "success" if not collector_errors else "partial_failure"
            error_message = "; ".join(collector_errors) if collector_errors else None

            log_capture_attempt(
                pg_conn=pg_conn,
                attempted_at=attempted_at,
                rows_captured=rows_captured,
                rows_inserted=rows_inserted,
                rows_activity=rows_activity,
                rows_blocking=rows_blocking,
                rows_query_stats=rows_query_stats,
                rows_health_counters=rows_health_counters,
                rows_xevents=rows_xevents,
                status=status,
                error_message=error_message,
            )

            logging.info(
                "Captured rows activity=%s blocking=%s query_stats=%s health_counters=%s xevents=%s status=%s",
                rows_activity,
                rows_blocking,
                rows_query_stats,
                rows_health_counters,
                rows_xevents,
                status,
            )

        except Exception as exc:
            logging.exception("Failed to capture activity: %s", exc)
            if pg_conn is not None:
                try:
                    log_capture_attempt(
                        pg_conn=pg_conn,
                        attempted_at=attempted_at,
                        rows_captured=rows_captured,
                        rows_inserted=rows_inserted,
                        rows_activity=rows_activity,
                        rows_blocking=rows_blocking,
                        rows_query_stats=rows_query_stats,
                        rows_health_counters=rows_health_counters,
                        rows_xevents=rows_xevents,
                        status="failed",
                        error_message=str(exc),
                    )
                except Exception as log_exc:
                    logging.exception("Failed to write capture_attempt_log: %s", log_exc)

        finally:
            if sql_conn is not None:
                try:
                    sql_conn.close()
                except Exception:
                    pass

            if pg_conn is not None:
                try:
                    pg_conn.close()
                except Exception:
                    pass

        elapsed = time.time() - started
        sleep_for = max(interval_seconds - elapsed, 1)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
