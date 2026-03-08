import os
import time
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, unquote

import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

SQLSERVER_QUERY = """
WITH running AS
(
    SELECT
        GETDATE() AS capture_time,
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
        r.plan_handle,
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

INSERT_SQL = """
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

CREATE_SQLSERVER_ACTIVITY_LOG_TABLE = """
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
    total_elapsed_ms INT,
    cpu_time_ms INT,
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

CREATE_ATTEMPT_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS capture_attempt_log (
    id SERIAL PRIMARY KEY,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows_captured INT NOT NULL,
    rows_inserted INT NOT NULL,
    status VARCHAR(20) NOT NULL,
    error_message TEXT
);
"""

INSERT_ATTEMPT_LOG_SQL = """
INSERT INTO capture_attempt_log (attempted_at, rows_captured, rows_inserted, status, error_message)
VALUES (%s, %s, %s, %s, %s)
"""

def _normalize_env_url(key: str) -> str:
    """Get URL from env and strip so multiline or quoted values parse correctly."""
    value = os.environ.get(key, "").strip()
    if "\n" in value:
        value = value.split("\n")[0].strip()
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        value = value[1:-1].strip()
    return value


def get_sqlserver_engine():
    url = _normalize_env_url("SQLSERVER_CONN_STR")
    if not url:
        raise ValueError("SQLSERVER_CONN_STR is not set")
    return create_engine(url, connect_args={"timeout": 15})


def get_sqlserver_connection():
    return get_sqlserver_engine().connect()

def _parse_postgres_url(url: str) -> dict:
    """Parse postgresql:// or postgresql+psycopg2:// URL into psycopg2.connect() kwargs."""
    # Normalize scheme so urlparse sees host/path (e.g. psycopg2+postgresql -> postgresql)
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
            if key == "sslmode" and values and values[0]:
                kwargs["sslmode"] = values[0]
            elif key == "sslrootcert" and values and values[0]:
                kwargs["sslrootcert"] = values[0]
    return kwargs


def get_postgres_connection():
    conn_str = _normalize_env_url("POSTGRES_CONN_STR")
    if not conn_str:
        raise ValueError("POSTGRES_CONN_STR is not set")
    if "://" in conn_str:
        kwargs = _parse_postgres_url(conn_str)
        return psycopg2.connect(**kwargs)
    return psycopg2.connect(conn_str)

def fetch_sqlserver_activity(sql_conn):
    result = sql_conn.execute(text(SQLSERVER_QUERY))
    columns = list(result.keys())
    rows = result.fetchall()

    results = []
    for row in rows:
        record = dict(zip(columns, row))
        results.append(record)
    return results

def _strip_nul(value):
    """Remove NUL bytes (0x00) that PostgreSQL rejects in string literals."""
    if isinstance(value, str):
        return value.replace('\x00', '')
    return value


def insert_into_postgres(pg_conn, rows):
    if not rows:
        return 0

    values = []
    for r in rows:
        values.append((
            r.get("capture_time"),
            r.get("session_id"),
            r.get("request_id"),
            r.get("blocking_session_id"),
            _strip_nul(r.get("status")),
            _strip_nul(r.get("command")),
            _strip_nul(r.get("database_name")),
            _strip_nul(r.get("login_name")),
            _strip_nul(r.get("host_name")),
            _strip_nul(r.get("program_name")),
            r.get("start_time"),
            r.get("total_elapsed_ms"),
            r.get("cpu_time_ms"),
            r.get("logical_reads"),
            r.get("reads"),
            r.get("writes"),
            _strip_nul(r.get("wait_type")),
            r.get("wait_time_ms"),
            _strip_nul(r.get("wait_resource")),
            r.get("open_transaction_count"),
            _strip_nul(r.get("object_schema")),
            _strip_nul(r.get("object_name")),
            _strip_nul(r.get("full_sql_text")),
            _strip_nul(r.get("statement_text")),
            _strip_nul(r.get("input_buffer")),
        ))

    with pg_conn.cursor() as cur:
        execute_batch(cur, INSERT_SQL, values, page_size=100)
    pg_conn.commit()
    return len(values)


def ensure_tables(pg_conn):
    """Create sqlserver_activity_log and capture_attempt_log if they do not exist."""
    with pg_conn.cursor() as cur:
        cur.execute(CREATE_SQLSERVER_ACTIVITY_LOG_TABLE)
        cur.execute(CREATE_ATTEMPT_LOG_TABLE)
    pg_conn.commit()


def log_capture_attempt(pg_conn, attempted_at, rows_captured, rows_inserted, status, error_message=None):
    """Write one row to capture_attempt_log."""
    with pg_conn.cursor() as cur:
        cur.execute(
            INSERT_ATTEMPT_LOG_SQL,
            (attempted_at, rows_captured, rows_inserted, status, error_message),
        )
    pg_conn.commit()


def main():
    interval_seconds = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

    logging.info("Starting SQL Server activity logger")

    # Ensure all required tables exist
    try:
        pg_conn_init = get_postgres_connection()
        ensure_tables(pg_conn_init)
        pg_conn_init.close()
        logging.info("Ensured tables exist: sqlserver_activity_log, capture_attempt_log")
    except Exception as exc:
        logging.exception("Failed to create tables: %s", exc)

    while True:
        started = time.time()
        sql_conn = None
        pg_conn = None
        attempted_at = datetime.now(timezone.utc)
        rows_captured = 0
        rows_inserted = 0

        try:
            sql_conn = get_sqlserver_connection()
            pg_conn = get_postgres_connection()

            rows = fetch_sqlserver_activity(sql_conn)
            rows_captured = len(rows)
            rows_inserted = insert_into_postgres(pg_conn, rows)

            log_capture_attempt(
                pg_conn, attempted_at, rows_captured, rows_inserted, "success", None
            )
            logging.info("Captured %s active requests", rows_inserted)

        except Exception as exc:
            logging.exception("Failed to capture activity: %s", exc)
            if pg_conn is not None:
                try:
                    log_capture_attempt(
                        pg_conn, attempted_at, rows_captured, rows_inserted, "failed", str(exc)
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