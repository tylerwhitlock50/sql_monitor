from psycopg2.extras import execute_batch, execute_values

from ..config import hash_xevent_record, to_utc_if_naive
from ..sql_queries import (
    ALTER_ATTEMPT_LOG_SQL,
    CREATE_ACTIVITY_TABLE_SQL,
    CREATE_ATTEMPT_LOG_TABLE_SQL,
    CREATE_BLOCKING_TABLE_SQL,
    CREATE_HEALTH_COUNTERS_TABLE_SQL,
    CREATE_QUERY_STATS_TABLE_SQL,
    CREATE_XEVENTS_TABLE_SQL,
    INDEX_SQL,
    INSERT_ACTIVITY_SQL,
    INSERT_ATTEMPT_LOG_SQL,
    INSERT_BLOCKING_SQL,
    INSERT_HEALTH_COUNTERS_SQL,
    INSERT_QUERY_STATS_SQL,
    INSERT_XEVENTS_SQL,
)


def insert_sqlserver_activity(pg_conn, rows):
    if not rows:
        return 0

    values = []
    for row in rows:
        values.append(
            (
                to_utc_if_naive(row.get("capture_time")),
                row.get("session_id"),
                row.get("request_id"),
                row.get("blocking_session_id"),
                row.get("status"),
                row.get("command"),
                row.get("database_name"),
                row.get("login_name"),
                row.get("host_name"),
                row.get("program_name"),
                to_utc_if_naive(row.get("start_time")),
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
                to_utc_if_naive(row.get("capture_time")),
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
                to_utc_if_naive(row.get("capture_time")),
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
                to_utc_if_naive(row.get("last_execution_time")),
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
                to_utc_if_naive(row.get("capture_time")),
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
                to_utc_if_naive(row.get("capture_time")),
                to_utc_if_naive(row.get("event_time")),
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
                hash_xevent_record(row),
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
