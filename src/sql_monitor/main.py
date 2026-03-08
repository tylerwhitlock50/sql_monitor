import logging
import os
import time
from datetime import datetime, timezone

from .config import is_enabled, safe_sqlserver_identifier
from .db.connections import get_postgres_connection, get_sqlserver_connection
from .db.postgres_store import (
    ensure_tables,
    insert_sqlserver_activity,
    insert_sqlserver_blocking,
    insert_sqlserver_health_counters,
    insert_sqlserver_query_stats,
    insert_sqlserver_xevents,
    log_capture_attempt,
)
from .db.sqlserver_collectors import (
    ensure_sqlserver_xevent_session,
    fetch_sqlserver_activity,
    fetch_sqlserver_blocking,
    fetch_sqlserver_health_counters,
    fetch_sqlserver_query_stats,
    fetch_sqlserver_xevents,
    is_sqlserver_xevent_session_running,
)


def main():
    interval_seconds = max(int(os.getenv("POLL_INTERVAL_SECONDS", "15")), 1)
    query_stats_top_n = max(int(os.getenv("QUERY_STATS_TOP_N", "50")), 1)
    capture_blocking = is_enabled("CAPTURE_BLOCKING", "1")
    capture_query_stats = is_enabled("CAPTURE_QUERY_STATS", "1")
    capture_health_counters = is_enabled("CAPTURE_HEALTH_COUNTERS", "1")
    capture_xevents = is_enabled("CAPTURE_XEVENTS", "1")
    xevent_session_name = (
        os.getenv("XEVENT_SESSION_NAME", "sql_monitor_diag").strip() or "sql_monitor_diag"
    )
    xevent_auto_create_session = is_enabled("XEVENT_AUTO_CREATE_SESSION", "1")

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
                    xevents_ready = is_sqlserver_xevent_session_running(
                        sql_conn, safe_sqlserver_identifier(xevent_session_name)
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
