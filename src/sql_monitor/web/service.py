import os
from datetime import datetime, timezone

from psycopg2.extras import RealDictCursor

from ..db.connections import get_postgres_connection


def _to_int(value, default: int = 0) -> int:
    if value is None:
        return default
    return int(value)


def _to_iso(value) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return None


def _table_exists(cur, table_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s) AS table_ref", (f"public.{table_name}",))
    row = cur.fetchone() or {}
    return row.get("table_ref") is not None


def _fetch_latest_attempt(cur):
    cur.execute(
        """
        SELECT
            attempted_at,
            status,
            rows_captured,
            rows_inserted,
            rows_activity,
            rows_blocking,
            rows_query_stats,
            rows_health_counters,
            rows_xevents,
            error_message
        FROM capture_attempt_log
        ORDER BY attempted_at DESC
        LIMIT 1
        """
    )
    return cur.fetchone()


def _fetch_rollup(cur, minutes: int):
    cur.execute(
        """
        SELECT
            COUNT(*)::int AS attempts,
            COUNT(*) FILTER (WHERE status = 'success')::int AS success_count,
            COUNT(*) FILTER (WHERE status = 'partial_failure')::int AS partial_failure_count,
            COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_count,
            COALESCE(SUM(rows_activity), 0)::bigint AS activity_rows,
            COALESCE(SUM(rows_blocking), 0)::bigint AS blocking_rows,
            COALESCE(SUM(rows_query_stats), 0)::bigint AS query_stats_rows,
            COALESCE(SUM(rows_health_counters), 0)::bigint AS health_counter_rows,
            COALESCE(SUM(rows_xevents), 0)::bigint AS xevent_rows
        FROM capture_attempt_log
        WHERE attempted_at >= NOW() - (%s || ' minutes')::interval
        """,
        (minutes,),
    )
    row = cur.fetchone() or {}
    return {
        "attempts": _to_int(row.get("attempts")),
        "success_count": _to_int(row.get("success_count")),
        "partial_failure_count": _to_int(row.get("partial_failure_count")),
        "failed_count": _to_int(row.get("failed_count")),
        "activity_rows": _to_int(row.get("activity_rows")),
        "blocking_rows": _to_int(row.get("blocking_rows")),
        "query_stats_rows": _to_int(row.get("query_stats_rows")),
        "health_counter_rows": _to_int(row.get("health_counter_rows")),
        "xevent_rows": _to_int(row.get("xevent_rows")),
    }


def _fetch_recent_blocking(cur, limit: int = 10):
    cur.execute(
        """
        SELECT
            capture_time,
            waiting_session_id,
            blocking_session_id,
            wait_type,
            wait_duration_ms,
            waiting_database_name
        FROM sqlserver_blocking_log
        ORDER BY capture_time DESC, wait_duration_ms DESC NULLS LAST
        LIMIT %s
        """,
        (limit,),
    )
    rows = []
    for row in cur.fetchall() or []:
        rows.append(
            {
                "capture_time": _to_iso(row.get("capture_time")),
                "waiting_session_id": row.get("waiting_session_id"),
                "blocking_session_id": row.get("blocking_session_id"),
                "wait_type": row.get("wait_type"),
                "wait_duration_ms": row.get("wait_duration_ms"),
                "waiting_database_name": row.get("waiting_database_name"),
            }
        )
    return rows


def _fetch_recent_xevent_summary(cur, minutes: int = 60):
    cur.execute(
        """
        SELECT
            COUNT(*)::int AS total_events,
            COUNT(*) FILTER (WHERE severity >= 16)::int AS severe_events,
            COUNT(*) FILTER (WHERE error_number IN (1205, 1222))::int AS locking_events
        FROM sqlserver_xevent_log
        WHERE event_time >= NOW() - (%s || ' minutes')::interval
        """,
        (minutes,),
    )
    row = cur.fetchone() or {}
    return {
        "total_events": _to_int(row.get("total_events")),
        "severe_events": _to_int(row.get("severe_events")),
        "locking_events": _to_int(row.get("locking_events")),
    }


def _build_problems(latest_attempt, rollup_1h, xevent_1h, stale_minutes: int):
    problems = []
    now = datetime.now(timezone.utc)

    if latest_attempt is None:
        problems.append(
            {
                "level": "critical",
                "code": "no_capture_attempts",
                "message": "No capture records found in capture_attempt_log.",
            }
        )
        return problems

    latest_time = latest_attempt.get("attempted_at")
    if isinstance(latest_time, datetime):
        if latest_time.tzinfo is None:
            latest_time = latest_time.replace(tzinfo=timezone.utc)
        age_minutes = (now - latest_time).total_seconds() / 60.0
        if age_minutes > stale_minutes:
            problems.append(
                {
                    "level": "critical",
                    "code": "stale_capture",
                    "message": f"Last capture is stale ({age_minutes:.1f} minutes old).",
                }
            )

    latest_status = (latest_attempt.get("status") or "").strip().lower()
    if latest_status == "failed":
        problems.append(
            {
                "level": "critical",
                "code": "latest_failed",
                "message": "Latest capture attempt failed.",
            }
        )
    elif latest_status == "partial_failure":
        problems.append(
            {
                "level": "warning",
                "code": "latest_partial_failure",
                "message": "Latest capture attempt reported partial failure.",
            }
        )

    if rollup_1h["failed_count"] > 0:
        problems.append(
            {
                "level": "warning",
                "code": "recent_failures",
                "message": f"{rollup_1h['failed_count']} failed capture attempts in the last hour.",
            }
        )

    if rollup_1h["blocking_rows"] > 0:
        problems.append(
            {
                "level": "warning",
                "code": "recent_blocking",
                "message": f"{rollup_1h['blocking_rows']} blocking rows captured in the last hour.",
            }
        )

    if xevent_1h["severe_events"] > 0:
        problems.append(
            {
                "level": "warning",
                "code": "recent_severe_xevents",
                "message": f"{xevent_1h['severe_events']} severe xevents in the last hour.",
            }
        )

    return problems


def _overall_status(problems) -> str:
    if any(problem["level"] == "critical" for problem in problems):
        return "critical"
    if any(problem["level"] == "warning" for problem in problems):
        return "warning"
    return "ok"


def _bucket_iso(bucket_index: int, bucket_minutes: int) -> str:
    interval_seconds = bucket_minutes * 60
    dt = datetime.fromtimestamp(bucket_index * interval_seconds, timezone.utc)
    return dt.isoformat()


def _fetch_attempt_timeseries(cur, window_minutes: int, bucket_minutes: int):
    cur.execute(
        """
        SELECT
            FLOOR(EXTRACT(EPOCH FROM attempted_at) / (%s * 60))::bigint AS bucket_idx,
            COUNT(*)::int AS attempts,
            COUNT(*) FILTER (WHERE status = 'success')::int AS success_count,
            COUNT(*) FILTER (WHERE status = 'partial_failure')::int AS partial_failure_count,
            COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_count,
            COALESCE(SUM(rows_inserted), 0)::bigint AS rows_inserted,
            COALESCE(SUM(rows_blocking), 0)::bigint AS rows_blocking,
            COALESCE(SUM(rows_xevents), 0)::bigint AS rows_xevents
        FROM capture_attempt_log
        WHERE attempted_at >= NOW() - (%s || ' minutes')::interval
        GROUP BY 1
        ORDER BY 1
        """,
        (bucket_minutes, window_minutes),
    )
    return cur.fetchall() or []


def _fetch_blocking_timeseries(cur, window_minutes: int, bucket_minutes: int):
    cur.execute(
        """
        SELECT
            FLOOR(EXTRACT(EPOCH FROM capture_time) / (%s * 60))::bigint AS bucket_idx,
            COUNT(*)::int AS blocking_events,
            COALESCE(MAX(wait_duration_ms), 0)::bigint AS max_wait_duration_ms
        FROM sqlserver_blocking_log
        WHERE capture_time >= NOW() - (%s || ' minutes')::interval
        GROUP BY 1
        ORDER BY 1
        """,
        (bucket_minutes, window_minutes),
    )
    return cur.fetchall() or []


def _fetch_xevent_timeseries(cur, window_minutes: int, bucket_minutes: int):
    cur.execute(
        """
        SELECT
            FLOOR(EXTRACT(EPOCH FROM event_time) / (%s * 60))::bigint AS bucket_idx,
            COUNT(*)::int AS xevent_total,
            COUNT(*) FILTER (WHERE severity >= 16)::int AS xevent_severe
        FROM sqlserver_xevent_log
        WHERE event_time >= NOW() - (%s || ' minutes')::interval
        GROUP BY 1
        ORDER BY 1
        """,
        (bucket_minutes, window_minutes),
    )
    return cur.fetchall() or []


def get_dashboard_snapshot() -> dict:
    stale_minutes = max(int(os.getenv("DASHBOARD_STALE_MINUTES", "5")), 1)

    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                has_attempt_log = _table_exists(cur, "capture_attempt_log")
                has_blocking_log = _table_exists(cur, "sqlserver_blocking_log")
                has_xevent_log = _table_exists(cur, "sqlserver_xevent_log")

                if not has_attempt_log:
                    return {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "overall_status": "critical",
                        "problems": [
                            {
                                "level": "critical",
                                "code": "missing_attempt_log_table",
                                "message": "capture_attempt_log table does not exist yet.",
                            }
                        ],
                        "latest_attempt": None,
                        "rollups": {
                            "last_hour": {},
                            "last_24_hours": {},
                        },
                        "recent_blocking": [],
                        "xevents_last_hour": {
                            "total_events": 0,
                            "severe_events": 0,
                            "locking_events": 0,
                        },
                    }

                latest_attempt = _fetch_latest_attempt(cur)
                rollup_1h = _fetch_rollup(cur, 60)
                rollup_24h = _fetch_rollup(cur, 24 * 60)
                recent_blocking = _fetch_recent_blocking(cur, limit=10) if has_blocking_log else []
                xevent_1h = (
                    _fetch_recent_xevent_summary(cur, minutes=60)
                    if has_xevent_log
                    else {"total_events": 0, "severe_events": 0, "locking_events": 0}
                )

        latest_attempt_payload = None
        if latest_attempt is not None:
            latest_attempt_payload = {
                "attempted_at": _to_iso(latest_attempt.get("attempted_at")),
                "status": latest_attempt.get("status"),
                "rows_captured": _to_int(latest_attempt.get("rows_captured")),
                "rows_inserted": _to_int(latest_attempt.get("rows_inserted")),
                "rows_activity": _to_int(latest_attempt.get("rows_activity")),
                "rows_blocking": _to_int(latest_attempt.get("rows_blocking")),
                "rows_query_stats": _to_int(latest_attempt.get("rows_query_stats")),
                "rows_health_counters": _to_int(
                    latest_attempt.get("rows_health_counters")
                ),
                "rows_xevents": _to_int(latest_attempt.get("rows_xevents")),
                "error_message": latest_attempt.get("error_message"),
            }

        problems = _build_problems(latest_attempt, rollup_1h, xevent_1h, stale_minutes)

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "overall_status": _overall_status(problems),
            "problems": problems,
            "latest_attempt": latest_attempt_payload,
            "rollups": {
                "last_hour": rollup_1h,
                "last_24_hours": rollup_24h,
            },
            "recent_blocking": recent_blocking,
            "xevents_last_hour": xevent_1h,
        }
    except Exception as exc:
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "overall_status": "critical",
            "problems": [
                {
                    "level": "critical",
                    "code": "dashboard_query_failed",
                    "message": str(exc),
                }
            ],
            "latest_attempt": None,
            "rollups": {
                "last_hour": {},
                "last_24_hours": {},
            },
            "recent_blocking": [],
            "xevents_last_hour": {
                "total_events": 0,
                "severe_events": 0,
                "locking_events": 0,
            },
        }


def get_dashboard_timeseries(window_minutes: int, bucket_minutes: int) -> dict:
    safe_window = max(int(window_minutes), 15)
    safe_bucket = max(int(bucket_minutes), 1)
    safe_window = min(safe_window, 7 * 24 * 60)
    safe_bucket = min(safe_bucket, 60)

    interval_seconds = safe_bucket * 60
    now_bucket_idx = int(datetime.now(timezone.utc).timestamp() // interval_seconds)
    start_bucket_idx = now_bucket_idx - (safe_window * 60 // interval_seconds)

    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                has_attempt_log = _table_exists(cur, "capture_attempt_log")
                has_blocking_log = _table_exists(cur, "sqlserver_blocking_log")
                has_xevent_log = _table_exists(cur, "sqlserver_xevent_log")

                if not has_attempt_log:
                    return {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "window_minutes": safe_window,
                        "bucket_minutes": safe_bucket,
                        "buckets": [],
                    }

                attempts = _fetch_attempt_timeseries(cur, safe_window, safe_bucket)
                blocking = (
                    _fetch_blocking_timeseries(cur, safe_window, safe_bucket)
                    if has_blocking_log
                    else []
                )
                xevents = (
                    _fetch_xevent_timeseries(cur, safe_window, safe_bucket)
                    if has_xevent_log
                    else []
                )

        buckets = {}
        for idx in range(start_bucket_idx, now_bucket_idx + 1):
            buckets[idx] = {
                "bucket_start": _bucket_iso(idx, safe_bucket),
                "attempts": 0,
                "success_count": 0,
                "partial_failure_count": 0,
                "failed_count": 0,
                "rows_inserted": 0,
                "rows_blocking": 0,
                "rows_xevents": 0,
                "blocking_events": 0,
                "max_wait_duration_ms": 0,
                "xevent_total": 0,
                "xevent_severe": 0,
            }

        for row in attempts:
            idx = int(row.get("bucket_idx"))
            if idx not in buckets:
                continue
            buckets[idx]["attempts"] = _to_int(row.get("attempts"))
            buckets[idx]["success_count"] = _to_int(row.get("success_count"))
            buckets[idx]["partial_failure_count"] = _to_int(row.get("partial_failure_count"))
            buckets[idx]["failed_count"] = _to_int(row.get("failed_count"))
            buckets[idx]["rows_inserted"] = _to_int(row.get("rows_inserted"))
            buckets[idx]["rows_blocking"] = _to_int(row.get("rows_blocking"))
            buckets[idx]["rows_xevents"] = _to_int(row.get("rows_xevents"))

        for row in blocking:
            idx = int(row.get("bucket_idx"))
            if idx not in buckets:
                continue
            buckets[idx]["blocking_events"] = _to_int(row.get("blocking_events"))
            buckets[idx]["max_wait_duration_ms"] = _to_int(row.get("max_wait_duration_ms"))

        for row in xevents:
            idx = int(row.get("bucket_idx"))
            if idx not in buckets:
                continue
            buckets[idx]["xevent_total"] = _to_int(row.get("xevent_total"))
            buckets[idx]["xevent_severe"] = _to_int(row.get("xevent_severe"))

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window_minutes": safe_window,
            "bucket_minutes": safe_bucket,
            "buckets": [buckets[idx] for idx in sorted(buckets.keys())],
        }
    except Exception as exc:
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window_minutes": safe_window,
            "bucket_minutes": safe_bucket,
            "buckets": [],
            "error": str(exc),
        }
