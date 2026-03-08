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
