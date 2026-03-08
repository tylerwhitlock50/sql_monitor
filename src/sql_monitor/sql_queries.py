from .sql_loader import load_sql, load_sql_statements


SQLSERVER_ACTIVITY_QUERY = load_sql("sqlserver", "activity.sql")
SQLSERVER_BLOCKING_QUERY = load_sql("sqlserver", "blocking.sql")
SQLSERVER_QUERY_STATS_QUERY_TEMPLATE = load_sql("sqlserver", "query_stats.sql")
SQLSERVER_HEALTH_COUNTERS_QUERY = load_sql("sqlserver", "health_counters.sql")
SQLSERVER_XEVENTS_QUERY = load_sql("sqlserver", "xevents.sql")
SQLSERVER_XEVENT_SESSION_EXISTS_QUERY = load_sql("sqlserver", "xevent_session_exists.sql")
SQLSERVER_XEVENT_SESSION_RUNNING_QUERY = load_sql("sqlserver", "xevent_session_running.sql")
SQLSERVER_CREATE_XEVENT_SESSION_TEMPLATE = load_sql("sqlserver", "create_xevent_session.sql")
SQLSERVER_START_XEVENT_SESSION_TEMPLATE = load_sql("sqlserver", "start_xevent_session.sql")

INSERT_ACTIVITY_SQL = load_sql("postgres", "insert_activity.sql")
INSERT_BLOCKING_SQL = load_sql("postgres", "insert_blocking.sql")
INSERT_QUERY_STATS_SQL = load_sql("postgres", "insert_query_stats.sql")
INSERT_HEALTH_COUNTERS_SQL = load_sql("postgres", "insert_health_counters.sql")
INSERT_XEVENTS_SQL = load_sql("postgres", "insert_xevents.sql")
INSERT_ATTEMPT_LOG_SQL = load_sql("postgres", "insert_capture_attempt.sql")

CREATE_ACTIVITY_TABLE_SQL = load_sql("postgres", "create_activity_table.sql")
CREATE_BLOCKING_TABLE_SQL = load_sql("postgres", "create_blocking_table.sql")
CREATE_QUERY_STATS_TABLE_SQL = load_sql("postgres", "create_query_stats_table.sql")
CREATE_HEALTH_COUNTERS_TABLE_SQL = load_sql("postgres", "create_health_counters_table.sql")
CREATE_XEVENTS_TABLE_SQL = load_sql("postgres", "create_xevents_table.sql")
CREATE_ATTEMPT_LOG_TABLE_SQL = load_sql("postgres", "create_capture_attempt_table.sql")

ALTER_ATTEMPT_LOG_SQL = load_sql_statements("postgres", "alter_capture_attempt_log.sql")
INDEX_SQL = load_sql_statements("postgres", "indexes.sql")
