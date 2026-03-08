from sqlalchemy import text

from ..config import safe_sqlserver_identifier
from ..sql_queries import (
    SQLSERVER_ACTIVITY_QUERY,
    SQLSERVER_BLOCKING_QUERY,
    SQLSERVER_CREATE_XEVENT_SESSION_TEMPLATE,
    SQLSERVER_HEALTH_COUNTERS_QUERY,
    SQLSERVER_QUERY_STATS_QUERY_TEMPLATE,
    SQLSERVER_START_XEVENT_SESSION_TEMPLATE,
    SQLSERVER_XEVENT_SESSION_EXISTS_QUERY,
    SQLSERVER_XEVENT_SESSION_RUNNING_QUERY,
    SQLSERVER_XEVENTS_QUERY,
)


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
    safe_name = safe_sqlserver_identifier(session_name)
    return fetch_rows(
        sql_conn,
        SQLSERVER_XEVENTS_QUERY,
        {"session_name": safe_name},
    )


def is_sqlserver_xevent_session_running(sql_conn, session_name: str) -> bool:
    row = sql_conn.execute(
        text(SQLSERVER_XEVENT_SESSION_RUNNING_QUERY), {"session_name": session_name}
    ).first()
    return row is not None


def create_sqlserver_xevent_session(sql_conn, session_name: str):
    create_session_sql = SQLSERVER_CREATE_XEVENT_SESSION_TEMPLATE.format(
        session_name=session_name
    )
    with sql_conn.engine.connect() as ddl_conn:
        ddl_conn = ddl_conn.execution_options(isolation_level="AUTOCOMMIT")
        ddl_conn.execute(text(create_session_sql))


def ensure_sqlserver_xevent_session(sql_conn, session_name: str, auto_create: bool) -> bool:
    safe_name = safe_sqlserver_identifier(session_name)

    if is_sqlserver_xevent_session_running(sql_conn, safe_name):
        return True

    if not auto_create:
        return False

    session_defined = sql_conn.execute(
        text(SQLSERVER_XEVENT_SESSION_EXISTS_QUERY), {"session_name": safe_name}
    ).first()
    if session_defined is None:
        create_sqlserver_xevent_session(sql_conn, safe_name)

    start_sql = SQLSERVER_START_XEVENT_SESSION_TEMPLATE.format(session_name=safe_name)
    with sql_conn.engine.connect() as ddl_conn:
        ddl_conn = ddl_conn.execution_options(isolation_level="AUTOCOMMIT")
        ddl_conn.execute(text(start_sql))
    return is_sqlserver_xevent_session_running(sql_conn, safe_name)
