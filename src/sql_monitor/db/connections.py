from urllib.parse import parse_qs, unquote, urlparse

import psycopg2
from sqlalchemy import create_engine

from ..config import normalize_env_url


def get_sqlserver_engine():
    url = normalize_env_url("SQLSERVER_CONN_STR")
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
    conn_str = normalize_env_url("POSTGRES_CONN_STR")
    if not conn_str:
        raise ValueError("POSTGRES_CONN_STR is not set")
    if "://" in conn_str:
        kwargs = _parse_postgres_url(conn_str)
        return psycopg2.connect(**kwargs)
    return psycopg2.connect(conn_str)
