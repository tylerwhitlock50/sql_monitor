import os
import re
from datetime import datetime, timezone
from hashlib import sha256


def normalize_env_url(key: str) -> str:
    value = os.environ.get(key, "").strip()
    if "\n" in value:
        value = value.split("\n", 1)[0].strip()
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        value = value[1:-1].strip()
    return value


def is_enabled(env_key: str, default: str = "1") -> bool:
    return os.getenv(env_key, default).strip().lower() not in {"0", "false", "no", "off"}


def to_utc_if_naive(value):
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def safe_sqlserver_identifier(value: str) -> str:
    identifier = (value or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", identifier):
        raise ValueError("XEVENT_SESSION_NAME must match [A-Za-z_][A-Za-z0-9_]{0,127}")
    return identifier


def hash_xevent_record(record: dict) -> str:
    event_time = to_utc_if_naive(record.get("event_time"))
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
