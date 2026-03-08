import os
import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials


security = HTTPBasic(auto_error=False)


def _is_truthy(value: str) -> bool:
    return value.strip().lower() not in {"0", "false", "no", "off"}


def is_auth_enabled() -> bool:
    explicit = os.getenv("DASHBOARD_AUTH_ENABLED", "").strip()
    if explicit:
        return _is_truthy(explicit)
    return bool(
        os.getenv("DASHBOARD_AUTH_USERNAME", "").strip()
        and os.getenv("DASHBOARD_AUTH_PASSWORD", "").strip()
    )


def require_dashboard_auth(
    credentials: Annotated[HTTPBasicCredentials | None, Depends(security)],
):
    if not is_auth_enabled():
        return

    expected_username = os.getenv("DASHBOARD_AUTH_USERNAME", "").strip()
    expected_password = os.getenv("DASHBOARD_AUTH_PASSWORD", "").strip()
    if not expected_username or not expected_password:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Dashboard auth enabled but credentials are not configured.",
        )

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )

    username_ok = secrets.compare_digest(credentials.username, expected_username)
    password_ok = secrets.compare_digest(credentials.password, expected_password)
    if not (username_ok and password_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )

