from __future__ import annotations

import os
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Iterator

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from api.config import get_settings
from api.models import User
from api.services.auth import verify_password

try:  # pragma: no cover - exercised in the JupyterHub runtime, not unit tests.
    from jupyterhub.auth import Authenticator as JupyterHubAuthenticator
except ImportError:  # pragma: no cover - fallback for unit tests and local imports.
    class JupyterHubAuthenticator:  # type: ignore[too-many-ancestors]
        pass

try:  # pragma: no cover - exercised when JupyterHub/traitlets are installed.
    from traitlets import Bool, Unicode
except ImportError:  # pragma: no cover - lightweight fallback for unit tests.
    def Bool(default_value: bool = False, **_kwargs: Any) -> bool:  # type: ignore[misc]
        return default_value

    def Unicode(default_value: str = "", **_kwargs: Any) -> str:  # type: ignore[misc]
        return default_value


def normalize_username(username: str) -> str:
    """Normalize a JupyterHub username before database lookup."""

    return username.strip()


def resolve_database_url(database_url: str | None = None) -> str:
    """Resolve the database URL from config, environment, or app defaults."""

    if database_url:
        return database_url
    env_database_url = os.environ.get("DETECDIV_HUB_DATABASE_URL")
    if env_database_url:
        return env_database_url
    return get_settings().database_url


@lru_cache(maxsize=8)
def _session_factory(database_url: str) -> sessionmaker[Session]:
    engine = create_engine(database_url, future=True, pool_pre_ping=True)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


@contextmanager
def db_session(database_url: str | None = None) -> Iterator[Session]:
    session_cls = _session_factory(resolve_database_url(database_url))
    session = session_cls()
    try:
        yield session
    finally:
        session.close()


def user_is_admin(user: User) -> bool:
    return user.role in {"admin", "service"} or bool(user.admin_portal_access)


def allowed_user(user: User, *, allowed_lab_statuses: set[str] | None = None) -> bool:
    if not user.is_active:
        return False
    if allowed_lab_statuses is not None and user.lab_status not in allowed_lab_statuses:
        return False
    return True


def user_to_auth_state(user: User) -> dict[str, Any]:
    return {
        "user_key": user.user_key,
        "display_name": user.display_name,
        "email": user.email,
        "role": user.role,
        "is_active": user.is_active,
        "admin_portal_access": user.admin_portal_access,
        "lab_status": user.lab_status,
        "default_path": user.default_path,
        "metadata": user.metadata_json,
        "admin": user_is_admin(user),
    }


def lookup_user(
    session: Session,
    *,
    username: str,
    case_insensitive_usernames: bool = True,
) -> User | None:
    normalized = normalize_username(username)
    if not normalized:
        return None

    user = session.scalars(select(User).where(User.user_key == normalized)).first()
    if user is not None or not case_insensitive_usernames:
        return user

    lowered = normalized.lower()
    return session.scalars(select(User).where(func.lower(User.user_key) == lowered)).first()


def authenticate_detecdiv_user(
    *,
    database_url: str | None,
    username: str,
    password: str,
    case_insensitive_usernames: bool = True,
    allowed_lab_statuses: set[str] | None = None,
) -> dict[str, Any] | None:
    with db_session(database_url) as session:
        user = lookup_user(
            session,
            username=username,
            case_insensitive_usernames=case_insensitive_usernames,
        )
        if user is None:
            return None
        if not allowed_user(user, allowed_lab_statuses=allowed_lab_statuses):
            return None
        if not verify_password(password, user.password_hash):
            return None
        return {
            "name": user.user_key,
            "auth_state": user_to_auth_state(user),
            "admin": user_is_admin(user),
        }


class DetecDivHubAuthenticator(JupyterHubAuthenticator):
    """JupyterHub authenticator backed by the detecdiv-hub PostgreSQL users table."""

    database_url = Unicode(
        "",
        config=True,
        help="SQLAlchemy URL for the detecdiv-hub database. Falls back to DETECDIV_HUB_DATABASE_URL.",
    )
    case_insensitive_usernames = Bool(
        True,
        config=True,
        help="Allow case-insensitive matching on user_key during login.",
    )
    allowed_lab_statuses = Unicode(
        "",
        config=True,
        help="Optional comma-separated lab_status values allowed to log in. Empty means allow all active users.",
    )

    def _allowed_lab_statuses(self) -> set[str] | None:
        raw = str(self.allowed_lab_statuses).strip()
        if not raw:
            return None
        return {status.strip() for status in raw.split(",") if status.strip()}

    async def authenticate(self, handler: Any, data: dict[str, Any]) -> dict[str, Any] | None:
        username = str(data.get("username") or data.get("user_key") or "").strip()
        password = str(data.get("password") or "")
        if not username or not password:
            return None
        return authenticate_detecdiv_user(
            database_url=self.database_url,
            username=username,
            password=password,
            case_insensitive_usernames=bool(self.case_insensitive_usernames),
            allowed_lab_statuses=self._allowed_lab_statuses(),
        )
