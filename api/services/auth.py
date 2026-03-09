from __future__ import annotations

import base64
import hashlib
import secrets
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from api.config import get_settings
from api.models import User, UserSession


def hash_password(password: str, *, salt: bytes | None = None, iterations: int = 600_000) -> str:
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return "pbkdf2_sha256${iterations}${salt}${digest}".format(
        iterations=iterations,
        salt=base64.b64encode(salt).decode("ascii"),
        digest=base64.b64encode(digest).decode("ascii"),
    )


def verify_password(password: str, encoded: str | None) -> bool:
    if not encoded:
        return False
    try:
        algorithm, iteration_text, salt_b64, digest_b64 = encoded.split("$", 3)
    except ValueError:
        return False
    if algorithm != "pbkdf2_sha256":
        return False
    iterations = int(iteration_text)
    salt = base64.b64decode(salt_b64.encode("ascii"))
    expected = base64.b64decode(digest_b64.encode("ascii"))
    computed = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return secrets.compare_digest(computed, expected)


def issue_user_session(
    session: Session,
    *,
    user: User,
    client_label: str | None = None,
) -> tuple[UserSession, str]:
    settings = get_settings()
    token = secrets.token_urlsafe(32)
    token_hash = hash_token(token)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=int(settings.session_duration_hours))
    user_session = UserSession(
        user_id=user.id,
        token_hash=token_hash,
        status="active",
        client_label=client_label,
        last_seen_at=datetime.now(timezone.utc),
        expires_at=expires_at,
    )
    session.add(user_session)
    session.flush()
    return user_session, token


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def get_user_by_session_token(session: Session, token: str | None) -> User | None:
    if not token:
        return None
    now = datetime.now(timezone.utc)
    token_hash = hash_token(token)
    stmt = (
        select(UserSession)
        .join(User)
        .where(
            UserSession.token_hash == token_hash,
            UserSession.status == "active",
            UserSession.expires_at >= now,
            User.is_active.is_(True),
        )
    )
    user_session = session.scalars(stmt).first()
    if user_session is None:
        return None
    user_session.last_seen_at = now
    session.flush()
    return user_session.user


def revoke_session_token(session: Session, token: str | None) -> None:
    if not token:
        return
    token_hash = hash_token(token)
    user_session = session.scalars(select(UserSession).where(UserSession.token_hash == token_hash)).first()
    if user_session is None:
        return
    user_session.status = "revoked"
    user_session.revoked_at = datetime.now(timezone.utc)
    session.flush()


def revoke_user_session(session: Session, *, session_id, acting_user: User) -> UserSession | None:
    user_session = session.get(UserSession, session_id)
    if user_session is None:
        return None
    if acting_user.role not in {"admin", "service"} and user_session.user_id != acting_user.id:
        return None
    user_session.status = "revoked"
    user_session.revoked_at = datetime.now(timezone.utc)
    session.flush()
    return user_session


def list_active_sessions(session: Session, *, acting_user: User, include_all: bool = False) -> list[UserSession]:
    stmt = select(UserSession).join(User).where(
        UserSession.status == "active",
        User.is_active.is_(True),
    )
    if not include_all or acting_user.role not in {"admin", "service"}:
        stmt = stmt.where(UserSession.user_id == acting_user.id)
    stmt = stmt.order_by(UserSession.last_seen_at.desc().nullslast(), UserSession.created_at.desc())
    return list(session.scalars(stmt))


def set_user_password(session: Session, *, user: User, password: str) -> User:
    user.password_hash = hash_password(password)
    session.flush()
    return user
