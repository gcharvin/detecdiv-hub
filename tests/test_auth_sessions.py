from datetime import datetime, timedelta, timezone
from uuid import uuid4

from api.models import User, UserSession
from api.services.auth import get_user_by_session_token, hash_token


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def first(self):
        return self.value


class _Session:
    def __init__(self, value):
        self.value = value
        self.flush_calls = 0

    def scalars(self, _statement):
        return _ScalarResult(self.value)

    def flush(self):
        self.flush_calls += 1


def _user_session(*, expires_at: datetime) -> tuple[User, UserSession]:
    user = User(id=uuid4(), user_key="gilles", display_name="Gilles", role="admin", is_active=True)
    user_session = UserSession(
        id=uuid4(),
        user_id=user.id,
        token_hash=hash_token("token"),
        status="active",
        expires_at=expires_at,
    )
    user_session.user = user
    return user, user_session


def test_expired_session_is_ignored() -> None:
    _user, user_session = _user_session(expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
    session = _Session(user_session)

    assert get_user_by_session_token(session, "token") is None
    assert user_session.last_seen_at is None
    assert session.flush_calls == 0


def test_active_session_updates_last_seen() -> None:
    user, user_session = _user_session(expires_at=datetime.now(timezone.utc) + timedelta(hours=1))
    session = _Session(user_session)

    assert get_user_by_session_token(session, "token") is user
    assert user_session.last_seen_at is not None
    assert session.flush_calls == 1
