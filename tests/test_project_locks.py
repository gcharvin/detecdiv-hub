from __future__ import annotations

from uuid import uuid4

from api.services import project_locks


class _DummyScalars:
    def __iter__(self):
        return iter(())


class _DummySession:
    def __init__(self) -> None:
        self.flush_calls = 0
        self.scalar_calls = 0

    def flush(self) -> None:
        self.flush_calls += 1

    def scalars(self, stmt):  # noqa: ANN001
        self.scalar_calls += 1
        return _DummyScalars()


def test_active_project_locks_flushes_after_expiring_stale_leases(monkeypatch) -> None:
    session = _DummySession()

    monkeypatch.setattr(project_locks, "expire_stale_project_locks", lambda _session: 1)

    locks = project_locks.active_project_locks(session, project_id=uuid4())

    assert locks == []
    assert session.flush_calls == 1
    assert session.scalar_calls == 1


def test_active_project_locks_skips_flush_when_nothing_expired(monkeypatch) -> None:
    session = _DummySession()

    monkeypatch.setattr(project_locks, "expire_stale_project_locks", lambda _session: 0)

    locks = project_locks.active_project_locks(session, project_id=uuid4())

    assert locks == []
    assert session.flush_calls == 0
    assert session.scalar_calls == 1
