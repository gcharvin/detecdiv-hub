from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

from worker import run_worker


class _DummySession:
    def __init__(self, jobs) -> None:  # noqa: ANN001
        self.jobs = jobs

    def scalars(self, _stmt):  # noqa: ANN001
        return self.jobs


def test_recover_orphaned_job_releases_its_project_locks(monkeypatch) -> None:
    job_id = uuid4()
    target = SimpleNamespace(id=uuid4(), display_name="DetecDiv Server")
    stale_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    job = SimpleNamespace(
        id=job_id,
        status="running",
        heartbeat_at=stale_time,
        updated_at=stale_time,
        started_at=stale_time,
        created_at=stale_time,
        error_text=None,
        finished_at=None,
        params_json={"job_kind": "pipeline_run"},
    )
    released_job_ids = []

    monkeypatch.setattr(run_worker, "active_worker_current_job_ids", lambda *_args, **_kwargs: set())
    monkeypatch.setattr(run_worker, "sync_indexing_job_from_worker_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run_worker, "update_worker_target_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        run_worker,
        "release_project_locks_for_job",
        lambda _session, *, job_id: released_job_ids.append(job_id),
    )

    recovered = run_worker.recover_orphaned_jobs(_DummySession([job]), target=target)

    assert recovered == 1
    assert job.status == "failed"
    assert released_job_ids == [job_id]
