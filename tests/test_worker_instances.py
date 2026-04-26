from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from api.services.worker_instances import summarize_worker_instances


def test_summarize_worker_instances_counts_active_workers():
    now = datetime.now(timezone.utc)
    workers = [
        SimpleNamespace(
            worker_instance=f"@{idx}",
            health="idle",
            current_job_id=None,
            claimed_at=None,
            last_seen_at=now,
            poll_interval_sec=5.0,
        )
        for idx in range(1, 4)
    ]

    summary = summarize_worker_instances(workers, max_concurrent_jobs=3, now=now)

    assert summary["registered_workers"] == 3
    assert summary["worker_count"] == 3
    assert summary["online_workers"] == 3
    assert summary["stale_workers"] == 0
    assert summary["capacity_full"] is False
    assert summary["worker_instances"] == ["@1", "@2", "@3"]


def test_summarize_worker_instances_reports_capacity_full():
    now = datetime.now(timezone.utc)
    workers = [
        SimpleNamespace(
            worker_instance=f"@{idx}",
            health="busy",
            current_job_id=uuid4(),
            claimed_at=now,
            last_seen_at=now,
            poll_interval_sec=5.0,
        )
        for idx in range(1, 4)
    ]

    summary = summarize_worker_instances(workers, max_concurrent_jobs=3, now=now)

    assert summary["busy_workers"] == 3
    assert summary["current_job_count"] == 3
    assert summary["capacity_full"] is True
