from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from worker import manage_workers, record_worker_exit


class Session:
    def __init__(self, target, active=0):
        self.target = target
        self.active = active

    def scalars(self, statement):
        assert statement._for_update_arg is not None
        return SimpleNamespace(one=lambda: self.target)

    def scalar(self, _statement):
        return self.active


@pytest.fixture
def pool(monkeypatch):
    target = SimpleNamespace(id="00000000-0000-0000-0000-000000000001", metadata_json={"worker_autoscale_enabled": False, "worker_scale_applied_instances": 3, "worker_instances_desired": 6, "worker_scale_requested_at": "request-1"})
    session = Session(target)
    @contextmanager
    def begin():
        yield session
    monkeypatch.setattr(manage_workers, "SessionLocal", SimpleNamespace(begin=begin))
    monkeypatch.setattr(manage_workers, "get_settings", lambda: SimpleNamespace(worker_target_key="compute"))
    args = SimpleNamespace(initial_instances=3, configure_script="/allowed/helper.sh", repo_root="/runtime", service_user="worker", env_file="/env", unit_dir="/units")
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        assert target.metadata_json.get("worker_scale_applied_instances") == 3
        return SimpleNamespace(stdout="Configured workers")
    monkeypatch.setattr(manage_workers.subprocess, "run", run)
    return session, args, calls


def test_scaling_drains_and_waits_without_interrupting_running_jobs(pool):
    session, args, calls = pool
    session.active = 2
    manage_workers.reconcile(args)
    assert not calls
    assert session.target.metadata_json["drain_new_jobs"]
    assert session.target.metadata_json["worker_scale_state"] == "waiting_for_jobs"
    session.active = 0
    manage_workers.reconcile(args)
    assert calls[0][-3:] == ["--worker-instances", "6", "--skip-manager-restart"]
    assert session.target.metadata_json["worker_scale_applied_instances"] == 6
    assert not session.target.metadata_json["drain_new_jobs"]


def test_failed_scale_keeps_target_drained_and_does_not_repeat_restarts(pool, monkeypatch):
    session, args, calls = pool
    def fail(command, **kwargs):
        calls.append(command)
        raise manage_workers.subprocess.CalledProcessError(1, command, stderr="Invalid budget")
    monkeypatch.setattr(manage_workers.subprocess, "run", fail)
    manage_workers.reconcile(args)
    manage_workers.reconcile(args)
    assert len(calls) == 1
    assert session.target.metadata_json["worker_scale_state"] == "failed"
    assert session.target.metadata_json["drain_new_jobs"]


def test_same_count_request_releases_temporary_drain_without_restart(pool):
    session, args, calls = pool
    session.target.metadata_json.update(worker_instances_desired=3, worker_scale_state="requested", drain_new_jobs=True, worker_scale_previous_drain=False)
    manage_workers.reconcile(args)
    assert not calls
    assert not session.target.metadata_json["drain_new_jobs"]


def test_existing_manual_drain_is_preserved_after_scaling(pool):
    session, args, calls = pool
    session.target.metadata_json["drain_new_jobs"] = True
    manage_workers.reconcile(args)
    assert calls
    assert session.target.metadata_json["drain_new_jobs"]


def test_oom_exit_is_attributed_only_to_its_active_job(monkeypatch):
    monkeypatch.setenv("SERVICE_RESULT", "oom-kill")
    monkeypatch.setenv("DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB", "32768")
    monkeypatch.setenv("DETECDIV_HUB_WORKER_SWAP_LIMIT_MB", "2048")
    job = SimpleNamespace(id="own-job", status="running")
    session = SimpleNamespace(scalars=lambda s: SimpleNamespace(first=lambda: SimpleNamespace(current_job_id=job.id)), get=lambda *a: job)
    @contextmanager
    def scope():
        yield session
    monkeypatch.setattr(record_worker_exit, "session_scope", scope)
    monkeypatch.setattr(record_worker_exit, "get_settings", lambda: SimpleNamespace(worker_target_key="compute"))
    monkeypatch.setattr(record_worker_exit, "get_worker_instance_id", lambda: "@2")
    failures = []
    monkeypatch.setattr(record_worker_exit, "mark_job_failed", lambda job_id, message: failures.append((job_id, message)))
    record_worker_exit.record_exit()
    assert failures[0][0] == "own-job"
    assert "32768 MiB RAM, 2048 MiB swap" in failures[0][1]
    job.status = "done"
    record_worker_exit.record_exit()
    assert len(failures) == 1
    monkeypatch.setenv("SERVICE_RESULT", "success")
    record_worker_exit.record_exit()
    assert len(failures) == 1
