from __future__ import annotations

import subprocess
import sys
import time
from contextlib import contextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psutil
import pytest
from fastapi import HTTPException

from api.models import Job, Project, User
from api.routes_pipeline_runs import force_stop_pipeline_run
from worker.executors.matlab_executor import MatlabCommandForcedStop, run_matlab_command
from worker.pipeline_run_executor import pipeline_force_stop_requested


class SessionStub:
    def __init__(self, job):
        self.job = job
        self.commits = 0

    def get(self, model, identity, **kwargs):
        if model is Job:
            assert kwargs == {"with_for_update": True, "populate_existing": True}
            return self.job
        assert model is Project
        return object()

    def commit(self):
        self.commits += 1

    def refresh(self, job):
        assert job is self.job


def job_fixture(status="running", supported=True):
    return Job(
        id=uuid4(),
        status=status,
        params_json={"job_kind": "pipeline_run"},
        result_json={"worker_runtime": {"force_stop_supported": supported}},
    )


def admin():
    return User(id=uuid4(), user_key="alex", display_name="Alex", role="admin")


@pytest.mark.parametrize("status", ["running", "cancelling"])
def test_force_request_keeps_resources_and_records_first_authenticated_actor(status):
    job = job_fixture(status)
    session = SessionStub(job)
    user = admin()
    original_result = job.result_json.copy()
    force_stop_pipeline_run(job.id, db=session, current_user=user)
    requested_at = job.force_stop_requested_at
    assert requested_at is not None
    assert job.status == "cancelling"
    assert job.force_stop_requested_by == "alex"
    assert job.force_stop_requested_by_user_id == user.id
    assert job.force_stopped_at is None
    assert job.finished_at is None
    assert job.result_json == original_result  # No progress/runtime JSON lost.
    force_stop_pipeline_run(job.id, db=session, current_user=admin())
    assert job.force_stop_requested_at == requested_at
    assert job.force_stop_requested_by_user_id == user.id


@pytest.mark.parametrize("status", ["queued", "done", "failed", "cancelled"])
def test_force_request_rejects_inactive_job(status):
    job = job_fixture(status)
    with pytest.raises(HTTPException) as exc:
        force_stop_pipeline_run(job.id, db=SessionStub(job), current_user=admin())
    assert exc.value.status_code == 409
    assert job.force_stop_requested_at is None


def test_force_request_rejects_uncontained_old_worker():
    job = job_fixture(supported=False)
    with pytest.raises(HTTPException, match="isolated process scope") as exc:
        force_stop_pipeline_run(job.id, db=SessionStub(job), current_user=admin())
    assert exc.value.status_code == 409


def test_force_request_requires_project_edit_access(monkeypatch):
    job = job_fixture()
    job.project_id = uuid4()
    monkeypatch.setattr("api.routes_pipeline_runs.ensure_project_readable", lambda p, u: p)
    monkeypatch.setattr("api.routes_pipeline_runs.user_can_edit_project", lambda p, u: False)
    with pytest.raises(HTTPException) as exc:
        force_stop_pipeline_run(job.id, db=SessionStub(job), current_user=admin())
    assert exc.value.status_code == 403
    assert job.force_stop_requested_at is None


def test_force_request_without_project_requires_admin_or_service():
    job = job_fixture()
    user = admin()
    user.role = "user"
    with pytest.raises(HTTPException) as exc:
        force_stop_pipeline_run(job.id, db=SessionStub(job), current_user=user)
    assert exc.value.status_code == 404


def test_force_control_reads_database_column_even_with_stale_job():
    job = job_fixture()

    class ControlSession:
        def scalar(self, stmt):
            assert "jobs.force_stop_requested_at" in str(stmt)
            return "fresh database request"

        def commit(self):
            pass

    assert job.force_stop_requested_at is None
    assert pipeline_force_stop_requested(ControlSession(), job=job)


TREE_SCRIPT = """
import json, os, pathlib, subprocess, sys, time
folder = pathlib.Path(sys.argv[1])
role = int(sys.argv[2])
detached = sys.argv[3] == "detached"
if role < 2:
    kwargs = {"start_new_session": True} if detached and os.name != "nt" else {}
    subprocess.Popen([sys.executable, __file__, str(folder), str(role+1), sys.argv[3]], **kwargs)
print("role=" + str(role), flush=True)
(folder / (str(role) + ".ready")).write_text(str(os.getpid()))
(folder / (str(role) + ".ready")).replace(folder / (str(role) + ".pid"))
if detached and role < 2:
    sys.exit(0)
while True:
    time.sleep(.1)
"""


def has_exited(process):
    try:
        return not process.is_running() or process.status() == psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return True


@pytest.mark.parametrize("detached", [False, True])
def test_real_tree_killed_other_job_survives_and_worker_can_execute_again(tmp_path, detached):
    script = tmp_path / "tree.py"
    script.write_text(TREE_SCRIPT)
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    deadline = time.monotonic() + 20
    pids = []
    started = []

    def stop_when_ready():
        assert time.monotonic() < deadline, "Job descendants failed to start"
        files = list(tmp_path.glob("*.pid"))
        if len(files) == 3:
            pids[:] = [
                psutil.Process(int(path.read_text()))
                for path in files
                if psutil.pid_exists(int(path.read_text()))
            ]
            return True
        return False

    try:
        with pytest.raises(MatlabCommandForcedStop, match="all job processes have exited"):
            run_matlab_command(
                [
                    sys.executable,
                    str(script),
                    str(tmp_path),
                    "0",
                    "detached" if detached else "normal",
                ],
                force_stop_callback=stop_when_ready,
                started_callback=started.append,
                stdout_path=tmp_path / "stdout.log",
                stderr_path=tmp_path / "stderr.log",
            )
        assert started
        assert pids
        assert all(has_exited(p) for p in pids)
        assert unrelated.poll() is None
        assert "role=2" in (tmp_path / "stdout.log").read_text()
        # Same worker process immediately executes another isolated command.
        result = run_matlab_command(
            [sys.executable, "-c", "import sys; print('next job'); sys.exit(7)"],
            force_stop_callback=lambda: False,
        )
        assert result.returncode == 7
        assert "next job" in result.stdout
        assert unrelated.poll() is None
    finally:
        unrelated.kill()
        unrelated.wait()


def test_prelaunch_request_does_not_start_a_process(tmp_path):
    started = []
    marker = tmp_path / "should-not-exist"
    with pytest.raises(MatlabCommandForcedStop, match="before MATLAB launch"):
        run_matlab_command(
            [sys.executable, "-c", f"open({str(marker)!r}, 'w').close()"],
            force_stop_callback=lambda: True,
            started_callback=started.append,
        )
    assert not started
    assert not marker.exists()


def test_callback_failure_cleans_up_entire_tree(tmp_path):
    script = tmp_path / "tree.py"
    script.write_text(TREE_SCRIPT)
    processes = []
    deadline = time.monotonic() + 20

    def broken_control():
        assert time.monotonic() < deadline
        files = list(tmp_path.glob("*.pid"))
        if len(files) == 3:
            processes[:] = [psutil.Process(int(path.read_text())) for path in files]
            raise RuntimeError("database unavailable")
        return False

    with pytest.raises(RuntimeError, match="database unavailable"):
        run_matlab_command(
            [sys.executable, str(script), str(tmp_path), "0", "normal"],
            force_stop_callback=broken_control,
        )
    assert processes
    assert all(has_exited(p) for p in processes)


def test_pipeline_worker_confirms_exit_preserves_logs_then_releases_locks(monkeypatch, tmp_path):
    from worker import pipeline_run_executor as executor
    from worker import run_worker

    script = tmp_path / "tree.py"
    script.write_text(TREE_SCRIPT)
    job = job_fixture()
    processes, artifacts, released = [], [], []
    deadline = time.monotonic() + 20

    class PipelineSession:
        def get(self, model, identity, **kwargs):
            assert model is Job and identity == job.id
            return job

        def scalar(self, stmt):
            assert time.monotonic() < deadline
            files = list(tmp_path.glob("*.pid"))
            if len(files) == 3 and job.force_stop_requested_at is None:
                processes[:] = [psutil.Process(int(path.read_text())) for path in files]
                force_stop_pipeline_run(job.id, db=self, current_user=admin())
            return job.force_stop_requested_at

        def commit(self):
            pass

        def refresh(self, record):
            assert record is job

        def add(self, artifact):
            artifacts.append(artifact)

        def flush(self):
            pass

    session = PipelineSession()
    monkeypatch.setattr(
        executor,
        "get_settings",
        lambda: SimpleNamespace(
            matlab_repo_root="test-runtime",
            matlab_command="matlab",
            worker_path_mappings="",
        ),
    )
    monkeypatch.setattr(
        "worker.matlab_code_checkout.prepare_job_checkout", lambda *args: ("test-runtime", "a" * 40)
    )
    monkeypatch.setattr(
        "worker.matlab_code_checkout.job_workspace", lambda *args: nullcontext(str(tmp_path))
    )
    monkeypatch.setattr("worker.matlab_code_checkout.job_environment", lambda *args: None)
    monkeypatch.setattr(
        executor, "normalize_pipeline_run_payload", lambda *args, **kwargs: {"execution": {}}
    )
    monkeypatch.setattr(executor, "persist_prepared_pipeline_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        executor,
        "evaluate_pipeline_dependency_preflight",
        lambda *args, **kwargs: {"status": "passed"},
    )
    monkeypatch.setattr(executor, "persist_pipeline_preflight", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        executor, "ensure_cancel_token_path", lambda *args, **kwargs: str(tmp_path / "cancel")
    )
    monkeypatch.setattr(executor, "resolve_matlab_max_threads", lambda *args, **kwargs: 1)
    monkeypatch.setattr(
        executor,
        "build_matlab_batch_command",
        lambda *args, **kwargs: [
            sys.executable,
            str(script),
            str(tmp_path),
            "0",
            "normal",
        ],
    )
    monkeypatch.setattr(executor, "heartbeat_project_locks_for_job", lambda *args, **kwargs: None)

    with pytest.raises(executor.PipelineRunCancelled):
        executor.execute_pipeline_run_job(session, job=job)
    assert job.status == "cancelling"
    assert job.force_stopped_at >= job.force_stop_requested_at
    assert all(has_exited(p) for p in processes)
    assert {a.artifact_kind for a in artifacts} == {"stdout_log", "stderr_log"}
    assert all(Path(a.uri).is_file() for a in artifacts)

    @contextmanager
    def session_scope():
        yield session

    def release(_session, *, job_id):
        assert all(has_exited(p) for p in processes)
        assert job.force_stopped_at is not None
        released.append(job_id)

    monkeypatch.setattr(run_worker, "session_scope", session_scope)
    monkeypatch.setattr(run_worker, "get_settings", lambda: SimpleNamespace(worker_target_key=""))
    monkeypatch.setattr(run_worker, "release_project_locks_for_job", release)
    monkeypatch.setattr(
        run_worker, "sync_indexing_job_from_worker_job", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(run_worker, "update_worker_target_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_worker, "resume_qwen_if_gpu_is_idle", lambda *args, **kwargs: None)
    run_worker.mark_job_cancelled(job.id, "Forced stop confirmed")
    assert job.status == "cancelled"
    assert job.finished_at >= job.force_stopped_at
    assert released == [job.id]
