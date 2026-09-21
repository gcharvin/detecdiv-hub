"""Execution of explicitly published legacy MATLAB routines.

This is deliberately not an arbitrary shell runner: the requested function must
live below an allow-listed shared code root and its name must match its file.
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from api.config import get_settings
from api.models import Job
from worker.executors.matlab_executor import build_matlab_batch_command, run_matlab_command


LEGACY_CODE_ROOT = Path("/data/Alexander/code/gillestest").resolve()


def execute_legacy_matlab_job(session: Session, *, job: Job) -> dict:
    settings = get_settings()
    repo_root = str(settings.matlab_repo_root or "").strip()
    if not repo_root:
        raise ValueError("DETECDIV_HUB_MATLAB_REPO_ROOT is required for legacy_matlab jobs.")

    payload = dict(job.params_json or {})
    routine_path = Path(str(payload.get("routine_path") or "")).resolve()
    function_name = str(payload.get("function_name") or "").strip()
    if not function_name or not function_name.replace("_", "").isalnum():
        raise ValueError("legacy_matlab function_name must be a MATLAB identifier.")
    if routine_path.suffix.lower() != ".m" or routine_path.stem != function_name:
        raise ValueError("legacy_matlab routine_path must be the matching .m function file.")
    try:
        routine_path.relative_to(LEGACY_CODE_ROOT)
    except ValueError as exc:
        raise ValueError(f"legacy_matlab routine must be under {LEGACY_CODE_ROOT}") from exc
    if not routine_path.is_file():
        raise ValueError(f"legacy_matlab routine does not exist: {routine_path}")

    with tempfile.TemporaryDirectory(prefix="detecdiv_legacy_matlab_") as tmpdir:
        tmp = Path(tmpdir)
        result_path = tmp / "result.json"
        payload_path = tmp / "job.json"
        stdout_path = tmp / "matlab_stdout.log"
        stderr_path = tmp / "matlab_stderr.log"
        payload.update({"job_id": str(job.id), "result_json_path": str(result_path)})
        payload_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        entrypoint = (
            "detecdiv_hub_run_legacy_matlab_job(" + matlab_quote(str(payload_path)) + ")"
        )
        command = build_matlab_batch_command(
            repo_root, entrypoint, matlab_command=str(settings.matlab_command or "matlab")
        )
        completed = run_matlab_command(
            command,
            heartbeat_callback=lambda: heartbeat(session, job),
            heartbeat_interval_sec=10.0,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
        result = read_result(result_path)
        if completed.returncode != 0:
            tail = (completed.stderr or completed.stdout or "")[-4000:]
            raise RuntimeError(f"legacy MATLAB routine failed with exit code {completed.returncode}.\n{tail}")
        return {
            **result,
            "worker_runtime": {
                "engine": "matlab",
                "routine_path": str(routine_path),
                "function_name": function_name,
                "stdout_log": str(stdout_path),
                "stderr_log": str(stderr_path),
            },
        }


def heartbeat(session: Session, job: Job) -> None:
    record = session.get(Job, job.id)
    if record is not None:
        record.heartbeat_at = datetime.now(timezone.utc)
        record.updated_at = record.heartbeat_at
        session.commit()


def read_result(path: Path) -> dict:
    if not path.is_file():
        return {"status": "done", "message": "Legacy MATLAB routine completed."}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"status": "done", "message": "Legacy MATLAB routine completed without readable result JSON."}


def matlab_quote(value: str) -> str:
    return "'" + value.replace("\\", "/").replace("'", "''") + "'"
