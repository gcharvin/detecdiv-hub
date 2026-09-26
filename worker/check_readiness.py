"""Read-only preflight for a worker before it starts claiming jobs."""

from __future__ import annotations

import shutil
import socket
from pathlib import Path

from sqlalchemy import select

from api.config import get_settings
from api.db import SessionLocal
from api.models import ExecutionTarget
from worker.path_mappings import parse_worker_path_mappings


def main() -> None:
    settings = get_settings()
    target_key = settings.worker_target_key.strip()
    if not target_key:
        raise RuntimeError("DETECDIV_HUB_WORKER_TARGET_KEY is required")
    mappings = parse_worker_path_mappings(settings.worker_path_mappings)
    command = settings.matlab_command.strip()
    if not command or shutil.which(command) is None:
        raise RuntimeError(f"MATLAB executable is not available: {command!r}")
    if not settings.matlab_repo_root.strip():
        raise RuntimeError("DETECDIV_HUB_MATLAB_REPO_ROOT is required")
    repo_root = Path(settings.matlab_repo_root).expanduser()
    if not repo_root.is_dir():
        raise RuntimeError(f"DetecDiv MATLAB repository is not accessible: {repo_root}")
    with SessionLocal() as session:
        target = session.scalars(
            select(ExecutionTarget).where(ExecutionTarget.target_key == target_key)
        ).first()
        if target is None:
            raise RuntimeError(f"Execution target does not exist: {target_key}")
        if not target.supports_matlab:
            raise RuntimeError(f"Execution target does not declare MATLAB support: {target_key}")
        print(f"Execution target: {target_key} ({target.id})")
    print(f"Worker host: {socket.gethostname()}")
    print(f"MATLAB executable: {shutil.which(command)}")
    print(f"DetecDiv repository: {repo_root}")
    print(f"Worker path mappings: {len(mappings)}")
    print(f"Allowed job kinds: {settings.worker_job_kinds or 'all'}")
    print(f"Excluded job kinds: {settings.worker_excluded_job_kinds or 'none'}")
    print(f"Claim unassigned jobs: {settings.worker_claim_unassigned_jobs}")
    print(f"Periodic schedulers enabled: {settings.worker_enable_schedulers}")


if __name__ == "__main__":
    main()
