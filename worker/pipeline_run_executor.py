from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.config import get_settings
from api.models import Artifact, Job, Pipeline, Project, ProjectLocation
from api.services.project_deletion import resolve_project_location_paths
from worker.executors.matlab_executor import build_matlab_batch_command, run_matlab_command


def execute_pipeline_run_job(session: Session, *, job: Job) -> dict[str, Any]:
    settings = get_settings()
    repo_root = str(settings.matlab_repo_root or "").strip()
    if not repo_root:
        raise ValueError("DETECDIV_HUB_MATLAB_REPO_ROOT is required for pipeline_run jobs.")

    matlab_command = str(settings.matlab_command or "matlab").strip() or "matlab"

    payload = normalize_pipeline_run_payload(session, job=job)
    with tempfile.TemporaryDirectory(prefix="detecdiv_pipeline_job_") as tmpdir:
        tmp_path = Path(tmpdir)
        payload_path = tmp_path / "pipeline_run_job.json"
        result_path = tmp_path / "pipeline_run_result.json"
        stdout_path = tmp_path / "matlab_stdout.log"
        stderr_path = tmp_path / "matlab_stderr.log"

        payload.setdefault("execution", {})
        payload["execution"]["result_json_path"] = str(result_path)

        payload_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        entrypoint = f"detecdiv_run_pipeline_job('{matlab_escape(str(payload_path))}')"
        command = build_matlab_batch_command(repo_root, entrypoint, matlab_command=matlab_command)
        completed = run_matlab_command(command)
        stdout_path.write_text(completed.stdout or "", encoding="utf-8")
        stderr_path.write_text(completed.stderr or "", encoding="utf-8")

        result_json: dict[str, Any] = {}
        if result_path.is_file():
            try:
                result_json = json.loads(result_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                result_json = {}

        if completed.returncode != 0:
            error_text = build_pipeline_run_error(
                completed.returncode,
                completed.stdout or "",
                completed.stderr or "",
                result_json,
            )
            raise RuntimeError(error_text)

        if not result_json:
            result_json = {
                "status": "done",
                "message": "MATLAB batch returned success without a result JSON payload.",
            }

        attach_pipeline_run_artifacts(
            session,
            job=job,
            result_json=result_json,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )

        return {
            **result_json,
            "worker_runtime": {
                "engine": "matlab",
                "command": matlab_command,
                "repo_root": repo_root,
                "returncode": completed.returncode,
                "stdout_log": str(stdout_path),
                "stderr_log": str(stderr_path),
            },
        }


def normalize_pipeline_run_payload(session: Session, *, job: Job) -> dict[str, Any]:
    payload = dict(job.params_json or {})
    payload["job_kind"] = "pipeline_run"
    payload["job_id"] = str(job.id)

    project_ref = dict(payload.get("project_ref") or {})
    if job.project_id and not project_ref.get("project_id"):
        project_ref["project_id"] = str(job.project_id)
    if not project_ref.get("project_mat_path") and job.project_id:
        project_ref["project_mat_path"] = resolve_project_mat_path(session, project_id=job.project_id)
    payload["project_ref"] = project_ref

    pipeline_ref = dict(payload.get("pipeline_ref") or {})
    if job.pipeline_id and not pipeline_ref.get("pipeline_id"):
        pipeline_ref["pipeline_id"] = str(job.pipeline_id)
    if job.pipeline_id:
        fill_pipeline_ref_from_registry(session, pipeline_id=job.pipeline_id, pipeline_ref=pipeline_ref)
    payload["pipeline_ref"] = pipeline_ref

    execution = dict(payload.get("execution") or {})
    execution.setdefault("requested_mode", job.requested_mode)
    if job.execution_target_id and not execution.get("execution_target_id"):
        execution["execution_target_id"] = str(job.execution_target_id)
    execution.setdefault("allow_gui", False)
    execution.setdefault("interactive", False)
    execution.setdefault("save_project", True)
    payload["execution"] = execution

    payload.setdefault("run_request", {})
    return payload


def resolve_project_mat_path(session: Session, *, project_id) -> str:
    project = session.scalars(
        select(Project)
        .options(joinedload(Project.locations).joinedload(ProjectLocation.storage_root))
        .where(Project.id == project_id)
    ).first()
    if project is None:
        raise ValueError(f"Project {project_id} not found.")
    if not project.locations:
        raise ValueError(f"Project {project.project_name} has no registered locations.")

    preferred = sorted(project.locations, key=project_location_priority)
    for location in preferred:
        file_path, _ = resolve_project_location_paths(location)
        if file_path:
            return file_path
    raise ValueError(f"Project {project.project_name} has no resolvable MAT location.")


def project_location_priority(location: ProjectLocation) -> tuple[int, int, str]:
    host_scope = getattr(location.storage_root, "host_scope", "") or ""
    return (
        0 if location.is_preferred else 1,
        0 if host_scope == "server" else 1,
        str(location.relative_path or ""),
    )


def fill_pipeline_ref_from_registry(session: Session, *, pipeline_id, pipeline_ref: dict[str, Any]) -> None:
    pipeline = session.get(Pipeline, pipeline_id)
    if pipeline is None:
        raise ValueError(f"Pipeline {pipeline_id} not found.")

    metadata = dict(pipeline.metadata_json or {})
    if not pipeline_ref.get("pipeline_key") and pipeline.pipeline_key:
        pipeline_ref["pipeline_key"] = pipeline.pipeline_key

    for key in ("pipeline_bundle_uri", "pipeline_json_path", "export_manifest_uri"):
        if not pipeline_ref.get(key) and metadata.get(key):
            pipeline_ref[key] = metadata.get(key)

    observed = metadata.get("observed") or {}
    observed_path = observed.get("pipeline_path")
    if not pipeline_ref.get("pipeline_json_path") and observed_path:
        pipeline_ref["pipeline_json_path"] = observed_path


def build_pipeline_run_error(returncode: int, stdout: str, stderr: str, result_json: dict[str, Any]) -> str:
    parts = [f"pipeline_run MATLAB batch failed with exit code {returncode}."]
    result_error = str(result_json.get("error") or "").strip()
    if result_error:
        parts.append(result_error)

    stdout_tail = tail_text(stdout)
    stderr_tail = tail_text(stderr)
    if stdout_tail:
        parts.append(f"STDOUT tail:\n{stdout_tail}")
    if stderr_tail:
        parts.append(f"STDERR tail:\n{stderr_tail}")
    return "\n\n".join(parts)


def tail_text(text: str, *, max_lines: int = 40) -> str:
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return ""
    return "\n".join(lines[-max_lines:])


def attach_pipeline_run_artifacts(
    session: Session,
    *,
    job: Job,
    result_json: dict[str, Any],
    stdout_path: Path,
    stderr_path: Path,
) -> None:
    artifact_rows: list[Artifact] = []

    for artifact in list(result_json.get("artifacts") or []):
        if not isinstance(artifact, dict):
            continue
        path = str(artifact.get("path") or "").strip()
        kind = str(artifact.get("kind") or "file").strip() or "file"
        if not path:
            continue
        artifact_rows.append(
            Artifact(
                job_id=job.id,
                artifact_kind=kind,
                uri=path,
                metadata_json={"source": "pipeline_run"},
            )
        )

    artifact_rows.append(
        Artifact(
            job_id=job.id,
            artifact_kind="stdout_log",
            uri=str(stdout_path),
            metadata_json={"source": "pipeline_run"},
        )
    )
    artifact_rows.append(
        Artifact(
            job_id=job.id,
            artifact_kind="stderr_log",
            uri=str(stderr_path),
            metadata_json={"source": "pipeline_run"},
        )
    )

    for row in artifact_rows:
        session.add(row)
    session.flush()


def matlab_escape(path: str) -> str:
    return path.replace("\\", "/").replace("'", "''")
