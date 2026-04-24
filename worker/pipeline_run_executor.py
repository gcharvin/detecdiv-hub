from __future__ import annotations

import json
import re
import tempfile
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.config import get_settings
from api.models import Artifact, ExecutionTarget, Job, Pipeline, Project, ProjectLocation
from api.services.project_deletion import resolve_project_location_paths
from worker.executors.matlab_executor import build_matlab_batch_command, run_matlab_command
from worker.pipeline_dependency_preflight import (
    build_preflight_error_text,
    evaluate_pipeline_dependency_preflight,
)
from worker.pipeline_prepared_run import merge_prepared_pipeline_run, persist_prepared_pipeline_run


PIPELINE_REF_PATH_KEYS = ("export_manifest_uri", "pipeline_bundle_uri", "pipeline_json_path")


class PipelineRunCancelled(RuntimeError):
    pass


class PipelinePreflightFailed(RuntimeError):
    pass


def execute_pipeline_run_job(session: Session, *, job: Job) -> dict[str, Any]:
    settings = get_settings()
    repo_root = str(settings.matlab_repo_root or "").strip()
    if not repo_root:
        raise ValueError("DETECDIV_HUB_MATLAB_REPO_ROOT is required for pipeline_run jobs.")

    matlab_command = str(settings.matlab_command or "matlab").strip() or "matlab"

    payload = normalize_pipeline_run_payload(session, job=job)
    persist_prepared_pipeline_run(session, job=job, payload=payload)
    preflight = evaluate_pipeline_dependency_preflight(payload)
    persist_pipeline_preflight(session, job=job, preflight=preflight)
    if preflight.get("status") == "failed":
        raise PipelinePreflightFailed(build_preflight_error_text(preflight))

    with tempfile.TemporaryDirectory(prefix="detecdiv_pipeline_job_") as tmpdir:
        tmp_path = Path(tmpdir)
        payload_path = tmp_path / "pipeline_run_job.json"
        result_path = tmp_path / "pipeline_run_result.json"
        stdout_path = tmp_path / "matlab_stdout.log"
        stderr_path = tmp_path / "matlab_stderr.log"

        payload.setdefault("execution", {})
        payload["execution"]["result_json_path"] = str(result_path)
        payload["execution"]["cancel_token_file"] = ensure_cancel_token_path(session, job=job, payload=payload)

        payload_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        matlab_max_threads = resolve_matlab_max_threads(session, job=job)
        entrypoint = build_pipeline_matlab_entrypoint(payload_path, matlab_max_threads=matlab_max_threads)
        command = build_matlab_batch_command(repo_root, entrypoint, matlab_command=matlab_command)
        completed = run_matlab_command(
            command,
            heartbeat_callback=lambda: update_job_heartbeat(session, job=job),
            progress_callback=lambda: update_pipeline_run_progress(
                session,
                job=job,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
            ),
            heartbeat_interval_sec=10.0,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )

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
            if is_pipeline_cancelled(session, job=job, result_json=result_json, error_text=error_text):
                raise PipelineRunCancelled(error_text)
            raise RuntimeError(error_text)

        if not result_json:
            result_json = {
                "status": "done",
                "message": "MATLAB batch returned success without a result JSON payload.",
            }
        result_json = merge_prepared_pipeline_run(payload=payload, result_json=result_json)
        result_json["preflight"] = preflight

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
                "matlab_max_threads": matlab_max_threads,
                "returncode": completed.returncode,
                "stdout_log": str(stdout_path),
                "stderr_log": str(stderr_path),
            },
        }


def persist_pipeline_preflight(session: Session, *, job: Job, preflight: dict[str, Any]) -> None:
    job_record = session.get(Job, job.id)
    if job_record is None:
        return
    result_json = dict(job_record.result_json or {})
    result_json["preflight"] = preflight
    job_record.result_json = result_json
    session.commit()


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
    resolve_pipeline_ref_for_server(session, job=job, project_ref=project_ref, pipeline_ref=pipeline_ref)
    payload["pipeline_ref"] = pipeline_ref

    execution = dict(payload.get("execution") or {})
    execution.setdefault("requested_mode", job.requested_mode)
    if job.execution_target_id and not execution.get("execution_target_id"):
        execution["execution_target_id"] = str(job.execution_target_id)
    execution.setdefault("allow_gui", False)
    execution.setdefault("interactive", False)
    execution.setdefault("save_project", True)
    payload["execution"] = execution

    run_request = dict(payload.get("run_request") or {})
    gpu = dict(run_request.get("gpu") or {})
    if not gpu.get("mode"):
        gpu["mode"] = default_gpu_mode_for_job(session, job=job)
    run_request["gpu"] = gpu
    payload["run_request"] = run_request
    return payload


def default_gpu_mode_for_job(session: Session, *, job: Job) -> str:
    if job.execution_target_id:
        target = session.get(ExecutionTarget, job.execution_target_id)
        if target is not None and bool(target.supports_gpu):
            return "force_gpu"
    return "module_default"


def ensure_cancel_token_path(session: Session, *, job: Job, payload: dict[str, Any]) -> str:
    execution = dict(payload.get("execution") or {})
    existing = str(execution.get("cancel_token_file") or "").strip()
    if existing:
        return existing

    token_dir = Path(tempfile.gettempdir()) / "detecdiv-hub" / "cancel-tokens"
    token_dir.mkdir(parents=True, exist_ok=True)
    token_path = token_dir / f"{job.id}.cancel"

    job_record = session.get(Job, job.id)
    if job_record is not None:
        params_json = dict(job_record.params_json or {})
        params_execution = dict(params_json.get("execution") or {})
        params_execution["cancel_token_file"] = str(token_path)
        params_json["execution"] = params_execution
        job_record.params_json = params_json
        session.commit()

    return str(token_path)


def build_pipeline_matlab_entrypoint(payload_path: Path, *, matlab_max_threads: int | None) -> str:
    run_command = f"detecdiv_run_pipeline_job('{matlab_escape(str(payload_path))}')"
    if matlab_max_threads is None:
        return run_command
    return f"maxNumCompThreads({matlab_max_threads}); {run_command}"


def resolve_matlab_max_threads(session: Session, *, job: Job) -> int | None:
    execution = dict((job.params_json or {}).get("execution") or {})
    from_payload = read_positive_int(execution.get("matlab_max_threads"))
    if from_payload is not None:
        return from_payload

    if not job.execution_target_id:
        return None
    target = session.get(ExecutionTarget, job.execution_target_id)
    if target is None:
        return None
    return read_positive_int((target.metadata_json or {}).get("matlab_max_threads"))


def read_positive_int(value) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 1:
        return None
    return parsed


def default_gpu_mode_for_job(session: Session, *, job: Job) -> str:
    if job.execution_target_id:
        target = session.get(ExecutionTarget, job.execution_target_id)
        if target is not None and bool(target.supports_gpu):
            return "force_gpu"
    return "module_default"


def ensure_cancel_token_path(session: Session, *, job: Job, payload: dict[str, Any]) -> str:
    execution = dict(payload.get("execution") or {})
    existing = str(execution.get("cancel_token_file") or "").strip()
    if existing:
        return existing

    token_dir = Path(tempfile.gettempdir()) / "detecdiv-hub" / "cancel-tokens"
    token_dir.mkdir(parents=True, exist_ok=True)
    token_path = token_dir / f"{job.id}.cancel"

    job_record = session.get(Job, job.id)
    if job_record is not None:
        params_json = dict(job_record.params_json or {})
        params_execution = dict(params_json.get("execution") or {})
        params_execution["cancel_token_file"] = str(token_path)
        params_json["execution"] = params_execution
        job_record.params_json = params_json
        session.commit()

    return str(token_path)


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


def update_job_heartbeat(session: Session, *, job: Job) -> None:
    now = datetime.now(timezone.utc)
    job_record = session.get(Job, job.id)
    if job_record is None:
        return
    job_record.heartbeat_at = now
    job_record.updated_at = now
    if job_record.status == "cancelling":
        write_cancel_token(job_record)
    session.commit()


def update_pipeline_run_progress(
    session: Session,
    *,
    job: Job,
    stdout_path: Path,
    stderr_path: Path,
) -> None:
    now = datetime.now(timezone.utc)
    job_record = session.get(Job, job.id)
    if job_record is None:
        return
    progress = build_pipeline_run_progress(stdout_path=stdout_path, stderr_path=stderr_path)
    result_json = dict(job_record.result_json or {})
    result_json["progress"] = progress
    job_record.result_json = result_json
    job_record.heartbeat_at = now
    job_record.updated_at = now
    if job_record.status == "cancelling":
        write_cancel_token(job_record)
    session.commit()


def write_cancel_token(job: Job) -> None:
    token_path = cancel_token_path_for_job(job)
    if not token_path:
        return
    try:
        path = Path(token_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text(f"cancelled_at={datetime.now(timezone.utc).isoformat()}\njob_id={job.id}\n", encoding="utf-8")
    except OSError:
        return


def cancel_token_path_for_job(job: Job) -> str:
    params_json = dict(job.params_json or {})
    execution = dict(params_json.get("execution") or {})
    token_path = str(execution.get("cancel_token_file") or "").strip()
    if token_path:
        return token_path
    return str(Path(tempfile.gettempdir()) / "detecdiv-hub" / "cancel-tokens" / f"{job.id}.cancel")


def is_pipeline_cancelled(
    session: Session,
    *,
    job: Job,
    result_json: dict[str, Any],
    error_text: str,
) -> bool:
    job_record = session.get(Job, job.id)
    if job_record is not None and job_record.status in {"cancelling", "cancelled"}:
        return True
    status = str(result_json.get("status") or "").lower()
    if status in {"cancelled", "canceled"}:
        return True
    lowered = error_text.lower()
    return "runpipeline:cancelled" in lowered or "cancelled by user" in lowered or "canceled by user" in lowered


def build_pipeline_run_progress(*, stdout_path: Path, stderr_path: Path) -> dict[str, Any]:
    stdout_lines = tail_log_lines(stdout_path, max_lines=30)
    stderr_lines = tail_log_lines(stderr_path, max_lines=15)
    recent_lines = [*stdout_lines, *stderr_lines]
    current_step = infer_current_step(recent_lines)
    return {
        "phase": "matlab_running",
        "current_step": current_step,
        "last_message": recent_lines[-1] if recent_lines else "MATLAB running",
        "recent_stdout": stdout_lines[-10:],
        "recent_stderr": stderr_lines[-10:],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def tail_log_lines(path: Path, *, max_lines: int) -> list[str]:
    if not path.is_file():
        return []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    lines = [clean_matlab_log_line(line) for line in text.splitlines()]
    lines = [line for line in lines if line]
    return lines[-max_lines:]


def clean_matlab_log_line(line: str) -> str:
    text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", line)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
    return text.strip()


def infer_current_step(lines: list[str]) -> str:
    patterns = (
        r"DETECDIV_PIPELINE_PROGRESS\s+node_start\s+id=([^\s]+)",
        r"DETECDIV_PIPELINE_PROGRESS\s+node_done\s+id=([^\s]+)",
        r"DETECDIV_PIPELINE_PROGRESS\s+node_failed\s+id=([^\s]+)",
        r"Executing node\s+(.+)",
        r"Running node\s+(.+)",
        r"Node\s+([A-Za-z0-9_.:-]+)",
        r"Pipeline run saved:\s+(.+)",
        r"Saving shallow project",
        r"Raw path relink candidate\s+(.+)",
    )
    for line in reversed(lines):
        for pattern in patterns:
            match = re.search(pattern, line, flags=re.IGNORECASE)
            if match:
                return match.group(1).strip() if match.groups() else line
    return lines[-1] if lines else "MATLAB running"


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

    # Prefer direct pipeline paths over manifest URIs to avoid cross-platform
    # separator issues from Windows-exported manifests.
    for key in ("pipeline_bundle_uri", "pipeline_json_path"):
        if not pipeline_ref.get(key) and metadata.get(key):
            pipeline_ref[key] = metadata.get(key)

    has_direct_path = bool(pipeline_ref.get("pipeline_bundle_uri") or pipeline_ref.get("pipeline_json_path"))
    if not has_direct_path and not pipeline_ref.get("export_manifest_uri") and metadata.get("export_manifest_uri"):
        pipeline_ref["export_manifest_uri"] = metadata.get("export_manifest_uri")

    observed = metadata.get("observed") or {}
    observed_path = observed.get("pipeline_path")
    if not pipeline_ref.get("pipeline_json_path") and observed_path:
        pipeline_ref["pipeline_json_path"] = observed_path


def resolve_pipeline_ref_for_server(
    session: Session,
    *,
    job: Job,
    project_ref: dict[str, Any],
    pipeline_ref: dict[str, Any],
) -> None:
    project_mat_path = str(project_ref.get("project_mat_path") or "").strip()
    if not project_mat_path and job.project_id:
        project_mat_path = resolve_project_mat_path(session, project_id=job.project_id)
    if not project_mat_path:
        return

    for key in PIPELINE_REF_PATH_KEYS:
        candidate = str(pipeline_ref.get(key) or "").strip()
        if not candidate or path_exists(candidate):
            continue

        resolved = resolve_pipeline_path_under_project_dir(project_mat_path, candidate)
        if resolved and path_exists(resolved):
            pipeline_ref[key] = resolved
            pipeline_ref[f"{key}_original"] = candidate
            pipeline_ref["resolution_method"] = "project_dir_relative_pipeline_name"
            return


def resolve_pipeline_path_under_project_dir(project_mat_path: str, candidate: str) -> str:
    project_path = Path(project_mat_path)
    project_dir = project_path.with_suffix("")
    pipeline_leaf = path_leaf(candidate)
    if not pipeline_leaf:
        return ""
    return str(project_dir / pipeline_leaf)


def path_exists(path_text: str) -> bool:
    return Path(path_text).exists()


def path_leaf(path_text: str) -> str:
    parts = str(path_text).replace("\\", "/").rstrip("/").split("/")
    return parts[-1] if parts else ""


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
