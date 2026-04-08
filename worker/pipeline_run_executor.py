from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.config import get_settings
from api.models import Artifact, Job, Pipeline, Project, ProjectLocation, ProjectRawLink, RawDataset, RawDatasetLocation
from api.services.project_deletion import resolve_project_location_paths
from worker.executors.matlab_executor import build_matlab_batch_command, run_matlab_command


PIPELINE_REF_PATH_KEYS = ("export_manifest_uri", "pipeline_bundle_uri", "pipeline_json_path")


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
        completed = run_matlab_command(
            command,
            heartbeat_callback=lambda: update_job_heartbeat(session, job=job),
            heartbeat_interval_sec=10.0,
        )
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
    if job.project_id:
        raw_root_candidates = resolve_raw_root_candidates(
            session,
            project_id=job.project_id,
            project_mat_path=str(project_ref.get("project_mat_path") or ""),
        )
        if raw_root_candidates:
            existing = ensure_list(project_ref.get("raw_root_candidates"))
            project_ref["raw_root_candidates"] = unique_strings([*existing, *raw_root_candidates])
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


def resolve_raw_root_candidates(session: Session, *, project_id, project_mat_path: str = "") -> list[str]:
    candidates: list[str] = []

    project = session.scalars(
        select(Project)
        .options(joinedload(Project.locations).joinedload(ProjectLocation.storage_root))
        .where(Project.id == project_id)
    ).first()
    if project is not None:
        for location in sorted(project.locations or [], key=project_location_priority):
            root = location.storage_root
            if root is None or root.host_scope != "server":
                continue
            candidates.extend(infer_raw_roots_from_project_root(root.path_prefix))

    linked_raw_datasets = session.scalars(
        select(RawDataset)
        .join(ProjectRawLink, ProjectRawLink.raw_dataset_id == RawDataset.id)
        .options(joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root))
        .where(ProjectRawLink.project_id == project_id)
    ).unique()
    for raw_dataset in linked_raw_datasets:
        preferred_locations = sorted(
            raw_dataset.locations or [],
            key=lambda loc: (
                0 if loc.is_preferred else 1,
                0 if getattr(loc.storage_root, "host_scope", "") == "server" else 1,
                str(loc.relative_path or ""),
            ),
        )
        for location in preferred_locations:
            root = location.storage_root
            if root is None or root.host_scope != "server":
                continue
            path = Path(root.path_prefix) / (location.relative_path or "")
            candidates.append(str(path))

    if project_mat_path:
        candidates.extend(infer_raw_roots_from_project_path(project_mat_path))

    return [path for path in unique_strings(candidates) if Path(path).is_dir()]


def infer_raw_roots_from_project_root(project_root: str) -> list[str]:
    root = Path(str(project_root or "").strip())
    if not str(root):
        return []
    return [
        str(root / "raw"),
        str(root / "Raw"),
        str(root / "RAWDATA"),
        str(root / "raw_data"),
    ]


def infer_raw_roots_from_project_path(project_mat_path: str) -> list[str]:
    path = Path(str(project_mat_path or "").strip())
    candidates: list[str] = []
    for parent in path.parents:
        if parent.name.lower() in {"projects", "analysis", "analyses"}:
            continue
        candidates.extend(infer_raw_roots_from_project_root(str(parent)))
        if parent.parent == parent:
            break
    return candidates


def ensure_list(value: Any) -> list[Any]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def unique_strings(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def update_job_heartbeat(session: Session, *, job: Job) -> None:
    now = datetime.now(timezone.utc)
    job_record = session.get(Job, job.id)
    if job_record is None:
        return
    job_record.heartbeat_at = now
    job_record.updated_at = now
    session.commit()


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
