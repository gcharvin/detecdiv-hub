from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
import time

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from api.config import get_settings
from api.models import Pipeline, Project, ProjectLocation, StorageRoot, User
from api.services.project_inventory import inspect_project_directory
from api.services.storage_metrics import safe_dir_size, safe_file_size
from api.services.users import get_or_create_user


@dataclass
class ProjectIndexResult:
    root_path: str
    storage_root_name: str
    owner_user_key: str
    visibility: str
    total_projects: int
    scanned_projects: int
    indexed_projects: int
    failed_projects: int
    deleted_projects: int
    stale_cleanup_skipped: bool
    indexed_pipelines: int = 0
    failed_pipelines: int = 0


def index_project_root(
    session: Session,
    *,
    root_path: str,
    storage_root_name: str | None = None,
    host_scope: str = "server",
    root_type: str = "project_root",
    owner_user_key: str,
    visibility: str = "private",
    clear_existing_for_root: bool = False,
    continue_on_error: bool = True,
    commit_each: bool = False,
    progress_callback: Callable[..., None] | None = None,
) -> ProjectIndexResult:
    root = Path(root_path).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"Project root does not exist: {root}")

    owner = get_or_create_user(session, user_key=owner_user_key, display_name=owner_user_key)
    storage_root = get_or_create_storage_root(
        session,
        root_path=str(root),
        storage_root_name=storage_root_name,
        host_scope=host_scope,
        root_type=root_type,
    )

    existing_project_ids: set = set()
    if clear_existing_for_root:
        existing_project_ids = set(
            session.scalars(
                select(ProjectLocation.project_id).where(ProjectLocation.storage_root_id == storage_root.id)
            )
        )

    seen_project_ids: set = set()
    indexed_projects = 0
    scanned_projects = 0
    failed_projects = 0
    total_projects = 0
    project_dirs: list[Path] = []
    if progress_callback is not None:
        progress_callback(
            status="running",
            total_projects=total_projects,
            scanned_projects=0,
            indexed_projects=0,
            failed_projects=0,
            deleted_projects=0,
            current_project_path=None,
            message=f"Scanning {root} for DetecDiv projects.",
        )

    for mat_path, project_dir in iter_project_candidates(root, progress_callback=progress_callback):
        total_projects += 1
        scanned_projects += 1
        try:
            project = upsert_project_from_paths(
                session,
                owner=owner,
                visibility=visibility,
                storage_root=storage_root,
                root_path=root,
                mat_path=mat_path,
                project_dir=project_dir,
            )
            seen_project_ids.add(project.id)
            project_dirs.append(project_dir)
            indexed_projects += 1
            if progress_callback is not None:
                progress_callback(
                    status="running",
                    phase="indexing",
                    total_projects=total_projects,
                    scanned_projects=scanned_projects,
                    indexed_projects=indexed_projects,
                    failed_projects=failed_projects,
                    deleted_projects=0,
                    mat_files_seen=total_projects,
                    current_project_path=str(mat_path),
                    message=f"Indexed {mat_path.name} ({indexed_projects} indexed so far).",
                )
            if commit_each:
                session.commit()
        except Exception as exc:
            session.rollback()
            failed_projects += 1
            if progress_callback is not None:
                progress_callback(
                    status="running",
                    phase="indexing",
                    total_projects=total_projects,
                    scanned_projects=scanned_projects,
                    indexed_projects=indexed_projects,
                    failed_projects=failed_projects,
                    deleted_projects=0,
                    mat_files_seen=total_projects,
                    current_project_path=str(mat_path),
                    message=f"Failed to index {mat_path.name}: {exc}",
                    error_text=str(exc),
                )
            if not continue_on_error:
                raise

    indexed_pipelines = 0
    failed_pipelines = 0
    for pipeline_candidate in iter_independent_pipeline_candidates(root, project_dirs=project_dirs):
        try:
            upsert_pipeline_from_candidate(session, pipeline_candidate)
            indexed_pipelines += 1
            if commit_each:
                session.commit()
        except Exception as exc:
            session.rollback()
            failed_pipelines += 1
            if progress_callback is not None:
                progress_callback(
                    status="running",
                    phase="indexing_pipelines",
                    total_projects=total_projects,
                    scanned_projects=scanned_projects,
                    indexed_projects=indexed_projects,
                    failed_projects=failed_projects,
                    deleted_projects=0,
                    mat_files_seen=total_projects,
                    current_project_path=str(pipeline_candidate.pipeline_json_path),
                    message=f"Failed to index pipeline {pipeline_candidate.pipeline_json_path.name}: {exc}",
                    error_text=str(exc),
                )
            if not continue_on_error:
                raise

    if progress_callback is not None and indexed_pipelines:
        progress_callback(
            status="running",
            phase="indexing_pipelines",
            total_projects=total_projects,
            scanned_projects=scanned_projects,
            indexed_projects=indexed_projects,
            failed_projects=failed_projects,
            deleted_projects=0,
            mat_files_seen=total_projects,
            current_project_path=None,
            message=f"Indexed {indexed_pipelines} independent pipeline(s).",
        )

    deleted_projects = 0
    stale_cleanup_skipped = False
    if clear_existing_for_root and existing_project_ids and failed_projects == 0:
        stale_ids = existing_project_ids - seen_project_ids
        for project_id in stale_ids:
            session.execute(
                delete(ProjectLocation).where(
                    ProjectLocation.project_id == project_id,
                    ProjectLocation.storage_root_id == storage_root.id,
                )
            )
            remaining = session.scalars(
                select(ProjectLocation.id).where(ProjectLocation.project_id == project_id)
            ).first()
            if remaining is None:
                session.execute(delete(Project).where(Project.id == project_id))
            deleted_projects += 1
        if commit_each:
            if progress_callback is not None:
                progress_callback(
                    status="running",
                    phase="cleanup",
                    total_projects=total_projects,
                    scanned_projects=scanned_projects,
                    indexed_projects=indexed_projects,
                    failed_projects=failed_projects,
                    deleted_projects=deleted_projects,
                    mat_files_seen=total_projects,
                    current_project_path=None,
                    message=f"Removed {deleted_projects} stale project rows.",
                )
            session.commit()
    elif clear_existing_for_root and failed_projects > 0:
        stale_cleanup_skipped = True
        if progress_callback is not None:
            progress_callback(
                status="running",
                phase="cleanup",
                total_projects=total_projects,
                scanned_projects=scanned_projects,
                indexed_projects=indexed_projects,
                failed_projects=failed_projects,
                deleted_projects=0,
                mat_files_seen=total_projects,
                current_project_path=None,
                message="Skipped stale-row cleanup because some projects failed to index.",
            )

    if progress_callback is not None and total_projects == 0:
        progress_callback(
            status="running",
            phase="completed",
            total_projects=0,
            scanned_projects=0,
            indexed_projects=0,
            failed_projects=0,
            deleted_projects=0,
            mat_files_seen=0,
            current_project_path=None,
            message=f"No DetecDiv project candidates found under {root}.",
        )

    return ProjectIndexResult(
        root_path=str(root),
        storage_root_name=storage_root.name,
        owner_user_key=owner.user_key,
        visibility=visibility,
        total_projects=total_projects,
        scanned_projects=scanned_projects,
        indexed_projects=indexed_projects,
        failed_projects=failed_projects,
        deleted_projects=deleted_projects,
        stale_cleanup_skipped=stale_cleanup_skipped,
        indexed_pipelines=indexed_pipelines,
        failed_pipelines=failed_pipelines,
    )


@dataclass
class PipelineIndexCandidate:
    pipeline_json_path: Path
    source_path: Path
    source_kind: str
    manifest_path: Path | None = None
    bundle_path: Path | None = None


def iter_independent_pipeline_candidates(root_path: Path, *, project_dirs: list[Path]):
    seen_pipeline_json_paths: set[Path] = set()
    excluded_run_parts = {"runs"}

    for manifest_path in sorted(root_path.rglob("export_manifest.json")):
        candidate = pipeline_candidate_from_manifest(manifest_path)
        if candidate is None:
            continue
        if is_relative_to_any(candidate.pipeline_json_path, project_dirs):
            continue
        if has_path_part(candidate.pipeline_json_path, excluded_run_parts):
            continue
        seen_pipeline_json_paths.add(candidate.pipeline_json_path)
        yield candidate

    for pipeline_json_path in sorted(root_path.rglob("pipeline.json")):
        pipeline_json_path = pipeline_json_path.resolve()
        if pipeline_json_path in seen_pipeline_json_paths:
            continue
        if is_relative_to_any(pipeline_json_path, project_dirs):
            continue
        if has_path_part(pipeline_json_path, excluded_run_parts):
            continue
        if not looks_like_pipeline_json(pipeline_json_path):
            continue
        yield PipelineIndexCandidate(
            pipeline_json_path=pipeline_json_path,
            source_path=pipeline_json_path.parent,
            source_kind="pipeline_json",
        )


def pipeline_candidate_from_manifest(manifest_path: Path) -> PipelineIndexCandidate | None:
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    pipeline_meta = payload.get("pipeline")
    if not isinstance(pipeline_meta, dict):
        return None
    bundle_pipeline_path = pipeline_meta.get("bundlePipelinePath")
    if not bundle_pipeline_path:
        return None
    pipeline_json_path = resolve_manifest_child_path(manifest_path.parent, str(bundle_pipeline_path))
    if not pipeline_json_path.is_file() or not looks_like_pipeline_json(pipeline_json_path):
        return None
    return PipelineIndexCandidate(
        pipeline_json_path=pipeline_json_path.resolve(),
        source_path=manifest_path.parent.resolve(),
        source_kind="export_bundle",
        manifest_path=manifest_path.resolve(),
        bundle_path=manifest_path.parent.resolve(),
    )


def looks_like_pipeline_json(path: Path) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    if "nodes" not in payload:
        return False
    return any(key in payload for key in ("name", "id", "version", "edges", "runProfiles"))


def resolve_manifest_child_path(base_path: Path, child_path: str) -> Path:
    candidate = Path(child_path)
    if candidate.is_absolute():
        return candidate
    return base_path / candidate


def has_path_part(path: Path, parts: set[str]) -> bool:
    lowered = {part.lower() for part in path.parts}
    return bool(lowered.intersection(parts))


def is_relative_to_any(path: Path, parents: list[Path]) -> bool:
    for parent in parents:
        try:
            path.relative_to(parent)
            return True
        except ValueError:
            continue
    return False


def upsert_pipeline_from_candidate(session: Session, candidate: PipelineIndexCandidate) -> Pipeline:
    payload = json.loads(candidate.pipeline_json_path.read_text(encoding="utf-8"))
    display_name = pipeline_display_name(payload, candidate.pipeline_json_path)
    pipeline_key = pipeline_key_from_payload(payload)
    if not pipeline_key:
        relative_label = str(candidate.pipeline_json_path.parent.name or display_name)
        pipeline_key = build_pipeline_key(str(candidate.pipeline_json_path), relative_label, display_name)
    version = local_text(payload.get("version")) or "1.0"

    pipeline = session.scalars(select(Pipeline).where(Pipeline.pipeline_key == pipeline_key)).first()
    metadata = pipeline_metadata_from_candidate(candidate, payload)
    if pipeline is None:
        pipeline = Pipeline(
            pipeline_key=pipeline_key,
            display_name=display_name,
            version=version,
            runtime_kind="matlab",
            metadata_json=metadata,
        )
        session.add(pipeline)
    else:
        merged_metadata = dict(pipeline.metadata_json or {})
        merged_metadata.update(metadata)
        pipeline.display_name = display_name
        pipeline.version = version
        pipeline.runtime_kind = pipeline.runtime_kind or "matlab"
        pipeline.metadata_json = merged_metadata
    session.flush()
    return pipeline


def pipeline_display_name(payload: dict, pipeline_json_path: Path) -> str:
    return local_text(payload.get("name")) or local_text(payload.get("strid")) or pipeline_json_path.parent.name


def pipeline_key_from_payload(payload: dict) -> str | None:
    for key in ("name", "strid", "pipeline_key", "pipelineKey"):
        value = local_text(payload.get(key))
        if value:
            return value
    value = payload.get("id")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def pipeline_metadata_from_candidate(candidate: PipelineIndexCandidate, payload: dict) -> dict:
    metadata = {
        "source": "hub_indexer",
        "pipeline_source": candidate.source_kind,
        "pipeline_json_path": str(candidate.pipeline_json_path),
        "node_count": len(payload.get("nodes") or []),
        "description": local_text(payload.get("description")),
        "detecdiv_pipeline_id": payload.get("id"),
    }
    if candidate.manifest_path is not None:
        metadata["export_manifest_uri"] = str(candidate.manifest_path)
    if candidate.bundle_path is not None:
        metadata["pipeline_bundle_uri"] = str(candidate.bundle_path)
    return metadata


def local_text(value) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def iter_project_candidates(root_path: Path, progress_callback: Callable[..., None] | None = None):
    mat_files_seen = 0
    candidate_projects = 0
    last_report_at = time.monotonic()
    for mat_path in root_path.rglob("*.mat"):
        mat_files_seen += 1
        now = time.monotonic()
        if progress_callback is not None and (mat_files_seen == 1 or mat_files_seen % 200 == 0 or now - last_report_at >= 3.0):
            progress_callback(
                status="running",
                phase="discovering",
                total_projects=candidate_projects,
                scanned_projects=0,
                indexed_projects=0,
                failed_projects=0,
                deleted_projects=0,
                mat_files_seen=mat_files_seen,
                current_project_path=str(mat_path),
                message=f"Discovering DetecDiv projects under {root_path} ({mat_files_seen} .mat files inspected).",
            )
            last_report_at = now
        project_dir = mat_path.with_suffix("")
        if not project_dir.is_dir():
            continue
        if not is_detecdiv_project_dir(project_dir):
            continue
        candidate_projects += 1
        if progress_callback is not None:
            progress_callback(
                status="running",
                phase="discovering",
                total_projects=candidate_projects,
                scanned_projects=0,
                indexed_projects=0,
                failed_projects=0,
                deleted_projects=0,
                mat_files_seen=mat_files_seen,
                current_project_path=str(mat_path),
                message=f"Found {candidate_projects} DetecDiv project candidate(s) after inspecting {mat_files_seen} .mat files.",
            )
        yield mat_path.resolve(), project_dir.resolve()


def is_detecdiv_project_dir(project_dir: Path) -> bool:
    """Heuristic filter to reject image/frame folders that happen to have sibling MAT files."""
    if not project_dir.is_dir():
        return False

    top_level_dir_markers = {"pipeline", "processor", "classification", "classifier", "fov"}
    try:
        entries = list(project_dir.iterdir())
    except OSError:
        return False

    child_dir_names = {entry.name.lower() for entry in entries if entry.is_dir()}
    if child_dir_names.intersection(top_level_dir_markers):
        return True

    # Many valid projects already contain derived H5 outputs somewhere in the tree.
    try:
        if next(project_dir.rglob("*.h5"), None) is not None:
            return True
    except OSError:
        return False

    # Some projects may have run metadata without the standard top-level folders.
    try:
        if next(project_dir.rglob("run.json"), None) is not None:
            return True
    except OSError:
        return False

    # Reject typical per-image folders such as Pos*/im_* trees when they have no project markers.
    return False


def get_or_create_storage_root(
    session: Session,
    *,
    root_path: str,
    storage_root_name: str | None,
    host_scope: str,
    root_type: str,
) -> StorageRoot:
    root_name = storage_root_name or build_storage_root_name(root_path)
    root = session.scalars(select(StorageRoot).where(StorageRoot.name == root_name)).first()
    if root is None:
        root = StorageRoot(
            name=root_name,
            root_type=root_type,
            host_scope=host_scope,
            path_prefix=root_path,
        )
        session.add(root)
        session.flush()
    else:
        root.root_type = root_type
        root.host_scope = host_scope
        root.path_prefix = root_path
        session.flush()
    return root


def upsert_project_from_paths(
    session: Session,
    *,
    owner: User,
    visibility: str,
    storage_root: StorageRoot,
    root_path: Path,
    mat_path: Path,
    project_dir: Path,
) -> Project:
    relative_parent = project_relative_parent(root_path, mat_path)
    project_key = build_project_key(str(mat_path), relative_parent, mat_path.stem)
    project = session.scalars(select(Project).where(Project.project_key == project_key)).first()

    project_mat_bytes = safe_file_size(mat_path)
    project_dir_bytes = safe_dir_size(project_dir)
    total_bytes = project_mat_bytes + project_dir_bytes
    size_timestamp = datetime.now(timezone.utc)
    inventory = inspect_project_directory(project_dir)
    raw_scan = scan_project_raw_sources(
        session,
        owner=owner,
        visibility=visibility,
        mat_path=mat_path,
        project_dir=project_dir,
    )
    metadata = {
        "source": "hub_indexer",
        "project_mat_abs": str(mat_path),
        "project_dir_abs": str(project_dir),
        "project_rel_from_root": str(project_dir.relative_to(root_path)),
        "inventory": inventory.metadata_json(),
        "raw_relink": raw_scan.metadata_json(),
    }

    if project is None:
        project = Project(
            owner_user_id=owner.id,
            project_key=project_key,
            project_name=mat_path.stem,
            visibility=visibility,
            status="indexed",
            health_status="raw_missing" if raw_scan.extraction_status == "ok" and raw_scan.missing_raw_count > 0 else "ok",
            fov_count=raw_scan.fov_count if raw_scan.extraction_status == "ok" else 0,
            roi_count=0,
            classifier_count=inventory.classifier_count,
            processor_count=inventory.processor_count,
            pipeline_run_count=inventory.pipeline_run_count,
            available_raw_count=raw_scan.available_raw_count if raw_scan.extraction_status == "ok" else 0,
            missing_raw_count=raw_scan.missing_raw_count if raw_scan.extraction_status == "ok" else 0,
            run_json_count=inventory.run_json_count,
            h5_count=inventory.h5_count,
            h5_bytes=inventory.h5_bytes,
            latest_run_status=inventory.latest_run_status,
            latest_run_at=inventory.latest_run_at,
            project_mat_bytes=project_mat_bytes,
            project_dir_bytes=project_dir_bytes,
            estimated_raw_bytes=raw_scan.estimated_raw_bytes if raw_scan.extraction_status == "ok" else 0,
            total_bytes=total_bytes,
            last_size_scan_at=size_timestamp,
            metadata_json=metadata,
        )
        session.add(project)
        session.flush()
    else:
        existing_fov_count = int(project.fov_count or 0)
        existing_roi_count = int(project.roi_count or 0)
        existing_available_raw_count = int(project.available_raw_count or 0)
        existing_missing_raw_count = int(project.missing_raw_count or 0)
        existing_estimated_raw_bytes = int(project.estimated_raw_bytes or 0)
        merged_metadata = dict(project.metadata_json or {})
        merged_metadata.update(metadata)
        project.owner_user_id = owner.id
        project.project_name = mat_path.stem
        project.visibility = visibility
        project.status = "indexed"
        if raw_scan.extraction_status == "ok":
            project.health_status = "raw_missing" if raw_scan.missing_raw_count > 0 else "ok"
            project.fov_count = raw_scan.fov_count or existing_fov_count
            project.available_raw_count = raw_scan.available_raw_count
            project.missing_raw_count = raw_scan.missing_raw_count
            project.estimated_raw_bytes = raw_scan.estimated_raw_bytes
        else:
            project.health_status = "raw_missing" if existing_missing_raw_count > 0 else "ok"
            project.fov_count = existing_fov_count
            project.available_raw_count = existing_available_raw_count
            project.missing_raw_count = existing_missing_raw_count
            project.estimated_raw_bytes = existing_estimated_raw_bytes
        project.roi_count = existing_roi_count
        project.classifier_count = inventory.classifier_count
        project.processor_count = inventory.processor_count
        project.pipeline_run_count = inventory.pipeline_run_count
        project.run_json_count = inventory.run_json_count
        project.h5_count = inventory.h5_count
        project.h5_bytes = inventory.h5_bytes
        project.latest_run_status = inventory.latest_run_status
        project.latest_run_at = inventory.latest_run_at
        project.project_mat_bytes = project_mat_bytes
        project.project_dir_bytes = project_dir_bytes
        project.total_bytes = total_bytes
        project.last_size_scan_at = size_timestamp
        project.metadata_json = merged_metadata
        session.flush()

    location = session.scalars(
        select(ProjectLocation).where(
            ProjectLocation.project_id == project.id,
            ProjectLocation.storage_root_id == storage_root.id,
        )
    ).first()
    if location is None:
        location = ProjectLocation(
            project_id=project.id,
            storage_root_id=storage_root.id,
            relative_path=relative_parent,
            project_file_name=mat_path.name,
            access_mode="readwrite",
            is_preferred=True,
        )
        session.add(location)
    else:
        location.relative_path = relative_parent
        location.project_file_name = mat_path.name
        location.access_mode = "readwrite"
        location.is_preferred = True

    session.flush()
    if raw_scan.extraction_status == "ok":
        synchronize_project_raw_links(session, project=project, linked_raw_dataset_ids=raw_scan.linked_raw_dataset_ids)
    return project


@dataclass
class RawSourceScanResult:
    fov_count: int
    available_raw_count: int
    missing_raw_count: int
    estimated_raw_bytes: int
    linked_raw_dataset_ids: list
    linked_sources: list[dict]
    missing_sources: list[dict]
    extraction_status: str
    extraction_error: str | None = None

    def metadata_json(self) -> dict:
        return {
            "fov_count": self.fov_count,
            "available_raw_count": self.available_raw_count,
            "missing_raw_count": self.missing_raw_count,
            "estimated_raw_bytes": self.estimated_raw_bytes,
            "linked_sources": self.linked_sources,
            "missing_sources": self.missing_sources,
            "extraction_status": self.extraction_status,
            "extraction_error": self.extraction_error,
        }


def scan_project_raw_sources(
    session: Session,
    *,
    owner: User,
    visibility: str,
    mat_path: Path,
    project_dir: Path,
) -> RawSourceScanResult:
    extracted = extract_project_raw_srcpaths(mat_path)
    if extracted["status"] != "ok":
        return RawSourceScanResult(
            fov_count=0,
            available_raw_count=0,
            missing_raw_count=0,
            estimated_raw_bytes=0,
            linked_raw_dataset_ids=[],
            linked_sources=[],
            missing_sources=[],
            extraction_status=extracted["status"],
            extraction_error=extracted.get("error"),
        )

    linked_raw_dataset_ids = []
    linked_sources: list[dict] = []
    missing_sources: list[dict] = []
    estimated_raw_bytes = 0
    seen_dataset_ids: set = set()
    seen_missing_paths: set[str] = set()
    for source_path in extracted.get("srcpaths", []):
        resolved = resolve_raw_dataset_candidate(session, source_path=source_path, project_dir=project_dir)
        if resolved is None:
            normalized = normalize_raw_source_path(source_path, project_dir=project_dir)
            if normalized and normalized not in seen_missing_paths:
                seen_missing_paths.add(normalized)
                missing_sources.append({"srcpath": source_path, "normalized_path": normalized})
            continue
        raw_dataset = get_or_create_raw_dataset_for_path(
            session,
            owner=owner,
            visibility=visibility,
            dataset_dir=resolved,
        )
        if raw_dataset.id in seen_dataset_ids:
            continue
        seen_dataset_ids.add(raw_dataset.id)
        linked_raw_dataset_ids.append(raw_dataset.id)
        estimated_raw_bytes += int(raw_dataset.total_bytes or 0)
        linked_sources.append(
            {
                "srcpath": source_path,
                "dataset_path": str(resolved),
                "raw_dataset_id": str(raw_dataset.id),
                "acquisition_label": raw_dataset.acquisition_label,
            }
        )

    return RawSourceScanResult(
        fov_count=int(extracted.get("fov_count") or len(extracted.get("srcpaths", []))),
        available_raw_count=len(linked_raw_dataset_ids),
        missing_raw_count=len(missing_sources),
        estimated_raw_bytes=estimated_raw_bytes,
        linked_raw_dataset_ids=linked_raw_dataset_ids,
        linked_sources=linked_sources,
        missing_sources=missing_sources,
        extraction_status="ok",
    )


def extract_project_raw_srcpaths(mat_path: Path) -> dict:
    settings = get_settings()
    matlab_command = str(settings.matlab_command or "matlab").strip() or "matlab"
    matlab_repo_root = str(settings.matlab_repo_root or "").strip()
    with tempfile.TemporaryDirectory(prefix="detecdiv_hub_raw_srcpaths_") as tmpdir:
        tmp_path = Path(tmpdir)
        output_path = tmp_path / "result.json"
        script_path = tmp_path / "extract_srcpaths.m"
        script_path.write_text(
            build_extract_srcpaths_matlab_script(
                mat_path=mat_path,
                output_path=output_path,
                matlab_repo_root=(Path(matlab_repo_root) if matlab_repo_root else None),
            ),
            encoding="utf-8",
        )
        command = [matlab_command, "-batch", f"run('{matlab_escape(script_path)}')"]
        try:
            completed = subprocess.run(command, check=False, text=True, capture_output=True)
        except OSError as exc:
            return {"status": "matlab_unavailable", "error": str(exc), "srcpaths": [], "fov_count": 0}
        if completed.returncode != 0:
            return {
                "status": "matlab_failed",
                "error": tail_text(completed.stderr or completed.stdout or ""),
                "srcpaths": [],
                "fov_count": 0,
            }
        if not output_path.is_file():
            return {"status": "no_result", "error": "MATLAB extraction produced no result file.", "srcpaths": [], "fov_count": 0}
        try:
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return {"status": "invalid_result", "error": str(exc), "srcpaths": [], "fov_count": 0}
        srcpaths = [str(value).strip() for value in list(payload.get("srcpaths") or []) if str(value).strip()]
        deduped_srcpaths = list(dict.fromkeys(srcpaths))
        return {
            "status": "ok" if payload.get("ok") else "matlab_error",
            "error": str(payload.get("error") or "").strip() or None,
            "srcpaths": deduped_srcpaths,
        "fov_count": int(payload.get("fov_count") or len(srcpaths)),
        }


def build_extract_srcpaths_matlab_script(
    *,
    mat_path: Path,
    output_path: Path,
    matlab_repo_root: Path | None = None,
) -> str:
    project_path = matlab_escape(mat_path)
    result_path = matlab_escape(output_path)
    repo_root = matlab_escape(matlab_repo_root) if matlab_repo_root else ""
    addpath_block = ""
    if repo_root:
        addpath_block = f"repoRoot = '{repo_root}';\nif isfolder(repoRoot)\n    addpath(genpath(repoRoot));\nend\n"
    return f"""
projectPath = '{project_path}';
outputPath = '{result_path}';
{addpath_block}result = struct('ok', false, 'srcpaths', {{{{}}}}, 'fov_count', 0, 'error', '');
try
    data = load(projectPath, 'shallowObj');
    if ~isfield(data, 'shallowObj')
        error('extract_srcpaths:MissingShallowObj', 'No shallowObj variable found in MAT file.');
    end
    shallowObj = data.shallowObj;
    fovs = [];
    if isobject(shallowObj) && isprop(shallowObj, 'fov')
        fovs = shallowObj.fov;
    elseif isstruct(shallowObj) && isfield(shallowObj, 'fov')
        fovs = shallowObj.fov;
    end
    rawPaths = {{}};
    fovCount = numel(fovs);
    fieldNames = {{'srcpath', 'tiffSource', 'ndtiffPath'}};
    for idx = 1:numel(fovs)
        item = fovs(idx);
        for fieldIdx = 1:numel(fieldNames)
            fieldName = fieldNames{{fieldIdx}};
            hasValue = false;
            value = [];
            if isobject(item) && isprop(item, fieldName)
                value = item.(fieldName);
                hasValue = true;
            elseif isstruct(item) && isfield(item, fieldName)
                value = item.(fieldName);
                hasValue = true;
            end
            if ~hasValue || isempty(value)
                continue;
            end

            if iscell(value)
                for valueIdx = 1:numel(value)
                    entry = value{{valueIdx}};
                    if isstring(entry)
                        for entryIdx = 1:numel(entry)
                            text = strtrim(char(entry(entryIdx)));
                            if ~isempty(text)
                                rawPaths{{end+1}} = text; %#ok<AGROW>
                            end
                        end
                    elseif ischar(entry)
                        if size(entry, 1) <= 1
                            text = strtrim(entry);
                            if ~isempty(text)
                                rawPaths{{end+1}} = text; %#ok<AGROW>
                            end
                        else
                            for rowIdx = 1:size(entry, 1)
                                text = strtrim(entry(rowIdx, :));
                                if ~isempty(text)
                                    rawPaths{{end+1}} = text; %#ok<AGROW>
                                end
                            end
                        end
                    end
                end
            elseif isstring(value)
                for valueIdx = 1:numel(value)
                    text = strtrim(char(value(valueIdx)));
                    if ~isempty(text)
                        rawPaths{{end+1}} = text; %#ok<AGROW>
                    end
                end
            elseif ischar(value)
                if size(value, 1) <= 1
                    text = strtrim(value);
                    if ~isempty(text)
                        rawPaths{{end+1}} = text; %#ok<AGROW>
                    end
                else
                    for rowIdx = 1:size(value, 1)
                        text = strtrim(value(rowIdx, :));
                        if ~isempty(text)
                            rawPaths{{end+1}} = text; %#ok<AGROW>
                        end
                    end
                end
            end
        end
    end
    rawPaths = rawPaths(~cellfun(@isempty, rawPaths));
    if isempty(rawPaths)
        result.srcpaths = {{}};
    else
        [~, idx] = unique(rawPaths, 'stable');
        result.srcpaths = rawPaths(sort(idx));
    end
    result.fov_count = fovCount;
    result.ok = true;
catch ME
    result.error = getReport(ME, 'extended', 'hyperlinks', 'off');
end
fid = fopen(outputPath, 'w');
fwrite(fid, jsonencode(result), 'char');
fclose(fid);
""".strip()


def resolve_raw_dataset_candidate(session: Session, *, source_path: str, project_dir: Path) -> Path | None:
    candidate_strings = build_rebased_raw_path_candidates(session, source_path=source_path, project_dir=project_dir)
    for candidate_string in candidate_strings:
        candidate = infer_raw_dataset_dir(Path(candidate_string))
        if candidate is not None:
            return candidate.resolve()
    return None


def normalize_raw_source_path(source_path: str, *, project_dir: Path) -> str:
    text = str(source_path or "").strip()
    if not text:
        return ""
    candidate = Path(text)
    try:
        if candidate.exists():
            return str(candidate)
    except OSError:
        pass
    if looks_like_client_absolute_path(text):
        return text
    try:
        project_candidate = (project_dir / text).resolve()
    except OSError:
        return text
    try:
        if project_candidate.exists():
            return str(project_candidate)
    except OSError:
        return text
    return text


def looks_like_client_absolute_path(path_text: str) -> bool:
    text = str(path_text or "").strip()
    if not text:
        return False
    if text.startswith(("\\\\", "//")):
        return True
    return bool(re.match(r"^[A-Za-z]:[\\\\/]", text))


def build_rebased_raw_path_candidates(session: Session, *, source_path: str, project_dir: Path) -> list[str]:
    normalized = normalize_raw_source_path(source_path, project_dir=project_dir)
    if not normalized:
        return []

    candidates: list[str] = []
    seen: set[str] = set()

    def push(path_text: str) -> None:
        text = str(path_text or "").strip()
        if not text or text in seen:
            return
        seen.add(text)
        candidates.append(text)

    push(normalized)

    inferred_relative = infer_relative_suffix(normalized)
    suffixes = suffix_candidates(normalized)
    root_candidates = raw_root_candidates(session, project_dir=project_dir)
    source_kind = classify_raw_source_path(normalized)

    for root in root_candidates:
        root_path = Path(root)
        if inferred_relative:
            push(str(root_path / inferred_relative))
        for suffix in suffixes:
            push(str(root_path / suffix))

    if source_kind == "file":
        for candidate in build_rebased_file_candidates(normalized, root_candidates=root_candidates):
            push(str(candidate))
    elif source_kind == "position":
        for candidate in build_rebased_position_candidates(normalized, root_candidates=root_candidates):
            push(str(candidate))
    else:
        for candidate in build_rebased_dataset_candidates(normalized, root_candidates=root_candidates):
            push(str(candidate))

    return candidates


def classify_raw_source_path(path_text: str) -> str:
    normalized = str(path_text or "").replace("\\", "/").rstrip("/")
    if not normalized:
        return "dataset"

    leaf = normalized.split("/")[-1]
    lowered_leaf = leaf.lower()
    if lowered_leaf.endswith((".tif", ".tiff")):
        return "file"
    if lowered_leaf.endswith(".zarr") or lowered_leaf.endswith(".ome.zarr"):
        return "dataset"
    if is_position_like_name(leaf):
        return "position"

    candidate = Path(path_text)
    if candidate.exists() and candidate.is_file():
        return "file"
    return "dataset"


def build_rebased_file_candidates(path_text: str, *, root_candidates: list[Path]) -> list[Path]:
    target_name, old_leaf = extract_target_and_leaf(path_text)
    if not target_name:
        return []

    candidates: list[Path] = []
    seen: set[str] = set()

    def push(path: Path | None) -> None:
        if path is None:
            return
        key = str(path)
        if key in seen:
            return
        seen.add(key)
        candidates.append(path)

    for root in root_candidates:
        push(root / target_name)
        if old_leaf:
            push(root / old_leaf / target_name)
        suffix = suffix_after_raw_data(path_text)
        if suffix is not None:
            push(root / suffix)

        if should_scan_under_root(root):
            found = find_named_file(root, target_name, max_depth=1)
            push(found)
            if old_leaf:
                push(find_named_file(root / old_leaf, target_name, max_depth=1))
            push(find_named_file(root, target_name, max_depth=4))

    return candidates


def build_rebased_position_candidates(path_text: str, *, root_candidates: list[Path]) -> list[Path]:
    info = position_path_info(path_text)
    if info is None:
        return []

    candidates: list[Path] = []
    seen: set[str] = set()

    def push(path: Path | None) -> None:
        if path is None:
            return
        key = str(path)
        if key in seen:
            return
        seen.add(key)
        candidates.append(path)

    for root in root_candidates:
        if normalize_name(root.name) == normalize_name(info["dataset_name"]):
            push(root / info["position_name"])

        dataset_direct = root / info["dataset_name"]
        if dataset_direct.is_dir():
            push(dataset_direct / info["position_name"])

        if should_scan_under_root(root):
            dataset_found = find_dataset_folder(root, info["dataset_name"], max_depth=6)
            if dataset_found is not None:
                push(dataset_found / info["position_name"])

    return candidates


def build_rebased_dataset_candidates(path_text: str, *, root_candidates: list[Path]) -> list[Path]:
    infos = dataset_info_candidates(path_text)
    if not infos:
        return []

    candidates: list[Path] = []
    seen: set[str] = set()

    def push(path: Path | None) -> None:
        if path is None:
            return
        key = str(path)
        if key in seen:
            return
        seen.add(key)
        candidates.append(path)

    dataset_names = [str(info["dataset_name"]) for info in infos if str(info["dataset_name"])]
    for root in root_candidates:
        for info in infos:
            push(root / str(info["dataset_name"]))
            parent_leaf = str(info["parent_leaf"] or "")
            if parent_leaf:
                push(root / parent_leaf / str(info["dataset_name"]))
            for suffix in info["suffixes"]:
                push(root / suffix)

        if should_scan_under_root(root):
            for dataset_name in dataset_names:
                push(find_dataset_folder(root, dataset_name, max_depth=4))

    return candidates


def raw_root_candidates(session: Session, *, project_dir: Path) -> list[Path]:
    from api.models import StorageRoot

    candidates: list[Path] = []
    seen: set[str] = set()

    def push(path_text: str) -> None:
        text = str(path_text or "").strip()
        if not text:
            return
        try:
            path = Path(text).resolve()
        except OSError:
            return
        key = str(path)
        if key in seen or not path.exists():
            return
        seen.add(key)
        candidates.append(path)

    for root in session.scalars(select(StorageRoot)):
        root_type = str(root.root_type or "").lower()
        if "raw" in root_type:
            push(root.path_prefix)

    for inferred in infer_raw_roots_from_project_dir(project_dir):
        push(str(inferred))

    return candidates


def infer_raw_roots_from_project_dir(project_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    seen: set[str] = set()
    for parent in [project_dir.parent, *project_dir.parents]:
        leaf = parent.name.lower()
        if leaf in {"projects", "analysis", "analyses", "analyse"}:
            continue
        if str(parent).startswith("/data"):
            key = str(parent)
            if key not in seen and parent.exists():
                seen.add(key)
                candidates.append(parent)
        for child in ("raw", "Raw", "RAWDATA", "raw_data", "Timelapses", "timelapses", "Acquisitions", "acquisitions"):
            candidate = parent / child
            key = str(candidate)
            if key in seen or not candidate.exists():
                continue
            seen.add(key)
            candidates.append(candidate)
    return candidates


def infer_relative_suffix(path_text: str) -> Path | None:
    normalized = str(path_text or "").replace("\\", "/")
    lowered = normalized.lower()
    drive_match = re.match(r"^[A-Za-z]:/(.+)$", normalized)
    if drive_match:
        suffix = drive_match.group(1)
        if suffix:
            return Path(*[part for part in suffix.split("/") if part])
    markers = [
        "/synologydrive/data/",
        "//10.20.11.250/data/",
    ]
    for marker in markers:
        index = lowered.find(marker)
        if index >= 0:
            suffix = normalized[index + len(marker) :]
            if suffix:
                return Path(*[part for part in suffix.split("/") if part])

    if normalized.startswith("//"):
        parts = [part for part in normalized.split("/") if part]
        if len(parts) >= 3:
            return Path(*parts[2:])
    return None


def suffix_candidates(path_text: str) -> list[Path]:
    parts = split_legacy_path_parts(path_text)
    suffixes: list[Path] = []
    for width in range(2, min(8, len(parts)) + 1):
        suffixes.append(Path(*parts[-width:]))
    return suffixes


def raw_dataset_leaf_name(path_text: str) -> str:
    candidate = infer_raw_dataset_dir(Path(str(path_text)))
    if candidate is not None:
        return candidate.name
    normalized = str(path_text or "").replace("\\", "/").rstrip("/")
    if not normalized:
        return ""
    return normalized.split("/")[-1]


def find_dataset_folder(root: Path, dataset_name: str, *, max_depth: int) -> Path | None:
    if max_depth <= 0 or not root.is_dir():
        return None
    try:
        children = [entry for entry in root.iterdir() if entry.is_dir()]
    except OSError:
        return None

    target = normalize_name(dataset_name)
    for child in children:
        if normalize_name(child.name) == target and infer_raw_dataset_dir(child) is not None:
            return child

    for child in children:
        found = find_dataset_folder(child, dataset_name, max_depth=max_depth - 1)
        if found is not None:
            return found
    return None


def find_named_file(root: Path, file_name: str, *, max_depth: int) -> Path | None:
    if max_depth <= 0 or not root.is_dir():
        return None
    target = normalize_name(file_name)
    try:
        children = list(root.iterdir())
    except OSError:
        return None

    for child in children:
        if child.is_file() and normalize_name(child.name) == target:
            return child

    for child in children:
        if not child.is_dir():
            continue
        found = find_named_file(child, file_name, max_depth=max_depth - 1)
        if found is not None:
            return found
    return None


def should_scan_under_root(root: Path) -> bool:
    try:
        root = root.resolve()
    except OSError:
        return False
    return root.is_dir() and len(root.parts) >= 4


def normalize_name(value: str) -> str:
    return "".join(str(value or "").strip().lower().split())


def split_legacy_path_parts(path_text: str) -> list[str]:
    normalized = str(path_text or "").replace("\\", "/").strip()
    if not normalized:
        return []
    parts = [part for part in re.split(r"/+", normalized) if part]
    if parts and re.fullmatch(r"[A-Za-z]:", parts[0]):
        parts = parts[1:]
    return parts


def extract_target_and_leaf(path_text: str) -> tuple[str, str]:
    parts = split_legacy_path_parts(path_text)
    if not parts:
        return "", ""
    target = parts[-1]
    old_leaf = parts[-2] if len(parts) >= 2 else ""
    return target, old_leaf


def suffix_after_raw_data(path_text: str) -> Path | None:
    normalized = str(path_text or "").replace("\\", "/")
    lowered = normalized.lower()
    token = "/raw_data/"
    index = lowered.rfind(token)
    if index < 0:
        return None
    suffix = normalized[index + len(token) :]
    parts = [part for part in suffix.split("/") if part]
    if not parts:
        return None
    return Path(*parts)


def position_path_info(path_text: str) -> dict | None:
    parts = split_legacy_path_parts(path_text)
    if len(parts) < 2:
        return None
    position_name = parts[-1]
    dataset_name = parts[-2]
    if not is_position_like_name(position_name):
        return None
    return {"dataset_name": dataset_name, "position_name": position_name}


def dataset_info_candidates(path_text: str) -> list[dict]:
    parts = split_legacy_path_parts(path_text)
    if not parts:
        return []

    dataset_name = parts[-1]
    parent_leaf = parts[-2] if len(parts) >= 2 else ""
    infos = [
        {
            "parts": parts,
            "dataset_name": dataset_name,
            "parent_leaf": parent_leaf,
            "suffixes": [Path(*parts[-width:]) for width in range(2, min(8, len(parts)) + 1)],
        }
    ]

    malformed = re.match(r"^(\d{4}[_-]\d{2}[_-]\d{2})([^/\\\\]+\.ome\.zarr)$", dataset_name, flags=re.IGNORECASE)
    if malformed:
        fixed_parts = [*parts[:-1], malformed.group(1), malformed.group(2)]
        infos.append(
            {
                "parts": fixed_parts,
                "dataset_name": malformed.group(2),
                "parent_leaf": malformed.group(1),
                "suffixes": [Path(*fixed_parts[-width:]) for width in range(2, min(8, len(fixed_parts)) + 1)],
            }
        )
    return infos


def looks_like_dataset_folder(path: Path) -> bool:
    if not path.is_dir():
        return False
    lowered = path.name.lower()
    if lowered.endswith(".ome.zarr"):
        return has_zarr_root_metadata(path)
    return has_zarr_root_metadata(path) or (path / "NDTiff.index").is_file()


def has_zarr_root_metadata(path: Path) -> bool:
    return (path / "zarr.json").is_file() or ((path / ".zattrs").is_file() and (path / ".zgroup").is_file())


def infer_raw_dataset_dir(path: Path) -> Path | None:
    if not path.exists():
        return None

    zarr_ancestor = next((ancestor for ancestor in [path, *path.parents] if ancestor.suffix.lower() == ".zarr"), None)
    if zarr_ancestor is not None and zarr_ancestor.exists():
        return zarr_ancestor

    for ancestor in [path, *path.parents]:
        if looks_like_dataset_folder(ancestor):
            return ancestor

    if path.is_file():
        if path.suffix.lower() in {".tif", ".tiff"}:
            parent = path.parent
            if is_position_like_name(parent.name) and parent.parent.exists():
                return parent.parent
            return parent
        return path.parent

    if is_position_like_name(path.name) and path.parent.exists():
        try:
            if next(path.glob("*.tif"), None) is not None or next(path.glob("*.tiff"), None) is not None:
                return path.parent
        except OSError:
            return path.parent

    try:
        child_dirs = [entry for entry in path.iterdir() if entry.is_dir()]
    except OSError:
        child_dirs = []
    if any(is_position_like_name(entry.name) for entry in child_dirs):
        return path
    return path


def is_position_like_name(name: str) -> bool:
    lowered = str(name or "").strip().lower()
    return lowered.startswith("pos") or lowered.startswith("position") or lowered.startswith("xy")


def get_or_create_raw_dataset_for_path(
    session: Session,
    *,
    owner: User,
    visibility: str,
    dataset_dir: Path,
):
    from api.models import StorageRoot
    from api.services.raw_dataset_ingest import ingest_raw_dataset_from_directory

    existing = find_existing_raw_dataset_for_path(session, dataset_dir=dataset_dir)
    if existing is not None:
        return existing

    root_path = resolve_raw_root_path(session, dataset_dir=dataset_dir)
    root = session.scalars(select(StorageRoot).where(StorageRoot.path_prefix == str(root_path))).first()
    root_name = root.name if root is not None else None
    return ingest_raw_dataset_from_directory(
        session,
        owner=owner,
        visibility=visibility,
        storage_root_name=root_name,
        host_scope=(root.host_scope if root is not None else "server"),
        root_type=(root.root_type if root is not None else "raw_root"),
        root_path=root_path,
        dataset_dir=dataset_dir,
        source_label="project_srcpath_relink",
        source_metadata={"source_project_scan": True},
        acquisition_label=dataset_dir.name,
        status="indexed",
        completeness_status="complete",
    )


def find_existing_raw_dataset_for_path(session: Session, *, dataset_dir: Path):
    from api.models import RawDataset, RawDatasetLocation, StorageRoot

    dataset_text = str(dataset_dir.resolve())
    rows = session.execute(
        select(RawDataset)
        .join(RawDatasetLocation, RawDatasetLocation.raw_dataset_id == RawDataset.id)
        .join(StorageRoot, StorageRoot.id == RawDatasetLocation.storage_root_id)
    ).all()
    for (raw_dataset,) in rows:
        for location in raw_dataset.locations or []:
            storage_root = getattr(location, "storage_root", None)
            if storage_root is None:
                continue
            candidate = storage_root.path_prefix
            if location.relative_path:
                candidate = str(Path(candidate) / location.relative_path)
            if str(Path(candidate).resolve()) == dataset_text:
                return raw_dataset
    return None


def resolve_raw_root_path(session: Session, *, dataset_dir: Path) -> Path:
    from api.models import StorageRoot

    dataset_text = str(dataset_dir.resolve())
    best_root: StorageRoot | None = None
    best_length = -1
    for root in session.scalars(select(StorageRoot)):
        prefix = str(root.path_prefix or "").strip()
        if not prefix:
            continue
        try:
            prefix_path = Path(prefix).resolve()
        except OSError:
            continue
        prefix_text = str(prefix_path)
        if dataset_text == prefix_text or dataset_text.startswith(prefix_text + os.sep):
            if len(prefix_text) > best_length:
                best_length = len(prefix_text)
                best_root = root
    if best_root is not None:
        return Path(best_root.path_prefix).resolve()
    return dataset_dir.parent.resolve()


def synchronize_project_raw_links(session: Session, *, project: Project, linked_raw_dataset_ids: list) -> None:
    from api.models import ProjectRawLink

    desired_ids = {raw_dataset_id for raw_dataset_id in linked_raw_dataset_ids}
    existing_links = list(session.scalars(select(ProjectRawLink).where(ProjectRawLink.project_id == project.id)))
    existing_ids = {link.raw_dataset_id for link in existing_links}
    for link in existing_links:
        if link.raw_dataset_id not in desired_ids:
            session.delete(link)
    for raw_dataset_id in desired_ids - existing_ids:
        session.add(ProjectRawLink(project_id=project.id, raw_dataset_id=raw_dataset_id, link_type="source"))
    session.flush()


def matlab_escape(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def tail_text(text: str, *, max_lines: int = 40) -> str:
    lines = [line for line in str(text or "").splitlines() if line.strip()]
    if not lines:
        return ""
    return "\n".join(lines[-max_lines:])


def project_relative_parent(root_path: Path, mat_path: Path) -> str:
    rel_parent = mat_path.parent.relative_to(root_path)
    rel_text = str(rel_parent)
    return "" if rel_text == "." else rel_text


def build_storage_root_name(root_path: str) -> str:
    stem = Path(root_path).name or "root"
    slug = slugify(stem)
    suffix = hashlib.sha1(root_path.encode("utf-8")).hexdigest()[:8]
    return f"hub_{slug}_{suffix}"


def build_project_key(project_mat_abs: str, relative_parent: str, project_name: str) -> str:
    label = str(Path(relative_parent) / project_name) if relative_parent else project_name
    slug = slugify(label)
    suffix = hashlib.sha1(project_mat_abs.encode("utf-8")).hexdigest()[:10]
    return f"{slug}_{suffix}"


def build_pipeline_key(pipeline_json_abs: str, relative_parent: str, pipeline_name: str) -> str:
    label = str(Path(relative_parent) / pipeline_name) if relative_parent else pipeline_name
    slug = slugify(label)
    suffix = hashlib.sha1(pipeline_json_abs.encode("utf-8")).hexdigest()[:10]
    return f"{slug}_{suffix}"


def slugify(value: str) -> str:
    out: list[str] = []
    last_was_sep = False
    for ch in value.lower():
        if ch.isalnum():
            out.append(ch)
            last_was_sep = False
        elif not last_was_sep:
            out.append("_")
            last_was_sep = True
    slug = "".join(out).strip("_")
    return slug or "item"
