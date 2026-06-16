from __future__ import annotations

import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session, joinedload

from api.models import (
    Project,
    ProjectDeletionEvent,
    ProjectLocation,
    ProjectRawLink,
    Job,
    RawDataset,
    RawDatasetLocation,
    User,
)
from api.services.path_resolution import compose_storage_path


@dataclass
class DeletionPreviewData:
    project: Project
    delete_project_files: bool
    delete_linked_raw_data: bool
    reclaimable_bytes: int
    preview_json: dict


class ProjectDeletionBlockedError(RuntimeError):
    pass


def build_deletion_preview(
    session: Session,
    *,
    project: Project,
    delete_project_files: bool,
    delete_linked_raw_data: bool,
) -> DeletionPreviewData:
    project_locations = list(project.locations)
    project_file_paths = []
    project_backup_file_paths = []
    project_dir_paths = []
    project_bytes = 0

    if delete_project_files:
        for location in project_locations:
            file_path, dir_path = resolve_project_location_paths(location)
            if file_path:
                project_file_paths.append(file_path)
                project_backup_file_paths.extend(resolve_project_backup_file_paths(file_path))
            if dir_path:
                project_dir_paths.append(dir_path)
        project_bytes = int(project.total_bytes or 0)

    linked_raw = []
    raw_bytes = 0
    skipped_raw = []
    if delete_linked_raw_data:
        linked_raw, raw_bytes, skipped_raw = preview_linked_raw_datasets(session, project)

    reclaimable_bytes = project_bytes + raw_bytes
    filesystem_targets = [
        *[filesystem_delete_target(path, target_kind="project_file") for path in project_file_paths],
        *[filesystem_delete_target(path, target_kind="project_backup_file") for path in project_backup_file_paths],
        *[filesystem_delete_target(path, target_kind="project_dir") for path in project_dir_paths],
        *[
            filesystem_delete_target(location, target_kind="raw_location")
            for item in linked_raw
            for location in item.get("locations", [])
        ],
    ]
    filesystem_blockers = [
        target
        for target in filesystem_targets
        if target.get("exists") and not target.get("writable")
    ]
    preview_json = {
        "deletion_plan": {
            "database_project_row": True,
            "project_file_count": len(project_file_paths) if delete_project_files else 0,
            "project_backup_file_count": len(project_backup_file_paths) if delete_project_files else 0,
            "project_dir_count": len(project_dir_paths) if delete_project_files else 0,
            "linked_raw_dataset_count": len(linked_raw),
            "linked_raw_location_count": sum(len(item.get("locations", [])) for item in linked_raw),
            "skipped_linked_raw_dataset_count": len(skipped_raw),
            "filesystem_blocked": bool(filesystem_blockers),
            "filesystem_blockers": filesystem_blockers,
        },
        "project": {
            "project_name": project.project_name,
            "project_mat_bytes": int(project.project_mat_bytes or 0),
            "project_dir_bytes": int(project.project_dir_bytes or 0),
            "total_bytes": int(project.total_bytes or 0),
            "paths": {
                "project_files": project_file_paths,
                "project_backup_files": project_backup_file_paths,
                "project_dirs": project_dir_paths,
            },
        },
        "linked_raw_datasets": linked_raw,
        "skipped_linked_raw_datasets": skipped_raw,
    }
    return DeletionPreviewData(
        project=project,
        delete_project_files=delete_project_files,
        delete_linked_raw_data=delete_linked_raw_data,
        reclaimable_bytes=reclaimable_bytes,
        preview_json=preview_json,
    )


def record_deletion_preview(
    session: Session,
    *,
    preview: DeletionPreviewData,
    requested_by_user: User,
) -> ProjectDeletionEvent:
    event = ProjectDeletionEvent(
        project_id=preview.project.id,
        requested_by_user_id=requested_by_user.id,
        status="previewed",
        delete_project_files=preview.delete_project_files,
        delete_linked_raw_data=preview.delete_linked_raw_data,
        reclaimable_bytes=preview.reclaimable_bytes,
        preview_json=preview.preview_json,
        result_json={},
    )
    session.add(event)
    session.flush()
    return event


def queue_project_deletion(
    session: Session,
    *,
    preview: DeletionPreviewData,
    requested_by_user: User,
) -> tuple[ProjectDeletionEvent, Job]:
    event = record_deletion_preview(session, preview=preview, requested_by_user=requested_by_user)
    event.status = "queued"
    job = Job(
        project_id=preview.project.id,
        requested_mode="server",
        priority=20,
        requested_by=requested_by_user.user_key,
        requested_from_host="api",
        params_json={
            "job_kind": "project_deletion",
            "project_deletion_event_id": str(event.id),
            "delete_project_files": preview.delete_project_files,
            "delete_linked_raw_data": preview.delete_linked_raw_data,
        },
        status="queued",
    )
    session.add(job)
    session.flush()
    event.result_json = {"job_id": str(job.id), "message": "Queued for worker filesystem deletion."}
    return event, job


def execute_project_deletion(
    session: Session,
    *,
    preview: DeletionPreviewData,
    requested_by_user: User | None = None,
    event: ProjectDeletionEvent | None = None,
) -> ProjectDeletionEvent:
    if event is None:
        event = ProjectDeletionEvent(
            project_id=preview.project.id,
            requested_by_user_id=requested_by_user.id if requested_by_user is not None else None,
            status="previewed",
            delete_project_files=preview.delete_project_files,
            delete_linked_raw_data=preview.delete_linked_raw_data,
            reclaimable_bytes=preview.reclaimable_bytes,
            preview_json=preview.preview_json,
            result_json={},
        )
        session.add(event)
        session.flush()
    else:
        event.delete_project_files = preview.delete_project_files
        event.delete_linked_raw_data = preview.delete_linked_raw_data
        event.reclaimable_bytes = preview.reclaimable_bytes
        event.preview_json = preview.preview_json
    deleted_project_files = []
    deleted_project_backup_files = []
    deleted_project_dirs = []
    deleted_raw_locations = []
    deleted_raw_datasets = []
    errors = []

    blockers = preview.preview_json.get("deletion_plan", {}).get("filesystem_blockers", [])
    if (preview.delete_project_files or preview.delete_linked_raw_data) and blockers:
        blocked_paths = ", ".join(str(item.get("path")) for item in blockers[:5])
        suffix = "..." if len(blockers) > 5 else ""
        raise ProjectDeletionBlockedError(
            f"Filesystem deletion is blocked for {len(blockers)} target(s): {blocked_paths}{suffix}"
        )

    if preview.delete_project_files:
        for file_path in preview.preview_json["project"]["paths"]["project_files"]:
            try:
                path = Path(file_path)
                if path.is_file():
                    path.unlink()
                    deleted_project_files.append(file_path)
            except OSError as exc:
                errors.append(f"Failed to delete file {file_path}: {exc}")

        for file_path in preview.preview_json["project"]["paths"].get("project_backup_files", []):
            try:
                path = Path(file_path)
                if path.is_file():
                    path.unlink()
                    deleted_project_backup_files.append(file_path)
            except OSError as exc:
                errors.append(f"Failed to delete project backup file {file_path}: {exc}")

        for dir_path in preview.preview_json["project"]["paths"]["project_dirs"]:
            try:
                path = Path(dir_path)
                if path.is_dir():
                    shutil.rmtree(path)
                    deleted_project_dirs.append(dir_path)
            except OSError as exc:
                errors.append(f"Failed to delete folder {dir_path}: {exc}")

    if preview.delete_linked_raw_data:
        raw_section = preview.preview_json.get("linked_raw_datasets", [])
        for raw_item in raw_section:
            raw_dataset = session.scalars(
                select(RawDataset)
                .options(joinedload(RawDataset.locations))
                .where(RawDataset.id == raw_item["raw_dataset_id"])
            ).first()
            if raw_dataset is None:
                continue
            for location in list(raw_dataset.locations):
                raw_path = resolve_raw_location_path(location)
                try:
                    if raw_path.is_file():
                        raw_path.unlink()
                    elif raw_path.is_dir():
                        shutil.rmtree(raw_path)
                    deleted_raw_locations.append(str(raw_path))
                except OSError as exc:
                    errors.append(f"Failed to delete raw location {raw_path}: {exc}")
            deleted_raw_datasets.append(str(raw_dataset.id))
            session.execute(delete(RawDataset).where(RawDataset.id == raw_dataset.id))

    preview.project.status = "deleted"
    preview.project.health_status = "deleted"
    preview.project.visibility = "private"
    preview.project.metadata_json = {
        **(preview.project.metadata_json or {}),
        "deleted_at": datetime.now(timezone.utc).isoformat(),
    }
    event.status = "deleted" if not errors else "partial_failed"
    event.result_json = {
        "deleted_project_files": deleted_project_files,
        "deleted_project_backup_files": deleted_project_backup_files,
        "deleted_project_dirs": deleted_project_dirs,
        "deleted_raw_locations": deleted_raw_locations,
        "deleted_raw_datasets": deleted_raw_datasets,
        "errors": errors,
    }
    event.executed_at = datetime.now(timezone.utc)
    session.flush()
    return event


def execute_project_deletion_job(session: Session, *, job: Job) -> dict:
    event_id = (job.params_json or {}).get("project_deletion_event_id")
    if not event_id:
        raise ValueError(f"Project deletion job {job.id} is missing project_deletion_event_id")
    event = session.get(ProjectDeletionEvent, uuid.UUID(str(event_id)))
    if event is None:
        raise ValueError(f"Project deletion event {event_id} does not exist")
    project = session.scalars(
        select(Project)
        .options(
            joinedload(Project.locations).joinedload(ProjectLocation.storage_root),
        )
        .where(Project.id == event.project_id, Project.status != "deleted")
    ).unique().first()
    if project is None:
        event.status = "failed"
        event.result_json = {"errors": [f"Project {event.project_id} does not exist or is already deleted."]}
        event.executed_at = datetime.now(timezone.utc)
        session.flush()
        return {"event_id": str(event.id), "status": event.status, "result_json": event.result_json}

    preview = build_deletion_preview(
        session,
        project=project,
        delete_project_files=event.delete_project_files,
        delete_linked_raw_data=event.delete_linked_raw_data,
    )
    executed = execute_project_deletion(session, preview=preview, event=event)
    return {
        "event_id": str(executed.id),
        "status": executed.status,
        "reclaimable_bytes": executed.reclaimable_bytes,
        "result_json": executed.result_json,
    }


def finalize_project_deletion_failure(session: Session, *, job: Job, error_text: str) -> None:
    if (job.params_json or {}).get("job_kind") != "project_deletion":
        return
    event_id = (job.params_json or {}).get("project_deletion_event_id")
    if not event_id:
        return
    event = session.get(ProjectDeletionEvent, uuid.UUID(str(event_id)))
    if event is None:
        return
    event.status = "failed"
    event.result_json = {
        **(event.result_json or {}),
        "errors": [error_text],
    }
    event.executed_at = datetime.now(timezone.utc)
    session.flush()


def resolve_project_location_paths(location: ProjectLocation) -> tuple[str, str]:
    root = location.storage_root.path_prefix
    rel = location.relative_path or ""
    file_name = location.project_file_name or ""
    file_path = compose_storage_path(root, rel, file_name) if file_name else ""
    project_dir = ""
    if file_name:
        stem = Path(file_name).stem
        project_dir = compose_storage_path(root, rel, stem)
    return file_path, project_dir


def resolve_project_backup_file_paths(project_file_path: str) -> list[str]:
    path = Path(project_file_path)
    backup_names = {
        f"{path.name}.bakk".lower(),
        f"{path.stem}.bakk".lower(),
    }
    candidates = []
    try:
        candidates.extend(
            entry
            for entry in path.parent.iterdir()
            if entry.is_file() and entry.name.lower() in backup_names
        )
    except OSError:
        pass
    candidates.extend(
        [
            path.with_name(f"{path.name}.bakk"),
            path.with_name(f"{path.stem}.bakk"),
        ]
    )
    paths = []
    seen = set()
    for candidate in candidates:
        if not candidate.is_file():
            continue
        key = str(candidate.resolve(strict=False)).casefold()
        if key in seen:
            continue
        seen.add(key)
        paths.append(key)
    return paths


def resolve_raw_location_path(location: RawDatasetLocation) -> Path:
    return Path(location.storage_root.path_prefix) / (location.relative_path or "")


def filesystem_delete_target(path_value: str, *, target_kind: str) -> dict:
    path = Path(path_value)
    exists = path.exists()
    check_path = path if exists else first_existing_parent(path)
    writable = bool(check_path and check_path.exists() and is_path_writable_for_delete(check_path))
    mount = mount_info_for_path(check_path) if check_path else None
    reason = None
    if not exists:
        reason = "path_not_found"
    elif mount and mount.get("read_only"):
        reason = "mount_read_only"
    elif not writable:
        reason = "not_writable"
    return {
        "kind": target_kind,
        "path": str(path),
        "exists": exists,
        "writable": writable,
        "reason": reason,
        "checked_path": str(check_path) if check_path else "",
        "mount_point": mount.get("mount_point") if mount else None,
        "mount_options": mount.get("options") if mount else None,
    }


def first_existing_parent(path: Path) -> Path | None:
    for candidate in [path, *path.parents]:
        if candidate.exists():
            return candidate
    return None


def is_path_writable_for_delete(path: Path) -> bool:
    mount = mount_info_for_path(path)
    if mount and mount.get("read_only"):
        return False
    if path.is_dir():
        return os_access_writable(path)
    return os_access_writable(path.parent)


def os_access_writable(path: Path) -> bool:
    try:
        import os

        return os.access(path, os.W_OK)
    except OSError:
        return False


def mount_info_for_path(path: Path | None) -> dict | None:
    if path is None:
        return None
    try:
        resolved = path.resolve(strict=False)
        mount_entries = []
        with Path("/proc/self/mountinfo").open("r", encoding="utf-8") as handle:
            for line in handle:
                fields = line.split()
                if len(fields) < 6:
                    continue
                mount_point = fields[4].replace("\\040", " ")
                options = fields[5].split(",")
                mount_entries.append(
                    {
                        "mount_point": mount_point,
                        "options": options,
                        "read_only": "ro" in options,
                    }
                )
        matching = [
            entry
            for entry in mount_entries
            if path_is_relative_to(resolved, Path(entry["mount_point"]))
        ]
        return max(matching, key=lambda entry: len(entry["mount_point"]), default=None)
    except OSError:
        return None


def path_is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def preview_linked_raw_datasets(session: Session, project: Project) -> tuple[list[dict], int, list[dict]]:
    links = list(
        session.scalars(
            select(ProjectRawLink).where(ProjectRawLink.project_id == project.id).order_by(ProjectRawLink.id.asc())
        )
    )
    linked_raw = []
    skipped = []
    total_bytes = 0
    for link in links:
        raw_dataset = session.scalars(
            select(RawDataset)
            .options(joinedload(RawDataset.locations))
            .where(RawDataset.id == link.raw_dataset_id)
        ).first()
        if raw_dataset is None:
            continue

        other_links = session.scalar(
            select(func.count(ProjectRawLink.id)).where(
                ProjectRawLink.raw_dataset_id == raw_dataset.id,
                ProjectRawLink.project_id != project.id,
            )
        )
        locations = [str(resolve_raw_location_path(loc)) for loc in raw_dataset.locations]
        raw_info = {
            "raw_dataset_id": str(raw_dataset.id),
            "acquisition_label": raw_dataset.acquisition_label,
            "total_bytes": int(raw_dataset.total_bytes or 0),
            "locations": locations,
        }
        if other_links and other_links > 0:
            raw_info["reason"] = "linked_to_other_projects"
            skipped.append(raw_info)
            continue

        linked_raw.append(raw_info)
        total_bytes += int(raw_dataset.total_bytes or 0)
    return linked_raw, total_bytes, skipped
