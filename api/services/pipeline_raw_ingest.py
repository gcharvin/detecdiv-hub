"""Catalog raw input used by a Hub pipeline run before MATLAB starts."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from api.models import Job, Project, ProjectRawLink, StorageRoot, User
from api.services.project_indexing import find_existing_raw_dataset_for_path
from api.services.raw_dataset_ingest import ingest_raw_dataset_from_directory


def pipeline_run_requests_raw_ingest(params_json: dict | None) -> bool:
    run_request = dict((params_json or {}).get("run_request") or {})
    return bool(run_request.get("ingest_raw_dataset"))


def ingest_pipeline_run_raw_dataset(session: Session, *, job: Job) -> dict:
    """Upsert and link the server-visible raw input of *job*.

    This deliberately runs on the storage-visible worker.  The API VM must not
    inspect server paths.  PostgreSQL advisory locking serializes two jobs that
    target the same physical directory; the existing catalog identity is then
    reused rather than duplicated.
    """
    params = dict(job.params_json or {})
    run_request = dict(params.get("run_request") or {})
    paths = dict(run_request.get("paths") or {})
    raw_path_text = str(paths.get("server_raw_data_path") or paths.get("raw_data_path") or "").strip()
    if not raw_path_text:
        raise ValueError("Raw-dataset ingestion was requested but no server raw-data path was supplied.")
    if job.project_id is None:
        raise ValueError("Raw-dataset ingestion requires a catalogued project.")

    dataset_dir = Path(raw_path_text).expanduser().resolve()
    if not dataset_dir.is_dir():
        raise ValueError(f"Raw-data path is not an accessible directory on this worker: {dataset_dir}")

    storage_root = resolve_server_raw_storage_root(session, dataset_dir=dataset_dir)
    if storage_root is None:
        raise ValueError(
            "Raw-data path is outside every registered server raw storage root: "
            f"{dataset_dir}"
        )

    # The database constraint on external_key covers persistence identity; the
    # advisory lock also avoids a race before a second session can observe it.
    if session.bind is not None and session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:path_key))"), {"path_key": str(dataset_dir)})

    existing = find_existing_raw_dataset_for_path(session, dataset_dir=dataset_dir)
    owner = resolve_raw_dataset_owner(session, job=job)
    raw_dataset = ingest_raw_dataset_from_directory(
        session,
        owner=owner,
        visibility=resolve_raw_dataset_visibility(session, job=job),
        storage_root_name=storage_root.name,
        host_scope=storage_root.host_scope,
        root_type=storage_root.root_type,
        root_path=Path(storage_root.path_prefix).resolve(),
        dataset_dir=dataset_dir,
        external_key=existing.external_key if existing is not None else None,
        source_label="pipeline_run_raw_ingest",
        source_metadata={"source_pipeline_job_id": str(job.id)},
        status="indexed",
        completeness_status="complete",
    )
    link = session.scalars(
        select(ProjectRawLink).where(
            ProjectRawLink.project_id == job.project_id,
            ProjectRawLink.raw_dataset_id == raw_dataset.id,
            ProjectRawLink.link_type == "source",
        )
    ).first()
    linked = link is None
    if linked:
        session.add(ProjectRawLink(project_id=job.project_id, raw_dataset_id=raw_dataset.id, link_type="source"))
    session.flush()
    return {
        "raw_dataset_id": str(raw_dataset.id),
        "catalog_action": "reused" if existing is not None else "created",
        "project_link_created": linked,
        "server_raw_data_path": str(dataset_dir),
        "storage_root_name": storage_root.name,
    }


def resolve_server_raw_storage_root(session: Session, *, dataset_dir: Path) -> StorageRoot | None:
    candidates = session.scalars(
        select(StorageRoot).where(StorageRoot.host_scope == "server")
    ).all()
    matching: list[StorageRoot] = []
    for root in candidates:
        if str(root.root_type or "") not in {"raw_root", "raw", "dataset_root"}:
            continue
        try:
            dataset_dir.relative_to(Path(root.path_prefix).expanduser().resolve())
        except ValueError:
            continue
        matching.append(root)
    return max(matching, key=lambda root: len(str(root.path_prefix))) if matching else None


def resolve_raw_dataset_owner(session: Session, *, job: Job) -> User:
    params = dict(job.params_json or {})
    user_key = str((params.get("client_context") or {}).get("submitted_by_user_key") or "").strip()
    if user_key:
        user = session.scalars(select(User).where(User.user_key == user_key)).first()
        if user is not None:
            return user
    project = session.get(Project, job.project_id)
    if project is not None and project.owner is not None:
        return project.owner
    raise ValueError("Cannot determine an owner for the raw dataset created by this pipeline run.")


def resolve_raw_dataset_visibility(session: Session, *, job: Job) -> str:
    project = session.get(Project, job.project_id)
    return str(project.visibility) if project is not None else "private"
