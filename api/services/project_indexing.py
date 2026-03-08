from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from api.models import Project, ProjectLocation, StorageRoot


@dataclass
class ProjectIndexResult:
    root_path: str
    storage_root_name: str
    scanned_projects: int
    indexed_projects: int
    deleted_projects: int


def index_project_root(
    session: Session,
    *,
    root_path: str,
    storage_root_name: str | None = None,
    host_scope: str = "server",
    root_type: str = "project_root",
    clear_existing_for_root: bool = False,
) -> ProjectIndexResult:
    root = Path(root_path).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"Project root does not exist: {root}")

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
    for mat_path, project_dir in iter_project_candidates(root):
        scanned_projects += 1
        project = upsert_project_from_paths(
            session,
            storage_root=storage_root,
            root_path=root,
            mat_path=mat_path,
            project_dir=project_dir,
        )
        seen_project_ids.add(project.id)
        indexed_projects += 1

    deleted_projects = 0
    if clear_existing_for_root and existing_project_ids:
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

    return ProjectIndexResult(
        root_path=str(root),
        storage_root_name=storage_root.name,
        scanned_projects=scanned_projects,
        indexed_projects=indexed_projects,
        deleted_projects=deleted_projects,
    )


def iter_project_candidates(root_path: Path):
    for mat_path in sorted(root_path.rglob("*.mat")):
        project_dir = mat_path.with_suffix("")
        if not project_dir.is_dir():
            continue
        yield mat_path.resolve(), project_dir.resolve()


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
    storage_root: StorageRoot,
    root_path: Path,
    mat_path: Path,
    project_dir: Path,
) -> Project:
    relative_parent = project_relative_parent(root_path, mat_path)
    project_key = build_project_key(str(mat_path), relative_parent, mat_path.stem)
    project = session.scalars(select(Project).where(Project.project_key == project_key)).first()
    metadata = {
        "source": "hub_indexer",
        "project_mat_abs": str(mat_path),
        "project_dir_abs": str(project_dir),
        "project_rel_from_root": str(project_dir.relative_to(root_path)),
    }

    if project is None:
        project = Project(
            project_key=project_key,
            project_name=mat_path.stem,
            status="indexed",
            health_status="ok",
            metadata_json=metadata,
        )
        session.add(project)
        session.flush()
    else:
        project.project_name = mat_path.stem
        project.status = "indexed"
        project.health_status = "ok"
        project.metadata_json = metadata
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
    return project


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
