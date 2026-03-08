from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

from sqlalchemy import select


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.db import SessionLocal
from api.models import Project, ProjectLocation, StorageRoot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import DetecDiv local SQLite catalog entries into detecdiv-hub."
    )
    parser.add_argument("sqlite_catalog", help="Path to detecdiv_catalog.sqlite")
    parser.add_argument(
        "--host-scope",
        default="client",
        choices=["client", "server", "all"],
        help="Host scope assigned to imported storage roots.",
    )
    parser.add_argument(
        "--root-type",
        default="project_root",
        help="Root type assigned to imported storage roots.",
    )
    return parser.parse_args()


def load_catalog_rows(sqlite_catalog: Path) -> list[sqlite3.Row]:
    conn = sqlite3.connect(sqlite_catalog)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                p.name,
                p.project_mat_abs,
                p.project_dir_abs,
                p.project_rel_from_root,
                p.health_status,
                p.raw_status,
                p.fov_count,
                p.roi_count,
                p.classifier_count,
                p.processor_count,
                p.pipeline_run_count,
                p.available_raw_count,
                p.missing_raw_count,
                p.metadata_json,
                r.abs_path AS root_abs_path
            FROM catalog_projects p
            INNER JOIN catalog_roots r ON r.id = p.root_id
            ORDER BY r.abs_path, p.project_rel_from_root, p.name
            """
        ).fetchall()
    finally:
        conn.close()
    return rows


def get_or_create_storage_root(session, *, root_path: str, root_type: str, host_scope: str) -> StorageRoot:
    root_name = build_storage_root_name(root_path)
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
    elif root.path_prefix != root_path or root.host_scope != host_scope or root.root_type != root_type:
        root.path_prefix = root_path
        root.host_scope = host_scope
        root.root_type = root_type
        session.flush()
    return root


def build_storage_root_name(root_path: str) -> str:
    stem = Path(root_path).name or "root"
    slug = slugify(stem)
    suffix = hashlib.sha1(root_path.encode("utf-8")).hexdigest()[:8]
    return f"catalog_{slug}_{suffix}"


def build_project_key(project_mat_abs: str, project_rel_from_root: str | None, project_name: str) -> str:
    label = project_rel_from_root or project_name
    slug = slugify(label or "project")
    suffix = hashlib.sha1(project_mat_abs.encode("utf-8")).hexdigest()[:10]
    return f"{slug}_{suffix}"


def slugify(value: str) -> str:
    out = []
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


def parse_metadata(row: sqlite3.Row) -> dict:
    metadata = {
        "source": "sqlite_catalog_import",
        "project_mat_abs": row["project_mat_abs"],
        "project_dir_abs": row["project_dir_abs"],
        "project_rel_from_root": row["project_rel_from_root"],
        "raw_status": row["raw_status"],
        "fov_count": row["fov_count"],
        "roi_count": row["roi_count"],
        "classifier_count": row["classifier_count"],
        "processor_count": row["processor_count"],
        "pipeline_run_count": row["pipeline_run_count"],
        "available_raw_count": row["available_raw_count"],
        "missing_raw_count": row["missing_raw_count"],
    }

    raw_metadata = row["metadata_json"]
    if raw_metadata:
        try:
            decoded = json.loads(raw_metadata)
        except json.JSONDecodeError:
            decoded = {"raw_text": raw_metadata}
        metadata["catalog_metadata"] = decoded

    return metadata


def upsert_project(session, row: sqlite3.Row, storage_root: StorageRoot) -> Project:
    project_key = build_project_key(
        row["project_mat_abs"],
        row["project_rel_from_root"],
        row["name"],
    )
    project = session.scalars(select(Project).where(Project.project_key == project_key)).first()
    if project is None:
        project = Project(
            project_key=project_key,
            project_name=row["name"],
            status="indexed",
            health_status=row["health_status"] or "ok",
            metadata_json=parse_metadata(row),
        )
        session.add(project)
        session.flush()
    else:
        project.project_name = row["name"]
        project.status = "indexed"
        project.health_status = row["health_status"] or "ok"
        project.metadata_json = parse_metadata(row)
        session.flush()

    relative_path = build_location_relative_path(
        row["project_rel_from_root"],
        row["project_mat_abs"],
        row["root_abs_path"],
    )
    project_file_name = Path(row["project_mat_abs"]).name
    location = session.scalars(
        select(ProjectLocation).where(
            ProjectLocation.project_id == project.id,
            ProjectLocation.storage_root_id == storage_root.id,
            ProjectLocation.relative_path == relative_path,
        )
    ).first()
    if location is None:
        location = ProjectLocation(
            project_id=project.id,
            storage_root_id=storage_root.id,
            relative_path=relative_path,
            project_file_name=project_file_name,
            access_mode="readwrite",
            is_preferred=True,
        )
        session.add(location)
    else:
        location.project_file_name = project_file_name
        location.access_mode = "readwrite"
        location.is_preferred = True

    session.flush()
    return project


def build_location_relative_path(
    project_rel_from_root: str | None, project_mat_abs: str, root_abs_path: str
) -> str:
    if project_rel_from_root:
        rel_parent = str(Path(project_rel_from_root).parent)
        if rel_parent not in (".", ""):
            return rel_parent

    try:
        return str(Path(project_mat_abs).resolve().parent.relative_to(Path(root_abs_path).resolve()))
    except ValueError:
        return ""


def main() -> None:
    args = parse_args()
    sqlite_catalog = Path(args.sqlite_catalog).expanduser().resolve()
    if not sqlite_catalog.is_file():
        raise SystemExit(f"SQLite catalog not found: {sqlite_catalog}")

    rows = load_catalog_rows(sqlite_catalog)
    if not rows:
        print(f"No catalog projects found in {sqlite_catalog}")
        return

    imported_projects = 0
    imported_roots: set[str] = set()
    with SessionLocal() as session:
        for row in rows:
            storage_root = get_or_create_storage_root(
                session,
                root_path=row["root_abs_path"],
                root_type=args.root_type,
                host_scope=args.host_scope,
            )
            imported_roots.add(storage_root.name)
            upsert_project(session, row, storage_root)
            imported_projects += 1

        session.commit()

    print(
        f"Imported {imported_projects} projects from {sqlite_catalog} "
        f"into {len(imported_roots)} storage roots."
    )


if __name__ == "__main__":
    main()
