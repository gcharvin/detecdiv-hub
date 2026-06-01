from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from api.models import (
    MiscStorageItem,
    Project,
    ProjectLocation,
    RawDataset,
    RawDatasetLocation,
    StorageRoot,
    User,
)
from api.services.project_indexing import get_or_create_storage_root, slugify


DEFAULT_MIN_SIZE_BYTES = 10 * 1024 * 1024 * 1024


@dataclass
class MiscInventoryResult:
    source_path: str
    storage_root_name: str
    scanned_count: int
    indexed_count: int
    skipped_count: int
    timeout_count: int


@dataclass
class ChildProbe:
    child_dir_count: int
    child_file_count: int
    has_project_mat: bool
    has_raw_marker: bool
    has_tif: bool
    truncated: bool


@dataclass
class SizeProbe:
    total_bytes: int
    scan_status: str
    error_text: str | None = None


@dataclass
class CatalogCoverage:
    covered: bool
    exact_match: bool
    descendant_count: int
    descendant_bytes: int
    coverage_ratio: float


def inventory_misc_storage(
    session: Session,
    *,
    source_path: str,
    storage_root_id: int | None = None,
    parent_item_id: Any | None = None,
    storage_root_name: str | None = None,
    host_scope: str = "server",
    root_type: str = "misc_root",
    owner_user_key: str | None = None,
    visibility: str = "private",
    min_size_bytes: int = DEFAULT_MIN_SIZE_BYTES,
    max_depth: int = 2,
    du_timeout_sec: float = 45.0,
    include_cataloged: bool = False,
    metadata_json: dict[str, Any] | None = None,
) -> MiscInventoryResult:
    root = Path(source_path).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"Misc inventory source path does not exist: {root}")

    if storage_root_id is not None:
        storage_root = session.get(StorageRoot, storage_root_id)
        if storage_root is None:
            raise ValueError(f"Storage root not found: {storage_root_id}")
    else:
        storage_root = get_or_create_storage_root(
            session,
            root_path=str(root),
            storage_root_name=storage_root_name,
            host_scope=host_scope,
            root_type=root_type,
        )
    cataloged_paths = {} if include_cataloged else load_cataloged_paths(session)
    explicit_owner = find_user_by_key(session, owner_user_key) if owner_user_key else None
    run_metadata = dict(metadata_json or {})
    now = datetime.now(timezone.utc)

    scanned_count = 0
    indexed_count = 0
    skipped_count = 0
    timeout_count = 0

    for path, depth in iter_inventory_candidates(root, max_depth=max_depth):
        scanned_count += 1
        path_key = normalize_path(path)
        if not include_cataloged and path_key in cataloged_paths:
            skipped_count += 1
            continue

        size_probe = measure_path_size(path, timeout_sec=du_timeout_sec)
        if size_probe.scan_status == "timeout":
            timeout_count += 1
        if size_probe.scan_status == "measured" and size_probe.total_bytes < min_size_bytes:
            skipped_count += 1
            continue

        child_probe = probe_children(path)
        relative_path = relative_path_text(storage_root, path)
        owner, owner_hint = resolve_item_owner(
            session,
            explicit_owner=explicit_owner,
            storage_root=storage_root,
            root=root,
            relative_path=relative_path,
        )
        category = classify_misc_item(path, child_probe=child_probe, scan_status=size_probe.scan_status)
        catalog_coverage = CatalogCoverage(False, False, 0, 0, 0.0)
        if not include_cataloged:
            catalog_coverage = calculate_catalog_coverage(
                path_key=path_key,
                total_bytes=size_probe.total_bytes,
                cataloged_paths=cataloged_paths,
            )
        metadata = {
            **run_metadata,
            "absolute_path": str(path),
            "owner_hint": owner_hint,
            "du_timeout_sec": du_timeout_sec,
            "min_size_bytes": min_size_bytes,
            "include_cataloged": include_cataloged,
            "catalog_overlap": path_key in cataloged_paths,
            "catalog_covered": catalog_coverage.covered,
            "catalog_descendant_count": catalog_coverage.descendant_count,
            "catalog_descendant_bytes": catalog_coverage.descendant_bytes,
            "catalog_coverage_ratio": catalog_coverage.coverage_ratio,
            "scan_error": size_probe.error_text,
            "child_probe_truncated": child_probe.truncated,
        }
        if catalog_coverage.covered:
            upsert_misc_item(
                session,
                storage_root=storage_root,
                parent_item_id=parent_item_id if depth == 1 else None,
                owner=owner,
                relative_path=relative_path,
                display_name=path.name,
                item_kind="directory" if path.is_dir() else "file",
                category="cataloged_container",
                visibility=visibility,
                status="cataloged",
                scan_depth=depth,
                scan_status=size_probe.scan_status,
                total_bytes=size_probe.total_bytes,
                child_dir_count=child_probe.child_dir_count,
                child_file_count=child_probe.child_file_count,
                metadata_json=metadata,
                now=now,
            )
            skipped_count += 1
            continue
        upsert_misc_item(
            session,
            storage_root=storage_root,
            parent_item_id=parent_item_id if depth == 1 else None,
            owner=owner,
            relative_path=relative_path,
            display_name=path.name,
            item_kind="directory" if path.is_dir() else "file",
            category=category,
            visibility=visibility,
            status="indexed",
            scan_depth=depth,
            scan_status=size_probe.scan_status,
            total_bytes=size_probe.total_bytes,
            child_dir_count=child_probe.child_dir_count,
            child_file_count=child_probe.child_file_count,
            metadata_json=metadata,
            now=now,
        )
        indexed_count += 1

    session.flush()
    return MiscInventoryResult(
        source_path=str(root),
        storage_root_name=storage_root.name,
        scanned_count=scanned_count,
        indexed_count=indexed_count,
        skipped_count=skipped_count,
        timeout_count=timeout_count,
    )


def iter_inventory_candidates(root: Path, *, max_depth: int):
    max_depth = max(1, min(int(max_depth or 1), 5))
    stack: list[tuple[Path, int]] = [(root, 0)]
    while stack:
        current, depth = stack.pop()
        if depth >= max_depth:
            continue
        try:
            entries = sorted(current.iterdir(), key=lambda entry: entry.name.lower())
        except OSError:
            continue
        for entry in entries:
            child_depth = depth + 1
            yield entry, child_depth
            if child_depth < max_depth and entry.is_dir():
                stack.append((entry, child_depth))


def measure_path_size(path: Path, *, timeout_sec: float) -> SizeProbe:
    if path.is_file():
        try:
            return SizeProbe(total_bytes=path.stat().st_size, scan_status="measured")
        except OSError as exc:
            return SizeProbe(total_bytes=0, scan_status="error", error_text=str(exc))

    command = ["du", "-sb", "--", str(path)]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(float(timeout_sec), 1.0),
        )
    except subprocess.TimeoutExpired:
        return SizeProbe(total_bytes=0, scan_status="timeout", error_text="du timed out")
    except OSError as exc:
        return python_size_fallback(path, error_prefix=str(exc))

    if completed.returncode != 0:
        return python_size_fallback(path, error_prefix=completed.stderr.strip())
    first_field = (completed.stdout.strip().split() or ["0"])[0]
    try:
        return SizeProbe(total_bytes=int(first_field), scan_status="measured")
    except ValueError:
        return SizeProbe(total_bytes=0, scan_status="error", error_text=completed.stdout.strip())


def python_size_fallback(path: Path, *, error_prefix: str | None = None) -> SizeProbe:
    total = 0
    try:
        for dirpath, _, filenames in os.walk(path):
            for name in filenames:
                try:
                    total += (Path(dirpath) / name).stat().st_size
                except OSError:
                    continue
    except OSError as exc:
        message = f"{error_prefix}; {exc}" if error_prefix else str(exc)
        return SizeProbe(total_bytes=0, scan_status="error", error_text=message)
    return SizeProbe(total_bytes=total, scan_status="measured", error_text=error_prefix)


def probe_children(path: Path, *, limit: int = 5000) -> ChildProbe:
    if not path.is_dir():
        return ChildProbe(0, 0, False, False, False, False)

    child_dir_count = 0
    child_file_count = 0
    has_project_mat = False
    has_raw_marker = False
    has_tif = False
    truncated = False
    marker_names = {"metadata.txt", "acquisitionmetadata.txt", "displaysettings.txt", "displaysettings.json", "ndtiff.index"}
    try:
        for index, entry in enumerate(path.iterdir()):
            if index >= limit:
                truncated = True
                break
            name_lower = entry.name.lower()
            try:
                if entry.is_dir():
                    child_dir_count += 1
                    if name_lower.endswith((".ome.zarr", ".zarr")):
                        has_raw_marker = True
                else:
                    child_file_count += 1
                    if name_lower.endswith(".mat"):
                        has_project_mat = True
                    if name_lower in marker_names:
                        has_raw_marker = True
                    if name_lower.endswith((".tif", ".tiff")):
                        has_tif = True
            except OSError:
                continue
    except OSError:
        pass
    return ChildProbe(child_dir_count, child_file_count, has_project_mat, has_raw_marker, has_tif, truncated)


def classify_misc_item(path: Path, *, child_probe: ChildProbe, scan_status: str) -> str:
    name = path.name.lower()
    if scan_status == "timeout":
        if "classi" in name or "training" in name:
            return "classifier_training"
        if "raw" in name or "data" in name or "manip" in name:
            return "large_unclassified_data"
        return "large_timeout"
    if name.endswith((".ome.zarr", ".zarr")) or child_probe.has_raw_marker:
        return "candidate_raw_dataset"
    if child_probe.has_project_mat:
        return "legacy_project_candidate"
    if "classi" in name or "training" in name or "classifier" in name:
        return "classifier_training"
    if "nobackup" in name or "no backup" in name:
        return "no_backup"
    if "old" in name or "archive" in name:
        return "legacy_or_archive"
    if "software" in name or "matlab" in name or "code" in name:
        return "software_or_code"
    if "movie" in name or "movies" in name:
        return "movies"
    if child_probe.has_tif:
        return "image_files"
    return "misc"


def upsert_misc_item(
    session: Session,
    *,
    storage_root: StorageRoot,
    parent_item_id: Any | None,
    owner: User | None,
    relative_path: str,
    display_name: str,
    item_kind: str,
    category: str,
    visibility: str,
    status: str,
    scan_depth: int,
    scan_status: str,
    total_bytes: int,
    child_dir_count: int,
    child_file_count: int,
    metadata_json: dict[str, Any],
    now: datetime,
) -> MiscStorageItem:
    item = session.scalars(
        select(MiscStorageItem).where(
            MiscStorageItem.storage_root_id == storage_root.id,
            MiscStorageItem.relative_path == relative_path,
        )
    ).first()
    if item is None:
        item = MiscStorageItem(
            storage_root_id=storage_root.id,
            relative_path=relative_path,
            display_name=display_name,
        )
        session.add(item)

    item.owner_user_id = owner.id if owner is not None else None
    if parent_item_id is not None:
        item.parent_item_id = parent_item_id
    item.display_name = display_name
    item.item_kind = item_kind
    item.category = category
    item.status = status
    item.visibility = visibility
    item.scan_depth = scan_depth
    item.scan_status = scan_status
    item.total_bytes = total_bytes
    item.child_dir_count = child_dir_count
    item.child_file_count = child_file_count
    item.last_size_scan_at = now if scan_status == "measured" else item.last_size_scan_at
    item.last_seen_at = now
    item.metadata_json = {**dict(item.metadata_json or {}), **metadata_json}
    return item


def load_cataloged_paths(session: Session) -> dict[str, int]:
    paths: dict[str, int] = {}
    rows = session.execute(
        select(StorageRoot.path_prefix, ProjectLocation.relative_path, Project.total_bytes)
        .join(ProjectLocation, ProjectLocation.storage_root_id == StorageRoot.id)
        .join(Project, Project.id == ProjectLocation.project_id)
    )
    for prefix, relative_path, total_bytes in rows:
        path_key = normalize_path(Path(prefix) / (relative_path or ""))
        paths[path_key] = max(paths.get(path_key, 0), int(total_bytes or 0))

    rows = session.execute(
        select(StorageRoot.path_prefix, RawDatasetLocation.relative_path, RawDataset.total_bytes)
        .join(RawDatasetLocation, RawDatasetLocation.storage_root_id == StorageRoot.id)
        .join(RawDataset, RawDataset.id == RawDatasetLocation.raw_dataset_id)
    )
    for prefix, relative_path, total_bytes in rows:
        path_key = normalize_path(Path(prefix) / (relative_path or ""))
        paths[path_key] = max(paths.get(path_key, 0), int(total_bytes or 0))
    return paths


def calculate_catalog_coverage(
    *,
    path_key: str,
    total_bytes: int,
    cataloged_paths: dict[str, int],
) -> CatalogCoverage:
    if path_key in cataloged_paths:
        return CatalogCoverage(True, True, 1, int(cataloged_paths.get(path_key) or 0), 1.0)

    prefix = f"{path_key.rstrip('/')}/"
    descendant_bytes = 0
    descendant_count = 0
    for catalog_path, catalog_bytes in cataloged_paths.items():
        if catalog_path.startswith(prefix):
            descendant_count += 1
            descendant_bytes += int(catalog_bytes or 0)

    if descendant_count == 0:
        return CatalogCoverage(False, False, 0, 0, 0.0)

    if total_bytes <= 0:
        return CatalogCoverage(False, False, descendant_count, descendant_bytes, 0.0)

    coverage_ratio = min(float(descendant_bytes) / float(total_bytes), 1.0)
    return CatalogCoverage(
        covered=coverage_ratio >= 0.9,
        exact_match=False,
        descendant_count=descendant_count,
        descendant_bytes=descendant_bytes,
        coverage_ratio=coverage_ratio,
    )


def relative_path_text(storage_root: StorageRoot, path: Path) -> str:
    root_path = Path(storage_root.path_prefix).expanduser().resolve()
    try:
        rel = path.resolve().relative_to(root_path)
    except ValueError:
        return str(path.resolve())
    text = str(rel)
    return "" if text == "." else text


def normalize_path(path: Path) -> str:
    try:
        return str(path.expanduser().resolve())
    except OSError:
        return str(path)


def find_user_by_key(session: Session, user_key: str | None) -> User | None:
    text = str(user_key or "").strip()
    if not text:
        return None
    return session.scalars(select(User).where(User.user_key == text)).first()


def resolve_item_owner(
    session: Session,
    *,
    explicit_owner: User | None,
    storage_root: StorageRoot,
    root: Path,
    relative_path: str,
) -> tuple[User | None, str | None]:
    if explicit_owner is not None:
        return explicit_owner, explicit_owner.user_key

    root_path = Path(storage_root.path_prefix).expanduser().resolve()
    owner_hint = ""
    parts = Path(relative_path).parts if relative_path else ()
    if root_path.name.lower() == "data" and parts:
        owner_hint = parts[0]
    elif root.parent.name.lower() == "data":
        owner_hint = root.name
    if not owner_hint:
        return None, None

    user = session.scalars(select(User).where(User.user_key == owner_hint)).first()
    if user is None:
        user = session.scalars(select(User).where(User.display_name == owner_hint)).first()
    return user, owner_hint


def build_misc_storage_root_name(root_path: str) -> str:
    root = Path(root_path)
    return f"misc_{slugify(root.name or 'root')}"
