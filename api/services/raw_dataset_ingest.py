from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from api.models import ExperimentProject, ExperimentRawLink, RawDataset, RawDatasetLocation, RawDatasetPosition, User
from api.services.project_indexing import get_or_create_storage_root, slugify
from api.services.storage_metrics import safe_dir_size


def build_raw_dataset_key(dataset_dir: Path) -> str:
    slug = slugify(dataset_dir.name)
    suffix = hashlib.sha1(str(dataset_dir).encode("utf-8")).hexdigest()[:10]
    return f"raw_{slug}_{suffix}"


def ingest_raw_dataset_from_directory(
    session: Session,
    *,
    owner: User,
    visibility: str,
    storage_root_name: str | None,
    host_scope: str,
    root_type: str,
    root_path: Path,
    dataset_dir: Path,
    preferred_experiment: ExperimentProject | None = None,
    source_label: str = "legacy_raw_ingest",
    source_metadata: dict | None = None,
    acquisition_label: str | None = None,
    microscope_name: str | None = None,
    external_key: str | None = None,
    status: str = "indexed",
    completeness_status: str = "complete",
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
) -> RawDataset:
    storage_root = get_or_create_storage_root(
        session,
        root_path=str(root_path),
        storage_root_name=storage_root_name,
        host_scope=host_scope,
        root_type=root_type,
    )

    external_key = external_key or build_raw_dataset_key(dataset_dir)
    raw_dataset = session.scalars(select(RawDataset).where(RawDataset.external_key == external_key)).first()
    total_bytes = safe_dir_size(dataset_dir)
    size_timestamp = datetime.now(timezone.utc)
    relative_path = str(dataset_dir.relative_to(root_path))
    data_format = detect_raw_dataset_format(dataset_dir, source_metadata or {})
    metadata = {
        "source": source_label,
        "dataset_dir_abs": str(dataset_dir),
        "dataset_rel_from_root": relative_path,
        "data_format": data_format,
    }
    if source_metadata:
        metadata.update(source_metadata)
    effective_label = acquisition_label or dataset_dir.name
    effective_started_at = started_at or size_timestamp
    effective_ended_at = ended_at or effective_started_at

    if raw_dataset is None:
        raw_dataset = RawDataset(
            owner_user_id=owner.id,
            external_key=external_key,
            microscope_name=microscope_name,
            acquisition_label=effective_label,
            data_format=data_format,
            visibility=visibility,
            status=status,
            completeness_status=completeness_status,
            lifecycle_tier="hot",
            archive_status="none",
            reclaimable_bytes=0,
            total_bytes=total_bytes,
            last_size_scan_at=size_timestamp,
            last_accessed_at=size_timestamp,
            started_at=effective_started_at,
            ended_at=effective_ended_at,
            metadata_json=metadata,
        )
        session.add(raw_dataset)
        session.flush()
    else:
        merged_metadata = dict(raw_dataset.metadata_json or {})
        merged_metadata.update(metadata)
        raw_dataset.owner_user_id = owner.id
        raw_dataset.microscope_name = microscope_name or raw_dataset.microscope_name
        raw_dataset.acquisition_label = effective_label
        raw_dataset.data_format = data_format
        raw_dataset.visibility = visibility
        raw_dataset.status = status
        raw_dataset.completeness_status = completeness_status
        raw_dataset.total_bytes = total_bytes
        raw_dataset.last_size_scan_at = size_timestamp
        raw_dataset.last_accessed_at = size_timestamp
        raw_dataset.started_at = effective_started_at
        raw_dataset.ended_at = effective_ended_at
        raw_dataset.metadata_json = merged_metadata
        session.flush()

    location = session.scalars(
        select(RawDatasetLocation).where(
            RawDatasetLocation.raw_dataset_id == raw_dataset.id,
            RawDatasetLocation.storage_root_id == storage_root.id,
        )
    ).first()
    if location is None:
        location = RawDatasetLocation(
            raw_dataset_id=raw_dataset.id,
            storage_root_id=storage_root.id,
            relative_path=relative_path,
            access_mode="read",
            is_preferred=True,
        )
        session.add(location)
    else:
        location.relative_path = relative_path
        location.access_mode = "read"
        location.is_preferred = True
    session.flush()

    upsert_raw_dataset_positions(
        session,
        raw_dataset=raw_dataset,
        dataset_dir=dataset_dir,
        source_metadata=source_metadata or {},
    )

    if preferred_experiment is not None:
        link = session.scalars(
            select(ExperimentRawLink).where(
                ExperimentRawLink.experiment_project_id == preferred_experiment.id,
                ExperimentRawLink.raw_dataset_id == raw_dataset.id,
            )
        ).first()
        if link is None:
            session.add(ExperimentRawLink(experiment_project_id=preferred_experiment.id, raw_dataset_id=raw_dataset.id))
            session.flush()
            preferred_experiment.total_raw_bytes = int(preferred_experiment.total_raw_bytes or 0) + int(
                raw_dataset.total_bytes or 0
            )
            preferred_experiment.last_indexed_at = size_timestamp
    return raw_dataset


def detect_raw_dataset_format(dataset_dir: Path, source_metadata: dict) -> str:
    for key in ("data_format", "raw_format", "format", "input_format"):
        value = str(source_metadata.get(key) or "").strip().lower()
        if value:
            return value

    name_lower = dataset_dir.name.lower()
    if name_lower.endswith(".ome.zarr"):
        return "ome_zarr"
    if name_lower.endswith(".zarr"):
        return "zarr"
    if (dataset_dir / "zarr.json").is_file():
        return "ome_zarr"
    if (dataset_dir / ".zattrs").is_file() and (dataset_dir / ".zgroup").is_file():
        return "zarr"
    if (dataset_dir / "NDTiff.index").is_file():
        return "ndtiff"

    try:
        entries = list(dataset_dir.iterdir())
    except OSError:
        return "unknown"

    file_names = [entry.name.lower() for entry in entries if entry.is_file()]
    child_dirs = [entry for entry in entries if entry.is_dir()]

    if any(name.endswith(".nd2") for name in file_names):
        return "nd2"
    if any(name.endswith(".czi") for name in file_names):
        return "czi"
    if any(name.endswith(".lif") for name in file_names):
        return "lif"
    if any(name.endswith(".ims") for name in file_names):
        return "ims"
    if any(name in {"metadata.txt", "acquisitionmetadata.txt"} for name in file_names):
        return "micromanager_tiff_dir"

    tiff_files = [name for name in file_names if name.endswith((".tif", ".tiff"))]
    if tiff_files:
        if any(name.endswith((".ome.tif", ".ome.tiff")) for name in tiff_files):
            return "ome_tiff"
        if len(tiff_files) == 1:
            return "single_tiff"
        return "tiff_sequence"

    for child in child_dirs:
        child_name = child.name.lower()
        if child_name.startswith(("pos", "position", "xy")):
            try:
                child_file_names = [entry.name.lower() for entry in child.iterdir() if entry.is_file()]
            except OSError:
                child_file_names = []
            if any(name == "NDTiff.index".lower() for name in child_file_names):
                return "ndtiff"
            child_tiffs = [name for name in child_file_names if name.endswith((".tif", ".tiff"))]
            if child_tiffs:
                if any(name.endswith((".ome.tif", ".ome.tiff")) for name in child_tiffs):
                    return "ome_tiff"
                return "tiff_sequence"

    return "unknown"


def upsert_raw_dataset_positions(
    session: Session,
    *,
    raw_dataset: RawDataset,
    dataset_dir: Path,
    source_metadata: dict,
) -> None:
    for position_index, position in enumerate(discover_raw_dataset_positions(dataset_dir, source_metadata)):
        position_key = position["position_key"]
        existing = session.scalars(
            select(RawDatasetPosition).where(
                RawDatasetPosition.raw_dataset_id == raw_dataset.id,
                RawDatasetPosition.position_key == position_key,
            )
        ).first()
        if existing is None:
            existing = RawDatasetPosition(
                raw_dataset_id=raw_dataset.id,
                position_key=position_key,
                display_name=position["display_name"],
                position_index=position.get("position_index", position_index),
                status="indexed",
                preview_status="missing",
                metadata_json=position.get("metadata_json") or {},
            )
            session.add(existing)
        else:
            existing.display_name = position["display_name"]
            existing.position_index = position.get("position_index", position_index)
            existing.status = "indexed"
            merged_metadata = dict(existing.metadata_json or {})
            merged_metadata.update(position.get("metadata_json") or {})
            existing.metadata_json = merged_metadata
    session.flush()


def discover_raw_dataset_positions(dataset_dir: Path, source_metadata: dict) -> list[dict]:
    metadata_positions = source_metadata.get("positions")
    if isinstance(metadata_positions, list):
        positions = []
        for index, item in enumerate(metadata_positions):
            if isinstance(item, dict):
                raw_key = item.get("position_key") or item.get("key") or item.get("name") or f"position_{index + 1}"
                display_name = item.get("display_name") or item.get("name") or str(raw_key)
                positions.append(
                    {
                        "position_key": slugify(str(raw_key)),
                        "display_name": str(display_name),
                        "position_index": item.get("position_index", index),
                        "metadata_json": item,
                    }
                )
            elif item:
                positions.append(
                    {
                        "position_key": slugify(str(item)),
                        "display_name": str(item),
                        "position_index": index,
                        "metadata_json": {"source": "metadata"},
                    }
                )
        if positions:
            return positions

    positions = []
    try:
        children = sorted((entry for entry in dataset_dir.iterdir() if entry.is_dir()), key=lambda entry: entry.name.lower())
    except OSError:
        return positions

    for index, entry in enumerate(children):
        name = entry.name
        lower_name = name.lower()
        if lower_name.startswith("pos") or lower_name.startswith("position") or lower_name.startswith("xy"):
            positions.append(
                {
                    "position_key": slugify(name),
                    "display_name": name,
                    "position_index": index,
                    "metadata_json": {"relative_path": name, "source": "directory_heuristic"},
                }
            )
    return positions
