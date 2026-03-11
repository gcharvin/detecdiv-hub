from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from api.models import ExperimentProject, ExperimentRawLink, RawDataset, RawDatasetLocation, User
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
) -> RawDataset:
    storage_root = get_or_create_storage_root(
        session,
        root_path=str(root_path),
        storage_root_name=storage_root_name,
        host_scope=host_scope,
        root_type=root_type,
    )

    external_key = build_raw_dataset_key(dataset_dir)
    raw_dataset = session.scalars(select(RawDataset).where(RawDataset.external_key == external_key)).first()
    total_bytes = safe_dir_size(dataset_dir)
    size_timestamp = datetime.now(timezone.utc)
    relative_path = str(dataset_dir.relative_to(root_path))
    metadata = {
        "source": "legacy_raw_ingest",
        "dataset_dir_abs": str(dataset_dir),
        "dataset_rel_from_root": relative_path,
    }

    if raw_dataset is None:
        raw_dataset = RawDataset(
            owner_user_id=owner.id,
            external_key=external_key,
            acquisition_label=dataset_dir.name,
            visibility=visibility,
            status="indexed",
            completeness_status="complete",
            lifecycle_tier="hot",
            archive_status="none",
            reclaimable_bytes=0,
            total_bytes=total_bytes,
            last_size_scan_at=size_timestamp,
            last_accessed_at=size_timestamp,
            started_at=size_timestamp,
            ended_at=size_timestamp,
            metadata_json=metadata,
        )
        session.add(raw_dataset)
        session.flush()
    else:
        merged_metadata = dict(raw_dataset.metadata_json or {})
        merged_metadata.update(metadata)
        raw_dataset.owner_user_id = owner.id
        raw_dataset.acquisition_label = dataset_dir.name
        raw_dataset.visibility = visibility
        raw_dataset.status = "indexed"
        raw_dataset.completeness_status = "complete"
        raw_dataset.total_bytes = total_bytes
        raw_dataset.last_size_scan_at = size_timestamp
        raw_dataset.last_accessed_at = size_timestamp
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
