from __future__ import annotations

import argparse
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from api.db import SessionLocal
from api.models import RawDataset, RawDatasetLocation
from api.services.raw_dataset_ingest import ingest_raw_dataset_from_directory


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill ND2 metadata and positions for indexed raw datasets.")
    parser.add_argument("--storage-root-name", help="Only backfill raw datasets under this storage root.")
    parser.add_argument("--path-contains", help="Only backfill datasets whose absolute path contains this text.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of datasets to update.")
    parser.add_argument("--dry-run", action="store_true", help="List candidates without writing changes.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    updated = 0
    candidates = 0
    with SessionLocal() as session:
        rows = session.scalars(
            select(RawDataset)
            .options(
                joinedload(RawDataset.owner),
                joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root),
            )
            .where(RawDataset.data_format == "nd2")
        ).unique()
        for raw_dataset in rows:
            location = preferred_location(raw_dataset)
            if location is None or raw_dataset.owner is None:
                continue
            storage_root = location.storage_root
            if args.storage_root_name and storage_root.name != args.storage_root_name:
                continue
            dataset_dir = Path(storage_root.path_prefix) / (location.relative_path or "")
            dataset_text = str(dataset_dir)
            if args.path_contains and args.path_contains not in dataset_text:
                continue
            if not dataset_dir.is_dir():
                continue
            candidates += 1
            print(f"{raw_dataset.id} {raw_dataset.acquisition_label} {dataset_text}")
            if args.dry_run:
                continue
            ingest_raw_dataset_from_directory(
                session,
                owner=raw_dataset.owner,
                visibility=raw_dataset.visibility,
                storage_root_name=storage_root.name,
                host_scope=storage_root.host_scope,
                root_type=storage_root.root_type,
                root_path=Path(storage_root.path_prefix),
                dataset_dir=dataset_dir,
                source_label="nd2_metadata_backfill",
                source_metadata={"source_backfill": "nd2_metadata"},
                acquisition_label=raw_dataset.acquisition_label,
                external_key=raw_dataset.external_key,
                status=raw_dataset.status,
                completeness_status=raw_dataset.completeness_status,
                started_at=raw_dataset.started_at,
                ended_at=raw_dataset.ended_at,
            )
            updated += 1
            if args.limit and updated >= args.limit:
                break
        if args.dry_run:
            session.rollback()
        else:
            session.commit()
    action = "would update" if args.dry_run else "updated"
    print(f"{action} {updated if not args.dry_run else candidates} ND2 raw dataset(s)")


def preferred_location(raw_dataset: RawDataset) -> RawDatasetLocation | None:
    locations = list(raw_dataset.locations or [])
    for location in locations:
        if location.is_preferred:
            return location
    return locations[0] if locations else None


if __name__ == "__main__":
    main()
