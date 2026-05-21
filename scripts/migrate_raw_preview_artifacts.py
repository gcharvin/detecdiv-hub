from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, joinedload

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.models import Artifact, Job, RawDataset, RawDatasetLocation, RawDatasetPosition
from api.services.path_resolution import compose_storage_path
from api.services.project_indexing import slugify


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate raw preview MP4s to dataset-local .detecdiv-previews folders and fix DB links."
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DETECDIV_HUB_DATABASE_URL", ""),
        help="SQLAlchemy database URL. Defaults to DETECDIV_HUB_DATABASE_URL.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply file moves and database updates. Without this flag, the script only reports changes.",
    )
    parser.add_argument(
        "--move-files",
        action="store_true",
        help="Move existing MP4 files into the dataset-local preview folder. Use only on a writable storage host.",
    )
    parser.add_argument(
        "--filesystem-view",
        choices=("auto", "server", "windows"),
        default="auto",
        help="How to resolve /data paths for existence checks. Defaults to server paths on Linux and X: mapping on Windows.",
    )
    parser.add_argument(
        "--mark-missing",
        action="store_true",
        help="When no preview file can be found, clear the position's ready preview state.",
    )
    return parser.parse_args()


def preferred_dataset_location(raw_dataset: RawDataset) -> RawDatasetLocation | None:
    locations = [
        location
        for location in raw_dataset.locations or []
        if location.storage_root is not None and location.relative_path
    ]
    if not locations:
        return None
    preferred = next((location for location in locations if location.is_preferred), None)
    if preferred is not None:
        return preferred
    return locations[0]


def local_dataset_preview_path(current_uri: str, acquisition_label: str) -> Path | None:
    path = Path(current_uri)
    try:
        preview_index = path.parts.index(".detecdiv-previews")
    except ValueError:
        return None

    if preview_index > 0 and path.parts[preview_index - 1] == acquisition_label:
        return path

    prefix = Path(*path.parts[:preview_index])
    suffix = Path(*path.parts[preview_index:])
    return prefix / acquisition_label / suffix


def server_preview_to_local_path(server_path: str, *, filesystem_view: str) -> Path:
    if filesystem_view == "auto":
        filesystem_view = "windows" if os.name == "nt" else "server"
    if filesystem_view == "server":
        return Path(server_path)

    text = str(server_path or "").strip().replace("/", "\\")
    if text.startswith("\\data\\"):
        return Path("X:\\" + text[len("\\data\\"):].lstrip("\\"))
    if text.startswith("\\\\data\\"):
        return Path("X:\\" + text[len("\\\\data\\"):].lstrip("\\"))
    if text.startswith("X:\\"):
        return Path(text)
    if text.startswith("\\"):
        return Path("X:" + text)
    return Path(text)


def legacy_local_preview_path(local_expected_path: Path, acquisition_label: str) -> Path | None:
    parts = local_expected_path.parts
    try:
        preview_index = parts.index(".detecdiv-previews")
    except ValueError:
        return None

    if preview_index == 0 or parts[preview_index - 1] != acquisition_label:
        return None

    prefix = Path(*parts[:preview_index - 1])
    suffix = Path(*parts[preview_index:])
    return prefix / suffix


def expected_server_preview_path(raw_dataset: RawDataset, artifact: Artifact) -> str | None:
    location = preferred_dataset_location(raw_dataset)
    if location is None:
        return None
    dataset_path = compose_storage_path(location.storage_root.path_prefix, location.relative_path)
    leaf = slugify(raw_dataset.external_key or raw_dataset.acquisition_label) or str(raw_dataset.id)
    filename = Path(str(artifact.uri)).name
    return f"{dataset_path}/.detecdiv-previews/{leaf}/{filename}"


def candidate_preview_paths(raw_dataset: RawDataset, position: RawDatasetPosition, artifact: Artifact) -> list[str]:
    location = preferred_dataset_location(raw_dataset)
    if location is None:
        return []
    dataset_path = compose_storage_path(location.storage_root.path_prefix, location.relative_path)
    leaf = slugify(raw_dataset.external_key or raw_dataset.acquisition_label) or str(raw_dataset.id)
    filename = Path(str(artifact.uri)).name or f"{position.position_key}.mp4"
    return [
        f"{dataset_path}/.detecdiv-previews/{leaf}/{filename}",
        f"{dataset_path}/.detecdiv-previews/{leaf}/{position.position_key}.mp4",
    ]


def update_job_result_paths(job: Job, artifact_id: str, new_path: str) -> bool:
    result_json = dict(job.result_json or {})
    generated = list(result_json.get("generated") or [])
    changed = False
    for item in generated:
        if str(item.get("artifact_id") or "") == artifact_id:
            item["path"] = new_path
            changed = True
    if changed:
        result_json["generated"] = generated
        job.result_json = result_json
    return changed


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("A database URL is required. Set DETECDIV_HUB_DATABASE_URL or pass --database-url.")

    engine = create_engine(args.database_url, future=True, pool_pre_ping=True)

    with Session(engine) as session:
        rows = list(
            session.execute(
                select(RawDatasetPosition, Artifact)
                .join(Artifact, RawDatasetPosition.preview_artifact_id == Artifact.id)
                .options(
                    joinedload(RawDatasetPosition.raw_dataset)
                    .joinedload(RawDataset.locations)
                    .joinedload(RawDatasetLocation.storage_root),
                    joinedload(RawDatasetPosition.preview_artifact).joinedload(Artifact.job),
                )
                .where(Artifact.artifact_kind == "raw_position_preview_mp4")
                .order_by(RawDatasetPosition.updated_at.asc(), Artifact.created_at.asc())
            ).unique()
        )

        scanned = 0
        moved = 0
        updated = 0
        job_updates = 0
        missing = 0
        marked_missing = 0
        already_ok = 0
        skipped = 0

        for position, artifact in rows:
            current_uri = str(artifact.uri or "").strip()
            if not current_uri:
                skipped += 1
                continue

            job = artifact.job
            raw_dataset = position.raw_dataset
            if raw_dataset is None:
                print(f"skip artifact {artifact.id}: missing raw dataset for position {position.id}")
                skipped += 1
                continue

            expected_server_path = expected_server_preview_path(raw_dataset, artifact)
            if expected_server_path is None:
                print(f"skip artifact {artifact.id}: missing raw dataset location")
                skipped += 1
                continue

            scanned += 1
            local_expected_path = server_preview_to_local_path(expected_server_path, filesystem_view=args.filesystem_view)
            legacy_local_path = legacy_local_preview_path(local_expected_path, raw_dataset.acquisition_label)

            physical_path: Path | None = None
            source_path = server_preview_to_local_path(current_uri, filesystem_view=args.filesystem_view)
            if local_expected_path.exists():
                physical_path = local_expected_path
                already_ok += 1
            elif source_path.exists() and source_path != local_expected_path and args.move_files:
                print(f"move {source_path} -> {local_expected_path}")
                if args.apply:
                    local_expected_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(source_path), str(local_expected_path))
                physical_path = local_expected_path
                moved += 1
            elif source_path.exists():
                physical_path = source_path
                expected_server_path = current_uri
                already_ok += 1
            elif legacy_local_path is not None and legacy_local_path.exists() and args.move_files:
                print(f"move {legacy_local_path} -> {local_expected_path}")
                if args.apply:
                    local_expected_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(legacy_local_path), str(local_expected_path))
                physical_path = local_expected_path
                moved += 1
            else:
                for candidate in candidate_preview_paths(raw_dataset, position, artifact):
                    candidate_path = server_preview_to_local_path(candidate, filesystem_view=args.filesystem_view)
                    if candidate_path.exists():
                        physical_path = candidate_path
                        expected_server_path = candidate
                        local_expected_path = candidate_path
                        break

            if physical_path is None:
                missing += 1
                print(
                    f"missing artifact {artifact.id} "
                    f"dataset={raw_dataset.acquisition_label} position={position.position_key} path={expected_server_path}"
                )
                if args.mark_missing:
                    position.preview_status = "missing"
                    position.preview_artifact_id = None
                    marked_missing += 1
                continue

            if current_uri != expected_server_path or str(physical_path) != expected_server_path:
                print(f"db {artifact.id}: {current_uri} -> {expected_server_path}")
                artifact.uri = expected_server_path
                metadata = dict(artifact.metadata_json or {})
                metadata["absolute_path"] = expected_server_path
                artifact.metadata_json = metadata
                updated += 1

                if job is not None and update_job_result_paths(job, str(artifact.id), expected_server_path):
                    job_updates += 1

        if args.apply:
            session.commit()

        print(
            "scanned="
            f"{scanned} moved={moved} updated={updated} job_updates={job_updates} "
            f"already_ok={already_ok} missing={missing} marked_missing={marked_missing} skipped={skipped}"
        )


if __name__ == "__main__":
    main()
