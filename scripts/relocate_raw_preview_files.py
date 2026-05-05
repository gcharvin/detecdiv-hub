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

from api.models import Artifact, Job, RawDataset
from api.services.project_indexing import slugify


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Move raw preview MP4s from the legacy shared root into dataset-local .detecdiv-previews folders."
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DETECDIV_HUB_DATABASE_URL", ""),
        help="SQLAlchemy database URL. Defaults to DETECDIV_HUB_DATABASE_URL.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply file moves and database updates. Without this flag, only reports are printed.",
    )
    return parser.parse_args()


def to_server_uri(path_text: str) -> str:
    text = str(path_text or "").strip().replace("\\", "/")
    if not text:
        return ""
    if text.startswith("/data/"):
        return text
    if len(text) >= 2 and text[1] == ":":
        return "/data/" + text[3:].lstrip("/")
    if text.startswith("/"):
        return "/data/" + text.lstrip("/")
    return "/data/" + text


def to_local_windows(path_text: str) -> Path:
    server = to_server_uri(path_text)
    if not server.startswith("/data/"):
        return Path(path_text)
    return Path("X:\\" + server[len("/data/"):].replace("/", "\\"))


def legacy_source_path(target_local_path: Path, acquisition_label: str) -> Path | None:
    parts = target_local_path.parts
    try:
        preview_idx = parts.index(".detecdiv-previews")
    except ValueError:
        return None
    if preview_idx == 0 or parts[preview_idx - 1] != acquisition_label:
        return None
    return Path(*parts[: preview_idx - 1]) / Path(*parts[preview_idx:])


def expected_target_server_path(raw_dataset: RawDataset, artifact: Artifact) -> str:
    dataset_path = raw_dataset_path_server(raw_dataset)
    leaf = slugify(raw_dataset.external_key or raw_dataset.acquisition_label) or str(raw_dataset.id)
    return f"{dataset_path}/.detecdiv-previews/{leaf}/{Path(str(artifact.uri)).name}"


def raw_dataset_path_server(raw_dataset: RawDataset) -> str:
    location = next(
        (
            location
            for location in raw_dataset.locations or []
            if location.storage_root is not None and location.relative_path
        ),
        None,
    )
    if location is None:
        return ""
    root = str(location.storage_root.path_prefix or "").strip().replace("\\", "/")
    rel = str(location.relative_path or "").strip().replace("\\", "/")
    if not root:
        return ""
    return f"{root.rstrip('/')}/{rel.lstrip('/')}"


def update_job_result_paths(job: Job, artifact_id: str, new_server_path: str) -> bool:
    result_json = dict(job.result_json or {})
    generated = list(result_json.get("generated") or [])
    changed = False
    for item in generated:
        if str(item.get("artifact_id") or "") == artifact_id:
            item["path"] = new_server_path
            changed = True
    if changed:
        result_json["generated"] = generated
        job.result_json = result_json
    return changed


def cleanup_empty_parents(path: Path) -> None:
    current = path
    while current.name != ".detecdiv-previews" and current.exists():
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent
    if current.name == ".detecdiv-previews":
        try:
            current.rmdir()
        except OSError:
            pass


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Set DETECDIV_HUB_DATABASE_URL or pass --database-url.")

    engine = create_engine(args.database_url, future=True, pool_pre_ping=True)

    with Session(engine) as session:
        artifacts = list(
            session.scalars(
                select(Artifact)
                .options(
                    joinedload(Artifact.job)
                    .joinedload(Job.raw_dataset)
                    .joinedload(RawDataset.locations)
                )
                .where(Artifact.artifact_kind == "raw_position_preview_mp4")
                .order_by(Artifact.created_at.asc())
            ).unique()
        )

        scanned = 0
        moved = 0
        updated = 0
        job_updates = 0
        missing = 0
        already_ok = 0

        for artifact in artifacts:
            job = artifact.job
            raw_dataset = job.raw_dataset if job is not None else None
            if raw_dataset is None:
                missing += 1
                print(f"skip artifact {artifact.id}: missing raw dataset")
                continue

            server_uri = to_server_uri(str(artifact.uri or ""))
            if not server_uri:
                missing += 1
                print(f"skip artifact {artifact.id}: empty uri")
                continue

            target_server = expected_target_server_path(raw_dataset, artifact)
            target_local = to_local_windows(target_server)
            legacy_local = legacy_source_path(target_local, raw_dataset.acquisition_label)
            source_local = to_local_windows(str(artifact.uri or ""))

            scanned += 1
            target_parent = target_local.parent
            target_parent.mkdir(parents=True, exist_ok=True)

            if target_local.exists():
                already_ok += 1
                if legacy_local is not None and legacy_local.exists() and legacy_local != target_local:
                    if legacy_local.stat().st_size == target_local.stat().st_size:
                        if args.apply:
                            legacy_local.unlink()
                        print(f"remove legacy duplicate {legacy_local}")
                    else:
                        print(f"warning: both paths exist with different sizes: {legacy_local} vs {target_local}")
            elif source_local.exists():
                if source_local != target_local:
                    print(f"move {source_local} -> {target_local}")
                    if args.apply:
                        source_local.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(source_local), str(target_local))
                    moved += 1
                else:
                    already_ok += 1
            elif legacy_local is not None and legacy_local.exists():
                print(f"move {legacy_local} -> {target_local}")
                if args.apply:
                    legacy_local.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(legacy_local), str(target_local))
                moved += 1
            else:
                missing += 1
                print(f"missing file for artifact {artifact.id}: {target_local}")

            if artifact.uri != target_server:
                artifact.uri = target_server
                metadata = dict(artifact.metadata_json or {})
                metadata["absolute_path"] = target_server
                artifact.metadata_json = metadata
                updated += 1

                if job is not None and update_job_result_paths(job, str(artifact.id), target_server):
                    job_updates += 1

            if legacy_local is not None and legacy_local.exists() and legacy_local != target_local:
                if args.apply:
                    cleanup_empty_parents(legacy_local.parent)

        if args.apply:
            session.commit()

        print(
            f"scanned={scanned} moved={moved} updated={updated} job_updates={job_updates} already_ok={already_ok} missing={missing}"
        )


if __name__ == "__main__":
    main()
