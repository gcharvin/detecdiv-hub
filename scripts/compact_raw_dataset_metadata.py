from __future__ import annotations

from pathlib import Path
import sys

from sqlalchemy import select

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.db import SessionLocal
from api.models import RawDataset
from api.services.micromanager_metadata import build_compact_micromanager_metadata


def resolve_display_settings_uri(metadata_json: dict) -> str | None:
    display_settings = metadata_json.get("display_settings")
    if isinstance(display_settings, dict):
        source_path = str(display_settings.get("source_path") or "").strip()
        if source_path:
            return source_path
    display_settings_uri = str(metadata_json.get("display_settings_uri") or "").strip()
    if display_settings_uri:
        return display_settings_uri
    dataset_dir_abs = str(metadata_json.get("dataset_dir_abs") or "").strip()
    if dataset_dir_abs:
        return str(Path(dataset_dir_abs) / "DisplaySettings.json")
    return None


def main() -> int:
    updated = 0
    skipped = 0
    with SessionLocal() as db:
        rows = list(
            db.scalars(
                select(RawDataset).where(
                    RawDataset.metadata_json.is_not(None),
                    RawDataset.metadata_json.has_key("dataset_dir_abs"),
                )
            )
        )
        for raw_dataset in rows:
            metadata = dict(raw_dataset.metadata_json or {})
            display_settings_uri = resolve_display_settings_uri(metadata)
            dataset_dir_abs = str(metadata.get("dataset_dir_abs") or "").strip()
            if not dataset_dir_abs:
                skipped += 1
                continue
            compact = build_compact_micromanager_metadata(
                dataset_dir=Path(dataset_dir_abs),
                relative_path=str(metadata.get("dataset_rel_from_root") or ""),
                source_label=str(metadata.get("source") or "micromanager_ingest"),
                parsed_metadata=metadata,
                data_format=str(metadata.get("data_format") or raw_dataset.data_format or "unknown"),
                source_metadata={
                    key: value
                    for key, value in {
                        "file_count": metadata.get("ingest", {}).get("file_count") if isinstance(metadata.get("ingest"), dict) else None,
                        "last_modified_at": metadata.get("ingest", {}).get("last_modified_at") if isinstance(metadata.get("ingest"), dict) else None,
                        "session_label": metadata.get("ingest", {}).get("session_label") if isinstance(metadata.get("ingest"), dict) else None,
                        "session_date": metadata.get("ingest", {}).get("session_date") if isinstance(metadata.get("ingest"), dict) else None,
                        "group_key": metadata.get("ingest", {}).get("group_key") if isinstance(metadata.get("ingest"), dict) else None,
                        "group_label": metadata.get("ingest", {}).get("group_label") if isinstance(metadata.get("ingest"), dict) else None,
                    }.items()
                    if value not in (None, "", [], {})
                },
            )
            raw_dataset.metadata_json = compact
            if display_settings_uri:
                raw_dataset.display_settings_uri = display_settings_uri
            updated += 1
        db.commit()
    print(f"updated={updated} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
