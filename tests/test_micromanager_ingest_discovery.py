from __future__ import annotations

import json
import os
import time

from api.services.micromanager_ingest import (
    DETECDIV_ACQUISITION_MANIFEST_FILE,
    classify_micromanager_dataset_dir,
    discover_micromanager_candidates,
)
from api.services.raw_dataset_ingest import discover_raw_dataset_positions


def test_classify_micromanager_dataset_dir_accepts_zarr_root(tmp_path):
    dataset_dir = tmp_path / "test.ome.zarr"
    dataset_dir.mkdir()
    (dataset_dir / "zarr.json").write_text(json.dumps({"zarr_format": 3}), encoding="utf-8")

    assert classify_micromanager_dataset_dir(dataset_dir) == dataset_dir


def test_classify_micromanager_dataset_dir_finds_nested_zarr_child(tmp_path):
    session_dir = tmp_path / "session"
    dataset_dir = session_dir / "test.ome.zarr"
    dataset_dir.mkdir(parents=True)
    (session_dir / "settings.yaml").write_text("name: test", encoding="utf-8")
    (dataset_dir / "zarr.json").write_text(json.dumps({"zarr_format": 3}), encoding="utf-8")

    assert classify_micromanager_dataset_dir(session_dir) == dataset_dir


def test_discover_micromanager_candidates_finds_orphan_zarr_child(tmp_path):
    session_dir = tmp_path / "session"
    dataset_dir = session_dir / "test.ome.zarr"
    dataset_dir.mkdir(parents=True)
    (session_dir / "settings.yaml").write_text("name: test", encoding="utf-8")
    (dataset_dir / "zarr.json").write_text(json.dumps({"zarr_format": 3}), encoding="utf-8")
    (dataset_dir / "0").mkdir()
    (dataset_dir / "0" / "zarr.json").write_text(json.dumps({"zarr_format": 3}), encoding="utf-8")
    past_timestamp = time.time() - 10
    for file_path in dataset_dir.rglob("*"):
        os.utime(file_path, (past_timestamp, past_timestamp))

    candidates = discover_micromanager_candidates(
        landing_root=session_dir,
        settle_seconds=0,
        grouping_window_hours=12,
        max_datasets=25,
    )

    assert [candidate.dataset_dir for candidate in candidates] == [dataset_dir.resolve()]
    assert candidates[0].relative_path == "test.ome.zarr"


def test_discover_micromanager_candidates_reads_detecdiv_manifest(tmp_path):
    session_dir = tmp_path / "session"
    dataset_dir = session_dir / "test.ome.zarr"
    dataset_dir.mkdir(parents=True)
    (session_dir / DETECDIV_ACQUISITION_MANIFEST_FILE).write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "user_key": "antoine",
                "acquisition_label": "widget acquisition",
                "microscope_name": "TiEclipse",
                "acquisition_session_id": "session-1",
                "mda_summary": {"channel_count": 2, "position_count": 1},
                "mda_settings_json": {"sequence": {"channels": [{"config": "DAPI"}]}},
                "positions": [{"position_key": "Pos0", "display_name": "Position 0"}],
            }
        ),
        encoding="utf-8",
    )
    (dataset_dir / "zarr.json").write_text(json.dumps({"zarr_format": 3}), encoding="utf-8")
    past_timestamp = time.time() - 10
    for file_path in session_dir.rglob("*"):
        os.utime(file_path, (past_timestamp, past_timestamp))

    candidates = discover_micromanager_candidates(
        landing_root=session_dir,
        settle_seconds=0,
        grouping_window_hours=12,
        max_datasets=25,
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.owner_user_key == "antoine"
    assert candidate.acquisition_label == "widget acquisition"
    assert candidate.microscope_name == "TiEclipse"
    assert candidate.metadata_json["detecdiv_acquisition_manifest"]["acquisition_session_id"] == "session-1"
    assert candidate.metadata_json["mda_summary"]["channel_count"] == 2
    assert candidate.metadata_json["positions"][0]["display_name"] == "Position 0"


def test_discover_raw_dataset_positions_keeps_widget_descriptions(tmp_path):
    dataset_dir = tmp_path / "test.ome.zarr"
    dataset_dir.mkdir()

    positions = discover_raw_dataset_positions(
        dataset_dir,
        {
            "positions": [
                {
                    "position_key": "Pos0",
                    "display_name": "Position 0",
                    "description": "control colony",
                    "strain": "BY4741",
                    "medium": "SC",
                }
            ]
        },
    )

    assert positions == [
        {
            "position_key": "pos0",
            "display_name": "Position 0",
            "description": "control colony",
            "position_index": 0,
            "metadata_json": {
                "position_key": "Pos0",
                "display_name": "Position 0",
                "description": "control colony",
                "strain": "BY4741",
                "medium": "SC",
            },
        }
    ]
