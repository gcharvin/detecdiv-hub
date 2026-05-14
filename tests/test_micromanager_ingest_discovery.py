from __future__ import annotations

import json
import os
import time

from api.services.micromanager_ingest import classify_micromanager_dataset_dir, discover_micromanager_candidates


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
