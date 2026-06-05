from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import numpy as np

from api.services.micromanager_ingest import (
    DETECDIV_ACQUISITION_MANIFEST_FILE,
    MicroManagerDatasetCandidate,
    MicroManagerLandingRootData,
    classify_micromanager_dataset_dir,
    discover_micromanager_candidates,
    promote_micromanager_candidate_to_user_home,
)
from api.routes_micromanager_ingest import micromanager_landing_root_summary
from api.services.raw_preview_settings import RawPreviewRuntimeConfig
from api.services.raw_dataset_ingest import (
    detect_raw_dataset_format,
    discover_raw_dataset_positions,
    is_short_single_position_micromanager_dataset,
)
from api.services.project_indexing import iter_orphan_raw_candidates, looks_like_raw_dataset_dir
from worker.raw_preview_video import (
    find_first_zarr_array_dir,
    try_read_v3_ome_writers_preview_frames,
)


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


def test_orphan_raw_candidates_accept_parent_with_position_metadata(tmp_path):
    dataset_dir = tmp_path / "20220128_wt_pma1_yra1_ifh1_1"
    pos_dir = dataset_dir / "Pos0"
    pos_dir.mkdir(parents=True)
    (pos_dir / "metadata.txt").write_text(json.dumps({"Summary": {"Frames": 3}}), encoding="utf-8")
    (pos_dir / "img_channel000_position000_time000000000_z000.tif").write_bytes(b"tiff")

    assert looks_like_raw_dataset_dir(dataset_dir)
    assert list(iter_orphan_raw_candidates(tmp_path, project_dirs=[])) == [dataset_dir]


def test_orphan_raw_candidates_accept_display_settings_with_generic_position_names(tmp_path):
    dataset_dir = tmp_path / "081025_4d_SC_005_1"
    for position_name, position_index in (
        ("10-X452_005_2d_LOG-37", "036"),
        ("6-X539_SC_2d_LOG-21", "020"),
    ):
        position_dir = dataset_dir / position_name
        position_dir.mkdir(parents=True)
        (position_dir / "metadata.txt").write_text(json.dumps({"Summary": {"Frames": 900}}), encoding="utf-8")
        (position_dir / f"img_channel000_position{position_index}_time000000000_z000.tif").write_bytes(b"tiff")
        (position_dir / f"img_channel000_position{position_index}_time000000001_z000.tif").write_bytes(b"tiff")
    (dataset_dir / "DisplaySettings.json").write_text(json.dumps({"Channels": []}), encoding="utf-8")

    assert looks_like_raw_dataset_dir(dataset_dir)
    assert detect_raw_dataset_format(dataset_dir, {}) == "micromanager_tiff_dir"
    assert list(iter_orphan_raw_candidates(tmp_path, project_dirs=[])) == [dataset_dir]
    positions = discover_raw_dataset_positions(dataset_dir, {})
    assert [position["display_name"] for position in positions] == [
        "10-X452_005_2d_LOG-37",
        "6-X539_SC_2d_LOG-21",
    ]
    assert positions[0]["metadata_json"]["source"] == "generic_micromanager_directory"


def test_orphan_raw_candidates_reject_single_generic_child_snapshot(tmp_path):
    dataset_dir = tmp_path / "single_snapshot"
    position_dir = dataset_dir / "not_a_pos_name"
    position_dir.mkdir(parents=True)
    (dataset_dir / "DisplaySettings.json").write_text(json.dumps({"Channels": []}), encoding="utf-8")
    (position_dir / "img_channel000_position000_time000000000_z000.tif").write_bytes(b"tiff")

    assert not looks_like_raw_dataset_dir(dataset_dir)
    assert list(iter_orphan_raw_candidates(tmp_path, project_dirs=[])) == []


def test_short_micromanager_filter_keeps_multi_position_single_timepoint_acquisitions():
    assert not is_short_single_position_micromanager_dataset(
        {"dimensions": {"frame_count": 1, "position_count": 20}}
    )
    assert is_short_single_position_micromanager_dataset(
        {"dimensions": {"frame_count": 1, "position_count": 1}}
    )


def test_discover_micromanager_candidates_infers_owner_from_acquisitions_path(tmp_path):
    landing_root = tmp_path / "landing"
    dataset_dir = landing_root / "acquisitions" / "antoine" / "20260518" / "session" / "test.ome.zarr"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "zarr.json").write_text(json.dumps({"zarr_format": 3}), encoding="utf-8")
    past_timestamp = time.time() - 10
    for file_path in dataset_dir.rglob("*"):
        os.utime(file_path, (past_timestamp, past_timestamp))

    candidates = discover_micromanager_candidates(
        landing_root=landing_root,
        settle_seconds=0,
        grouping_window_hours=12,
        max_datasets=25,
    )

    assert len(candidates) == 1
    assert candidates[0].owner_user_key == "antoine"


def test_user_home_landing_root_summary_does_not_require_api_filesystem_visibility():
    summary = micromanager_landing_root_summary(
        MicroManagerLandingRootData(
            root_key="user:gilles",
            label="Gilles landing",
            path="/homes/Gilles/DetecdivHub/landing",
            source="user_home",
            user_key="gilles",
            is_default=True,
        )
    )

    assert summary.status == "ready"
    assert summary.exists is True
    assert summary.accessible is True


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
                "position_annotations": [{"position_key": "Pos0", "description": "control colony"}],
                "labguru": {"enabled": True, "request": {"title": "Experiment 1"}},
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
    assert candidate.metadata_json["position_annotations"][0]["description"] == "control colony"
    assert candidate.metadata_json["labguru"]["request"]["title"] == "Experiment 1"


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


def test_promote_micromanager_candidate_renames_into_user_raw_home(tmp_path):
    landing_root = tmp_path / "landing"
    dataset_dir = landing_root / "acquisitions" / "antoine" / "test.ome.zarr"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "zarr.json").write_text("{}", encoding="utf-8")
    home_root = tmp_path / "homes"
    (home_root / "Antoine" / "DetecdivHub").mkdir(parents=True)
    owner = SimpleNamespace(id="user-1", user_key="antoine")
    account = SimpleNamespace(
        id="account-1",
        provisioning_status="ready",
        home_relative_path="Antoine/DetecdivHub",
        home_storage_root=SimpleNamespace(name="user-homes", path_prefix=str(home_root)),
        provider=SimpleNamespace(provider_kind="posix_mount"),
        updated_at=datetime(2026, 5, 18, tzinfo=timezone.utc),
    )

    class ScalarResult:
        def first(self):
            return account

    class FakeSession:
        def scalars(self, _stmt):
            return ScalarResult()

    candidate = MicroManagerDatasetCandidate(
        dataset_dir=dataset_dir,
        relative_path="acquisitions/antoine/test.ome.zarr",
        acquisition_label="test",
        microscope_name="scope",
        session_label="test",
        session_date=datetime(2026, 5, 18, 10, tzinfo=timezone.utc),
        group_key="group",
        group_label="group",
        last_modified_at=datetime(2026, 5, 18, 11, tzinfo=timezone.utc),
        file_count=1,
        metadata_json={"source": "micromanager_ingest"},
        completeness_status="complete",
        owner_user_key="antoine",
    )

    promoted = promote_micromanager_candidate_to_user_home(
        FakeSession(),
        owner=owner,
        candidate=candidate,
        landing_root=landing_root,
        fallback_storage_root_name="landing",
    )

    expected_dir = home_root / "Antoine" / "DetecdivHub" / "raw" / "20260518" / "test.ome.zarr"
    assert promoted.dataset_dir == expected_dir
    assert promoted.root_path == home_root / "Antoine" / "DetecdivHub" / "raw"
    assert promoted.storage_root_name == "raw-antoine"
    assert expected_dir.is_dir()
    assert not dataset_dir.exists()
    assert promoted.metadata_json["landing_zone_promotion"]["status"] == "promoted"
    assert promoted.metadata_json["landing_zone_promotion"]["original_landing_relative_path"] == (
        "acquisitions/antoine/test.ome.zarr"
    )


def test_preview_reader_accepts_ome_writers_tcyx_layout(tmp_path):
    series_dir = tmp_path / "test.ome.zarr" / "0"
    array_dir = series_dir / "0"
    chunk_path = array_dir / "c" / "0" / "0" / "0" / "0"
    chunk_path.parent.mkdir(parents=True)
    frame = (np.arange(16, dtype="<u2").reshape(4, 4))
    chunk_path.write_bytes(frame.tobytes())
    (series_dir / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "group",
                "attributes": {
                    "ome": {
                        "version": "0.5",
                        "multiscales": [
                            {
                                "axes": [
                                    {"name": "t", "type": "time"},
                                    {"name": "c", "type": "channel"},
                                    {"name": "y", "type": "space"},
                                    {"name": "x", "type": "space"},
                                ],
                                "datasets": [{"path": "0"}],
                            }
                        ],
                        "omero": {"channels": [{"label": "GFP"}]},
                    },
                    "ome_writers": {
                        "frame_metadata": [
                            {"storage_index": [0, 0]},
                        ]
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    (array_dir / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "array",
                "shape": [1, 1, 4, 4],
                "data_type": "uint16",
                "chunk_grid": {
                    "name": "regular",
                    "configuration": {"chunk_shape": [1, 1, 4, 4]},
                },
                "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
            }
        ),
        encoding="utf-8",
    )

    assert find_first_zarr_array_dir(series_dir) == array_dir
    sequence = try_read_v3_ome_writers_preview_frames(
        series_dir,
        runtime_config=RawPreviewRuntimeConfig(
            fps=6,
            frame_mode="full",
            max_frames=0,
            max_dimension=768,
            binning_factor=1,
            crf=24,
            preset="medium",
            include_existing=False,
            artifact_root=None,
            ffmpeg_command=None,
        ),
    )

    assert sequence is not None
    assert len(sequence.frames) == 1
    assert sequence.channel_labels == ["GFP"]
