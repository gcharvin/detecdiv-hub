import json
import re
from types import SimpleNamespace
from pathlib import Path

import numpy as np
import tifffile

from worker.preview_text import fit_text_scale
from worker.raw_preview_video import (
    annotate_preview_frames,
    read_preview_frames,
    read_zarr_preview_frames,
    render_legacy_matlab_jpg_preview_video,
    should_use_legacy_matlab_jpg_preview,
)


def test_fit_text_scale_shrinks_long_text_on_small_frames():
    text = "CH1 BRIGHTFIELD_10X"

    assert fit_text_scale(text, available_width=200, desired_scale=4) == 1
    assert fit_text_scale(text, available_width=1200, desired_scale=4) == 4


def test_fit_text_scale_honors_lower_bounds():
    assert fit_text_scale("", available_width=200, desired_scale=4) == 1
    assert fit_text_scale("FRAME 1", available_width=20, desired_scale=4) == 1


def test_annotate_preview_frames_uses_visible_compact_overlay():
    frame = np.zeros((256, 256), dtype=np.uint8)

    annotated = annotate_preview_frames(
        [frame],
        project_label="recircu_SCL1_DAD2_ABP1GFP_LV3mC",
        position_label="Position1",
        channel_labels=["Ch 1", "Ch 2", "Ch 3"],
    )[0]

    assert annotated.shape == (256, 256)
    assert int((annotated[:32, :32] == 255).sum()) > 80


def test_read_zarr_preview_frames_falls_back_to_series_array_child(tmp_path, monkeypatch):
    dataset_path = tmp_path / "2026_04_09Yam740Yak108_18_004.ome.zarr"
    series_dir = dataset_path / "Yam_1"
    array_dir = series_dir / "0"
    array_dir.mkdir(parents=True)
    (series_dir / "zarr.json").write_text("{}", encoding="utf-8")
    (array_dir / "zarr.json").write_text("{}", encoding="utf-8")

    position = SimpleNamespace(
        display_name="Yam_1",
        metadata_json={"source": "ome_series", "series_name": "Yam_1"},
    )
    runtime_config = SimpleNamespace(max_frames=2, frame_mode="limit", max_dimension=768, binning_factor=4)
    array = np.arange(2 * 3 * 3 * 4 * 4, dtype=np.uint16).reshape((2, 3, 3, 4, 4))
    attempted_paths: list[str] = []

    def fake_open_best_zarr_node(path):
        attempted_paths.append(str(path))
        if path == series_dir:
            raise ValueError(f"Unable to open Zarr path {path}: nothing found at path ''")
        if path == array_dir:
            return array
        raise AssertionError(f"Unexpected Zarr path: {path}")

    monkeypatch.setattr("worker.raw_preview_video.open_best_zarr_node", fake_open_best_zarr_node)

    sequence = read_zarr_preview_frames(
        dataset_path=dataset_path,
        position=position,
        runtime_config=runtime_config,
    )

    assert attempted_paths[:2] == [str(series_dir), str(array_dir)]
    assert len(sequence.frames) == 2


def test_read_zarr_preview_frames_prefers_ome_multiscales_array_path(tmp_path, monkeypatch):
    dataset_path = tmp_path / "2026_04_09Yam740Yak108_18_004.ome.zarr"
    series_dir = dataset_path / "Yam_1"
    array_dir = series_dir / "0"
    array_dir.mkdir(parents=True)
    (series_dir / "zarr.json").write_text(
        json.dumps({"attributes": {"multiscales": [{"datasets": [{"path": "0"}]}]}}),
        encoding="utf-8",
    )

    position = SimpleNamespace(
        display_name="Yam_1",
        metadata_json={"source": "ome_series", "series_name": "Yam_1"},
    )
    runtime_config = SimpleNamespace(max_frames=2, frame_mode="limit", max_dimension=768, binning_factor=4)
    array = np.arange(2 * 3 * 3 * 4 * 4, dtype=np.uint16).reshape((2, 3, 3, 4, 4))
    attempted_paths: list[str] = []

    def fake_open_best_zarr_node(path):
        attempted_paths.append(str(path))
        if path == array_dir:
            return array
        if path == series_dir:
            raise ValueError(f"Unable to open Zarr path {path}: nothing found at path ''")
        raise AssertionError(f"Unexpected Zarr path: {path}")

    monkeypatch.setattr("worker.raw_preview_video.open_best_zarr_node", fake_open_best_zarr_node)

    sequence = read_zarr_preview_frames(
        dataset_path=dataset_path,
        position=position,
        runtime_config=runtime_config,
    )

    assert attempted_paths[0] == str(array_dir)
    assert len(sequence.frames) == 2


def test_read_zarr_preview_frames_handles_v3_ome_writers_chunks(tmp_path, monkeypatch):
    dataset_path = tmp_path / "2026_04_09Yam740Yak108_18_004.ome.zarr"
    series_dir = dataset_path / "Yam_1"
    array_dir = series_dir / "0"
    array_dir.mkdir(parents=True)
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
                                    {"name": "z", "type": "space"},
                                    {"name": "y", "type": "space"},
                                    {"name": "x", "type": "space"},
                                ],
                                "datasets": [{"path": "0"}],
                            }
                        ],
                    },
                    "pymmcore_plus": {
                        "summary_metadata": {
                            "mda_sequence": {
                                "channels": [
                                    {"config": "A", "exposure": 10.0},
                                    {"config": "B", "exposure": 60.0},
                                ]
                            }
                        }
                    },
                    "ome_writers": {
                        "frame_metadata": [
                            {"storage_index": [0, 0, 0]},
                            {"storage_index": [0, 1, 0]},
                            {"storage_index": [1, 0, 0]},
                            {"storage_index": [1, 1, 0]},
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
                "data_type": "uint16",
                "shape": [2, 2, 1, 4, 4],
                "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [1, 1, 1, 4, 4]}},
                "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
            }
        ),
        encoding="utf-8",
    )
    chunk0 = np.arange(16, dtype=np.uint16).reshape(4, 4)
    chunk1 = np.arange(16, dtype=np.uint16).reshape(4, 4) + 100
    chunk_paths = {
        (0, 0, 0): chunk0,
        (0, 1, 0): chunk1,
        (1, 0, 0): chunk0 + 200,
        (1, 1, 0): chunk1 + 200,
    }
    for storage_index, chunk in chunk_paths.items():
        chunk_path = array_dir / "c"
        for part in storage_index:
            chunk_path /= str(part)
        chunk_path /= "0"
        chunk_path /= "0"
        chunk_path.parent.mkdir(parents=True, exist_ok=True)
        chunk_path.write_bytes(chunk.tobytes(order="C"))

    position = SimpleNamespace(
        display_name="Yam_1",
        metadata_json={"source": "ome_series", "series_name": "Yam_1"},
    )
    runtime_config = SimpleNamespace(max_frames=2, frame_mode="limit", max_dimension=768, binning_factor=4)

    def fake_open_best_zarr_node(path):
        raise ValueError(f"Unable to open Zarr path {path}: nothing found at path ''")

    monkeypatch.setattr("worker.raw_preview_video.open_best_zarr_node", fake_open_best_zarr_node)

    sequence = read_zarr_preview_frames(
        dataset_path=dataset_path,
        position=position,
        runtime_config=runtime_config,
    )

    assert len(sequence.frames) == 2
    assert sequence.channel_labels == ["A", "B"]
    assert sequence.frames[0].shape == (4, 8)


def test_read_zarr_preview_frames_respects_ome_zarr_axes_without_flattening_channels(tmp_path, monkeypatch):
    dataset_path = tmp_path / "21_05_2027.ome.zarr"
    series_dir = dataset_path / "0.2_1"
    array_dir = series_dir / "0"
    array_dir.mkdir(parents=True)
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
                                    {"name": "z", "type": "space"},
                                    {"name": "y", "type": "space"},
                                    {"name": "x", "type": "space"},
                                ],
                                "datasets": [{"path": "0"}],
                            }
                        ],
                    },
                    "pymmcore_plus": {
                        "summary_metadata": {
                            "mda_sequence": {
                                "channels": [
                                    {"config": "0 TL", "exposure": 10.0},
                                    {"config": "2 SB GFP", "exposure": 60.0},
                                ]
                            }
                        }
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
                "shape": [3, 2, 2, 2, 2],
                "dimension_names": ["t", "c", "z", "y", "x"],
            }
        ),
        encoding="utf-8",
    )
    array = np.zeros((3, 2, 2, 2, 2), dtype=np.uint16)
    for time_index in range(3):
        array[time_index, 0, 1] = np.array([[time_index, time_index + 1], [time_index + 2, time_index + 3]])
        array[time_index, 1, 1] = 1000

    position = SimpleNamespace(
        display_name="0.2_1",
        metadata_json={"source": "ome_zarr_multiscales_child", "relative_path": "0.2_1"},
    )
    runtime_config = SimpleNamespace(max_frames=10, frame_mode="full", max_dimension=768, binning_factor=4)

    def fake_open_best_zarr_node(path):
        if path == array_dir:
            return array
        raise ValueError(f"Unable to open Zarr path {path}: nothing found at path ''")

    monkeypatch.setattr("worker.raw_preview_video.open_best_zarr_node", fake_open_best_zarr_node)

    sequence = read_zarr_preview_frames(
        dataset_path=dataset_path,
        position=position,
        runtime_config=runtime_config,
    )

    assert len(sequence.frames) == 3
    assert sequence.channel_labels == ["0 TL", "2 SB GFP"]
    assert sequence.frames[0].shape == (2, 4)


def test_should_use_legacy_matlab_jpg_preview_detects_legacy_root(tmp_path):
    dataset_path = tmp_path / "legacy_dataset"
    position_dir = dataset_path / "legacy_dataset-pos1" / "legacy_dataset-pos1-ch1--"
    position_dir.mkdir(parents=True)
    (dataset_path / "legacy_dataset-project.mat").write_text("mat", encoding="utf-8")
    (dataset_path / "legacy_dataset-ID.txt").write_text("Time-Lapse Assay ID File", encoding="utf-8")
    (position_dir / "legacy_dataset-pos1-ch1---001.jpg").write_bytes(b"jpeg-fixture")

    raw_dataset = SimpleNamespace(data_format="unknown")
    position = SimpleNamespace(
        position_index=0,
        position_key="position_1",
        display_name="Position1",
        metadata_json={},
    )

    assert should_use_legacy_matlab_jpg_preview(
        dataset_path=dataset_path,
        raw_dataset=raw_dataset,
        position=position,
    )


def test_tiff_sequence_ignores_legacy_jpeg_preview_fallback(tmp_path):
    dataset_path = tmp_path / "mixed_dataset"
    tiff_position_dir = dataset_path / "Pos0"
    legacy_jpeg_dir = dataset_path / "mixed_dataset-pos1" / "mixed_dataset-pos1-ch1-Ph"
    tiff_position_dir.mkdir(parents=True)
    legacy_jpeg_dir.mkdir(parents=True)
    (dataset_path / "mixed_dataset-project.mat").write_text("mat", encoding="utf-8")
    (dataset_path / "mixed_dataset-ID.txt").write_text("Time-Lapse Assay ID File", encoding="utf-8")
    (legacy_jpeg_dir / "mixed_dataset-pos1-ch1-Ph-001.jpg").write_bytes(b"jpeg-fixture")

    raw_dataset = SimpleNamespace(data_format="tiff_sequence")
    position = SimpleNamespace(
        position_index=0,
        position_key="pos0",
        display_name="Pos0",
        metadata_json={"relative_path": "Pos0"},
    )

    assert not should_use_legacy_matlab_jpg_preview(
        dataset_path=dataset_path,
        raw_dataset=raw_dataset,
        position=position,
    )


def test_read_preview_frames_does_not_guess_jpegs_for_non_legacy_dataset(tmp_path):
    dataset_path = tmp_path / "plain_jpegs"
    dataset_path.mkdir()
    (dataset_path / "frame_0001.jpg").write_bytes(b"jpeg-fixture")

    raw_dataset = SimpleNamespace(data_format="unknown")
    position = SimpleNamespace(display_name="frame_0001", metadata_json={})
    runtime_config = SimpleNamespace(max_frames=1, frame_mode="full", max_dimension=768, binning_factor=1)

    try:
        read_preview_frames(
            dataset_path=dataset_path,
            raw_dataset=raw_dataset,
            position=position,
            runtime_config=runtime_config,
        )
    except ValueError as exc:
        assert "TIFF files" in str(exc)
    else:
        raise AssertionError("Expected non-legacy JPEG folder to stay on the TIFF path")


def test_render_legacy_matlab_jpg_preview_video_uses_matlab(tmp_path, monkeypatch):
    dataset_path = tmp_path / "legacy_dataset"
    position_dir = dataset_path / "legacy_dataset-pos1" / "legacy_dataset-pos1-ch1--"
    position_dir.mkdir(parents=True)
    (dataset_path / "legacy_dataset-project.mat").write_text("mat", encoding="utf-8")
    (dataset_path / "legacy_dataset-ID.txt").write_text("Time-Lapse Assay ID File", encoding="utf-8")
    (position_dir / "legacy_dataset-pos1-ch1---001.jpg").write_bytes(b"jpeg-fixture")

    raw_dataset = SimpleNamespace(data_format="legacy_matlab_jpg_timelapse")
    raw_dataset.metadata_json = {
        "dimensions": {
            "channel_settings": [
                {"index": 0, "channel": "Ch 1", "binning_factor": 1},
                {"index": 1, "channel": "Ch 2", "binning_factor": 2},
            ]
        }
    }
    position = SimpleNamespace(
        position_index=0,
        position_key="position_1",
        display_name="Position1",
        metadata_json={},
    )
    runtime_config = SimpleNamespace(max_frames=1, frame_mode="full", max_dimension=256, fps=2)
    output_dir = tmp_path / "previews"
    calls = []
    encoded_calls = []

    def fake_run_matlab_command(command, **kwargs):
        calls.append(command)
        script = command[-1]
        match = re.search(r"legacy_matlab_jpg_preview\('([^']+)'\)", script)
        assert match is not None
        config_path = Path(match.group(1))
        config = json.loads(config_path.read_text(encoding="utf-8"))
        assert config["channel_settings"][0]["binning_factor"] == 1
        assert config["channel_settings"][1]["binning_factor"] == 2
        frame_dir = Path(config["frame_dir"])
        frame_dir.mkdir(parents=True, exist_ok=True)
        tifffile.imwrite(frame_dir / "frame_000001.tif", np.full((8, 8), 127, dtype=np.uint8))
        Path(config["result_path"]).write_text(
            json.dumps(
                {
                    "status": "ok",
                    "frame_count": 1,
                    "source_width": 8,
                    "source_height": 8,
                    "channel_labels": ["Ch 1"],
                }
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("worker.raw_preview_video.run_matlab_command", fake_run_matlab_command)

    def fake_encode_preview_video(*, video_path, frames, project_label, position_label, channel_labels, runtime_config):
        encoded_calls.append(
            {
                "video_path": video_path,
                "frame_shapes": [frame.shape for frame in frames],
                "project_label": project_label,
                "position_label": position_label,
                "channel_labels": channel_labels,
            }
        )
        video_path.write_bytes(b"mp4")
        return 8, 8

    monkeypatch.setattr("worker.raw_preview_video.encode_preview_video", fake_encode_preview_video)

    result = render_legacy_matlab_jpg_preview_video(
        dataset_path=dataset_path,
        raw_dataset=raw_dataset,
        position=position,
        runtime_config=runtime_config,
        output_dir=output_dir,
    )

    assert calls
    assert encoded_calls[0]["frame_shapes"] == [(8, 8)]
    assert result.video_path.exists()
    assert result.frame_count == 1
    assert result.channel_labels == ["Ch 1"]
