import json
from types import SimpleNamespace

import numpy as np

from worker.preview_text import fit_text_scale
from worker.raw_preview_video import read_zarr_preview_frames


def test_fit_text_scale_shrinks_long_text_on_small_frames():
    text = "CH1 BRIGHTFIELD_10X"

    assert fit_text_scale(text, available_width=200, desired_scale=4) == 1
    assert fit_text_scale(text, available_width=1200, desired_scale=4) == 4


def test_fit_text_scale_honors_lower_bounds():
    assert fit_text_scale("", available_width=200, desired_scale=4) == 1
    assert fit_text_scale("FRAME 1", available_width=20, desired_scale=4) == 1


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
                        "omero": {"channels": [{"label": "A"}, {"label": "B"}]},
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
