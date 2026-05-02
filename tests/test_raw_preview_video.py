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
