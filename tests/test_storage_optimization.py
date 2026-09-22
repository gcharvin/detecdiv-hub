from types import SimpleNamespace
from uuid import uuid4

import numpy as np
import pytest
import tifffile

import worker.storage_optimization as optimization


def test_protected_metadata_digest_changes_when_micromanager_tag_changes(tmp_path):
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    common = [(50838, "I", 2, (12, 4), False)]
    tifffile.imwrite(first, np.array([[1]], dtype=np.uint16), description="ImageJ=1.51s", extratags=common + [(50839, "B", 8, b"one-0001", False)])
    tifffile.imwrite(second, np.array([[1]], dtype=np.uint16), description="ImageJ=1.51s", extratags=common + [(50839, "B", 8, b"two-0002", False)])

    assert optimization._protected_metadata_digest(first) != optimization._protected_metadata_digest(second)


def test_deflate_writer_preserves_imagej_private_metadata_and_pixels(tmp_path):
    source = tmp_path / "source.tif"
    target = tmp_path / "target.tif"
    pixels = np.arange(256, dtype=np.uint16).reshape(16, 16)
    tifffile.imwrite(
        source,
        pixels,
        description="ImageJ=1.51s",
        extratags=[
            (50838, "I", 2, (12, 8), False),
            (50839, "B", 8, b"one-0001", False),
        ],
    )

    optimization._write_deflate_tiff(source, target)

    assert optimization._protected_metadata_digest(source) == optimization._protected_metadata_digest(target)
    with tifffile.TiffFile(target) as image:
        assert np.array_equal(image.pages[0].asarray(), pixels)


def test_pixel_check_allows_expected_micromanager_tag_warnings(monkeypatch, tmp_path):
    monkeypatch.setattr(
        optimization.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="Compression: 1 8\n",
            stderr=(
                "TIFFReadDirectory: Warning, Unknown field with tag 50838 (0xc696) encountered.\n"
                "TIFFReadDirectory: Warning, Unknown field with tag 50839 (0xc697) encountered.\n"
            ),
        ),
    )

    optimization._assert_same_pixels(tmp_path / "source.tif", tmp_path / "target.tif")


def test_pixel_check_rejects_any_unexpected_difference(monkeypatch, tmp_path):
    monkeypatch.setattr(
        optimization.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=1, stdout="Pixels differ\n", stderr=""),
    )

    with pytest.raises(RuntimeError, match="TIFF pixel verification failed"):
        optimization._assert_same_pixels(tmp_path / "source.tif", tmp_path / "target.tif")


def test_scan_directory_entries_returns_child_directories_tiffs_and_size(tmp_path):
    position = tmp_path / "Pos0"
    (position / "subfolder").mkdir(parents=True)
    tiff = position / "frame.tif"
    metadata = position / "metadata.txt"
    tiff.write_bytes(b"tiff-data")
    metadata.write_bytes(b"meta")

    child_directories, tiff_files, total_bytes = optimization._scan_directory_entries(
        position,
        position.relative_to(tmp_path),
    )

    assert child_directories == ["Pos0/subfolder"]
    assert tiff_files == [("Pos0/frame.tif", len(b"tiff-data"))]
    assert total_bytes == len(b"tiff-data") + len(b"meta")


def test_incomplete_scan_slice_requeues_the_same_dataset_job(monkeypatch):
    run = SimpleNamespace(
        id=uuid4(),
        status="scanning",
        metadata_json={"scan_initialized": True},
    )

    class Session:
        def scalar(self, statement):
            if statement.column_descriptions[0].get("entity") is optimization.StorageOptimizationRun:
                return run
            return 0

    scan_result = {
        "run_id": str(run.id),
        "status": "scanning",
        "scan_complete": False,
        "scan_directories_remaining": 7,
    }
    monkeypatch.setattr(optimization, "_scan_run", lambda *_args, **_kwargs: scan_result)

    result = optimization._run_stable_optimization(
        Session(),
        job=SimpleNamespace(id=uuid4()),
        run_id=run.id,
    )

    assert result["requeue"] is True
    assert result["scan_complete"] is False
    assert result["scan_directories_remaining"] == 7
