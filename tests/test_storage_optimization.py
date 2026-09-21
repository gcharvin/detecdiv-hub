from types import SimpleNamespace

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
