from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from worker.storage_lifecycle import (
    delete_source_path,
    is_preview_only_archive_placeholder,
    write_archive_marker_file,
)


def test_cold_archive_keeps_dataset_local_preview_directory(tmp_path):
    dataset_dir = tmp_path / "raw_dataset"
    position_dir = dataset_dir / "Pos0"
    preview_dir = dataset_dir / ".detecdiv-previews" / "raw_dataset"
    position_dir.mkdir(parents=True)
    preview_dir.mkdir(parents=True)
    (position_dir / "frame_0001.tif").write_bytes(b"raw")
    preview_path = preview_dir / "Pos0.mp4"
    preview_path.write_bytes(b"mp4-preview")

    preserved = delete_source_path(dataset_dir, preserve_preview_dirs=True)

    assert dataset_dir.is_dir()
    assert preserved == [str(dataset_dir / ".detecdiv-previews")]
    assert preview_path.is_file()
    assert preview_path.read_bytes() == b"mp4-preview"
    assert not position_dir.exists()
    assert is_preview_only_archive_placeholder(dataset_dir) is True


def test_preview_placeholder_allows_archive_marker_file(tmp_path):
    dataset_dir = tmp_path / "raw_dataset"
    (dataset_dir / ".detecdiv-previews").mkdir(parents=True)
    write_archive_marker_file(source_path=dataset_dir, archive_path=tmp_path / "archive.zip")

    assert is_preview_only_archive_placeholder(dataset_dir) is True


def test_cold_archive_removes_dataset_directory_when_no_previews(tmp_path):
    dataset_dir = tmp_path / "raw_dataset"
    dataset_dir.mkdir()
    (dataset_dir / "frame_0001.tif").write_bytes(b"raw")

    preserved = delete_source_path(dataset_dir, preserve_preview_dirs=True)

    assert preserved == []
    assert not dataset_dir.exists()


def test_preview_placeholder_requires_only_preview_directory(tmp_path):
    dataset_dir = tmp_path / "raw_dataset"
    (dataset_dir / ".detecdiv-previews").mkdir(parents=True)
    (dataset_dir / "Pos0").mkdir()

    assert is_preview_only_archive_placeholder(dataset_dir) is False


def test_archive_marker_documents_archive_path(tmp_path):
    dataset_dir = tmp_path / "raw_dataset"
    dataset_dir.mkdir()
    archive_path = tmp_path / "archive" / "raw_dataset.zip"

    marker_path = write_archive_marker_file(source_path=dataset_dir, archive_path=archive_path)

    assert marker_path == dataset_dir / "DO_NOT_DELETE_PARENT_FOLDER"
    marker_text = marker_path.read_text(encoding="utf-8")
    assert str(archive_path) in marker_text
    assert "lightweight previews" in marker_text
