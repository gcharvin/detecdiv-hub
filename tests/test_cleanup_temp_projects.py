from pathlib import Path

import pytest

from scripts.cleanup_temp_projects import (
    build_summary,
    discover_cleanup_targets,
    execute_cleanup,
    targets_for_temp_project,
)


def test_cleanup_discovers_only_temp_project_and_immediate_temp_positions(tmp_path: Path):
    channel_dir = tmp_path / "legacy-pos1-ch1"
    channel_dir.mkdir()
    project_file = channel_dir / "temp-project.mat"
    project_file.write_bytes(b"temp")
    temp_position = channel_dir / "temp-pos1"
    temp_position.mkdir()
    (temp_position / "derived.jpg").write_bytes(b"derived")
    original_image = channel_dir / "legacy-001.jpg"
    original_image.write_bytes(b"original")
    unrelated_dir = channel_dir / "attempt-position"
    unrelated_dir.mkdir()

    targets = discover_cleanup_targets([tmp_path])

    assert {Path(target.target_path) for target in targets} == {project_file, temp_position}
    assert original_image.exists()
    assert unrelated_dir.exists()
    summary = build_summary(targets, executed=False)
    assert summary["temp_project_count"] == 1
    assert summary["directory_count"] == 1
    assert summary["total_bytes"] == len(b"temp") + len(b"derived")


def test_cleanup_execute_preserves_parent_and_original_images(tmp_path: Path):
    channel_dir = tmp_path / "legacy-pos1-ch1"
    channel_dir.mkdir()
    project_file = channel_dir / "temp-project.mat"
    project_file.write_bytes(b"temp")
    temp_position = channel_dir / "temp-pos1"
    temp_position.mkdir()
    (temp_position / "derived.jpg").write_bytes(b"derived")
    original_image = channel_dir / "legacy-001.jpg"
    original_image.write_bytes(b"original")

    targets = discover_cleanup_targets([tmp_path])
    execute_cleanup(targets)

    assert channel_dir.is_dir()
    assert original_image.read_bytes() == b"original"
    assert not project_file.exists()
    assert not temp_position.exists()


def test_cleanup_can_use_catalog_candidate_list(tmp_path: Path):
    indexed_dir = tmp_path / "indexed"
    indexed_dir.mkdir()
    indexed_project = indexed_dir / "temp-project.mat"
    indexed_project.write_bytes(b"indexed")
    (indexed_dir / "temp-pos1").mkdir()
    unlisted_dir = tmp_path / "unlisted"
    unlisted_dir.mkdir()
    (unlisted_dir / "temp-project.mat").write_bytes(b"unlisted")

    targets = discover_cleanup_targets([tmp_path], project_files=[indexed_project])

    assert {Path(target.target_path) for target in targets} == {
        indexed_project,
        indexed_dir / "temp-pos1",
    }


def test_cleanup_rejects_project_outside_root(tmp_path: Path):
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    project_file = outside / "temp-project.mat"
    project_file.write_bytes(b"temp")

    with pytest.raises(ValueError, match="outside cleanup root"):
        targets_for_temp_project(root=root, project_file=project_file)
