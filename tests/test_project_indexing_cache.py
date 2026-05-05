from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from api.services.project_indexing import (
    SCAN_SIGNATURE_KEY,
    build_project_scan_state,
    existing_project_scan_signature,
)


def test_build_project_scan_state_is_stable_for_unchanged_tree(tmp_path: Path):
    project_dir = tmp_path / "sample"
    project_dir.mkdir()
    mat_path = project_dir / "sample-project.mat"
    mat_path.write_bytes(b"mat-data")
    (project_dir / "pipeline").mkdir()
    (project_dir / "pipeline" / "run1").mkdir(parents=True, exist_ok=True)
    (project_dir / "pipeline" / "run1" / "run.json").write_text('{"status":"done"}', encoding="utf-8")
    (project_dir / "classification").mkdir()
    (project_dir / "classification" / "runs").mkdir(parents=True, exist_ok=True)
    (project_dir / "classification" / "runs" / "run.json").write_text('{"status":"done"}', encoding="utf-8")

    first = build_project_scan_state(project_dir=project_dir, mat_path=mat_path)
    second = build_project_scan_state(project_dir=project_dir, mat_path=mat_path)

    assert first is not None
    assert second is not None
    assert first.signature == second.signature
    assert first.project_mat_bytes == len(b"mat-data")
    assert first.project_dir_bytes >= first.project_mat_bytes


def test_build_project_scan_state_changes_when_tree_changes(tmp_path: Path):
    project_dir = tmp_path / "sample"
    project_dir.mkdir()
    mat_path = project_dir / "sample-project.mat"
    mat_path.write_bytes(b"mat-data")
    run_json = project_dir / "run.json"
    run_json.write_text('{"status":"done"}', encoding="utf-8")

    first = build_project_scan_state(project_dir=project_dir, mat_path=mat_path)
    run_json.write_text('{"status":"failed","message":"changed"}', encoding="utf-8")
    second = build_project_scan_state(project_dir=project_dir, mat_path=mat_path)

    assert first is not None
    assert second is not None
    assert first.signature != second.signature


def test_existing_project_scan_signature_reads_current_metadata():
    project = SimpleNamespace(metadata_json={SCAN_SIGNATURE_KEY: {"version": 1, "tree_digest": "abc"}})

    signature = existing_project_scan_signature(project)

    assert signature == {"version": 1, "tree_digest": "abc"}
