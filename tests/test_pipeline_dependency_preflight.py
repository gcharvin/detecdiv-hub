import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from worker import pipeline_dependency_preflight as preflight_mod
from worker import pipeline_run_executor


def test_preflight_skips_when_audit_missing(monkeypatch):
    monkeypatch.setattr(preflight_mod, "read_dependency_audit_for_payload", lambda payload: {})
    report = preflight_mod.evaluate_pipeline_dependency_preflight({"pipeline_ref": {}})
    assert report["status"] == "skipped"
    assert report["reason"] == "dependency_audit_missing"


def test_preflight_fails_when_required_dependency_path_missing(tmp_path, monkeypatch):
    audit = {
        "pipelineStatus": "linked_resolvable",
        "summary": {"required_missing_count": 0, "legacy_count": 0},
        "dependencies": [
            {
                "node_id": "classifier_6",
                "is_required_for_run": True,
                "is_resolved": True,
                "source": {"configured_path": "modules/classifier_6", "resolved_path": ""},
                "assets": {"inference": {"count": 2}},
            }
        ],
    }
    monkeypatch.setattr(preflight_mod, "read_dependency_audit_for_payload", lambda payload: audit)
    monkeypatch.setattr(
        preflight_mod,
        "normalized_pipeline_path_from_ref",
        lambda pipeline_ref: str(tmp_path / "projectpipeline_pipeline"),
    )

    report = preflight_mod.evaluate_pipeline_dependency_preflight({"pipeline_ref": {}})
    assert report["status"] == "failed"
    assert any("required dependency path not found" in err for err in report["errors"])


def test_preflight_allows_required_derived_package_without_external_path(monkeypatch):
    audit = {
        "pipelineStatus": "linked_unresolvable",
        "summary": {"required_missing_count": 1, "legacy_count": 0},
        "dependencies": [
            {
                "node_id": "combineMultipleChannels",
                "module_kind": "processor",
                "dependency_mode": "derived",
                "is_required_for_run": True,
                "is_resolved": False,
                "source": {"configured_path": "", "resolved_path": ""},
                "assets": {"inference": {"count": 0}},
            }
        ],
    }
    monkeypatch.setattr(preflight_mod, "read_dependency_audit_for_payload", lambda payload: audit)

    report = preflight_mod.evaluate_pipeline_dependency_preflight({"pipeline_ref": {}})
    assert report["status"] == "ok"
    assert report["errors"] == []


def test_preflight_passes_with_existing_required_dependency(tmp_path, monkeypatch):
    pipeline_dir = tmp_path / "projectpipeline_pipeline"
    dep_dir = pipeline_dir / "modules" / "classifier_6"
    dep_dir.mkdir(parents=True)
    (dep_dir / "dummy.mat").write_text("ok", encoding="utf-8")

    audit = {
        "pipelineStatus": "linked_resolvable",
        "summary": {"required_missing_count": 0, "legacy_count": 0},
        "dependencies": [
            {
                "node_id": "classifier_6",
                "is_required_for_run": True,
                "is_resolved": True,
                "source": {"configured_path": "modules/classifier_6", "resolved_path": ""},
                "assets": {"inference": {"count": 1}},
            }
        ],
    }
    monkeypatch.setattr(preflight_mod, "read_dependency_audit_for_payload", lambda payload: audit)
    monkeypatch.setattr(
        preflight_mod,
        "normalized_pipeline_path_from_ref",
        lambda pipeline_ref: str(pipeline_dir),
    )

    report = preflight_mod.evaluate_pipeline_dependency_preflight({"pipeline_ref": {}})
    assert report["status"] == "ok"
    assert report["errors"] == []


def test_worker_dependency_path_maps_windows_data_drive():
    path = preflight_mod.resolve_dependency_path_for_worker(
        r"X:\matlab\ClassiRepository\cellpose_4",
        Path("/data/Gilles/abhilasha/testproject/pipeline/pipelineTemplate_1"),
    )

    assert path == Path("/data/matlab/ClassiRepository/cellpose_4")


def test_worker_dependency_path_maps_known_classifier_repository():
    path = preflight_mod.resolve_dependency_path_for_worker(
        r"C:\Users\Gilles Charvin\SynologyDrive\DetecDivProjects\Repository\div_1",
        Path("/data/Gilles/abhilasha/testproject/pipeline/pipelineTemplate_1"),
    )

    assert path == Path("/data/Gilles/abhilasha/classifiers/div_1")


def test_worker_pipeline_json_path_accepts_pipeline_directory(tmp_path):
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    pipeline_json = pipeline_dir / "pipeline.json"
    pipeline_json.write_text("{}", encoding="utf-8")

    assert pipeline_run_executor.resolve_pipeline_json_source_path(str(pipeline_dir)) == pipeline_json
