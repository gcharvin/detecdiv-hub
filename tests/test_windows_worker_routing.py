import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from api.models import Job
from worker.path_mappings import (
    map_payload_path_fields,
    map_worker_path,
    parse_worker_path_mappings,
)
from worker.pipeline_dependency_preflight import normalize_worker_path_text
from worker.pipeline_prepared_run import read_dependency_audit_for_payload
from worker.pipeline_run_executor import normalize_pipeline_ref_paths_for_posix
from worker.run_worker import apply_worker_job_filters


def test_windows_worker_maps_canonical_paths_and_keeps_other_fields():
    mappings = parse_worker_path_mappings(
        '[{"source":"/data","target":"//fileserver/share"},'
        '{"source":"/data/classifiers","target":"D:/models"}]'
    )
    payload = {
        "project_ref": {"project_mat_path": "/data/projects/P1.mat"},
        "pipeline_ref": {"pipeline_json_path": "/data/classifiers/seg/pipeline.json"},
        "notes": "/data/projects/this is not a path field",
    }

    mapped = map_payload_path_fields(payload, mappings)

    assert mapped["project_ref"]["project_mat_path"] == r"\\fileserver\share\projects\P1.mat"
    assert mapped["pipeline_ref"]["pipeline_json_path"] == r"D:\models\seg\pipeline.json"
    assert mapped["notes"] == payload["notes"]
    assert map_worker_path("/database/P1.mat", mappings) == "/database/P1.mat"
    assert payload["project_ref"]["project_mat_path"] == "/data/projects/P1.mat"


def test_windows_worker_preserves_native_windows_dependencies():
    path = r"X:\Classifiers\cellpose"
    assert normalize_worker_path_text(path, worker_platform="windows") == path
    assert normalize_worker_path_text(path, worker_platform="linux") == "/data/Classifiers/cellpose"


def test_windows_worker_only_selects_explicit_target_and_pipeline_runs():
    target_id = uuid4()
    stmt = apply_worker_job_filters(
        select(Job),
        target=SimpleNamespace(id=target_id),
        claim_unassigned_jobs=False,
        allowed_job_kinds=("pipeline_run",),
    )
    sql = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert f"jobs.execution_target_id = '{target_id}'" in sql
    assert "jobs.execution_target_id IS NULL" not in sql
    assert "pipeline_run" in sql


def test_worker_path_mapping_rejects_relative_roots():
    with pytest.raises(ValueError, match="absolute"):
        parse_worker_path_mappings('[{"source":"data","target":"//fileserver/share"}]')


def test_worker_path_mapping_accepts_windows_drive_root():
    mappings = parse_worker_path_mappings(
        r'[{"source":"X:\\","target":"//fileserver/share"}]'
    )
    assert map_worker_path(r"X:\data\P1.mat", mappings) == r"\\fileserver\share\data\P1.mat"


def test_pipeline_copy_uses_worker_paths_and_keeps_original_audit(tmp_path):
    pipeline_dir = tmp_path / "share" / "pipeline"
    pipeline_dir.mkdir(parents=True)
    source = pipeline_dir / "pipeline.json"
    source.write_text(json.dumps({"modulePath": "/data/models/model.mat"}), encoding="utf-8")
    audit = {"summary": {"legacy_count": 0}, "dependencies": []}
    (pipeline_dir / "dependency_audit.json").write_text(json.dumps(audit), encoding="utf-8")
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    mappings = parse_worker_path_mappings(
        json.dumps([{"source": "/data", "target": str(tmp_path / "share")}])
    )

    payload = normalize_pipeline_ref_paths_for_posix(
        payload={"pipeline_ref": {"pipeline_json_path": str(source)}},
        job=SimpleNamespace(id=uuid4()),
        path_mappings=mappings,
        runtime_dir=runtime_dir,
    )

    runtime_path = runtime_dir / next(runtime_dir.iterdir()).name
    assert json.loads(runtime_path.read_text(encoding="utf-8"))["modulePath"] == str(
        tmp_path / "share" / "models" / "model.mat"
    )
    assert read_dependency_audit_for_payload(payload) == audit
