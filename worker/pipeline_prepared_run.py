from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from api.models import Job


def persist_prepared_pipeline_run(session, *, job: Job, payload: dict[str, Any]) -> None:
    job_record = session.get(Job, job.id)
    if job_record is None:
        return
    result_json = dict(job_record.result_json or {})
    prepared = build_prepared_pipeline_run(payload=payload)
    if prepared:
        result_json["prepared_run"] = prepared
    dependency_audit = read_dependency_audit_for_payload(payload)
    if dependency_audit:
        result_json["dependency_audit"] = dependency_audit
    job_record.result_json = result_json
    session.commit()


def merge_prepared_pipeline_run(*, payload: dict[str, Any], result_json: dict[str, Any]) -> dict[str, Any]:
    merged = dict(result_json or {})
    if "prepared_run" not in merged:
        prepared = build_prepared_pipeline_run(payload=payload)
        if prepared:
            merged["prepared_run"] = prepared
    if "dependency_audit" not in merged:
        dependency_audit = read_dependency_audit_for_payload(payload)
        if dependency_audit:
            merged["dependency_audit"] = dependency_audit
    return merged


def build_prepared_pipeline_run(*, payload: dict[str, Any]) -> dict[str, Any]:
    project_ref = dict(payload.get("project_ref") or {})
    pipeline_ref = dict(payload.get("pipeline_ref") or {})
    execution = dict(payload.get("execution") or {})
    return {
        "project_ref": project_ref,
        "pipeline_ref": pipeline_ref,
        "execution": execution,
        "normalized_pipeline_path": normalized_pipeline_path_from_ref(pipeline_ref),
        "pipeline_resolution_method": str(pipeline_ref.get("resolution_method") or "").strip() or None,
    }


def normalized_pipeline_path_from_ref(pipeline_ref: dict[str, Any]) -> str:
    for key in ("pipeline_json_path", "pipeline_bundle_uri", "export_manifest_uri"):
        value = str(pipeline_ref.get(key) or "").strip()
        if value:
            return value
    return ""


def read_dependency_audit_for_payload(payload: dict[str, Any]) -> dict[str, Any]:
    pipeline_ref = dict(payload.get("pipeline_ref") or {})
    pipeline_path = normalized_pipeline_path_from_ref(pipeline_ref)
    if not pipeline_path:
        return {}

    candidate_paths: list[Path] = []
    path_obj = Path(pipeline_path)
    if path_obj.is_dir():
        candidate_paths.append(path_obj / "dependency_audit.json")
        candidate_paths.append(path_obj / "pipeline" / "dependency_audit.json")
    else:
        candidate_paths.append(path_obj.parent / "dependency_audit.json")
        candidate_paths.append(path_obj.parent.parent / "dependency_audit.json")

    for candidate in candidate_paths:
        try:
            if candidate.is_file():
                return json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
    return {}
