from __future__ import annotations

from pathlib import Path
from typing import Any

from worker.pipeline_prepared_run import normalized_pipeline_path_from_ref, read_dependency_audit_for_payload


def evaluate_pipeline_dependency_preflight(payload: dict[str, Any]) -> dict[str, Any]:
    dependency_audit = read_dependency_audit_for_payload(payload)
    if not dependency_audit:
        return {
            "status": "skipped",
            "reason": "dependency_audit_missing",
            "errors": [],
            "warnings": ["No dependency_audit.json was found for pipeline payload."],
        }

    pipeline_ref = dict(payload.get("pipeline_ref") or {})
    pipeline_path = normalized_pipeline_path_from_ref(pipeline_ref)
    pipeline_root = resolve_pipeline_root_for_preflight(pipeline_path)

    errors: list[str] = []
    warnings: list[str] = []
    summary = dependency_audit.get("summary") or {}
    dependencies = dependency_audit.get("dependencies") or []

    required_missing_count = safe_int(summary.get("required_missing_count"))
    if required_missing_count > 0:
        errors.append(f"dependency_audit reports {required_missing_count} required missing dependency(ies).")

    pipeline_status = str(dependency_audit.get("pipelineStatus") or "").strip().lower()
    if pipeline_status == "linked_unresolvable":
        errors.append("dependency_audit pipelineStatus is linked_unresolvable.")

    legacy_count = safe_int(summary.get("legacy_count"))
    if legacy_count > 0:
        warnings.append(f"{legacy_count} legacy absolute-path dependency(ies) detected.")

    for dep in dependencies:
        if not isinstance(dep, dict):
            continue
        if not dep.get("is_required_for_run"):
            continue

        node_id = str(dep.get("node_id") or "node")
        source = dep.get("source") or {}
        if not isinstance(source, dict):
            source = {}
        configured_path = str(source.get("configured_path") or "").strip()
        resolved_path = str(source.get("resolved_path") or "").strip()
        candidate_path = resolved_path or configured_path
        if not candidate_path:
            errors.append(f"{node_id}: missing configured/resolved module path for required dependency.")
            continue

        host_path = resolve_dependency_path_for_worker(candidate_path, pipeline_root)
        if not host_path.exists():
            errors.append(f"{node_id}: required dependency path not found on worker host: {host_path}")
            continue

        if not dep.get("is_resolved"):
            errors.append(f"{node_id}: dependency audit marks required dependency as unresolved.")
            continue

        inference_count = safe_int(((dep.get("assets") or {}).get("inference") or {}).get("count"))
        if inference_count == 0:
            errors.append(f"{node_id}: no inference assets reported for required dependency.")

    status = "failed" if errors else "ok"
    return {
        "status": status,
        "pipeline_status": pipeline_status or None,
        "errors": errors,
        "warnings": warnings,
        "checked_dependencies": len([d for d in dependencies if isinstance(d, dict)]),
        "required_dependencies": len(
            [d for d in dependencies if isinstance(d, dict) and bool(d.get("is_required_for_run"))]
        ),
    }


def build_preflight_error_text(preflight: dict[str, Any]) -> str:
    errors = list(preflight.get("errors") or [])
    if not errors:
        return "Pipeline dependency preflight failed."
    joined = "\n".join(f"- {item}" for item in errors)
    return f"Pipeline dependency preflight failed:\n{joined}"


def resolve_pipeline_root_for_preflight(pipeline_path: str) -> Path:
    path_text = str(pipeline_path or "").strip()
    if not path_text:
        return Path.cwd()
    path_obj = Path(path_text)
    if path_obj.is_dir():
        return path_obj
    return path_obj.parent


def resolve_dependency_path_for_worker(candidate_path: str, pipeline_root: Path) -> Path:
    candidate = Path(str(candidate_path).strip())
    if candidate.is_absolute():
        return candidate
    return (pipeline_root / candidate).resolve()


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
