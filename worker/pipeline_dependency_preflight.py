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

    pipeline_status = str(dependency_audit.get("pipelineStatus") or "").strip().lower()

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
        dependency_mode = str(dep.get("dependency_mode") or "").strip().lower()
        locator_kind = str(dep.get("locator_kind") or "").strip().lower()
        module_kind = str(dep.get("module_kind") or dep.get("node_type") or "").strip().lower()
        configured_path = str(source.get("configured_path") or "").strip()
        resolved_path = str(source.get("resolved_path") or "").strip()
        candidate_path = resolved_path or configured_path
        if not candidate_path:
            if requires_external_path_for_preflight(dependency_mode, locator_kind, module_kind):
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


def requires_external_path_for_preflight(dependency_mode: str, locator_kind: str, module_kind: str) -> bool:
    if dependency_mode == "linked":
        return True
    if locator_kind in {"external_path", "absolute_path"}:
        return True
    return module_kind == "classifier"


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
    path_text = normalize_worker_path_text(str(candidate_path).strip())
    candidate = Path(path_text)
    if candidate.is_absolute():
        return candidate
    return (pipeline_root / candidate).resolve()


def normalize_worker_path_text(path_text: str) -> str:
    if len(path_text) >= 3 and path_text[1] == ":" and path_text[2] in ("\\", "/"):
        drive = path_text[0].upper()
        rest = path_text[3:].replace("\\", "/").lstrip("/")
        if drive == "X":
            return f"/data/{rest}" if rest else "/data"
        if drive == "C":
            mapped = map_known_windows_c_path(rest)
            if mapped:
                return mapped
    return path_text.replace("\\", "/") if path_text.startswith("\\\\") else path_text


def map_known_windows_c_path(rest: str) -> str:
    normalized = rest.replace("\\", "/").strip("/")
    mappings = (
        (
            "Users/Gilles Charvin/SynologyDrive/DetecDivProjects/Repository/",
            "/data/Gilles/abhilasha/classifiers/",
        ),
    )
    lower_normalized = normalized.lower()
    for source_prefix, target_prefix in mappings:
        if lower_normalized.startswith(source_prefix.lower()):
            suffix = normalized[len(source_prefix) :].lstrip("/")
            return f"{target_prefix}{suffix}" if suffix else target_prefix.rstrip("/")
    return ""


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
