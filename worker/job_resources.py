from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from api.config import get_settings
from api.models import ExecutionTarget, Job
from api.services.job_priority_settings import (
    JobResourceRuntimeConfig,
    job_resource_profile_for_kind,
    resolve_job_resource_runtime_config,
)


HUB_RESOURCE_ALLOCATION_KEY = "_hub_resource_allocation"
DEEP_LEARNING_MODULES = {
    "cellposesam",
    "sam31",
    "trackastra",
    "celllatenttracker",
    "celllatentmodel",
    "deeplab_pixel_classification",
    "cnn_lstm",
    "cnn",
}
GPU_DEVICE_KEYS = ("executionEnvironment", "execution_environment", "device")


def resolve_job_resource_allocation(
    session,
    *,
    job: Job,
    target: ExecutionTarget | None,
    config: JobResourceRuntimeConfig | None = None,
) -> dict[str, Any]:
    config = config or resolve_job_resource_runtime_config(session)
    job_kind = str((job.params_json or {}).get("job_kind") or "generic").strip() or "generic"
    profile = job_resource_profile_for_kind(config, job_kind)
    allocation: dict[str, Any] = {
        "cpu_cores": profile.cpu_cores,
        "gpu_permitted": profile.gpu_enabled,
        "gpu_required": bool(profile.gpu_enabled and job_kind != "pipeline_run"),
        "gpu_vram_mb": profile.gpu_vram_mb if profile.gpu_enabled and job_kind != "pipeline_run" else 0,
        "disk_io_units": profile.disk_io_units,
        "source": "job_resource_settings",
    }

    if job_kind == "pipeline_run":
        pipeline = inspect_pipeline_gpu_requirements(session, job=job, target=target)
        allocation["pipeline_gpu_profile"] = pipeline
        if pipeline["status"] == "known":
            allocation["gpu_required"] = bool(profile.gpu_enabled and pipeline["gpu_required"])
            allocation["gpu_vram_mb"] = profile.gpu_vram_mb if allocation["gpu_required"] else 0
            allocation["source"] = "pipeline_nodes_and_job_resource_settings"
        else:
            # A missing or unparseable pipeline must not be treated as a
            # zero-VRAM job. Reserve the target GPU conservatively when the
            # run allows GPU execution; the runtime preflight reports details.
            run_request = dict((job.params_json or {}).get("run_request") or {})
            gpu = dict(run_request.get("gpu") or {})
            mode = str(gpu.get("mode") or "").strip().lower()
            supports_gpu = bool(target is not None and target.supports_gpu)
            allocation["gpu_required"] = bool(
                profile.gpu_enabled
                and supports_gpu
                and mode not in {"force_cpu", "disabled", "none"}
            )
            allocation["gpu_vram_mb"] = profile.gpu_vram_mb if allocation["gpu_required"] else 0
            allocation["source"] = "conservative_pipeline_fallback"
    return allocation


def active_resource_totals(
    session,
    *,
    target: ExecutionTarget | None,
    config: JobResourceRuntimeConfig,
) -> dict[str, Any]:
    from sqlalchemy import select

    target_filter = Job.execution_target_id.is_(None) if target is None else Job.execution_target_id == target.id
    active_jobs = list(session.scalars(
        select(Job)
        .where(target_filter, Job.status.in_(("running", "cancelling")))
        .order_by(Job.started_at.asc())
    ))
    totals: dict[str, Any] = {
        "cpu_cores": 0,
        "disk_io_units": 0,
        "gpu_jobs": 0,
        "gpu_vram_mb": 0,
        "gpu_exclusive": False,
        "allocations": {},
    }
    for active in active_jobs:
        params = dict(active.params_json or {})
        allocation = params.get(HUB_RESOURCE_ALLOCATION_KEY)
        if not isinstance(allocation, dict):
            allocation = resolve_job_resource_allocation(
                session,
                job=active,
                target=target,
                config=config,
            )
        allocation = normalize_allocation(allocation, config=config)
        totals["cpu_cores"] += allocation["cpu_cores"]
        totals["disk_io_units"] += allocation["disk_io_units"]
        if allocation["gpu_required"]:
            totals["gpu_jobs"] += 1
            if allocation["gpu_vram_mb"] == 0:
                totals["gpu_exclusive"] = True
            totals["gpu_vram_mb"] += allocation["gpu_vram_mb"]
        totals["allocations"][str(active.id)] = allocation
    return totals


def normalize_allocation(
    allocation: dict[str, Any],
    *,
    config: JobResourceRuntimeConfig,
) -> dict[str, Any]:
    def safe_int(value: object, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    cpu_cores = max(1, min(36, safe_int(allocation.get("cpu_cores"), 1)))
    disk_io_units = max(0, min(36, safe_int(allocation.get("disk_io_units"), 1)))
    gpu_required = bool(allocation.get("gpu_required"))
    gpu_vram_mb = max(0, safe_int(allocation.get("gpu_vram_mb"), 0)) if gpu_required else 0
    return {
        **allocation,
        "cpu_cores": cpu_cores,
        "gpu_required": gpu_required,
        "gpu_vram_mb": gpu_vram_mb,
        "disk_io_units": disk_io_units,
    }


def allocation_fits(
    allocation: dict[str, Any],
    *,
    totals: dict[str, Any],
    config: JobResourceRuntimeConfig,
) -> tuple[bool, str | None]:
    allocation = normalize_allocation(allocation, config=config)
    if totals["cpu_cores"] + allocation["cpu_cores"] > config.cpu_capacity_cores:
        return False, "cpu_capacity"
    if totals["disk_io_units"] + allocation["disk_io_units"] > config.disk_io_capacity_units:
        return False, "disk_io_capacity"
    if not allocation["gpu_required"]:
        return True, None
    if totals["gpu_exclusive"]:
        return False, "gpu_exclusive"
    if config.gpu_vram_capacity_mb == 0:
        if totals["gpu_jobs"] > 0:
            return False, "gpu_exclusive"
        return True, None
    requested_mb = allocation["gpu_vram_mb"] or config.gpu_vram_capacity_mb
    if totals["gpu_vram_mb"] + requested_mb > config.gpu_vram_capacity_mb:
        return False, "gpu_vram_capacity"
    return True, None


def allocation_fits_with_reservation(
    allocation: dict[str, Any],
    *,
    reserved_allocation: dict[str, Any],
    totals: dict[str, Any],
    config: JobResourceRuntimeConfig,
) -> bool:
    """Check a lower-priority job while leaving room for a blocked higher one."""
    allocation = normalize_allocation(allocation, config=config)
    reserved = normalize_allocation(reserved_allocation, config=config)
    reserved_cpu_total = totals["cpu_cores"] + allocation["cpu_cores"] + reserved["cpu_cores"]
    if reserved_cpu_total > config.cpu_capacity_cores:
        return False
    reserved_disk_total = totals["disk_io_units"] + allocation["disk_io_units"] + reserved["disk_io_units"]
    if reserved_disk_total > config.disk_io_capacity_units:
        return False

    if allocation["gpu_required"] and reserved["gpu_required"]:
        if totals["gpu_exclusive"] or config.gpu_vram_capacity_mb == 0:
            return False
        if allocation["gpu_vram_mb"] == 0 or reserved["gpu_vram_mb"] == 0:
            return False
        reserved_vram_total = totals["gpu_vram_mb"] + allocation["gpu_vram_mb"] + reserved["gpu_vram_mb"]
        if reserved_vram_total > config.gpu_vram_capacity_mb:
            return False
    elif allocation["gpu_required"]:
        fits, _reason = allocation_fits(allocation, totals=totals, config=config)
        if not fits:
            return False
    return True


def inspect_pipeline_gpu_requirements(
    session,
    *,
    job: Job,
    target: ExecutionTarget | None,
) -> dict[str, Any]:
    try:
        from worker.pipeline_run_executor import normalize_pipeline_run_payload
        from worker.path_mappings import map_payload_path_fields, parse_worker_path_mappings

        settings = get_settings()
        payload = normalize_pipeline_run_payload(session, job=job)
        mappings = parse_worker_path_mappings(settings.worker_path_mappings)
        payload = map_payload_path_fields(payload, mappings)
        pipeline_ref = dict(payload.get("pipeline_ref") or {})
        pipeline_path = next(
            (
                str(pipeline_ref.get(key) or "").strip()
                for key in ("pipeline_json_path", "pipeline_bundle_uri", "export_manifest_uri")
                if str(pipeline_ref.get(key) or "").strip()
            ),
            "",
        )
        pipeline_data = read_pipeline_json(pipeline_path)
        if not isinstance(pipeline_data, dict) or not isinstance(pipeline_data.get("nodes"), list):
            return {"status": "unknown", "reason": "pipeline_nodes_unavailable", "selected_node_ids": [], "deep_learning_node_ids": []}

        nodes = pipeline_data["nodes"]
        run_request = dict(payload.get("run_request") or {})
        selected_value = run_request.get("selected_nodes")
        selected_ids = {str(value) for value in selected_value if str(value)} if isinstance(selected_value, list) else set()
        node_overrides = {
            str(item.get("id")): dict(item.get("params") or {})
            for item in (run_request.get("node_params") or [])
            if isinstance(item, dict) and item.get("id")
        }
        gpu_settings = dict(run_request.get("gpu") or {})
        run_mode = str(gpu_settings.get("mode") or "").strip().lower()
        if not run_mode:
            run_mode = "force_gpu" if target is not None and target.supports_gpu else "module_default"

        selected_node_ids: list[str] = []
        deep_learning_node_ids: list[str] = []
        gpu_node_ids: list[str] = []
        for index, raw_node in enumerate(nodes):
            if not isinstance(raw_node, dict):
                continue
            node_id = str(raw_node.get("id") or f"node_{index + 1}")
            if selected_ids and node_id not in selected_ids:
                continue
            selected_node_ids.append(node_id)
            node_params = dict(raw_node.get("params") or {})
            node_params.update(node_overrides.get(node_id, {}))
            if not is_deep_learning_node(raw_node, node_params):
                continue
            deep_learning_node_ids.append(node_id)
            node_device = normalize_gpu_device(node_params)
            if run_mode in {"force_cpu", "disabled", "none"} or node_device == "cpu":
                continue
            if node_device == "gpu" or run_mode == "force_gpu" or (
                run_mode == "module_default" and bool(target is not None and target.supports_gpu)
            ):
                gpu_node_ids.append(node_id)

        return {
            "status": "known",
            "source": pipeline_path,
            "selected_node_ids": selected_node_ids,
            "deep_learning_node_ids": deep_learning_node_ids,
            "gpu_node_ids": gpu_node_ids,
            "gpu_required": bool(gpu_node_ids),
            "run_gpu_mode": run_mode,
        }
    except Exception as exc:
        return {
            "status": "unknown",
            "reason": f"pipeline_resource_inspection_failed:{type(exc).__name__}",
            "selected_node_ids": [],
            "deep_learning_node_ids": [],
        }


def is_deep_learning_node(node: dict[str, Any], params: dict[str, Any]) -> bool:
    package = str(node.get("pkg") or node.get("module_kind") or "").strip().lower()
    package = package.replace(" ", "_").replace("-", "_")
    if package in DEEP_LEARNING_MODULES:
        return True
    node_type = str(node.get("type") or "").strip().lower()
    if node_type == "classifier":
        return True
    return False


def normalize_gpu_device(params: dict[str, Any]) -> str:
    for key in GPU_DEVICE_KEYS:
        value = str(params.get(key) or "").strip().lower().replace("-", "_")
        if value in {"gpu", "cuda", "multi_gpu", "force_gpu"}:
            return "gpu"
        if value in {"cpu", "force_cpu"}:
            return "cpu"
    return "module_default"


def read_pipeline_json(path_text: str) -> dict[str, Any] | None:
    if not path_text:
        return None
    path = Path(path_text)
    if path.is_dir():
        path = path / "pipeline.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) and path.name.lower() == "export_manifest.json":
        return None
    if isinstance(payload, dict) and not isinstance(payload.get("nodes"), list):
        nested_path = payload.get("pipeline_json_path") or payload.get("pipeline_path")
        if isinstance(nested_path, str):
            candidate = Path(nested_path)
            if not candidate.is_absolute():
                candidate = path.parent / candidate
            if candidate.is_dir():
                candidate = candidate / "pipeline.json"
            if candidate.is_file():
                try:
                    nested = json.loads(candidate.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    return None
                return nested if isinstance(nested, dict) else None
    return payload if isinstance(payload, dict) else None
