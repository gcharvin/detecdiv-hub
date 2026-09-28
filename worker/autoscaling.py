"""Plan worker slots using the same resource admission rules as job claims."""
from __future__ import annotations

from sqlalchemy import func, select

from api.models import Job, WorkerInstance
from api.services.job_priority_settings import (
    effective_job_priority_expression, resolve_job_priority_runtime_config,
    resolve_job_resource_runtime_config,
)
from worker.job_resources import (
    active_resource_totals, allocation_fits, allocation_fits_with_reservation,
    normalize_allocation, resolve_job_resource_allocation,
)
from worker.memory_resources import memory_policy, worker_allocation_fits


def plan_workers(session, *, target, settings, baseline: int) -> dict:
    config = resolve_job_resource_runtime_config(session)
    policy = memory_policy(target)
    # Reserve idle overhead for the largest possible pool, independent of the
    # current count, so a growth decision cannot invalidate active allocations.
    ceiling = min(config.cpu_capacity_cores, int((target.metadata_json or {}).get("worker_cpu_capacity") or config.cpu_capacity_cores))
    totals = active_resource_totals(session, target=target, config=config)
    active_count = len(totals["allocations"])
    stmt = select(Job).where(Job.status == "queued")
    if settings.worker_claim_unassigned_jobs:
        stmt = stmt.where((Job.execution_target_id.is_(None)) | (Job.execution_target_id == target.id))
    else:
        stmt = stmt.where(Job.execution_target_id == target.id)
    allowed = [v.strip() for v in settings.worker_job_kinds.split(",") if v.strip()]
    excluded = [v.strip() for v in settings.worker_excluded_job_kinds.split(",") if v.strip()]
    if allowed:
        stmt = stmt.where(Job.params_json["job_kind"].as_string().in_(allowed))
    if excluded:
        stmt = stmt.where(~func.coalesce(Job.params_json["job_kind"].as_string(), "generic").in_(excluded))
    priorities = resolve_job_priority_runtime_config(session)
    stmt = stmt.order_by(effective_job_priority_expression(priorities).asc(), Job.created_at.asc()).limit(100)
    waiting = list(session.scalars(stmt))
    empty = {**totals, "cpu_cores": 0, "memory_mb": 0, "disk_io_units": 0,
             "gpu_jobs": 0, "gpu_vram_mb": 0, "gpu_exclusive": False, "allocations": {}}
    reserved = None
    admitted = 0
    blocked = {}
    for job in waiting:
        try:
            allocation = normalize_allocation(resolve_job_resource_allocation(
                session, job=job, target=target, config=config), config=config)
        except ValueError:
            blocked["invalid_request"] = blocked.get("invalid_request", 0) + 1
            continue
        fits, reason = worker_allocation_fits(allocation, policy=policy)
        if fits:
            fits, reason = allocation_fits(allocation, totals=totals, config=config)
        if fits and reserved is not None:
            fits = allocation_fits_with_reservation(allocation, reserved_allocation=reserved,
                                                   totals=totals, config=config)
            reason = "priority_reservation" if not fits else None
        if not fits:
            blocked[reason or "capacity"] = blocked.get(reason or "capacity", 0) + 1
            if reserved is None and allocation_fits(allocation, totals=empty, config=config)[0]:
                reserved = allocation
            continue
        if active_count + admitted >= ceiling:
            break
        admitted += 1
        for key in ("cpu_cores", "memory_mb", "disk_io_units"):
            totals[key] += allocation[key]
        if allocation["gpu_required"]:
            totals["gpu_jobs"] += 1
            totals["gpu_vram_mb"] += allocation["gpu_vram_mb"] or config.gpu_vram_capacity_mb
            totals["gpu_exclusive"] |= allocation["gpu_vram_mb"] == 0 or config.gpu_vram_capacity_mb == 0
    desired = min(ceiling, max(baseline, active_count + admitted + int(reserved is not None)))
    # Never stop a busy high-numbered worker when shrinking the contiguous pool.
    for worker in session.scalars(select(WorkerInstance).where(WorkerInstance.execution_target_id == target.id)):
        if str(worker.current_job_id) in totals["allocations"]:
            try:
                desired = max(desired, int(worker.worker_instance.lstrip("@")))
            except ValueError:
                pass
    return {"desired": desired, "baseline": baseline, "ceiling": ceiling,
            "active_jobs": active_count, "admissible_queued_jobs": admitted,
            "blocked": blocked, "queue_sample": len(waiting)}
