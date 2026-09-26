from __future__ import annotations

import logging
import os
import socket
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import func, select

from api.config import get_settings
from api.db import SessionLocal
from api.models import ExecutionTarget, IndexingJob, Job, RawDatasetPosition
from api.services.external_eln import sync_external_eln_system
from api.services.indexing_jobs import execute_indexing_job
from api.services.job_priority_settings import (
    effective_job_priority_expression,
    resolve_job_resource_runtime_config,
    resolve_job_priority_runtime_config,
)
from api.services.project_deletion import (
    execute_project_deletion_job,
    finalize_project_deletion_failure,
)
from api.services.project_locks import (
    heartbeat_project_locks_for_job,
    release_project_locks_for_job,
)
from api.services.raw_dataset_deletion import execute_raw_dataset_deletion_job
from api.services.raw_dataset_position_deletion import execute_raw_dataset_position_deletion_job
from api.services.worker_instances import (
    active_worker_current_job_ids,
    execution_target_worker_metadata,
    upsert_worker_instance,
)
from worker.archive_policy_scheduler import run_archive_policy_if_due
from worker.backup_executor import BACKUP_JOB_KINDS, execute_backup_job, finalize_backup_failure
from worker.backup_scheduler import run_backup_if_due
from worker.cpu_usage import JobCpuMonitor, get_cpu_topology, merge_cpu_usage
from worker.gpu_arbitration import (
    GpuArbitrationError,
    execute_assistant_control_job,
    job_requires_gpu,
    pause_qwen_for_gpu_job,
    resume_qwen_if_gpu_is_idle,
)
from worker.job_resources import (
    HUB_RESOURCE_ALLOCATION_KEY,
    active_resource_totals,
    allocation_fits,
    allocation_fits_with_reservation,
    resolve_job_resource_allocation,
)
from worker.legacy_matlab_executor import execute_legacy_matlab_job
from worker.micromanager_ingest_scheduler import (
    execute_micromanager_ingest_job,
    run_micromanager_ingest_if_due,
)
from worker.misc_storage_inventory import execute_misc_storage_inventory_job
from worker.path_mappings import parse_worker_path_mappings
from worker.pipeline_run_executor import PipelineRunCancelled, execute_pipeline_run_job
from worker.storage_lifecycle import (
    execute_storage_lifecycle_job,
    finalize_storage_lifecycle_failure,
)
from worker.storage_optimization import (
    execute_storage_optimization_job,
    finalize_storage_optimization_failure,
)
from worker.user_home_storage import (
    execute_user_home_storage_job,
    finalize_user_home_storage_failure,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("detecdiv-hub-worker")


@contextmanager
def session_scope():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_worker_instance_id() -> str:
    configured = str(get_settings().worker_instance or "").strip()
    if configured:
        return normalize_worker_instance_id(configured)
    return normalize_worker_instance_id(f"{socket.gethostname()}-pid{os.getpid()}")


def finish_job_cpu_monitor(monitor: JobCpuMonitor | None, job: Job) -> dict | None:
    if monitor is None:
        return None
    usage = monitor.finish()
    allocation = (job.params_json or {}).get(HUB_RESOURCE_ALLOCATION_KEY)
    if isinstance(allocation, dict):
        usage["allocated_cores"] = allocation.get("cpu_cores")
    return usage


def normalize_worker_instance_id(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return text
    if text.lower() == "main":
        return "main"
    core = text[1:] if text.startswith("@") else text
    if core.isdigit():
        return f"@{core}"
    return text


def running_job_is_stale(job: Job) -> bool:
    now = datetime.now(timezone.utc)
    reference_time = job.heartbeat_at or job.updated_at or job.started_at or job.created_at
    if reference_time is None:
        return True
    return (now - reference_time).total_seconds() > 60


def keep_running_job_alive(
    stop_event: threading.Event,
    *,
    job_id,
    cpu_monitor: JobCpuMonitor | None = None,
) -> None:
    settings = get_settings()
    while not stop_event.wait(10.0):
        try:
            with session_scope() as session:
                job = session.get(Job, job_id)
                if job is None or job.status not in {"running", "cancelling"}:
                    return
                now = datetime.now(timezone.utc)
                job.heartbeat_at = now
                job.updated_at = now
                heartbeat_project_locks_for_job(session, job_id=job.id)
                sync_indexing_job_heartbeat(session, job=job, now=now)
                target = resolve_target_for_job(session, job=job, configured_target_key=settings.worker_target_key)
                update_worker_target_state(
                    session,
                    target=target,
                    health="busy",
                    current_job=job,
                    last_job_status="running",
                    current_job_cpu_cores=cpu_monitor.current_cores if cpu_monitor is not None else 0.0,
                )
        except Exception:  # pragma: no cover - defensive around keepalive
            LOGGER.exception("Job keepalive update failed for %s", job_id)


def sync_indexing_job_heartbeat(session, *, job: Job, now: datetime) -> None:
    if (job.params_json or {}).get("job_kind") != "project_indexing":
        return
    indexing_job_id = (job.params_json or {}).get("indexing_job_id")
    if not indexing_job_id:
        return
    indexing_job = session.get(IndexingJob, indexing_job_id)
    if indexing_job is None or indexing_job.status != "running":
        return
    indexing_job.heartbeat_at = now
    indexing_job.updated_at = now


def recover_orphaned_jobs(session, *, target: ExecutionTarget | None) -> int:
    if target is None:
        return 0

    active_job_ids = active_worker_current_job_ids(session, target=target)
    running_jobs = list(
        session.scalars(
            select(Job).where(
                Job.execution_target_id == target.id,
                Job.status.in_(("running", "cancelling")),
            )
        )
    )
    recovered = 0
    for job in running_jobs:
        if str(job.id) in active_job_ids:
            continue
        if not running_job_is_stale(job):
            continue
        job.status = "failed"
        job.error_text = "Job was orphaned after worker restart or heartbeat loss."
        job.finished_at = datetime.now(timezone.utc)
        job.heartbeat_at = job.finished_at
        job.updated_at = job.finished_at
        allocation = (job.params_json or {}).get(HUB_RESOURCE_ALLOCATION_KEY)
        if isinstance(allocation, dict):
            result_json = dict(job.result_json or {})
            result_json["resource_allocation"] = allocation
            job.result_json = result_json
        sync_indexing_job_from_worker_job(
            session,
            job=job,
            status="stale",
            phase="stale",
            message="Marked stale after worker heartbeat loss.",
            error_text=job.error_text,
            finished_at=job.finished_at,
        )
        if (job.params_json or {}).get("job_kind") == "raw_preview_video":
            update_raw_preview_position_state(session, job=job, status="failed")
        release_project_locks_for_job(session, job_id=job.id)
        recovered += 1
        LOGGER.warning("Recovered orphaned job %s on target %s", job.id, target.display_name)
    if recovered:
        update_worker_target_state(
            session,
            target=target,
            health="online",
            current_job=None,
            last_job_status="recovered_orphaned_jobs",
        )
    return recovered


def claim_next_job() -> Job | None:
    settings = get_settings()
    with session_scope() as session:
        target = resolve_worker_target(session, settings.worker_target_key)
        if settings.worker_target_key and target is None:
            raise RuntimeError(
                f"Configured worker target '{settings.worker_target_key}' does not exist."
            )
        if target is None and not settings.worker_claim_unassigned_jobs:
            raise RuntimeError("A worker target is required when unassigned jobs are disabled.")
        if target is not None:
            target = session.scalars(select(ExecutionTarget).where(ExecutionTarget.id == target.id).with_for_update()).one()
            if bool((target.metadata_json or {}).get("drain_new_jobs")):
                update_worker_target_state(
                    session,
                    target=target,
                    health="online",
                    current_job=None,
                    last_job_status="drain_active",
                )
                return None
            max_concurrent_jobs = read_positive_int((target.metadata_json or {}).get("max_concurrent_jobs"))
            if max_concurrent_jobs is not None:
                running_jobs = session.scalar(
                    select(func.count(Job.id)).where(
                        Job.execution_target_id == target.id,
                        Job.status.in_(("running", "cancelling")),
                    )
                )
                if int(running_jobs or 0) >= max_concurrent_jobs:
                    update_worker_target_state(
                        session,
                        target=target,
                        health="busy",
                        current_job=None,
                        last_job_status="capacity_full",
                    )
                    return None
        priority_config = resolve_job_priority_runtime_config(session)
        resource_config = resolve_job_resource_runtime_config(session)
        effective_priority = effective_job_priority_expression(priority_config)
        stmt = (
            select(Job)
            .where(Job.status == "queued")
            .order_by(effective_priority.asc(), Job.created_at.asc())
            .limit(100)
            .with_for_update(skip_locked=True)
        )
        allowed_job_kinds = tuple(
            kind.strip() for kind in settings.worker_job_kinds.split(",") if kind.strip()
        )
        excluded_job_kinds = tuple(
            kind.strip()
            for kind in settings.worker_excluded_job_kinds.split(",")
            if kind.strip()
        )
        stmt = apply_worker_job_filters(
            stmt,
            target=target,
            claim_unassigned_jobs=settings.worker_claim_unassigned_jobs,
            allowed_job_kinds=allowed_job_kinds,
            excluded_job_kinds=excluded_job_kinds,
        )
        candidates = list(session.scalars(stmt))
        totals = active_resource_totals(session, target=target, config=resource_config)
        empty_totals = {
            "cpu_cores": 0,
            "disk_io_units": 0,
            "gpu_jobs": 0,
            "gpu_vram_mb": 0,
            "gpu_exclusive": False,
            "allocations": {},
        }
        reserved_allocation = None
        job = None
        for candidate in candidates:
            allocation = resolve_job_resource_allocation(
                session,
                job=candidate,
                target=target,
                config=resource_config,
            )
            fits, _reason = allocation_fits(allocation, totals=totals, config=resource_config)
            if fits:
                if reserved_allocation is not None:
                    # Keep both resource headroom and one slot in the bounded
                    # worker pool for the first higher-priority job that fits
                    # once currently running jobs release resources.
                    if not allocation_fits_with_reservation(
                        allocation,
                        reserved_allocation=reserved_allocation,
                        totals=totals,
                        config=resource_config,
                    ):
                        continue
                    if target is not None:
                        target_metadata = target.metadata_json or {}
                        slot_limits = [
                            read_positive_int(target_metadata.get("max_concurrent_jobs")),
                            read_positive_int(target_metadata.get("worker_instances_desired")),
                        ]
                        slot_limits = [limit for limit in slot_limits if limit is not None]
                        worker_slot_capacity = min(slot_limits) if slot_limits else None
                        if (
                            worker_slot_capacity is not None
                            and len(totals["allocations"]) + 1 >= worker_slot_capacity
                        ):
                            continue
                job = candidate
                params = dict(candidate.params_json or {})
                params[HUB_RESOURCE_ALLOCATION_KEY] = allocation
                candidate.params_json = params
                break
            if reserved_allocation is None:
                fits_when_idle, _reason = allocation_fits(allocation, totals=empty_totals, config=resource_config)
                if fits_when_idle:
                    reserved_allocation = allocation
        if job is None:
            update_worker_target_state(
                session,
                target=target,
                health="busy" if candidates else "idle",
                current_job=None,
                last_job_status="resource_capacity_full" if candidates else None,
            )
            return None

        job.status = "running"
        job.resolved_mode = job.requested_mode if job.requested_mode != "auto" else "server"
        job.started_at = job.started_at or datetime.now(timezone.utc)
        job.heartbeat_at = job.started_at
        job.updated_at = datetime.now(timezone.utc)
        if target is not None and job.execution_target_id is None:
            job.execution_target_id = target.id
        update_worker_target_state(
            session,
            target=target,
            health="busy",
            current_job=job,
            last_job_status="running",
        )
        session.flush()
        session.expunge(job)
        return job


def apply_worker_job_filters(
    stmt,
    *,
    target: ExecutionTarget | None,
    claim_unassigned_jobs: bool,
    allowed_job_kinds: tuple[str, ...],
    excluded_job_kinds: tuple[str, ...] = (),
):
    if target is not None:
        if claim_unassigned_jobs:
            stmt = stmt.where((Job.execution_target_id.is_(None)) | (Job.execution_target_id == target.id))
        else:
            stmt = stmt.where(Job.execution_target_id == target.id)
    if allowed_job_kinds:
        stmt = stmt.where(Job.params_json["job_kind"].as_string().in_(allowed_job_kinds))
    if excluded_job_kinds:
        job_kind = func.coalesce(Job.params_json["job_kind"].as_string(), "generic")
        stmt = stmt.where(~job_kind.in_(excluded_job_kinds))
    return stmt


def mark_job_done(job_id, result_json: dict) -> None:
    settings = get_settings()
    with session_scope() as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        job.status = "done"
        completed_result = dict(job.result_json or {})
        previous_cpu_usage = completed_result.get("cpu_usage")
        completed_result.update(result_json)
        cpu_usage = merge_cpu_usage(previous_cpu_usage, result_json.get("cpu_usage"))
        if cpu_usage is not None:
            completed_result["cpu_usage"] = cpu_usage
        allocation = (job.params_json or {}).get(HUB_RESOURCE_ALLOCATION_KEY)
        if isinstance(allocation, dict):
            completed_result["resource_allocation"] = allocation
        job.result_json = completed_result
        job.finished_at = datetime.now(timezone.utc)
        job.heartbeat_at = job.finished_at
        job.updated_at = datetime.now(timezone.utc)
        release_project_locks_for_job(session, job_id=job.id)
        if (job.params_json or {}).get("job_kind") == "raw_preview_video":
            update_raw_preview_position_state(session, job=job, status=result_json.get("preview_status", "done"))
        target = resolve_target_for_job(session, job=job, configured_target_key=settings.worker_target_key)
        update_worker_target_state(
            session,
            target=target,
            health="online",
            current_job=None,
            last_job_status="done",
            last_job=job,
        )
        resume_qwen_if_gpu_is_idle(session, settings=settings)


def requeue_job(job_id, result_json: dict) -> None:
    """Return a resumable job to the queue while retaining its job ID."""
    settings = get_settings()
    with session_scope() as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        now = datetime.now(timezone.utc)
        job.status = "queued"
        previous_result = dict(job.result_json or {})
        cpu_usage = merge_cpu_usage(previous_result.get("cpu_usage"), result_json.get("cpu_usage"))
        result_json = {**previous_result, **result_json}
        if cpu_usage is not None:
            result_json["cpu_usage"] = cpu_usage
        allocation = (job.params_json or {}).get(HUB_RESOURCE_ALLOCATION_KEY)
        if isinstance(allocation, dict):
            result_json = {**result_json, "last_resource_allocation": allocation}
        job.result_json = result_json
        params = dict(job.params_json or {})
        params.pop(HUB_RESOURCE_ALLOCATION_KEY, None)
        job.params_json = params
        job.heartbeat_at = now
        job.updated_at = now
        target = resolve_target_for_job(session, job=job, configured_target_key=settings.worker_target_key)
        update_worker_target_state(
            session,
            target=target,
            health="online",
            current_job=None,
            last_job_status="requeued",
            last_job=job,
        )


def mark_job_failed(job_id, error_text: str, *, cpu_usage: dict | None = None) -> None:
    settings = get_settings()
    with session_scope() as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        job.status = "failed"
        job.error_text = error_text
        result_json = dict(job.result_json or {})
        merged_cpu_usage = merge_cpu_usage(result_json.get("cpu_usage"), cpu_usage)
        if merged_cpu_usage is not None:
            result_json["cpu_usage"] = merged_cpu_usage
        allocation = (job.params_json or {}).get(HUB_RESOURCE_ALLOCATION_KEY)
        if isinstance(allocation, dict):
            result_json["resource_allocation"] = allocation
        job.result_json = result_json
        job.finished_at = datetime.now(timezone.utc)
        job.heartbeat_at = job.finished_at
        job.updated_at = datetime.now(timezone.utc)
        release_project_locks_for_job(session, job_id=job.id)
        sync_indexing_job_from_worker_job(
            session,
            job=job,
            status="failed",
            phase="failed",
            message="Indexing failed.",
            error_text=error_text,
            finished_at=job.finished_at,
        )
        if (job.params_json or {}).get("job_kind") == "raw_preview_video":
            update_raw_preview_position_state(session, job=job, status="failed")
        target = resolve_target_for_job(session, job=job, configured_target_key=settings.worker_target_key)
        update_worker_target_state(
            session,
            target=target,
            health="error",
            current_job=None,
            last_job_status="failed",
            last_job=job,
            error_text=error_text,
        )
        resume_qwen_if_gpu_is_idle(session, settings=settings)


def mark_job_cancelled(job_id, message: str, *, cpu_usage: dict | None = None) -> None:
    settings = get_settings()
    with session_scope() as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        job.status = "cancelled"
        result_json = dict(job.result_json or {})
        result_json["status"] = "cancelled"
        result_json["message"] = message or "Pipeline run cancelled by user."
        merged_cpu_usage = merge_cpu_usage(result_json.get("cpu_usage"), cpu_usage)
        if merged_cpu_usage is not None:
            result_json["cpu_usage"] = merged_cpu_usage
        allocation = (job.params_json or {}).get(HUB_RESOURCE_ALLOCATION_KEY)
        if isinstance(allocation, dict):
            result_json["resource_allocation"] = allocation
        job.result_json = result_json
        job.error_text = message or None
        job.finished_at = datetime.now(timezone.utc)
        job.heartbeat_at = job.finished_at
        job.updated_at = datetime.now(timezone.utc)
        release_project_locks_for_job(session, job_id=job.id)
        sync_indexing_job_from_worker_job(
            session,
            job=job,
            status="failed",
            phase="failed",
            message=message or "Indexing cancelled.",
            error_text=message or None,
            finished_at=job.finished_at,
        )
        target = resolve_target_for_job(session, job=job, configured_target_key=settings.worker_target_key)
        update_worker_target_state(
            session,
            target=target,
            health="online",
            current_job=None,
            last_job_status="cancelled",
            last_job=job,
        )
        resume_qwen_if_gpu_is_idle(session, settings=settings)


def execute_job(job: Job) -> dict:
    job_kind = (job.params_json or {}).get("job_kind")
    LOGGER.info("Executing job %s on host %s instance %s", job.id, socket.gethostname(), get_worker_instance_id())
    if job_kind == "assistant_service_control":
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Assistant control job {job.id} disappeared before execution")
            result_json = execute_assistant_control_job(session, job=job_record, settings=get_settings())
            result_json["worker_host"] = socket.gethostname()
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind == "pipeline_run":
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            if job_requires_gpu(session, job=job_record):
                try:
                    pause_qwen_for_gpu_job(settings=get_settings())
                except GpuArbitrationError as exc:
                    raise RuntimeError(f"GPU arbitration blocked this pipeline job: {exc}") from exc
            result_json = execute_pipeline_run_job(session, job=job_record)
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind == "project_indexing":
        indexing_job_id = (job.params_json or {}).get("indexing_job_id")
        if not indexing_job_id:
            raise ValueError(f"Indexing worker job {job.id} is missing indexing_job_id")
        result_json = execute_indexing_job(indexing_job_id)
        result_json["worker_host"] = socket.gethostname()
        result_json["worker_instance"] = get_worker_instance_id()
        result_json["requested_mode"] = job.requested_mode
        result_json["resolved_mode"] = job.resolved_mode
        return result_json
    if job_kind == "micromanager_ingest":
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Micro-Manager ingestion job {job.id} disappeared before execution")
            result_json = execute_micromanager_ingest_job(session, job=job_record)
            result_json["worker_host"] = socket.gethostname()
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind in {"archive_raw_dataset", "restore_raw_dataset"}:
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            result_json = execute_storage_lifecycle_job(session, job=job_record)
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind == "legacy_matlab":
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            result_json = execute_legacy_matlab_job(session, job=job_record)
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind in {"storage_optimization", "storage_optimization_scan", "storage_optimization_chunk"}:
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Storage optimization job {job.id} disappeared before execution")
            result_json = execute_storage_optimization_job(session, job=job_record)
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind == "project_deletion":
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            result_json = execute_project_deletion_job(session, job=job_record)
            result_json["worker_host"] = socket.gethostname()
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind == "raw_dataset_deletion":
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            result_json = execute_raw_dataset_deletion_job(session, job=job_record)
            result_json["worker_host"] = socket.gethostname()
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind == "raw_dataset_position_deletion":
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            result_json = execute_raw_dataset_position_deletion_job(session, job=job_record)
            result_json["worker_host"] = socket.gethostname()
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind in BACKUP_JOB_KINDS:
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            result_json = execute_backup_job(session, job=job_record)
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind == "raw_preview_video":
        try:
            from worker.raw_preview_video import execute_raw_preview_video_job
        except ImportError as exc:
            raise RuntimeError(
                "Raw preview video dependencies are missing. Install numpy, Pillow, tifffile, and zarr in the worker environment."
            ) from exc
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            result_json = execute_raw_preview_video_job(session, job=job_record)
            result_json["worker_host"] = socket.gethostname()
            result_json["worker_instance"] = get_worker_instance_id()
            result_json["requested_mode"] = job.requested_mode
            result_json["resolved_mode"] = job.resolved_mode
            return result_json
    if job_kind == "external_eln_sync":
        system_key = (job.params_json or {}).get("system_key")
        if not system_key:
            raise ValueError(f"External ELN sync job {job.id} is missing system_key")
        with session_scope() as session:
            result = sync_external_eln_system(session, system_key=system_key)
            payload = result.model_dump(mode="json")
            payload["worker_host"] = socket.gethostname()
            payload["worker_instance"] = get_worker_instance_id()
            return payload
    if job_kind == "prepare_user_home_storage":
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            result_json = execute_user_home_storage_job(session, job=job_record)
            result_json["worker_host"] = socket.gethostname()
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json
    if job_kind == "misc_storage_inventory":
        with session_scope() as session:
            job_record = session.get(Job, job.id)
            if job_record is None:
                raise ValueError(f"Job {job.id} disappeared before execution")
            result_json = execute_misc_storage_inventory_job(session, job=job_record)
            result_json["worker_instance"] = get_worker_instance_id()
            return result_json

    return {
        "worker_host": socket.gethostname(),
        "worker_instance": get_worker_instance_id(),
        "message": "Placeholder worker execution completed.",
        "job_kind": job_kind or "generic",
        "requested_mode": job.requested_mode,
        "resolved_mode": job.resolved_mode,
    }


def sync_indexing_job_from_worker_job(
    session,
    *,
    job: Job,
    status: str,
    phase: str,
    message: str,
    error_text: str | None,
    finished_at: datetime,
) -> None:
    if (job.params_json or {}).get("job_kind") != "project_indexing":
        return
    indexing_job_id = (job.params_json or {}).get("indexing_job_id")
    if not indexing_job_id:
        return
    indexing_job = session.get(IndexingJob, indexing_job_id)
    if indexing_job is None:
        return
    if indexing_job.finished_at is not None and indexing_job.status in {"completed", "completed_with_errors"}:
        return
    indexing_job.status = status
    indexing_job.phase = phase
    indexing_job.message = message
    indexing_job.error_text = error_text
    indexing_job.finished_at = finished_at
    indexing_job.heartbeat_at = finished_at
    indexing_job.updated_at = finished_at


def run_forever() -> None:
    settings = get_settings()
    parse_worker_path_mappings(settings.worker_path_mappings)
    with session_scope() as session:
        target = resolve_worker_target(session, settings.worker_target_key)
        if settings.worker_target_key and target is None:
            raise RuntimeError(
                f"Configured worker target '{settings.worker_target_key}' does not exist."
            )
        if target is None and not settings.worker_claim_unassigned_jobs:
            raise RuntimeError("A worker target is required when unassigned jobs are disabled.")
    last_archive_policy_run_at: datetime | None = None
    last_micromanager_ingest_run_at: datetime | None = None
    last_backup_run_at: datetime | None = None
    LOGGER.info("Starting DetecDiv hub worker instance %s", get_worker_instance_id())
    while True:
        try:
            with session_scope() as session:
                target = resolve_worker_target(session, settings.worker_target_key)
                update_worker_target_state(session, target=target, health="online", current_job=None, last_job_status=None)
                recover_orphaned_jobs(session, target=target)
        except Exception:  # pragma: no cover - defensive around heartbeat
            LOGGER.exception("Execution target heartbeat update failed")

        if settings.worker_enable_schedulers:
            try:
                with session_scope() as session:
                    last_archive_policy_run_at = run_archive_policy_if_due(
                        session,
                        last_run_at=last_archive_policy_run_at,
                    )
            except Exception:  # pragma: no cover - defensive around periodic maintenance
                LOGGER.exception("Automatic archive policy run failed")

            try:
                with session_scope() as session:
                    last_micromanager_ingest_run_at = run_micromanager_ingest_if_due(
                        session,
                        last_run_at=last_micromanager_ingest_run_at,
                        target=target,
                    )
            except Exception:  # pragma: no cover - defensive around periodic maintenance
                LOGGER.exception("Micro-Manager ingest run failed")

            try:
                with session_scope() as session:
                    last_backup_run_at = run_backup_if_due(session, last_run_at=last_backup_run_at)
            except Exception:  # pragma: no cover - defensive around periodic maintenance
                LOGGER.exception("Backup scheduler run failed")

        job = claim_next_job()
        if job is None:
            time.sleep(settings.worker_poll_interval_sec)
            continue

        keepalive_stop: threading.Event | None = None
        keepalive_thread: threading.Thread | None = None
        cpu_monitor: JobCpuMonitor | None = None
        try:
            cpu_monitor = JobCpuMonitor()
            cpu_monitor.start()
            keepalive_stop = threading.Event()
            keepalive_thread = threading.Thread(
                target=keep_running_job_alive,
                args=(keepalive_stop,),
                kwargs={"job_id": job.id, "cpu_monitor": cpu_monitor},
                daemon=True,
                name=f"job-keepalive-{job.id}",
            )
            keepalive_thread.start()
            result_json = execute_job(job)
            keepalive_stop.set()
            keepalive_thread.join(timeout=1.0)
            cpu_usage = finish_job_cpu_monitor(cpu_monitor, job)
            if cpu_usage is not None:
                result_json["cpu_usage"] = cpu_usage
            if result_json.get("requeue"):
                requeue_job(job.id, result_json)
                LOGGER.info("Job %s processed one storage optimization chunk and was requeued", job.id)
            else:
                mark_job_done(job.id, result_json)
                LOGGER.info("Job %s completed", job.id)
        except PipelineRunCancelled as exc:
            if isinstance(keepalive_stop, threading.Event):
                keepalive_stop.set()
            if isinstance(keepalive_thread, threading.Thread):
                keepalive_thread.join(timeout=1.0)
            mark_job_cancelled(job.id, str(exc), cpu_usage=finish_job_cpu_monitor(cpu_monitor, job))
            LOGGER.info("Job %s cancelled", job.id)
        except Exception as exc:  # pragma: no cover - defensive for worker loop
            if isinstance(keepalive_stop, threading.Event):
                keepalive_stop.set()
            if isinstance(keepalive_thread, threading.Thread):
                keepalive_thread.join(timeout=1.0)
            with session_scope() as session:
                job_record = session.get(Job, job.id)
                if job_record is not None:
                    finalize_storage_lifecycle_failure(session, job=job_record, error_text=str(exc))
                    finalize_storage_optimization_failure(session, job=job_record, error_text=str(exc))
                    finalize_backup_failure(session, job=job_record, error_text=str(exc))
                    finalize_user_home_storage_failure(session, job=job_record, error_text=str(exc))
                    finalize_project_deletion_failure(session, job=job_record, error_text=str(exc))
            mark_job_failed(job.id, str(exc), cpu_usage=finish_job_cpu_monitor(cpu_monitor, job))
            LOGGER.exception("Job %s failed", job.id)


def resolve_worker_target(session, configured_target_key: str | None) -> ExecutionTarget | None:
    target_key = str(configured_target_key or "").strip()
    if not target_key:
        return None
    return session.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key == target_key)).first()


def read_positive_int(value) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 1:
        return None
    return parsed


def resolve_target_for_job(session, *, job: Job, configured_target_key: str | None) -> ExecutionTarget | None:
    if job.execution_target_id:
        return session.get(ExecutionTarget, job.execution_target_id)
    return resolve_worker_target(session, configured_target_key)


def update_worker_target_state(
    session,
    *,
    target: ExecutionTarget | None,
    health: str,
    current_job: Job | None,
    last_job_status: str | None,
    last_job: Job | None = None,
    error_text: str | None = None,
    current_job_cpu_cores: float | None = None,
) -> None:
    if target is None:
        return

    now = datetime.now(timezone.utc)
    worker_instance_id = get_worker_instance_id()
    host_cpu_count, available_cpu_count = get_cpu_topology()
    if current_job is None or current_job_cpu_cores is None:
        current_job_cpu_cores = 0.0
    upsert_worker_instance(
        session,
        target=target,
        worker_instance=worker_instance_id,
        health=health,
        current_job=current_job,
        last_job=last_job,
        last_job_status=last_job_status,
        error_text=error_text,
        host_cpu_count=host_cpu_count,
        available_cpu_count=available_cpu_count,
        current_job_cpu_cores=current_job_cpu_cores,
        poll_interval_sec=get_settings().worker_poll_interval_sec,
        now=now,
    )
    metadata = execution_target_worker_metadata(session, target)
    target.metadata_json = metadata
    if health == "error":
        target.status = "degraded"
    elif health == "busy":
        target.status = "online"
    else:
        target.status = "online"


def update_raw_preview_position_state(session, *, job: Job, status: str) -> None:
    position_id = (job.params_json or {}).get("position_id")
    if position_id:
        position = session.get(RawDatasetPosition, position_id)
        if position is None:
            return
        position.preview_status = status
        position.updated_at = datetime.now(timezone.utc)
        return

    # Dataset-level preview jobs can fail before entering the per-position loop
    # (for example when the catalogued source path has been moved). In that
    # case, clear only active placeholders so the UI does not stay stuck on
    # "queued" after the worker job has failed.
    if status not in {"failed", "cancelled"} or job.raw_dataset_id is None:
        return
    positions = session.scalars(
        select(RawDatasetPosition).where(
            RawDatasetPosition.raw_dataset_id == job.raw_dataset_id,
            RawDatasetPosition.preview_status.in_(("queued", "running")),
        )
    ).all()
    now = datetime.now(timezone.utc)
    for position in positions:
        position.preview_status = status
        position.updated_at = now


if __name__ == "__main__":
    run_forever()
