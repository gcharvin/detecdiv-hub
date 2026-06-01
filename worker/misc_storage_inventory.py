from __future__ import annotations

import socket

from sqlalchemy.orm import Session

from api.models import Job
from api.services.misc_storage_inventory import DEFAULT_MIN_SIZE_BYTES, inventory_misc_storage


def execute_misc_storage_inventory_job(session: Session, *, job: Job) -> dict:
    params = dict(job.params_json or {})
    if params.get("job_kind") != "misc_storage_inventory":
        raise ValueError(f"Unsupported misc storage inventory job kind: {params.get('job_kind')}")

    source_path = str(params.get("source_path") or "").strip()
    if not source_path:
        raise ValueError("misc_storage_inventory job is missing source_path")

    result = inventory_misc_storage(
        session,
        source_path=source_path,
        storage_root_id=params.get("storage_root_id"),
        parent_item_id=params.get("parent_item_id"),
        storage_root_name=params.get("storage_root_name"),
        host_scope=str(params.get("host_scope") or "server"),
        root_type=str(params.get("root_type") or "misc_root"),
        owner_user_key=params.get("owner_user_key"),
        visibility=str(params.get("visibility") or "private"),
        min_size_bytes=int(params.get("min_size_bytes") or DEFAULT_MIN_SIZE_BYTES),
        max_depth=int(params.get("max_depth") or 2),
        du_timeout_sec=float(params.get("du_timeout_sec") or 45.0),
        include_cataloged=bool(params.get("include_cataloged", False)),
        metadata_json=params.get("metadata_json") if isinstance(params.get("metadata_json"), dict) else {},
    )
    return {
        "job_kind": "misc_storage_inventory",
        "source_path": result.source_path,
        "storage_root_name": result.storage_root_name,
        "scanned_count": result.scanned_count,
        "indexed_count": result.indexed_count,
        "skipped_count": result.skipped_count,
        "timeout_count": result.timeout_count,
        "worker_host": socket.gethostname(),
    }
