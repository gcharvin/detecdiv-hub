from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from api.models import Job, RawDataset, RawDatasetPosition, User


def queue_raw_preview_job_for_dataset(
    session: Session,
    *,
    raw_dataset: RawDataset,
    requested_by_user: User,
    requested_mode: str = "auto",
    priority: int = 100,
    force: bool = False,
    requested_from_host: str = "raw_dataset_ingest",
    source: str = "raw_dataset_ingest",
) -> Job | None:
    positions = list(
        session.scalars(
            select(RawDatasetPosition).where(RawDatasetPosition.raw_dataset_id == raw_dataset.id)
        )
    )
    if not positions:
        return None

    needs_generation = force or any(
        position.preview_artifact_id is None or str(position.preview_status or "").lower() != "ready"
        for position in positions
    )
    if not needs_generation:
        return None

    existing = session.scalars(
        select(Job)
        .where(Job.raw_dataset_id == raw_dataset.id)
        .order_by(Job.created_at.desc())
    ).all()
    for job in existing:
        params = dict(job.params_json or {})
        if params.get("job_kind") != "raw_preview_video":
            continue
        if job.status in {"queued", "running", "cancelling"} and params.get("scope") == "dataset":
            return job

    job = Job(
        raw_dataset_id=raw_dataset.id,
        requested_mode=requested_mode,
        priority=priority,
        requested_by=requested_by_user.user_key,
        requested_from_host=requested_from_host,
        params_json={
            "job_kind": "raw_preview_video",
            "force": force,
            "position_id": None,
            "position_key": None,
            "scope": "dataset",
            "source": source,
            "queued_at": datetime.now(timezone.utc).isoformat(),
        },
        status="queued",
    )
    session.add(job)
    for position in positions:
        if force or position.preview_artifact_id is None or str(position.preview_status or "").lower() != "ready":
            position.preview_status = "queued"
    session.flush()
    return job
