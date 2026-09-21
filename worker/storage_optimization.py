"""Resumable, low-priority TIFF DEFLATE optimization for storage-visible workers."""
from __future__ import annotations

import os
import subprocess
from hashlib import sha256
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload
import tifffile

from api.models import Job, RawDataset, StorageOptimizationFile, StorageOptimizationRun
from api.services.raw_dataset_lifecycle import pick_preferred_raw_location, resolve_raw_location_path

CHUNK_FILE_LIMIT = 25


def execute_storage_optimization_job(session: Session, *, job: Job) -> dict:
    kind = (job.params_json or {}).get("job_kind")
    run_id = UUID(str((job.params_json or {}).get("storage_optimization_run_id")))
    if kind == "storage_optimization_scan":
        return _scan_run(session, run_id=run_id)
    if kind == "storage_optimization_chunk":
        return _run_chunk(session, job=job, run_id=run_id)
    raise ValueError(f"Unsupported storage optimization job: {kind}")


def finalize_storage_optimization_failure(session: Session, *, job: Job, error_text: str) -> None:
    if (job.params_json or {}).get("job_kind") not in {"storage_optimization_scan", "storage_optimization_chunk"}:
        return
    try:
        run_id = UUID(str((job.params_json or {}).get("storage_optimization_run_id")))
    except (TypeError, ValueError):
        return
    run = session.get(StorageOptimizationRun, run_id)
    if run is None:
        return
    run.status, run.finished_at = "failed", datetime.now(timezone.utc)
    metadata = dict(run.metadata_json or {})
    metadata["last_error"] = error_text
    run.metadata_json = metadata
    if run.raw_dataset_id:
        raw = session.get(RawDataset, run.raw_dataset_id)
        if raw is not None:
            raw.storage_optimization_status = "failed"


def _load_run(session: Session, run_id: UUID) -> tuple[StorageOptimizationRun, RawDataset, Path]:
    run = session.get(StorageOptimizationRun, run_id)
    if run is None or run.raw_dataset_id is None:
        raise ValueError(f"Storage optimization run {run_id} is missing its raw dataset")
    raw = session.scalars(select(RawDataset).options(joinedload(RawDataset.locations)).where(RawDataset.id == run.raw_dataset_id)).unique().one()
    source = resolve_raw_location_path(pick_preferred_raw_location(raw))
    if not source.is_dir():
        raise FileNotFoundError(f"Raw dataset directory is unavailable: {source}")
    return run, raw, source


def _scan_run(session: Session, *, run_id: UUID) -> dict:
    run, raw, source = _load_run(session, run_id)
    if run.status not in {"queued", "scanning"}:
        return {"run_id": str(run.id), "status": run.status, "message": "Run already planned."}
    run.status = "scanning"
    raw.storage_optimization_status = "running"
    run.started_at = run.started_at or datetime.now(timezone.utc)
    session.flush()
    count = 0
    total = 0
    for candidate in source.rglob("*"):
        if not candidate.is_file() or candidate.suffix.lower() not in {".tif", ".tiff"}:
            continue
        relative_path = str(candidate.relative_to(source))
        source_bytes = candidate.stat().st_size
        session.add(StorageOptimizationFile(run_id=run.id, relative_path=relative_path, file_format="tiff", source_bytes=source_bytes))
        count += 1
        total += source_bytes
        if count % 500 == 0:
            session.flush()
    run.total_files = count
    run.source_bytes = total
    run.status = "queued"
    _queue_chunk(session, run=run, raw=raw)
    return {"run_id": str(run.id), "status": run.status, "total_files": count, "source_bytes": total}


def _run_chunk(session: Session, *, job: Job, run_id: UUID) -> dict:
    run, raw, source = _load_run(session, run_id)
    if run.status in {"cancelled", "completed", "failed"}:
        return {"run_id": str(run.id), "status": run.status}
    run.status = "running"
    files = list(session.scalars(select(StorageOptimizationFile).where(StorageOptimizationFile.run_id == run.id, StorageOptimizationFile.status == "pending").order_by(StorageOptimizationFile.relative_path).limit(CHUNK_FILE_LIMIT).with_for_update(skip_locked=True)))
    for item in files:
        item.status = "running"
        item.job_id = job.id
        item.attempts += 1
    session.flush()
    for item in files:
        _compress_one(item, source / item.relative_path)
    _refresh_run(session, run=run, raw=raw)
    if run.status == "running":
        _queue_chunk(session, run=run, raw=raw, exclude_job_id=job.id)
    return {"run_id": str(run.id), "status": run.status, "completed_files": run.completed_files, "failed_files": run.failed_files, "saved_bytes": run.saved_bytes}


def _compress_one(item: StorageOptimizationFile, path: Path) -> None:
    now = datetime.now(timezone.utc)
    if not path.is_file():
        item.status, item.error_text = "failed", "Source TIFF disappeared"
        return
    # A worker may have restarted after the atomic replacement but before its DB
    # commit. Detect the target encoding to avoid needlessly rewriting it again.
    if _is_deflate_tiff(path):
        item.status, item.completed_at = "skipped", now
        item.output_bytes = path.stat().st_size
        item.saved_bytes = max(0, item.source_bytes - item.output_bytes)
        item.verification_json = {"reason": "already_deflate"}
        return
    tmp = path.with_name(f".{path.name}.detecdiv-compress-{item.id}.tmp")
    backup = path.with_name(f".{path.name}.detecdiv-original-{item.id}.bak")
    try:
        os.link(path, backup)  # same-volume recovery point, no extra data copy
        subprocess.run(["tiffcp", "-c", "zip:2", str(path), str(tmp)], check=True, capture_output=True, text=True)
        if not _is_deflate_tiff(tmp):
            raise RuntimeError("Temporary TIFF does not report AdobeDeflate")
        _assert_same_pixels(path, tmp)
        protected_metadata = _protected_metadata_digest(path)
        if protected_metadata != _protected_metadata_digest(tmp):
            raise RuntimeError("Temporary TIFF changed protected ImageJ/Micro-Manager metadata")
        output_bytes = tmp.stat().st_size
        if output_bytes >= item.source_bytes:
            item.status, item.output_bytes, item.saved_bytes, item.completed_at = "skipped", output_bytes, 0, now
            item.verification_json = {"reason": "no_size_gain"}
            tmp.unlink(missing_ok=True)
            return
        os.replace(tmp, path)
        item.status, item.output_bytes = "completed", output_bytes
        item.saved_bytes, item.completed_at = item.source_bytes - output_bytes, now
        item.verification_json = {"codec": "deflate", "predictor": 2, "atomic_replace": True, "protected_metadata_sha256": protected_metadata}
    except Exception as exc:
        item.status, item.error_text = "failed", str(exc)
    finally:
        tmp.unlink(missing_ok=True)
        backup.unlink(missing_ok=True)


def _is_deflate_tiff(path: Path) -> bool:
    result = subprocess.run(["tiffinfo", str(path)], capture_output=True, text=True)
    return "Compression Scheme: AdobeDeflate" in result.stdout


def _assert_same_pixels(source: Path, target: Path) -> None:
    """tiffcmp compares decoded samples; the Compression tag difference is expected."""
    result = subprocess.run(["tiffcmp", str(source), str(target)], capture_output=True, text=True)
    output = "\n".join(part for part in (result.stdout, result.stderr) if part).splitlines()
    expected = (
        "Compression:",
        "TIFFReadDirectory: Warning, Unknown field with tag 50838",
        "TIFFReadDirectory: Warning, Unknown field with tag 50839",
    )
    unexpected = [line for line in output if line.strip() and not line.lstrip().startswith(expected)]
    if result.returncode or unexpected:
        raise RuntimeError("TIFF pixel verification failed: " + " | ".join(unexpected[:3]))


def _protected_metadata_digest(path: Path) -> str:
    """Digest metadata that makes these ImageJ/Micro-Manager frames interpretable.

    TIFF layout and compression tags are expected to change.  ImageDescription
    and the two Micro-Manager metadata tags (50838/50839) must be byte-for-byte
    identical on every page before the atomic replacement is allowed.
    """
    digest = sha256()
    with tifffile.TiffFile(path) as image:
        for page_index, page in enumerate(image.pages):
            digest.update(f"page:{page_index};".encode("ascii"))
            for code in (270, 50838, 50839):
                tag = page.tags.get(code)
                if tag is None:
                    digest.update(f"{code}:missing;".encode("ascii"))
                    continue
                digest.update(f"{code}:{tag.dtype}:{tag.count}:".encode("ascii"))
                value = tag.value
                if isinstance(value, bytes):
                    digest.update(value)
                elif isinstance(value, str):
                    digest.update(value.encode("utf-8"))
                else:
                    digest.update(repr(value).encode("utf-8"))
                digest.update(b";")
    return digest.hexdigest()


def _refresh_run(session: Session, *, run: StorageOptimizationRun, raw: RawDataset) -> None:
    rows = session.execute(select(StorageOptimizationFile.status, func.count(), func.coalesce(func.sum(StorageOptimizationFile.output_bytes), 0), func.coalesce(func.sum(StorageOptimizationFile.saved_bytes), 0)).where(StorageOptimizationFile.run_id == run.id).group_by(StorageOptimizationFile.status)).all()
    totals = {status: (int(count), int(output), int(saved)) for status, count, output, saved in rows}
    run.completed_files = sum(totals.get(status, (0, 0, 0))[0] for status in ("completed", "skipped"))
    run.failed_files = totals.get("failed", (0, 0, 0))[0]
    run.output_bytes = sum(value[1] for value in totals.values())
    run.saved_bytes = sum(value[2] for value in totals.values())
    pending = totals.get("pending", (0, 0, 0))[0] + totals.get("running", (0, 0, 0))[0]
    if pending:
        return
    run.finished_at = datetime.now(timezone.utc)
    run.status = "completed" if run.failed_files == 0 else "partial"
    raw.storage_optimization_status = run.status
    raw.storage_optimization_saved_bytes = run.saved_bytes
    raw.storage_optimized_at = run.finished_at


def _queue_chunk(session: Session, *, run: StorageOptimizationRun, raw: RawDataset, exclude_job_id: UUID | None = None) -> None:
    stmt = select(func.count()).select_from(Job).where(Job.params_json.contains({"job_kind": "storage_optimization_chunk", "storage_optimization_run_id": str(run.id)}), Job.status.in_(("queued", "running")))
    if exclude_job_id is not None:
        stmt = stmt.where(Job.id != exclude_job_id)
    exists = session.scalar(stmt)
    if exists:
        return
    session.add(Job(raw_dataset_id=raw.id, requested_mode="server", priority=20, requested_by=run.requested_by, requested_from_host="storage_optimization", params_json={"job_kind": "storage_optimization_chunk", "storage_optimization_run_id": str(run.id)}, status="queued"))
