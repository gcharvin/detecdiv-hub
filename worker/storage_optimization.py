"""Resumable, low-priority TIFF DEFLATE optimization for storage-visible workers."""
from __future__ import annotations

import logging
import os
import subprocess
import struct
from hashlib import sha256
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload
import tifffile

from api.models import (
    Job,
    RawDataset,
    StorageOptimizationFile,
    StorageOptimizationRun,
    StorageOptimizationScanDirectory,
)
from api.services.raw_dataset_lifecycle import pick_preferred_raw_location, resolve_raw_location_path

# Yield to higher-priority work after each TIFF and each directory. This keeps
# long campaigns from occupying every worker for a whole batch at a time.
CHUNK_FILE_LIMIT = 1
SCAN_DIRECTORY_LIMIT = 1
STORAGE_OPTIMIZATION_JOB_KINDS = {
    "storage_optimization",
    "storage_optimization_scan",
    "storage_optimization_chunk",
}
logger = logging.getLogger(__name__)


def execute_storage_optimization_job(session: Session, *, job: Job) -> dict:
    kind = (job.params_json or {}).get("job_kind")
    run_id = UUID(str((job.params_json or {}).get("storage_optimization_run_id")))
    if kind in STORAGE_OPTIMIZATION_JOB_KINDS:
        _promote_to_stable_job(job)
        return _run_stable_optimization(session, job=job, run_id=run_id)
    raise ValueError(f"Unsupported storage optimization job: {kind}")


def finalize_storage_optimization_failure(session: Session, *, job: Job, error_text: str) -> None:
    if (job.params_json or {}).get("job_kind") not in STORAGE_OPTIMIZATION_JOB_KINDS:
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
        return {
            "run_id": str(run.id),
            "status": run.status,
            "scan_complete": bool((run.metadata_json or {}).get("scan_complete")),
        }

    metadata = dict(run.metadata_json or {})
    if metadata.get("scan_complete"):
        return {"run_id": str(run.id), "status": run.status, "scan_complete": True}

    run.status = "scanning"
    raw.storage_optimization_status = "running"
    run.started_at = run.started_at or datetime.now(timezone.utc)

    if not metadata.get("scan_initialized"):
        session.add(StorageOptimizationScanDirectory(run_id=run.id, relative_path=""))
        metadata["scan_initialized"] = True
        metadata["scan_directories_completed"] = 0
        metadata["scan_total_bytes"] = 0
        run.metadata_json = metadata
    session.flush()

    directories = list(
        session.scalars(
            select(StorageOptimizationScanDirectory)
            .where(
                StorageOptimizationScanDirectory.run_id == run.id,
                StorageOptimizationScanDirectory.status == "pending",
            )
            .order_by(StorageOptimizationScanDirectory.relative_path)
            .limit(SCAN_DIRECTORY_LIMIT)
            .with_for_update(skip_locked=True)
        )
    )
    for directory in directories:
        directory.status = "running"
        session.flush()
        relative_directory = Path(directory.relative_path) if directory.relative_path else Path()
        absolute_directory = source / relative_directory
        child_directories, tiff_files, directory_bytes = _scan_directory_entries(
            absolute_directory,
            relative_directory,
        )
        for child in child_directories:
            session.add(StorageOptimizationScanDirectory(run_id=run.id, relative_path=child))
        for relative_path, source_bytes in tiff_files:
            session.add(
                StorageOptimizationFile(
                    run_id=run.id,
                    relative_path=relative_path,
                    file_format="tiff",
                    source_bytes=source_bytes,
                )
            )
            run.total_files += 1
            run.source_bytes += source_bytes
        metadata["scan_total_bytes"] = int(metadata.get("scan_total_bytes", 0)) + directory_bytes
        metadata["scan_directories_completed"] = int(metadata.get("scan_directories_completed", 0)) + 1
        directory.status = "completed"
        directory.completed_at = datetime.now(timezone.utc)
        session.flush()

    remaining = session.scalar(
        select(func.count())
        .select_from(StorageOptimizationScanDirectory)
        .where(
            StorageOptimizationScanDirectory.run_id == run.id,
            StorageOptimizationScanDirectory.status.in_(("pending", "running")),
        )
    )
    scan_complete = int(remaining or 0) == 0
    if scan_complete:
        metadata["scan_complete"] = True
        run.status = "queued"
    else:
        run.status = "scanning"
    run.metadata_json = metadata
    session.flush()
    return {
        "run_id": str(run.id),
        "status": run.status,
        "scan_complete": scan_complete,
        "scan_directories_completed": metadata["scan_directories_completed"],
        "scan_directories_remaining": int(remaining or 0),
        "total_files": run.total_files,
        "source_bytes": run.source_bytes,
    }


def _scan_directory_entries(
    directory: Path,
    relative_directory: Path,
) -> tuple[list[str], list[tuple[str, int]], int]:
    """Inspect one directory and return child directories, TIFFs, and all-file bytes."""
    child_directories: list[str] = []
    tiff_files: list[tuple[str, int]] = []
    total_bytes = 0
    with os.scandir(directory) as entries:
        for entry in entries:
            relative_path = relative_directory / entry.name
            if entry.is_dir(follow_symlinks=False):
                child_directories.append(relative_path.as_posix())
                continue
            if not entry.is_file(follow_symlinks=True):
                continue
            source_bytes = entry.stat(follow_symlinks=True).st_size
            total_bytes += source_bytes
            if Path(entry.name).suffix.lower() in {".tif", ".tiff"} and not entry.is_symlink():
                tiff_files.append((relative_path.as_posix(), source_bytes))
    return child_directories, tiff_files, total_bytes


def _run_stable_optimization(session: Session, *, job: Job, run_id: UUID) -> dict:
    # A dataset may still have duplicate legacy queue rows. Serialize them on
    # the run record so only one worker advances its scan/compression at once.
    run = session.scalar(
        select(StorageOptimizationRun).where(StorageOptimizationRun.id == run_id).with_for_update()
    )
    if run is None:
        raise ValueError(f"Storage optimization run {run_id} is missing")
    if run.status in {"cancelled", "completed", "failed"}:
        return {"run_id": str(run.id), "status": run.status}
    planned_files = session.scalar(
        select(func.count()).select_from(StorageOptimizationFile).where(StorageOptimizationFile.run_id == run.id)
    )
    metadata = dict(run.metadata_json or {})
    # Runs scanned by the original all-at-once implementation already have a
    # complete file plan, even though they have no scan-directory records.
    legacy_plan_complete = bool(planned_files) and not metadata.get("scan_initialized")
    if run.status in {"queued", "scanning"} and not metadata.get("scan_complete") and not legacy_plan_complete:
        scan_result = _scan_run(session, run_id=run_id)
        return {**scan_result, "requeue": True}
    if legacy_plan_complete:
        metadata["scan_complete"] = True
        run.metadata_json = metadata
    result = _run_chunk(session, job=job, run_id=run_id, queue_next=False)
    if result["status"] == "running":
        result["requeue"] = True
    return result


def _run_chunk(session: Session, *, job: Job, run_id: UUID, queue_next: bool = True) -> dict:
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
    _refresh_run(session, run=run, raw=raw, source=source)
    if run.status == "running" and queue_next:
        _queue_chunk(session, run=run, raw=raw, exclude_job_id=job.id)
    return {"run_id": str(run.id), "status": run.status, "completed_files": run.completed_files, "failed_files": run.failed_files, "saved_bytes": run.saved_bytes}


def _promote_to_stable_job(job: Job) -> None:
    params = dict(job.params_json or {})
    params["job_kind"] = "storage_optimization"
    job.params_json = params


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
        _write_deflate_tiff(path, tmp)
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
    try:
        with tifffile.TiffFile(path) as image:
            return all(int(page.tags[259].value) in {8, 32946} for page in image.pages)
    except (OSError, KeyError, ValueError):
        return False


def _write_deflate_tiff(source: Path, target: Path) -> None:
    """Write a DEFLATE TIFF without discarding ImageJ private metadata.

    libtiff's ``tiffcp`` does not copy tags 50838 and 50839.  They contain the
    ImageJ/Micro-Manager metadata used by these acquisitions, so for files
    carrying either tag we use tifffile and explicitly preserve the original
    on-disk tag payloads.  Ordinary TIFF files keep the broadly compatible
    tiffcp path.
    """
    with tifffile.TiffFile(source) as image:
        if not any(page.tags.get(code) is not None for page in image.pages for code in (50838, 50839)):
            subprocess.run(["tiffcp", "-c", "zip:2", str(source), str(target)], check=True, capture_output=True, text=True)
            return
        if len(image.pages) != 1:
            raise RuntimeError("TIFF with protected ImageJ metadata has multiple pages and is not supported yet")
        page = image.pages[0]
        tifffile.imwrite(
            target,
            page.asarray(),
            photometric=page.photometric,
            description=page.description,
            metadata=None,
            compression="adobe_deflate",
            predictor=True,
            byteorder=image.byteorder,
            subfiletype=page.subfiletype,
            extratags=_protected_metadata_extratags(image, page),
        )


def _protected_metadata_extratags(image: tifffile.TiffFile, page: tifffile.TiffPage) -> list[tuple]:
    """Return ImageJ private tags with their payloads untouched.

    ``tifffile`` parses tag 50839 into a dict on input.  Re-serializing that
    dict is not equivalent to copying the original metadata bytes, therefore
    read those bytes directly from the TIFF stream instead.
    """
    tags: list[tuple] = []
    byte_counts = page.tags.get(50838)
    if byte_counts is not None:
        if byte_counts.dtype != 4:
            raise RuntimeError("Unsupported ImageJ metadata byte-count tag type")
        raw_counts = _tag_value_bytes(image, byte_counts)
        expected_size = byte_counts.count * 4
        if len(raw_counts) != expected_size:
            raise RuntimeError("ImageJ metadata byte-count tag is truncated")
        values = struct.unpack(f"{image.byteorder}{byte_counts.count}I", raw_counts)
        tags.append((50838, "I", byte_counts.count, values, False))
    metadata = page.tags.get(50839)
    if metadata is not None:
        if metadata.dtype != 1:
            raise RuntimeError("Unsupported ImageJ metadata tag type")
        tags.append((50839, "B", metadata.count, _tag_value_bytes(image, metadata), False))
    return tags


def _tag_value_bytes(image: tifffile.TiffFile, tag: tifffile.TiffTag) -> bytes:
    """Return the exact serialized payload of a non-inline TIFF tag."""
    if tag.valuebytecount <= 4:
        raise RuntimeError(f"Protected TIFF tag {tag.code} is unexpectedly inline")
    handle = image.filehandle
    position = handle.tell()
    try:
        handle.seek(tag.valueoffset)
        value = handle.read(tag.valuebytecount)
    finally:
        handle.seek(position)
    if len(value) != tag.valuebytecount:
        raise RuntimeError(f"Protected TIFF tag {tag.code} is truncated")
    return value


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
                if code in (50838, 50839):
                    digest.update(f"{code}:{tag.dtype}:{tag.count}:".encode("ascii"))
                    digest.update(_tag_value_bytes(image, tag))
                else:
                    # TIFF ASCII fields include a terminating NUL in their tag
                    # count. tifffile normalizes that terminator while retaining
                    # the exact description text, so the count is not semantic.
                    digest.update(f"{code}:".encode("ascii"))
                    value = tag.value
                    if isinstance(value, bytes):
                        digest.update(value)
                    elif isinstance(value, str):
                        digest.update(value.encode("utf-8"))
                    else:
                        digest.update(repr(value).encode("utf-8"))
                digest.update(b";")
    return digest.hexdigest()


def _refresh_run(session: Session, *, run: StorageOptimizationRun, raw: RawDataset, source: Path) -> None:
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
    metadata = dict(run.metadata_json or {})
    scan_total_bytes = metadata.get("scan_total_bytes")
    try:
        if metadata.get("scan_complete") and scan_total_bytes is not None:
            # The completed inventory is our size snapshot; subtract only the
            # verified per-file savings instead of walking the huge tree again.
            raw.total_bytes = max(0, int(scan_total_bytes) - run.saved_bytes)
            method = "scan_inventory_minus_verified_savings"
        else:
            # Compatibility fallback for runs planned by older worker versions.
            raw.total_bytes = _measure_directory_size(source)
            method = "full_directory_walk"
        raw.last_size_scan_at = run.finished_at
        metadata["size_recalculation"] = {
            "status": "completed",
            "total_bytes": raw.total_bytes,
            "measured_at": run.finished_at.isoformat(),
            "method": method,
        }
    except OSError as exc:
        metadata["size_recalculation"] = {
            "status": "failed",
            "error": str(exc),
            "attempted_at": run.finished_at.isoformat(),
        }
        logger.exception("Could not refresh raw dataset size after TIFF optimization run %s", run.id)
    run.metadata_json = metadata


def _measure_directory_size(path: Path) -> int:
    """Measure the full dataset tree, failing instead of publishing a partial sum."""
    total = 0

    def raise_walk_error(error: OSError) -> None:
        raise error

    for root, _directories, files in os.walk(path, onerror=raise_walk_error):
        for filename in files:
            total += (Path(root) / filename).stat().st_size
    return total


def _queue_chunk(session: Session, *, run: StorageOptimizationRun, raw: RawDataset, exclude_job_id: UUID | None = None) -> None:
    stmt = select(func.count()).select_from(Job).where(Job.params_json.contains({"job_kind": "storage_optimization_chunk", "storage_optimization_run_id": str(run.id)}), Job.status.in_(("queued", "running")))
    if exclude_job_id is not None:
        stmt = stmt.where(Job.id != exclude_job_id)
    exists = session.scalar(stmt)
    if exists:
        return
    session.add(Job(raw_dataset_id=raw.id, requested_mode="server", priority=200, requested_by=run.requested_by, requested_from_host="storage_optimization", params_json={"job_kind": "storage_optimization_chunk", "storage_optimization_run_id": str(run.id)}, status="queued"))
