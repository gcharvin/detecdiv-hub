from __future__ import annotations

import math
import re
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import tifffile
import zarr
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from api.models import Artifact, Job, Project, ProjectRawLink, RawDataset, RawDatasetLocation, RawDatasetPosition
from api.services.path_resolution import compose_storage_path
from api.services.project_indexing import slugify
from api.services.raw_preview_settings import RawPreviewRuntimeConfig, resolve_raw_preview_runtime_config
from worker.preview_text import fit_text_scale


TIFF_SUFFIXES = (".tif", ".tiff")
TIFF_LIKE_FORMATS = {"single_tiff", "ome_tiff", "tiff_sequence", "micromanager_tiff_dir", "ndtiff"}
ZARR_LIKE_FORMATS = {"zarr", "ome_zarr"}
TIME_TOKEN_PATTERNS = (
    re.compile(r"(?:^|[_\-.])(time|frame|tp|t)[_\- ]*(\d+)(?=[_\-.]|$)", flags=re.IGNORECASE),
    re.compile(r"(?:^|[_\-.])img[_\- ]*(\d+)(?=[_\-.]|$)", flags=re.IGNORECASE),
)
BITMAP_FONT = {
    "A": ["01110", "10001", "10001", "11111", "10001", "10001", "10001"],
    "B": ["11110", "10001", "10001", "11110", "10001", "10001", "11110"],
    "C": ["01111", "10000", "10000", "10000", "10000", "10000", "01111"],
    "D": ["11110", "10001", "10001", "10001", "10001", "10001", "11110"],
    "E": ["11111", "10000", "10000", "11110", "10000", "10000", "11111"],
    "F": ["11111", "10000", "10000", "11110", "10000", "10000", "10000"],
    "G": ["01111", "10000", "10000", "10011", "10001", "10001", "01111"],
    "H": ["10001", "10001", "10001", "11111", "10001", "10001", "10001"],
    "I": ["11111", "00100", "00100", "00100", "00100", "00100", "11111"],
    "J": ["00111", "00010", "00010", "00010", "10010", "10010", "01100"],
    "K": ["10001", "10010", "10100", "11000", "10100", "10010", "10001"],
    "L": ["10000", "10000", "10000", "10000", "10000", "10000", "11111"],
    "M": ["10001", "11011", "10101", "10101", "10001", "10001", "10001"],
    "N": ["10001", "11001", "10101", "10011", "10001", "10001", "10001"],
    "O": ["01110", "10001", "10001", "10001", "10001", "10001", "01110"],
    "P": ["11110", "10001", "10001", "11110", "10000", "10000", "10000"],
    "Q": ["01110", "10001", "10001", "10001", "10101", "10010", "01101"],
    "R": ["11110", "10001", "10001", "11110", "10100", "10010", "10001"],
    "S": ["01111", "10000", "10000", "01110", "00001", "00001", "11110"],
    "T": ["11111", "00100", "00100", "00100", "00100", "00100", "00100"],
    "U": ["10001", "10001", "10001", "10001", "10001", "10001", "01110"],
    "V": ["10001", "10001", "10001", "10001", "10001", "01010", "00100"],
    "W": ["10001", "10001", "10001", "10101", "10101", "10101", "01010"],
    "X": ["10001", "10001", "01010", "00100", "01010", "10001", "10001"],
    "Y": ["10001", "10001", "01010", "00100", "00100", "00100", "00100"],
    "Z": ["11111", "00001", "00010", "00100", "01000", "10000", "11111"],
    "0": ["01110", "10001", "10011", "10101", "11001", "10001", "01110"],
    "1": ["00100", "01100", "00100", "00100", "00100", "00100", "01110"],
    "2": ["01110", "10001", "00001", "00010", "00100", "01000", "11111"],
    "3": ["11110", "00001", "00001", "01110", "00001", "00001", "11110"],
    "4": ["00010", "00110", "01010", "10010", "11111", "00010", "00010"],
    "5": ["11111", "10000", "10000", "11110", "00001", "00001", "11110"],
    "6": ["01110", "10000", "10000", "11110", "10001", "10001", "01110"],
    "7": ["11111", "00001", "00010", "00100", "01000", "01000", "01000"],
    "8": ["01110", "10001", "10001", "01110", "10001", "10001", "01110"],
    "9": ["01110", "10001", "10001", "01111", "00001", "00001", "01110"],
    "_": ["00000", "00000", "00000", "00000", "00000", "00000", "11111"],
    "-": ["00000", "00000", "00000", "11111", "00000", "00000", "00000"],
    ":": ["00000", "00100", "00100", "00000", "00100", "00100", "00000"],
    " ": ["00000", "00000", "00000", "00000", "00000", "00000", "00000"],
}
CHANNEL_TOKEN_PATTERNS = (
    re.compile(r"(?:^|[_\-.])(channel|ch)[_\- ]*([a-z0-9]+)(?=[_\-.]|$)", flags=re.IGNORECASE),
    re.compile(r"(?:^|[_\-.])w(\d+)([a-z0-9]*)", flags=re.IGNORECASE),
)


@dataclass(frozen=True)
class PreviewSequence:
    frames: list[np.ndarray]
    channel_labels: list[str]


@dataclass(frozen=True)
class TiffFrameRecord:
    path: Path
    order_index: int
    channel_key: str
    channel_label: str
    time_index: int | None


def execute_raw_preview_video_job(session: Session, *, job: Job) -> dict[str, Any]:
    raw_dataset = session.scalars(
        select(RawDataset)
        .options(
            joinedload(RawDataset.locations).joinedload(RawDatasetLocation.storage_root),
            joinedload(RawDataset.positions).joinedload(RawDatasetPosition.preview_artifact),
        )
        .where(RawDataset.id == job.raw_dataset_id)
    ).unique().first()
    if raw_dataset is None:
        raise ValueError(f"Raw dataset {job.raw_dataset_id} not found for preview job {job.id}")

    dataset_path = resolve_dataset_path(raw_dataset)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Raw dataset path does not exist: {dataset_path}")

    positions = resolve_preview_positions(session, raw_dataset=raw_dataset, job=job)
    if not positions:
        raise ValueError(f"No preview positions available for raw dataset {raw_dataset.id}")

    runtime_config = resolve_raw_preview_runtime_config(session)
    output_dir = resolve_preview_output_dir(
        raw_dataset=raw_dataset,
        dataset_path=dataset_path,
        runtime_config=runtime_config,
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    generated: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    project_label = resolve_preview_project_label(session, job=job, raw_dataset=raw_dataset)
    update_raw_preview_job_progress(
        session,
        job=job,
        raw_dataset=raw_dataset,
        generated=generated,
        skipped=skipped,
        failed=failed,
        current_position_key=None,
        stage="starting",
    )
    for position in positions:
        if position.preview_artifact_id and not should_force_generation(job):
            if not runtime_config.include_existing:
                position.preview_status = "ready"
                position.updated_at = datetime.now(timezone.utc)
                session.flush()
                session.commit()
                skipped.append({"position_id": str(position.id), "position_key": position.position_key, "reason": "existing"})
                update_raw_preview_job_progress(
                    session,
                    job=job,
                    raw_dataset=raw_dataset,
                    generated=generated,
                    skipped=skipped,
                    failed=failed,
                    current_position_key=position.position_key,
                    stage="skipped",
                )
                continue

        try:
            position.preview_status = "running"
            position.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.commit()
            update_raw_preview_job_progress(
                session,
                job=job,
                raw_dataset=raw_dataset,
                generated=generated,
                skipped=skipped,
                failed=failed,
                current_position_key=position.position_key,
                stage="running",
            )
            sequence = read_preview_frames(
                dataset_path=dataset_path,
                raw_dataset=raw_dataset,
                position=position,
                runtime_config=runtime_config,
            )
            if not sequence.frames:
                raise ValueError(
                    f"No preview frames extracted for raw dataset {raw_dataset.acquisition_label} position {position.position_key}"
                )

            video_path = output_dir / f"{slugify(position.position_key) or 'position'}.mp4"
            encoded_width, encoded_height = encode_preview_video(
                video_path=video_path,
                frames=sequence.frames,
                project_label=project_label,
                position_label=str(position.display_name or position.position_key or ""),
                channel_labels=sequence.channel_labels,
                runtime_config=runtime_config,
            )
            artifact = upsert_preview_artifact(
                session,
                job=job,
                raw_dataset=raw_dataset,
                position=position,
                video_path=video_path,
                frames=sequence.frames,
                encoded_width=encoded_width,
                encoded_height=encoded_height,
                channel_labels=sequence.channel_labels,
                project_label=project_label,
                runtime_config=runtime_config,
            )
            position.preview_artifact_id = artifact.id
            position.preview_status = "ready"
            position.updated_at = datetime.now(timezone.utc)
            generated.append(
                {
                    "position_id": str(position.id),
                    "position_key": position.position_key,
                    "artifact_id": str(artifact.id),
                    "path": str(video_path),
                    "frame_count": len(sequence.frames),
                }
            )
            update_raw_preview_job_progress(
                session,
                job=job,
                raw_dataset=raw_dataset,
                generated=generated,
                skipped=skipped,
                failed=failed,
                current_position_key=position.position_key,
                stage="generated",
            )
        except Exception as exc:
            position.preview_status = "failed"
            position.updated_at = datetime.now(timezone.utc)
            failed.append(
                {
                    "position_id": str(position.id),
                    "position_key": position.position_key,
                    "error": str(exc),
                }
            )
            update_raw_preview_job_progress(
                session,
                job=job,
                raw_dataset=raw_dataset,
                generated=generated,
                skipped=skipped,
                failed=failed,
                current_position_key=position.position_key,
                stage="failed",
            )
        session.flush()
        session.commit()

    preview_status = "ready"
    if failed and generated:
        preview_status = "partial"
    elif failed and not generated:
        preview_status = "failed"
    elif skipped and not generated:
        preview_status = "skipped"
    return {
        "job_kind": "raw_preview_video",
        "raw_dataset_id": str(raw_dataset.id),
        "dataset_path": str(dataset_path),
        "generated_count": len(generated),
        "skipped_count": len(skipped),
        "failed_count": len(failed),
        "generated": generated,
        "skipped": skipped,
        "failed": failed,
        "preview_status": preview_status,
        "scope": str((job.params_json or {}).get("scope") or "dataset"),
    }


def update_raw_preview_job_progress(
    session: Session,
    *,
    job: Job,
    raw_dataset: RawDataset,
    generated: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    failed: list[dict[str, Any]],
    current_position_key: str | None,
    stage: str,
) -> None:
    job_record = session.get(Job, job.id)
    if job_record is None:
        return
    now = datetime.now(timezone.utc)
    result_json = dict(job_record.result_json or {})
    result_json["progress"] = {
        "job_kind": "raw_preview_video",
        "dataset_label": raw_dataset.acquisition_label,
        "current_position_key": current_position_key,
        "stage": stage,
        "generated_count": len(generated),
        "skipped_count": len(skipped),
        "failed_count": len(failed),
        "total_positions": len(generated) + len(skipped) + len(failed) + (1 if current_position_key else 0),
    }
    job_record.result_json = result_json
    job_record.heartbeat_at = now
    job_record.updated_at = now
    session.flush()
    session.commit()


def should_force_generation(job: Job) -> bool:
    return bool((job.params_json or {}).get("force"))


def resolve_dataset_path(raw_dataset: RawDataset) -> Path:
    preferred = next((location for location in raw_dataset.locations or [] if location.is_preferred), None)
    location = preferred or next(iter(raw_dataset.locations or []), None)
    if location is None or location.storage_root is None:
        metadata_path = str((raw_dataset.metadata_json or {}).get("dataset_dir_abs") or "").strip()
        if metadata_path:
            return Path(metadata_path)
        raise ValueError(f"Raw dataset {raw_dataset.id} has no storage location")
    return Path(compose_storage_path(location.storage_root.path_prefix, location.relative_path))


def resolve_preview_positions(session: Session, *, raw_dataset: RawDataset, job: Job) -> list[RawDatasetPosition]:
    position_id = str((job.params_json or {}).get("position_id") or "").strip()
    position_key = str((job.params_json or {}).get("position_key") or "").strip()
    ordered_positions = sorted(
        raw_dataset.positions or [],
        key=lambda value: (
            value.position_index if value.position_index is not None else 1_000_000_000,
            value.position_key,
        ),
    )
    if position_id:
        selected = next((position for position in ordered_positions if str(position.id) == position_id), None)
        if selected is None:
            raise ValueError(f"Requested raw preview position {position_id} not found in dataset {raw_dataset.id}")
        return [selected]
    if position_key:
        selected = next((position for position in ordered_positions if position.position_key == position_key), None)
        if selected is None:
            raise ValueError(f"Requested raw preview position {position_key} not found in dataset {raw_dataset.id}")
        return [selected]
    if ordered_positions:
        return ordered_positions

    fallback = RawDatasetPosition(
        raw_dataset_id=raw_dataset.id,
        position_key="dataset",
        display_name="Dataset",
        position_index=0,
        status="indexed",
        preview_status="missing",
        metadata_json={"source": "raw_preview_fallback"},
    )
    session.add(fallback)
    session.flush()
    return [fallback]


def resolve_preview_output_dir(
    *,
    raw_dataset: RawDataset,
    dataset_path: Path,
    runtime_config: RawPreviewRuntimeConfig,
) -> Path:
    configured_root = str(runtime_config.artifact_root or "").strip()
    if configured_root:
        base_root = Path(configured_root)
    else:
        base_root = dataset_path / ".detecdiv-previews"
    dataset_leaf = slugify(raw_dataset.external_key or raw_dataset.acquisition_label) or str(raw_dataset.id)
    return base_root / dataset_leaf


def resolve_preview_project_label(session: Session, *, job: Job, raw_dataset: RawDataset) -> str:
    if job.project_id is None:
        project = session.scalars(
            select(Project)
            .join(ProjectRawLink, ProjectRawLink.project_id == Project.id)
            .where(ProjectRawLink.raw_dataset_id == raw_dataset.id)
            .order_by(ProjectRawLink.created_at.asc())
        ).first()
        if project is None:
            return ""
        return str(project.project_name or "").strip()
    project = session.get(Project, job.project_id)
    if project is None:
        return ""
    return str(project.project_name or "").strip()


def read_preview_frames(
    *,
    dataset_path: Path,
    raw_dataset: RawDataset,
    position: RawDatasetPosition,
    runtime_config: RawPreviewRuntimeConfig,
) -> PreviewSequence:
    data_format = str(raw_dataset.data_format or "unknown").lower()
    if data_format in ZARR_LIKE_FORMATS:
        return read_zarr_preview_frames(dataset_path=dataset_path, position=position, runtime_config=runtime_config)
    return read_tiff_preview_frames(
        dataset_path=dataset_path,
        raw_dataset=raw_dataset,
        position=position,
        runtime_config=runtime_config,
    )


def read_tiff_preview_frames(
    *,
    dataset_path: Path,
    raw_dataset: RawDataset,
    position: RawDatasetPosition,
    runtime_config: RawPreviewRuntimeConfig,
) -> PreviewSequence:
    candidate_path = resolve_position_source_path(dataset_path=dataset_path, position=position)
    tiff_paths = collect_tiff_paths(candidate_path)
    if not tiff_paths and candidate_path != dataset_path:
        tiff_paths = collect_tiff_paths(dataset_path)
    if not tiff_paths:
        raise ValueError(f"No TIFF files found for dataset {dataset_path}")

    records = build_tiff_frame_records(tiff_paths)
    channel_labels = ordered_unique([record.channel_label for record in records])
    if len(channel_labels) > 1:
        return build_multichannel_tiff_sequence(records, runtime_config=runtime_config)

    frame_paths = sample_evenly(
        tiff_paths,
        max_count=resolve_frame_limit(total_count=len(tiff_paths), runtime_config=runtime_config),
    )
    if len(frame_paths) == 1:
        array = np.asarray(tifffile.imread(frame_paths[0]))
        if array.ndim > 2:
            return sample_frames_from_ndarray(array, runtime_config=runtime_config)

    frames: list[np.ndarray] = []
    for path in frame_paths:
        array = tifffile.imread(path)
        frames.append(normalize_frame(reduce_array_to_frame(np.asarray(array))))
    return PreviewSequence(frames=frames, channel_labels=channel_labels)


def read_zarr_preview_frames(
    *,
    dataset_path: Path,
    position: RawDatasetPosition,
    runtime_config: RawPreviewRuntimeConfig,
) -> PreviewSequence:
    candidate_path = resolve_position_source_path(dataset_path=dataset_path, position=position)
    attempted_targets = list(iter_zarr_preview_targets(candidate_path, dataset_path))
    last_error: Exception | None = None

    for target_path in attempted_targets:
        try:
            node = open_best_zarr_node(target_path)
            array = select_best_zarr_array(node)
        except Exception as exc:
            last_error = exc
            continue
        if array is not None:
            return sample_frames_from_ndarray(array, runtime_config=runtime_config)

    if last_error is None:
        raise ValueError(f"No readable Zarr array found under {dataset_path}")
    raise ValueError(f"No readable Zarr array found under {dataset_path}: {last_error}") from last_error


def resolve_position_source_path(*, dataset_path: Path, position: RawDatasetPosition) -> Path:
    relative_path = str((position.metadata_json or {}).get("relative_path") or "").strip()
    if relative_path:
        candidate = dataset_path / relative_path
        if candidate.exists():
            return candidate
    display_candidate = dataset_path / position.display_name
    if display_candidate.exists():
        return display_candidate
    return dataset_path


def iter_zarr_preview_targets(candidate_path: Path, dataset_path: Path) -> list[Path]:
    targets: list[Path] = []
    seen: set[Path] = set()

    def add_target(path: Path | None) -> None:
        if path is None:
            return
        if not path.exists():
            return
        resolved = path.resolve(strict=False)
        if resolved in seen:
            return
        seen.add(resolved)
        targets.append(path)

    def add_group_and_first_array(path: Path) -> None:
        add_target(path)
        add_target(find_first_zarr_array_dir(path))

    add_group_and_first_array(candidate_path)
    if candidate_path != dataset_path:
        add_group_and_first_array(dataset_path)

    return targets


def collect_tiff_paths(path: Path) -> list[Path]:
    if path.is_file() and path.suffix.lower() in TIFF_SUFFIXES:
        return [path]
    if not path.is_dir():
        return []
    files = [entry for entry in sorted(path.iterdir(), key=lambda value: value.name.lower()) if entry.is_file()]
    return [entry for entry in files if entry.suffix.lower() in TIFF_SUFFIXES]


def build_tiff_frame_records(paths: list[Path]) -> list[TiffFrameRecord]:
    records: list[TiffFrameRecord] = []
    for order_index, path in enumerate(paths):
        channel_label = infer_channel_label(path)
        records.append(
            TiffFrameRecord(
                path=path,
                order_index=order_index,
                channel_key=slugify(channel_label) or f"channel_{order_index}",
                channel_label=channel_label,
                time_index=infer_time_index(path),
            )
        )
    return records


def build_multichannel_tiff_sequence(
    records: list[TiffFrameRecord],
    *,
    runtime_config: RawPreviewRuntimeConfig,
) -> PreviewSequence:
    channel_keys = ordered_unique([record.channel_key for record in records])
    records_by_channel = {
        channel_key: [record for record in records if record.channel_key == channel_key]
        for channel_key in channel_keys
    }
    labels_by_channel = {record.channel_key: record.channel_label for record in records}
    cache: dict[Path, np.ndarray] = {}
    explicit_times = [record.time_index for record in records if record.time_index is not None]
    max_frames = resolve_frame_limit(
        total_count=max(
            len(sorted(set(explicit_times))) if explicit_times else 0,
            max(len(channel_records) for channel_records in records_by_channel.values()),
        ),
        runtime_config=runtime_config,
    )

    composed_frames: list[np.ndarray] = []
    if explicit_times:
        timeline = sorted(set(explicit_times))
        selected_times = sample_evenly_ints(timeline, max_count=max_frames)
        for time_index in selected_times:
            channel_frames = [
                load_channel_frame(
                    select_record_for_time(records_by_channel[channel_key], time_index),
                    cache=cache,
                )
                for channel_key in channel_keys
            ]
            composed_frames.append(compose_channel_strip(channel_frames))
    else:
        max_count = max(len(channel_records) for channel_records in records_by_channel.values())
        selected_indices = sample_index_values(max_count, max_count=max_frames)
        for index in selected_indices:
            channel_frames = [
                load_channel_frame(channel_records[min(index, len(channel_records) - 1)], cache=cache)
                for channel_records in records_by_channel.values()
            ]
            composed_frames.append(compose_channel_strip(channel_frames))

    return PreviewSequence(
        frames=composed_frames,
        channel_labels=[labels_by_channel[channel_key] for channel_key in channel_keys],
    )


def select_record_for_time(records: list[TiffFrameRecord], time_index: int) -> TiffFrameRecord:
    eligible = [record for record in records if record.time_index is not None and int(record.time_index) <= time_index]
    if eligible:
        return eligible[-1]
    return records[0]


def load_channel_frame(record: TiffFrameRecord, *, cache: dict[Path, np.ndarray]) -> np.ndarray:
    cached = cache.get(record.path)
    if cached is not None:
        return cached
    array = np.asarray(tifffile.imread(record.path))
    frame = normalize_frame(reduce_array_to_frame(array))
    cache[record.path] = frame
    return frame


def open_best_zarr_node(path: Path):
    if path.is_file():
        raise ValueError(f"Expected a Zarr directory but got file {path}")
    try:
        return zarr.open(str(path), mode="r")
    except Exception as exc:
        raise ValueError(f"Unable to open Zarr path {path}: {exc}") from exc


def find_first_zarr_array_dir(path: Path) -> Path | None:
    if not path.exists() or not path.is_dir():
        return None
    try:
        children = sorted((entry for entry in path.iterdir() if entry.is_dir()), key=lambda entry: entry.name.lower())
    except OSError:
        return None
    for child in children:
        if (child / "zarr.json").is_file() or (child / ".zarray").is_file():
            return child
    return None


def select_best_zarr_array(node):
    if hasattr(node, "shape") and hasattr(node, "ndim"):
        return node

    arrays: list[tuple[str, Any]] = []

    def visit(group, prefix: str = "") -> None:
        for key in getattr(group, "array_keys", lambda: [])():
            arrays.append((f"{prefix}/{key}" if prefix else key, group[key]))
        for key in getattr(group, "group_keys", lambda: [])():
            visit(group[key], f"{prefix}/{key}" if prefix else key)

    visit(node)
    if not arrays:
        return None
    arrays.sort(key=lambda item: (array_priority(item[0], item[1]), -int(np.prod(item[1].shape or (1,)))))
    return arrays[0][1]


def array_priority(name: str, array) -> tuple[int, int]:
    lowered = name.lower()
    priority = 1
    if lowered.endswith("/0") or lowered == "0":
        priority = 0
    return priority, int(getattr(array, "ndim", 0))


def sample_frames_from_ndarray(array, *, runtime_config: RawPreviewRuntimeConfig) -> PreviewSequence:
    shape = tuple(int(value) for value in array.shape)
    if len(shape) < 2:
        raise ValueError(f"Expected at least 2 dimensions for preview array, got shape {shape}")

    leading_shape = shape[:-2]
    total_frames = math.prod(leading_shape) if leading_shape else 1
    frame_indices = sample_index_values(
        total_frames,
        max_count=resolve_frame_limit(total_count=total_frames, runtime_config=runtime_config),
    )

    frames: list[np.ndarray] = []
    channel_labels: list[str] = []
    if not leading_shape:
        reduced = reduce_array_to_frame(np.asarray(array[:]))
        frames.append(normalize_frame(reduced))
        channel_count = infer_channel_count(np.asarray(array[:]))
        if channel_count > 1:
            channel_labels = [f"Ch {index + 1}" for index in range(channel_count)]
        return PreviewSequence(frames=frames, channel_labels=channel_labels)

    for frame_index in frame_indices:
        leading_index = np.unravel_index(frame_index, leading_shape)
        view = array[leading_index]
        frames.append(normalize_frame(reduce_array_to_frame(np.asarray(view))))
    channel_count = infer_channel_count(np.asarray(array[tuple(0 for _ in leading_shape)]))
    if channel_count > 1:
        channel_labels = [f"Ch {index + 1}" for index in range(channel_count)]
    return PreviewSequence(frames=frames, channel_labels=channel_labels)


def reduce_array_to_frame(array: np.ndarray) -> np.ndarray:
    frame = np.asarray(array)
    channel_axis = detect_channel_axis(frame.shape)
    if channel_axis is not None:
        channel_frames = []
        for index in range(frame.shape[channel_axis]):
            channel_view = np.take(frame, index, axis=channel_axis)
            channel_frames.append(collapse_to_2d(np.asarray(channel_view)))
        return compose_channel_strip([normalize_frame(channel_frame) for channel_frame in channel_frames])
    return collapse_to_2d(frame)


def collapse_to_2d(frame: np.ndarray) -> np.ndarray:
    while frame.ndim > 2:
        frame = frame[frame.shape[0] // 2]
    if frame.ndim != 2:
        raise ValueError(f"Unable to reduce array to 2D frame, got shape {frame.shape}")
    return frame


def detect_channel_axis(shape: tuple[int, ...]) -> int | None:
    if len(shape) < 3:
        return None
    candidates = [axis for axis, size in enumerate(shape) if 1 < int(size) <= 4]
    if not candidates:
        return None
    spatial_axes = {len(shape) - 2, len(shape) - 1}
    for axis in candidates:
        if axis not in spatial_axes:
            return axis
    return None


def infer_channel_count(array: np.ndarray) -> int:
    channel_axis = detect_channel_axis(tuple(int(value) for value in array.shape))
    if channel_axis is None:
        return 1
    return int(array.shape[channel_axis])


def normalize_frame(frame: np.ndarray) -> np.ndarray:
    finite = np.asarray(frame, dtype=np.float32)
    finite = np.nan_to_num(finite, nan=0.0, posinf=0.0, neginf=0.0)
    lower = float(np.percentile(finite, 1))
    upper = float(np.percentile(finite, 99))
    if not math.isfinite(lower) or not math.isfinite(upper) or upper <= lower:
        upper = lower + 1.0
    scaled = np.clip((finite - lower) / (upper - lower), 0.0, 1.0)
    return (scaled * 255.0).astype(np.uint8)


def sample_evenly(values: list[Path], *, max_count: int) -> list[Path]:
    if len(values) <= max_count:
        return values
    indices = sample_index_values(len(values), max_count=max_count)
    return [values[index] for index in indices]


def sample_index_values(length: int, *, max_count: int) -> list[int]:
    if length <= 0:
        return []
    if length <= max_count:
        return list(range(length))
    if max_count <= 1:
        return [0]
    step = (length - 1) / float(max_count - 1)
    indices = {min(length - 1, max(0, int(round(index * step)))) for index in range(max_count)}
    return sorted(indices)


def sample_evenly_ints(values: list[int], *, max_count: int) -> list[int]:
    if len(values) <= max_count:
        return values
    indices = sample_index_values(len(values), max_count=max_count)
    return [values[index] for index in indices]


def encode_preview_video(
    *,
    video_path: Path,
    frames: Iterable[np.ndarray],
    project_label: str = "",
    position_label: str = "",
    channel_labels: list[str] | None = None,
    runtime_config: RawPreviewRuntimeConfig,
) -> tuple[int, int]:
    frame_list = list(frames)
    if not frame_list:
        raise ValueError(f"No frames available to encode preview video {video_path}")

    prepared_frames = [prepare_frame_for_video(frame, runtime_config=runtime_config) for frame in frame_list]
    annotated_frames = annotate_preview_frames(
        prepared_frames,
        project_label=project_label,
        position_label=position_label,
        channel_labels=channel_labels or [],
    )
    annotated_frames = ensure_even_video_dimensions(annotated_frames)
    height, width, _ = annotated_frames[0].shape
    command = [
        resolve_ffmpeg_command(runtime_config),
        "-y",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "rgb24",
        "-s",
        f"{width}x{height}",
        "-r",
        str(max(1, runtime_config.fps)),
        "-i",
        "-",
    ]
    command.extend(
        [
        "-an",
        "-c:v",
        "libx264",
        "-preset",
        str(runtime_config.preset or "medium"),
        "-crf",
        str(int(runtime_config.crf)),
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        str(video_path),
        ]
    )

    try:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError as exc:
        raise RuntimeError(f"FFmpeg not found: {runtime_config.ffmpeg_command or 'auto'}") from exc

    stderr = b""
    try:
        assert process.stdin is not None
        for frame in annotated_frames:
            process.stdin.write(frame.tobytes(order="C"))
        process.stdin.close()
        stderr = process.stderr.read() if process.stderr is not None else b""
        process.wait()
    except BrokenPipeError as exc:
        stderr = process.stderr.read() if process.stderr is not None else b""
        process.kill()
        process.wait()
        stderr_text = stderr.decode("utf-8", errors="replace").strip()
        detail = f" FFmpeg stderr: {stderr_text}" if stderr_text else ""
        raise RuntimeError(f"FFmpeg terminated while streaming raw frames.{detail}") from exc
    except Exception:
        process.kill()
        process.wait()
        raise

    if process.returncode != 0:
        stderr_text = stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"FFmpeg failed while encoding preview video: {stderr_text}")
    return width, height


def ensure_even_video_dimensions(frames: list[np.ndarray]) -> list[np.ndarray]:
    if not frames:
        return frames
    height, width = frames[0].shape[:2]
    target_height = height if height % 2 == 0 else height + 1
    target_width = width if width % 2 == 0 else width + 1
    if target_height == height and target_width == width:
        return frames

    adjusted: list[np.ndarray] = []
    for frame in frames:
        if frame.shape[0] == target_height and frame.shape[1] == target_width:
            adjusted.append(frame)
            continue
        padded = np.zeros((target_height, target_width, frame.shape[2]), dtype=frame.dtype)
        padded[: frame.shape[0], : frame.shape[1], :] = frame
        adjusted.append(padded)
    return adjusted


def resolve_ffmpeg_command(runtime_config: RawPreviewRuntimeConfig) -> str:
    configured = str(runtime_config.ffmpeg_command or "").strip()
    if configured:
        return configured
    try:
        import imageio_ffmpeg

        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return "ffmpeg"


def annotate_preview_frames(
    frames: list[np.ndarray],
    *,
    project_label: str,
    position_label: str,
    channel_labels: list[str],
) -> list[np.ndarray]:
    overlay_project_label = sanitize_overlay_text(project_label)
    overlay_position_label = sanitize_overlay_text(position_label)
    compact_overlay = False
    overlay_channel_labels = []
    for index, label in enumerate(channel_labels):
        label_suffix = sanitize_overlay_text(label)
        if label_suffix and label_suffix != f"CH_{index + 1}":
            overlay_channel_labels.append(f"CH{index + 1} {label_suffix}")
        else:
            overlay_channel_labels.append(f"CH{index + 1}")
    annotated: list[np.ndarray] = []
    for index, frame in enumerate(frames):
        canvas = np.array(frame, copy=True)
        height, width = canvas.shape[:2]
        compact_overlay = min(height, width) < 512
        frame_scale = max(1, min(4, min(height, width) // 320))
        label_scale = 1 if compact_overlay else max(1, frame_scale - 1)
        title_scale = 1 if compact_overlay else max(1, frame_scale - 1)
        margin = 2 if compact_overlay else max(4, frame_scale * 3)
        line_gap = 2 if compact_overlay else max(4, label_scale * 8)
        frame_label = f"F{index + 1}" if compact_overlay else f"FRAME {index + 1}"
        frame_scale = fit_text_scale(frame_label, available_width=max(32, width - (margin * 2)), desired_scale=frame_scale)
        draw_text_with_box(canvas, frame_label, x=margin, y=margin, scale=frame_scale)
        if overlay_project_label:
            project_text = overlay_project_label[:18] if compact_overlay else overlay_project_label[:48]
            project_scale = fit_text_scale(
                project_text,
                available_width=max(32, width - (margin * 2)),
                desired_scale=title_scale,
            )
            draw_text_with_box(
                canvas,
                project_text,
                x=margin,
                y=max(margin, height - (7 * project_scale) - margin),
                scale=project_scale,
            )
        if overlay_position_label:
            position_text = f"P{overlay_position_label[:10]}" if compact_overlay else f"POS {overlay_position_label[:28]}"
            position_scale = fit_text_scale(
                position_text,
                available_width=max(32, width - (margin * 2)),
                desired_scale=title_scale,
            )
            draw_text_with_box(
                canvas,
                position_text,
                x=margin,
                y=max(margin, height - (7 * project_scale if overlay_project_label else 7 * position_scale) - line_gap - margin),
                scale=position_scale,
            )
        if overlay_channel_labels:
            panel_width = max(1, canvas.shape[1] // len(overlay_channel_labels))
            for channel_index, label in enumerate(overlay_channel_labels):
                channel_text = f"CH{channel_index + 1}" if compact_overlay else label
                channel_scale = fit_text_scale(
                    channel_text,
                    available_width=max(24, panel_width - (margin * 2)),
                    desired_scale=label_scale,
                )
                draw_text_with_box(
                    canvas,
                    channel_text,
                    x=channel_index * panel_width + margin,
                    y=margin + line_gap,
                    scale=channel_scale,
                )
        annotated.append(canvas)
    return annotated


def sanitize_overlay_text(value: str) -> str:
    normalized = slugify(value or "").upper().replace("-", "_")
    return "".join(character if character in BITMAP_FONT else " " for character in normalized)


def draw_text_with_box(frame: np.ndarray, text: str, *, x: int, y: int, scale: int) -> None:
    if not text:
        return
    width = text_pixel_width(text, scale=scale)
    height = 7 * scale
    draw_filled_rect(frame, x=max(0, x - 3), y=max(0, y - 2), width=width + 6, height=height + 4, value=0)
    draw_text(frame, text, x=x, y=y, scale=scale, value=255)


def text_pixel_width(text: str, *, scale: int) -> int:
    return max(0, sum((5 * scale) + scale for _ in text) - scale)


def draw_filled_rect(frame: np.ndarray, *, x: int, y: int, width: int, height: int, value: int) -> None:
    x_end = min(frame.shape[1], x + width)
    y_end = min(frame.shape[0], y + height)
    if x >= x_end or y >= y_end:
        return
    frame[y:y_end, x:x_end] = np.uint8(value)


def draw_text(frame: np.ndarray, text: str, *, x: int, y: int, scale: int, value: int) -> None:
    cursor_x = x
    for character in text:
        draw_character(frame, character, x=cursor_x, y=y, scale=scale, value=value)
        cursor_x += (5 * scale) + scale


def draw_character(frame: np.ndarray, character: str, *, x: int, y: int, scale: int, value: int) -> None:
    bitmap = BITMAP_FONT.get(character.upper(), BITMAP_FONT[" "])
    for row_index, row in enumerate(bitmap):
        for col_index, pixel in enumerate(row):
            if pixel != "1":
                continue
            row_start = y + row_index * scale
            col_start = x + col_index * scale
            row_end = min(frame.shape[0], row_start + scale)
            col_end = min(frame.shape[1], col_start + scale)
            if row_start >= frame.shape[0] or col_start >= frame.shape[1]:
                continue
            frame[row_start:row_end, col_start:col_end] = np.uint8(value)


def prepare_frame_for_video(frame: np.ndarray, *, runtime_config: RawPreviewRuntimeConfig) -> np.ndarray:
    binned = apply_binning(frame, factor=max(1, runtime_config.binning_factor))
    resized = resize_frame_to_limit(binned, max_dimension=max(64, runtime_config.max_dimension))
    rgb = np.stack([resized, resized, resized], axis=-1)
    return np.ascontiguousarray(rgb, dtype=np.uint8)


def resize_frame_to_limit(frame: np.ndarray, *, max_dimension: int) -> np.ndarray:
    height, width = frame.shape
    largest_dimension = max(height, width)
    if largest_dimension <= max_dimension:
        return frame

    scale = max_dimension / float(largest_dimension)
    target_height = max(1, int(round(height * scale)))
    target_width = max(1, int(round(width * scale)))
    source = frame.astype(np.float32, copy=False)
    y_coords = np.linspace(0.0, max(0.0, float(height - 1)), target_height, dtype=np.float32)
    x_coords = np.linspace(0.0, max(0.0, float(width - 1)), target_width, dtype=np.float32)

    y0 = np.floor(y_coords).astype(np.int32)
    x0 = np.floor(x_coords).astype(np.int32)
    y1 = np.minimum(y0 + 1, height - 1)
    x1 = np.minimum(x0 + 1, width - 1)

    wy = (y_coords - y0).reshape(-1, 1)
    wx = (x_coords - x0).reshape(1, -1)

    top_left = source[y0[:, None], x0[None, :]]
    top_right = source[y0[:, None], x1[None, :]]
    bottom_left = source[y1[:, None], x0[None, :]]
    bottom_right = source[y1[:, None], x1[None, :]]

    top = top_left * (1.0 - wx) + top_right * wx
    bottom = bottom_left * (1.0 - wx) + bottom_right * wx
    resized = top * (1.0 - wy) + bottom * wy
    return np.clip(np.round(resized), 0, 255).astype(np.uint8)


def apply_binning(frame: np.ndarray, *, factor: int) -> np.ndarray:
    if factor <= 1:
        return frame
    height, width = frame.shape
    target_height = (height // factor) * factor
    target_width = (width // factor) * factor
    if target_height < factor or target_width < factor:
        return frame
    cropped = frame[:target_height, :target_width].astype(np.float32, copy=False)
    rebinned = cropped.reshape(target_height // factor, factor, target_width // factor, factor).mean(axis=(1, 3))
    return np.clip(np.round(rebinned), 0, 255).astype(np.uint8)


def resolve_frame_limit(*, total_count: int, runtime_config: RawPreviewRuntimeConfig) -> int:
    if total_count <= 0:
        return 1
    if str(runtime_config.frame_mode or "").lower() == "full":
        return total_count
    requested = max(1, int(runtime_config.max_frames or 1))
    return min(total_count, requested)


def compose_channel_strip(channel_frames: list[np.ndarray]) -> np.ndarray:
    if not channel_frames:
        raise ValueError("Cannot compose an empty channel strip")
    target_height = max(frame.shape[0] for frame in channel_frames)
    target_width = max(frame.shape[1] for frame in channel_frames)
    padded = [pad_frame(frame, target_height=target_height, target_width=target_width) for frame in channel_frames]
    return np.concatenate(padded, axis=1)


def pad_frame(frame: np.ndarray, *, target_height: int, target_width: int) -> np.ndarray:
    result = np.zeros((target_height, target_width), dtype=np.uint8)
    height, width = frame.shape
    result[:height, :width] = frame
    return result


def upsert_preview_artifact(
    session: Session,
    *,
    job: Job,
    raw_dataset: RawDataset,
    position: RawDatasetPosition,
    video_path: Path,
    frames: list[np.ndarray],
    encoded_width: int,
    encoded_height: int,
    channel_labels: list[str],
    project_label: str,
    runtime_config: RawPreviewRuntimeConfig,
) -> Artifact:
    fps_value = max(1, runtime_config.fps)
    frame_count = len(frames)
    duration_seconds = float(frame_count) / float(fps_value) if frame_count > 0 else 0.0
    file_size_bytes = int(video_path.stat().st_size) if video_path.exists() else 0
    bitrate_kbps = (
        round((float(file_size_bytes) * 8.0) / duration_seconds / 1000.0, 2)
        if duration_seconds > 0.0 and file_size_bytes > 0
        else None
    )
    metadata = {
        "source": "raw_preview_video",
        "raw_dataset_id": str(raw_dataset.id),
        "position_id": str(position.id),
        "position_key": position.position_key,
        "frame_count": frame_count,
        "width": int(encoded_width),
        "height": int(encoded_height),
        "source_width": int(frames[0].shape[1]),
        "source_height": int(frames[0].shape[0]),
        "fps": fps_value,
        "frame_mode": runtime_config.frame_mode,
        "max_frames_setting": runtime_config.max_frames,
        "duration_seconds": duration_seconds,
        "file_size_bytes": file_size_bytes,
        "bitrate_kbps": bitrate_kbps,
        "binning_factor": max(1, runtime_config.binning_factor),
        "format": "mp4",
        "absolute_path": str(video_path),
        "channel_labels": channel_labels,
        "project_label": project_label,
    }
    artifact = position.preview_artifact
    if artifact is None:
        artifact = Artifact(
            job_id=job.id,
            artifact_kind="raw_position_preview_mp4",
            uri=str(video_path),
            metadata_json=metadata,
        )
        session.add(artifact)
        session.flush()
    else:
        artifact.job_id = job.id
        artifact.artifact_kind = "raw_position_preview_mp4"
        artifact.uri = str(video_path)
        merged_metadata = dict(artifact.metadata_json or {})
        merged_metadata.update(metadata)
        artifact.metadata_json = merged_metadata
        session.flush()
    return artifact


def infer_channel_label(path: Path) -> str:
    stem = path.stem
    keyword_match = CHANNEL_TOKEN_PATTERNS[0].search(stem)
    if keyword_match:
        return normalize_channel_label(keyword_match.group(2))
    wave_match = CHANNEL_TOKEN_PATTERNS[1].search(stem)
    if wave_match:
        suffix = wave_match.group(2) or ""
        return normalize_channel_label(f"W{wave_match.group(1)}{suffix}")
    return "Ch 1"


def infer_time_index(path: Path) -> int | None:
    stem = path.stem
    for pattern in TIME_TOKEN_PATTERNS:
        match = pattern.search(stem)
        if match:
            return int(match.group(2 if len(match.groups()) > 1 else 1))
    return None


def normalize_channel_label(value: str) -> str:
    text = str(value or "").replace("_", " ").replace("-", " ").strip()
    if not text:
        return "Ch 1"
    if text.isdigit():
        return f"Ch {int(text) + 1}"
    if text.lower().startswith("ch") and text[2:].strip().isdigit():
        return f"Ch {int(text[2:].strip()) + 1}"
    return text.upper()


def ordered_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered
