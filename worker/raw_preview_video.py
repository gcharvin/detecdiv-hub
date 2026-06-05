from __future__ import annotations

import math
import json
import re
import subprocess
import tempfile
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
from api.services.ome_zarr_metadata import extract_ome_zarr_channel_settings, read_ome_zarr_group_metadata
from api.services.path_resolution import compose_storage_path
from api.config import get_settings
from api.services.project_indexing import is_legacy_matlab_timelapse_dataset_dir, legacy_matlab_position_dir, slugify
from api.services.raw_preview_settings import RawPreviewRuntimeConfig, resolve_raw_preview_runtime_config
from worker.executors.matlab_executor import build_matlab_batch_command, run_matlab_command
from worker.preview_text import fit_text_scale


TIFF_SUFFIXES = (".tif", ".tiff")
IMAGE_SUFFIXES = (".jpg", ".jpeg")
TIFF_LIKE_FORMATS = {"single_tiff", "ome_tiff", "tiff_sequence", "micromanager_tiff_dir", "ndtiff"}
LEGACY_IMAGE_FORMATS = {"legacy_matlab_jpg_timelapse"}
ZARR_LIKE_FORMATS = {"zarr", "ome_zarr"}
ND2_FORMATS = {"nd2"}
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
class LegacyMatlabPreviewResult:
    video_path: Path
    frame_count: int
    source_width: int
    source_height: int
    encoded_width: int
    encoded_height: int
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
            if should_use_legacy_matlab_jpg_preview(
                dataset_path=dataset_path,
                raw_dataset=raw_dataset,
                position=position,
            ):
                render_result = render_legacy_matlab_jpg_preview_video(
                    dataset_path=dataset_path,
                    raw_dataset=raw_dataset,
                    position=position,
                    runtime_config=runtime_config,
                    output_dir=output_dir,
                )
                video_path = render_result.video_path
                encoded_width = render_result.encoded_width
                encoded_height = render_result.encoded_height
                source_width = render_result.source_width
                source_height = render_result.source_height
                frame_count = render_result.frame_count
                channel_labels = render_result.channel_labels
            else:
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
                source_width = int(sequence.frames[0].shape[1])
                source_height = int(sequence.frames[0].shape[0])
                frame_count = len(sequence.frames)
                channel_labels = sequence.channel_labels

            artifact = upsert_preview_artifact(
                session,
                job=job,
                raw_dataset=raw_dataset,
                position=position,
                video_path=video_path,
                frame_count=frame_count,
                source_width=source_width,
                source_height=source_height,
                encoded_width=encoded_width,
                encoded_height=encoded_height,
                channel_labels=channel_labels,
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
                    "frame_count": frame_count,
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
    if data_format in ND2_FORMATS:
        return read_nd2_preview_frames(
            dataset_path=dataset_path,
            raw_dataset=raw_dataset,
            position=position,
            runtime_config=runtime_config,
        )
    if data_format in ZARR_LIKE_FORMATS:
        return read_zarr_preview_frames(dataset_path=dataset_path, position=position, runtime_config=runtime_config)
    return read_tiff_preview_frames(
        dataset_path=dataset_path,
        raw_dataset=raw_dataset,
        position=position,
        runtime_config=runtime_config,
    )


def read_nd2_preview_frames(
    *,
    dataset_path: Path,
    raw_dataset: RawDataset,
    position: RawDatasetPosition,
    runtime_config: RawPreviewRuntimeConfig,
) -> PreviewSequence:
    nd2_path = resolve_nd2_source_path(dataset_path=dataset_path, raw_dataset=raw_dataset)
    try:
        import nd2
    except ImportError as exc:
        raise RuntimeError("ND2 preview generation requires the nd2[legacy] package.") from exc

    position_index = resolve_nd2_position_index(position)
    channel_labels = nd2_channel_labels(raw_dataset)
    with nd2.ND2File(nd2_path) as nd2_file:
        axis_names = [str(axis) for axis in dict(nd2_file.sizes).keys()]
        array = nd2_file.to_dask()
        shape = tuple(int(value) for value in array.shape)
        axis_sizes = dict(zip(axis_names, shape, strict=False))
        total_times = int(axis_sizes.get("T", 1) or 1)
        time_indices = sample_index_values(
            total_times,
            max_count=resolve_frame_limit(total_count=total_times, runtime_config=runtime_config),
        )
        channel_count = int(axis_sizes.get("C", 1) or 1)
        if not channel_labels:
            channel_labels = [f"Channel {index + 1}" for index in range(channel_count)]
        elif len(channel_labels) < channel_count:
            channel_labels = channel_labels + [
                f"Channel {index + 1}" for index in range(len(channel_labels), channel_count)
            ]

        frames: list[np.ndarray] = []
        for time_index in time_indices:
            channel_frames = []
            for channel_index in range(max(1, channel_count)):
                raw_frame = read_nd2_frame(
                    array,
                    axis_names=axis_names,
                    axis_sizes=axis_sizes,
                    time_index=time_index,
                    position_index=position_index,
                    channel_index=channel_index,
                )
                channel_frames.append(normalize_frame(reduce_array_to_frame(raw_frame)))
            if len(channel_frames) > 1:
                frames.append(compose_channel_strip(channel_frames))
            else:
                frames.append(channel_frames[0])
    return PreviewSequence(frames=frames, channel_labels=channel_labels[: max(1, channel_count)])


def resolve_nd2_source_path(*, dataset_path: Path, raw_dataset: RawDataset) -> Path:
    metadata = dict(raw_dataset.metadata_json or {})
    nd2_metadata = metadata.get("nd2") if isinstance(metadata.get("nd2"), dict) else {}
    source_path = str(nd2_metadata.get("source_path") or "").strip()
    if source_path:
        candidate = Path(source_path)
        if candidate.is_file():
            return candidate

    source_file = str(nd2_metadata.get("source_file") or "").strip()
    if source_file:
        candidate = dataset_path / source_file
        if candidate.is_file():
            return candidate

    try:
        nd2_files = sorted(
            (entry for entry in dataset_path.iterdir() if entry.is_file() and entry.name.lower().endswith(".nd2")),
            key=lambda path: path.stat().st_size,
            reverse=True,
        )
    except OSError:
        nd2_files = []
    if nd2_files:
        return nd2_files[0]
    raise ValueError(f"No ND2 file found for dataset {dataset_path}")


def resolve_nd2_position_index(position: RawDatasetPosition) -> int:
    metadata = dict(position.metadata_json or {})
    for value in (metadata.get("nd2_position_index"), metadata.get("position_index"), position.position_index):
        if value is None:
            continue
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            continue
    return 0


def nd2_channel_labels(raw_dataset: RawDataset) -> list[str]:
    metadata = dict(raw_dataset.metadata_json or {})
    nd2_metadata = metadata.get("nd2") if isinstance(metadata.get("nd2"), dict) else {}
    channels = nd2_metadata.get("channels") if isinstance(nd2_metadata, dict) else None
    labels = []
    if isinstance(channels, list):
        for index, item in enumerate(channels):
            if not isinstance(item, dict):
                continue
            label = str(item.get("name") or "").strip()
            labels.append(label or f"Channel {index + 1}")
    if labels:
        return labels
    dimensions = metadata.get("dimensions") if isinstance(metadata.get("dimensions"), dict) else {}
    channel_names = dimensions.get("channel_names") if isinstance(dimensions, dict) else None
    if isinstance(channel_names, list):
        return [str(value).strip() or f"Channel {index + 1}" for index, value in enumerate(channel_names)]
    return []


def read_nd2_frame(
    array,
    *,
    axis_names: list[str],
    axis_sizes: dict[str, int],
    time_index: int,
    position_index: int,
    channel_index: int,
) -> np.ndarray:
    selectors: list[Any] = []
    for axis_name in axis_names:
        axis_size = int(axis_sizes.get(axis_name, 1) or 1)
        if axis_name == "T":
            selectors.append(clamp_index(time_index, axis_size))
        elif axis_name == "P":
            selectors.append(clamp_index(position_index, axis_size))
        elif axis_name == "C":
            selectors.append(clamp_index(channel_index, axis_size))
        elif axis_name == "Z":
            selectors.append(axis_size // 2)
        elif axis_name in {"Y", "X"}:
            selectors.append(slice(None))
        else:
            selectors.append(0)
    view = array[tuple(selectors)]
    if hasattr(view, "compute"):
        view = view.compute()
    return np.asarray(view)


def clamp_index(value: int, size: int) -> int:
    if size <= 0:
        return 0
    return max(0, min(size - 1, int(value)))


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
        return build_multichannel_frame_sequence(
            records,
            load_frame_fn=load_tiff_frame,
            runtime_config=runtime_config,
        )

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
        array = load_tiff_frame(path)
        frames.append(normalize_frame(reduce_array_to_frame(np.asarray(array))))
    return PreviewSequence(frames=frames, channel_labels=channel_labels)


def should_use_legacy_matlab_jpg_preview(
    *,
    dataset_path: Path,
    raw_dataset: RawDataset,
    position: RawDatasetPosition,
) -> bool:
    data_format = str(raw_dataset.data_format or "").strip().lower()
    if data_format in LEGACY_IMAGE_FORMATS:
        return True
    if data_format in TIFF_LIKE_FORMATS:
        return False

    candidate_path = resolve_position_source_path(dataset_path=dataset_path, position=position)
    if not has_legacy_matlab_timelapse_marker(candidate_path):
        if candidate_path != dataset_path and has_legacy_matlab_timelapse_marker(dataset_path):
            pass
        else:
            return False

    image_source = resolve_legacy_matlab_image_source(candidate_path=candidate_path, dataset_path=dataset_path)
    return image_source is not None and bool(collect_legacy_image_paths(image_source))


def has_legacy_matlab_timelapse_marker(path: Path) -> bool:
    if not path.exists():
        return False
    return is_legacy_matlab_timelapse_dataset_dir(path)


def resolve_legacy_matlab_image_source(*, candidate_path: Path, dataset_path: Path) -> Path | None:
    direct_candidates = [candidate_path]
    if candidate_path != dataset_path:
        direct_candidates.append(dataset_path)

    for path in direct_candidates:
        image_paths = collect_legacy_image_paths(path)
        if image_paths:
            return path

    if not is_legacy_matlab_timelapse_dataset_dir(dataset_path):
        return None

    try:
        child_dirs = sorted((entry for entry in dataset_path.iterdir() if entry.is_dir()), key=lambda value: value.name.lower())
    except OSError:
        return None
    prefix = f"{dataset_path.name.lower()}-pos"
    for child in child_dirs:
        if not child.name.lower().startswith(prefix):
            continue
        if collect_legacy_image_paths(child):
            return child
    return None


def render_legacy_matlab_jpg_preview_video(
    *,
    dataset_path: Path,
    raw_dataset: RawDataset,
    position: RawDatasetPosition,
    runtime_config: RawPreviewRuntimeConfig,
    output_dir: Path,
) -> LegacyMatlabPreviewResult:
    candidate_path = resolve_position_source_path(dataset_path=dataset_path, position=position)
    image_source = resolve_legacy_matlab_image_source(candidate_path=candidate_path, dataset_path=dataset_path)
    if image_source is None:
        raise ValueError(f"No legacy MATLAB JPEG source found for dataset {dataset_path}")

    settings = get_settings()
    matlab_command = str(settings.matlab_command or "matlab").strip() or "matlab"
    repo_root = str(Path(__file__).resolve().parents[1])

    output_dir.mkdir(parents=True, exist_ok=True)
    video_path = output_dir / f"{slugify(position.position_key) or 'position'}.mp4"

    with tempfile.TemporaryDirectory(prefix="detecdiv_legacy_preview_") as tmpdir:
        tmp_path = Path(tmpdir)
        frame_dir = tmp_path / "frames"
        config_path = tmp_path / "preview_config.json"
        result_path = tmp_path / "preview_result.json"
        config = {
            "source_path": str(image_source),
            "frame_dir": str(frame_dir),
            "result_path": str(result_path),
            "fps": int(max(1, runtime_config.fps)),
            "max_frames": int(max(0, runtime_config.max_frames)),
            "frame_mode": str(runtime_config.frame_mode or "full"),
            "max_dimension": int(max(64, runtime_config.max_dimension)),
            "channel_settings": legacy_channel_settings_from_raw_dataset(raw_dataset),
        }
        config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")

        entrypoint = (
            f"addpath(genpath(pwd)); "
            f"legacy_matlab_jpg_preview('{matlab_escape(config_path)}')"
        )
        command = build_matlab_batch_command(repo_root, entrypoint, matlab_command=matlab_command)
        completed = run_matlab_command(command)
        if completed.returncode != 0:
            stderr_text = (completed.stderr or completed.stdout or "").strip()
            raise RuntimeError(f"MATLAB legacy preview renderer failed: {stderr_text}")
        if not result_path.is_file():
            raise RuntimeError("MATLAB legacy preview renderer did not write a result JSON file.")

        result = json.loads(result_path.read_text(encoding="utf-8"))
        if str(result.get("status") or "").lower() != "ok":
            raise RuntimeError(str(result.get("error") or "MATLAB legacy preview renderer failed."))

        frame_paths = sorted(
            path for path in frame_dir.glob("frame_*.tif*") if path.is_file()
        )
        if not frame_paths:
            raise RuntimeError("MATLAB legacy preview renderer did not write any TIFF frames.")

        frames = [np.asarray(tifffile.imread(path)) for path in frame_paths]
        project_label = str(
            getattr(raw_dataset, "external_key", "") or getattr(raw_dataset, "acquisition_label", "") or ""
        ).strip()
        encoded_width, encoded_height = encode_preview_video(
            video_path=video_path,
            frames=frames,
            project_label=project_label,
            position_label=str(position.display_name or position.position_key or ""),
            channel_labels=[str(value) for value in list(result.get("channel_labels") or []) if str(value).strip()],
            runtime_config=runtime_config,
        )

    if not video_path.is_file():
        raise RuntimeError(f"MATLAB legacy preview renderer did not create {video_path}")

    return LegacyMatlabPreviewResult(
        video_path=video_path,
        frame_count=int(result.get("frame_count") or 0),
        source_width=int(result.get("source_width") or 0),
        source_height=int(result.get("source_height") or 0),
        encoded_width=int(encoded_width),
        encoded_height=int(encoded_height),
        channel_labels=[str(value) for value in list(result.get("channel_labels") or []) if str(value).strip()],
    )


def legacy_channel_settings_from_raw_dataset(raw_dataset: RawDataset) -> list[dict[str, Any]]:
    metadata = dict(raw_dataset.metadata_json or {})
    dimensions = metadata.get("dimensions") if isinstance(metadata, dict) else None
    if not isinstance(dimensions, dict):
        return []
    channel_settings = dimensions.get("channel_settings")
    if not isinstance(channel_settings, list):
        return []
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(channel_settings):
        if not isinstance(item, dict):
            continue
        detail = dict(item)
        detail.setdefault("index", index)
        normalized.append(detail)
    return normalized


def read_zarr_preview_frames(
    *,
    dataset_path: Path,
    position: RawDatasetPosition,
    runtime_config: RawPreviewRuntimeConfig,
) -> PreviewSequence:
    candidate_path = resolve_position_source_path(dataset_path=dataset_path, position=position)
    attempted_targets = list(iter_zarr_preview_targets(candidate_path, dataset_path, position=position))
    last_error: Exception | None = None

    for target_path in attempted_targets:
        try:
            node = open_best_zarr_node(target_path)
            array = select_best_zarr_array(node)
        except Exception as exc:
            last_error = exc
            v3_sequence = try_read_v3_ome_writers_preview_frames(target_path, runtime_config=runtime_config)
            if v3_sequence is not None:
                return v3_sequence
            continue
        if array is not None:
            axis_aware_sequence = sample_ome_zarr_axis_aware_frames(
                array,
                target_path=target_path,
                runtime_config=runtime_config,
            )
            if axis_aware_sequence is not None:
                return axis_aware_sequence
            return sample_frames_from_ndarray(array, runtime_config=runtime_config)

    if last_error is None:
        raise ValueError(f"No readable Zarr array found under {dataset_path}")
    raise ValueError(f"No readable Zarr array found under {dataset_path}: {last_error}") from last_error


def sample_ome_zarr_axis_aware_frames(
    array,
    *,
    target_path: Path,
    runtime_config: RawPreviewRuntimeConfig,
) -> PreviewSequence | None:
    shape = tuple(int(value) for value in getattr(array, "shape", ()) or ())
    if len(shape) < 3:
        return None

    series_path = target_path.parent if is_zarr_array_dir(target_path) else target_path
    series_metadata = read_ome_zarr_group_metadata(series_path)
    axes = extract_ome_zarr_axes(series_metadata, target_path)
    if len(axes) != len(shape):
        return None

    leading_axes = axes[:-2]
    channel_axis = next((index for index, role in enumerate(leading_axes) if role == "channel"), None)
    time_axis = next((index for index, role in enumerate(leading_axes) if role == "time"), None)
    if channel_axis is None and time_axis is None:
        return None

    time_count = shape[time_axis] if time_axis is not None else 1
    sample_count = resolve_frame_limit(total_count=time_count, runtime_config=runtime_config)
    time_indices = sample_index_values(time_count, max_count=sample_count)
    channel_settings = extract_ome_zarr_channel_settings(series_metadata)
    channel_indices = select_preview_channel_indices(shape=shape, channel_axis=channel_axis)
    channel_labels = [
        channel_label_for_index(channel_settings=channel_settings, channel_index=index)
        for index in channel_indices
    ]

    frames: list[np.ndarray] = []
    for time_index in time_indices:
        channel_frames: list[np.ndarray] = []
        for channel_index in channel_indices:
            leading_index: list[int] = []
            for axis_index, axis_size in enumerate(shape[:-2]):
                if axis_index == time_axis:
                    leading_index.append(int(time_index))
                elif axis_index == channel_axis:
                    leading_index.append(int(channel_index))
                else:
                    leading_index.append(int(axis_size) // 2)
            frame = collapse_to_2d(np.asarray(array[tuple(leading_index)]))
            channel_frames.append(normalize_frame(frame))
        if not channel_frames:
            continue
        frames.append(channel_frames[0] if len(channel_frames) == 1 else compose_channel_strip(channel_frames))

    if not frames:
        return None
    return PreviewSequence(frames=frames, channel_labels=channel_labels)


def extract_ome_zarr_axes(series_metadata: dict[str, Any], target_path: Path) -> list[str]:
    ome = series_metadata.get("ome") if isinstance(series_metadata, dict) else None
    multiscales = ome.get("multiscales") if isinstance(ome, dict) else series_metadata.get("multiscales")
    axes = None
    if isinstance(multiscales, list):
        first_multiscales = next((item for item in multiscales if isinstance(item, dict)), None)
        if isinstance(first_multiscales, dict):
            axes = first_multiscales.get("axes")
    normalized = normalize_ome_axis_roles(axes)
    if normalized:
        return normalized

    array_metadata = {}
    metadata_path = target_path / "zarr.json"
    if metadata_path.is_file():
        try:
            array_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception:
            array_metadata = {}
    dimension_names = array_metadata.get("dimension_names") if isinstance(array_metadata, dict) else None
    return normalize_ome_axis_roles(dimension_names)


def normalize_ome_axis_roles(axes: Any) -> list[str]:
    if not isinstance(axes, list):
        return []
    roles: list[str] = []
    for axis in axes:
        if isinstance(axis, dict):
            raw_name = str(axis.get("name") or "").strip().lower()
            raw_type = str(axis.get("type") or "").strip().lower()
        else:
            raw_name = str(axis or "").strip().lower()
            raw_type = ""
        if raw_name in {"c", "ch", "channel"} or raw_type == "channel":
            roles.append("channel")
        elif raw_name in {"t", "time"} or raw_type == "time":
            roles.append("time")
        elif raw_name == "z":
            roles.append("z")
        elif raw_name in {"x", "y"}:
            roles.append(raw_name)
        else:
            roles.append(raw_type or raw_name)
    return roles


def select_preview_channel_indices(*, shape: tuple[int, ...], channel_axis: int | None) -> list[int]:
    if channel_axis is None:
        return [0]
    channel_count = int(shape[channel_axis])
    if channel_count <= 0:
        return []
    return list(range(channel_count))


def channel_label_for_index(*, channel_settings: list[dict[str, Any]], channel_index: int) -> str:
    for item in channel_settings:
        if not isinstance(item, dict):
            continue
        try:
            item_index = int(item.get("index", channel_index))
        except (TypeError, ValueError):
            item_index = channel_index
        if item_index != channel_index:
            continue
        label = str(item.get("channel") or item.get("label") or item.get("name") or "").strip()
        if label:
            return label
    return f"Channel {channel_index + 1}"


def resolve_position_source_path(*, dataset_path: Path, position: RawDatasetPosition) -> Path:
    for candidate in iter_position_source_candidates(dataset_path=dataset_path, position=position):
        if candidate.exists():
            return candidate
    return dataset_path


def iter_position_source_candidates(*, dataset_path: Path, position: RawDatasetPosition) -> list[Path]:
    metadata = dict(position.metadata_json or {})
    candidates: list[Path] = []
    seen: set[Path] = set()

    def add_candidate(value: str | Path | None) -> None:
        text = str(value or "").strip()
        if not text:
            return
        path = Path(text)
        candidate = path if path.is_absolute() else dataset_path / path
        resolved = candidate.resolve(strict=False)
        if resolved in seen:
            return
        seen.add(resolved)
        candidates.append(candidate)

    add_candidate(metadata.get("relative_path"))
    well_path = str(metadata.get("well_path") or "").strip()
    image_path = str(metadata.get("image_path") or metadata.get("path") or "").strip()
    if well_path and image_path:
        add_candidate(Path(well_path) / Path(image_path))
    add_candidate(well_path)
    add_candidate(image_path)
    add_candidate(metadata.get("series_name"))
    add_candidate(position.display_name)
    if is_legacy_matlab_timelapse_dataset_dir(dataset_path):
        legacy_position_number = infer_legacy_matlab_position_number(position=position, metadata=metadata)
        if legacy_position_number is not None:
            legacy_position_dir = legacy_matlab_position_dir(dataset_path, legacy_position_number)
            if legacy_position_dir is not None:
                add_candidate(legacy_position_dir)
            add_candidate(Path(f"{dataset_path.name}-pos{legacy_position_number}"))
    return candidates


def iter_zarr_preview_targets(
    candidate_path: Path,
    dataset_path: Path,
    *,
    position: RawDatasetPosition | None = None,
) -> list[Path]:
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
        for preferred_path in iter_preferred_zarr_targets(path):
            add_target(preferred_path)
        add_target(path)
        add_target(find_first_zarr_array_dir(path))

    if position is not None:
        for position_path in iter_position_source_candidates(dataset_path=dataset_path, position=position):
            add_group_and_first_array(position_path)

    add_group_and_first_array(candidate_path)
    if candidate_path != dataset_path:
        add_group_and_first_array(dataset_path)

    return targets


def iter_preferred_zarr_targets(path: Path) -> list[Path]:
    metadata = read_ome_zarr_group_metadata(path)
    if not metadata:
        return []

    preferred: list[Path] = []
    seen: set[Path] = set()

    def add_path(value: str | Path | None) -> None:
        text = str(value or "").strip()
        if not text:
            return
        candidate = path / Path(text)
        if not candidate.exists():
            return
        resolved = candidate.resolve(strict=False)
        if resolved in seen:
            return
        seen.add(resolved)
        preferred.append(candidate)

    multiscales = metadata.get("multiscales")
    if isinstance(multiscales, list):
        first_multiscales = next((item for item in multiscales if isinstance(item, dict)), None)
        if isinstance(first_multiscales, dict):
            datasets = first_multiscales.get("datasets")
            if isinstance(datasets, list):
                first_dataset = next((item for item in datasets if isinstance(item, dict)), None)
                if isinstance(first_dataset, dict):
                    add_path(first_dataset.get("path"))

    ome = metadata.get("ome")
    if isinstance(ome, dict):
        series = ome.get("series")
        if isinstance(series, list):
            for item in series:
                series_name = first_text(
                    item.get("name"),
                    item.get("path"),
                    item.get("label"),
                ) if isinstance(item, dict) else first_text(item)
                if not series_name:
                    continue
                series_dir = path / Path(series_name)
                if series_dir.exists():
                    resolved = series_dir.resolve(strict=False)
                    if resolved not in seen:
                        seen.add(resolved)
                        preferred.append(series_dir)
                first_array_dir = find_first_zarr_array_dir(series_dir)
                if first_array_dir is not None:
                    resolved = first_array_dir.resolve(strict=False)
                    if resolved not in seen:
                        seen.add(resolved)
                        preferred.append(first_array_dir)

    return preferred


def first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def collect_tiff_paths(path: Path) -> list[Path]:
    if path.is_file() and path.suffix.lower() in TIFF_SUFFIXES:
        return [path]
    if not path.is_dir():
        return []
    files = [entry for entry in sorted(path.iterdir(), key=lambda value: value.name.lower()) if entry.is_file()]
    return [entry for entry in files if entry.suffix.lower() in TIFF_SUFFIXES]


def collect_legacy_image_paths(path: Path) -> list[Path]:
    if path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES:
        return [path]
    if not path.is_dir():
        return []
    try:
        files = sorted(
            (entry for entry in path.rglob("*") if entry.is_file() and entry.suffix.lower() in IMAGE_SUFFIXES),
            key=lambda value: value.relative_to(path).as_posix().lower(),
        )
    except OSError:
        return []
    return files


def infer_legacy_matlab_position_number(*, position: RawDatasetPosition, metadata: dict[str, Any]) -> int | None:
    if position.position_index is not None:
        return int(position.position_index) + 1

    for value in (
        metadata.get("position_index"),
        metadata.get("position"),
        metadata.get("index"),
        position.position_key,
        position.display_name,
    ):
        if value is None:
            continue
        match = re.search(r"(\d+)", str(value))
        if match:
            return int(match.group(1))
    return None


def matlab_escape(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


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


def build_multichannel_frame_sequence(
    records: list[TiffFrameRecord],
    *,
    load_frame_fn,
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
                    load_frame_fn=load_frame_fn,
                )
                for channel_key in channel_keys
            ]
            composed_frames.append(compose_channel_strip(channel_frames))
    else:
        max_count = max(len(channel_records) for channel_records in records_by_channel.values())
        selected_indices = sample_index_values(max_count, max_count=max_frames)
        for index in selected_indices:
            channel_frames = [
                load_channel_frame(
                    channel_records[min(index, len(channel_records) - 1)],
                    cache=cache,
                    load_frame_fn=load_frame_fn,
                )
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


def load_channel_frame(
    record: TiffFrameRecord,
    *,
    cache: dict[Path, np.ndarray],
    load_frame_fn,
) -> np.ndarray:
    cached = cache.get(record.path)
    if cached is not None:
        return cached
    array = np.asarray(load_frame_fn(record.path))
    frame = normalize_frame(reduce_array_to_frame(array))
    cache[record.path] = frame
    return frame


def load_tiff_frame(path: Path) -> np.ndarray:
    return np.asarray(tifffile.imread(path))


def open_best_zarr_node(path: Path):
    if path.is_file():
        raise ValueError(f"Expected a Zarr directory but got file {path}")
    try:
        return zarr.open(str(path), mode="r")
    except Exception as exc:
        raise ValueError(f"Unable to open Zarr path {path}: {exc}") from exc


def try_read_v3_ome_writers_preview_frames(
    target_path: Path,
    *,
    runtime_config: RawPreviewRuntimeConfig,
) -> PreviewSequence | None:
    if not target_path.is_dir():
        return None

    series_path = target_path
    array_path = find_first_zarr_array_dir(series_path)
    if array_path is None and (target_path / "zarr.json").is_file():
        array_metadata = json.loads((target_path / "zarr.json").read_text(encoding="utf-8"))
        if isinstance(array_metadata, dict) and str(array_metadata.get("node_type") or "").strip().lower() == "array":
            array_path = target_path
            series_path = target_path.parent

    if array_path is None:
        return None

    series_metadata = read_ome_zarr_group_metadata(series_path)
    frame_metadata = (series_metadata.get("ome_writers") or {}).get("frame_metadata") if isinstance(series_metadata, dict) else None
    if not isinstance(frame_metadata, list) or not frame_metadata:
        return None

    array_metadata = json.loads((array_path / "zarr.json").read_text(encoding="utf-8"))
    if not isinstance(array_metadata, dict) or str(array_metadata.get("node_type") or "").strip().lower() != "array":
        return None

    try:
        array_view = V3FrameMetadataArrayView(array_path=array_path, array_metadata=array_metadata, frame_metadata=frame_metadata)
    except Exception:
        return None

    channel_settings = extract_ome_zarr_channel_settings(series_metadata)
    channel_names = [
        str(item.get("channel") or item.get("label") or item.get("name") or "").strip()
        for item in channel_settings
        if isinstance(item, dict) and str(item.get("channel") or item.get("label") or item.get("name") or "").strip()
    ]
    channel_axis, time_axis = infer_v3_axes_roles(
        series_metadata=series_metadata,
        shape=array_view.shape,
        channel_count=len(channel_names),
    )
    if channel_axis is None:
        return None

    context_axes = [axis for axis in range(len(array_view.shape) - 2) if axis not in {channel_axis, time_axis}]
    grouped_records: dict[tuple[int, ...], dict[tuple[int, ...], dict[int, dict[str, Any]]]] = {}
    ordered_time_keys: list[tuple[int, ...]] = []
    for record in frame_metadata:
        if not isinstance(record, dict):
            continue
        storage_index = record.get("storage_index")
        if not isinstance(storage_index, list):
            continue
        if len(storage_index) != len(array_view.shape) - 2:
            continue
        try:
            storage_index_tuple = tuple(int(value) for value in storage_index)
        except (TypeError, ValueError):
            continue
        channel_index = storage_index_tuple[channel_axis]
        time_key = (storage_index_tuple[time_axis],) if time_axis is not None else ()
        context_key = tuple(storage_index_tuple[axis] for axis in context_axes)
        if time_key not in grouped_records:
            grouped_records[time_key] = {}
            ordered_time_keys.append(time_key)
        if context_key not in grouped_records[time_key]:
            grouped_records[time_key][context_key] = {}
        grouped_records[time_key][context_key][channel_index] = record

    if not ordered_time_keys:
        return None

    sample_count = resolve_frame_limit(total_count=len(ordered_time_keys), runtime_config=runtime_config)
    sampled_time_keys = [ordered_time_keys[index] for index in sample_index_values(len(ordered_time_keys), max_count=sample_count)]

    frames: list[np.ndarray] = []
    for time_key in sampled_time_keys:
        context_groups = grouped_records.get(time_key, {})
        if not context_groups:
            continue
        best_context_key = max(
            context_groups,
            key=lambda context_key: (
                len(context_groups[context_key]),
                -sum(
                    abs(float(value) - ((float(array_view.shape[axis]) - 1.0) / 2.0))
                    for axis, value in zip(context_axes, context_key)
                ),
            ),
        )
        channel_records = context_groups.get(best_context_key, {})
        selected_channel_indices = select_preview_channel_indices(shape=array_view.shape, channel_axis=channel_axis)
        if not selected_channel_indices:
            selected_channel_indices = sorted(channel_records)
        channel_frames: list[np.ndarray] = []
        for channel_index in selected_channel_indices:
            record = channel_records.get(channel_index)
            if record is None:
                channel_frames.append(np.zeros(array_view.shape[-2:], dtype=np.uint8))
                continue
            storage_index = record.get("storage_index")
            if not isinstance(storage_index, list):
                channel_frames.append(np.zeros(array_view.shape[-2:], dtype=np.uint8))
                continue
            frame = array_view.read_frame(storage_index)
            channel_frames.append(normalize_frame(frame))
        if not channel_frames:
            continue
        frames.append(compose_channel_strip(channel_frames) if len(channel_frames) > 1 else channel_frames[0])

    if not frames:
        return None
    inferred_channel_count = len(channel_names)
    if inferred_channel_count <= 0:
        inferred_channel_count = int(array_view.shape[channel_axis])
    selected_labels = [
        channel_label_for_index(channel_settings=channel_settings, channel_index=index)
        for index in select_preview_channel_indices(shape=array_view.shape, channel_axis=channel_axis)
    ]
    if not selected_labels:
        selected_labels = channel_names or [f"Channel {index + 1}" for index in range(inferred_channel_count)]
    return PreviewSequence(frames=frames, channel_labels=selected_labels)


def infer_v3_axes_roles(
    *,
    series_metadata: dict[str, Any],
    shape: tuple[int, ...],
    channel_count: int,
) -> tuple[int | None, int | None]:
    ome = series_metadata.get("ome") if isinstance(series_metadata, dict) else None
    multiscales = ome.get("multiscales") if isinstance(ome, dict) else series_metadata.get("multiscales")
    axes = None
    if isinstance(multiscales, list):
        first_multiscales = next((item for item in multiscales if isinstance(item, dict)), None)
        if isinstance(first_multiscales, dict):
            axes = first_multiscales.get("axes")

    channel_axis: int | None = None
    time_axis: int | None = None
    if isinstance(axes, list):
        for index, axis in enumerate(axes[: max(0, len(shape) - 2)]):
            if not isinstance(axis, dict):
                continue
            axis_name = str(axis.get("name") or "").strip().lower()
            axis_type = str(axis.get("type") or "").strip().lower()
            if axis_name == "c" or axis_type == "channel":
                channel_axis = index
            elif axis_name == "t" or axis_type == "time":
                time_axis = index
    if channel_axis is not None:
        return channel_axis, time_axis

    leading_shape = shape[:-2]
    candidates = [
        axis
        for axis, size in enumerate(leading_shape)
        if channel_count > 0 and int(size) == int(channel_count)
    ]
    if candidates:
        channel_axis = candidates[-1]
        if time_axis == channel_axis:
            time_axis = None
        return channel_axis, time_axis

    candidates = [axis for axis, size in enumerate(leading_shape) if 1 < int(size) <= 4]
    if candidates:
        channel_axis = candidates[-1]
        if time_axis == channel_axis:
            time_axis = None
        return channel_axis, time_axis
    return None, time_axis


@dataclass(frozen=True)
class V3FrameMetadataArrayView:
    array_path: Path
    array_metadata: dict[str, Any]
    frame_metadata: list[dict[str, Any]]

    def __post_init__(self) -> None:
        shape = tuple(int(value) for value in self.array_metadata.get("shape") or [])
        if len(shape) < 3:
            raise ValueError(f"Unsupported Zarr v3 preview shape for {self.array_path}: {shape}")
        chunk_grid = self.array_metadata.get("chunk_grid")
        if not isinstance(chunk_grid, dict):
            raise ValueError(f"Missing chunk grid for {self.array_path}")
        configuration = chunk_grid.get("configuration")
        if not isinstance(configuration, dict):
            raise ValueError(f"Missing chunk grid configuration for {self.array_path}")
        chunk_shape = tuple(int(value) for value in configuration.get("chunk_shape") or [])
        if len(chunk_shape) != len(shape):
            raise ValueError(f"Chunk shape mismatch for {self.array_path}: {chunk_shape} vs {shape}")
        object.__setattr__(self, "shape", shape)
        object.__setattr__(self, "ndim", len(shape))
        object.__setattr__(self, "chunk_shape", chunk_shape)
        data_type = str(self.array_metadata.get("data_type") or "").strip()
        if not data_type:
            raise ValueError(f"Missing data type for {self.array_path}")
        dtype = np.dtype(data_type)
        codecs = self.array_metadata.get("codecs")
        endian = "little"
        if isinstance(codecs, list):
            bytes_codec = next((codec for codec in codecs if isinstance(codec, dict) and codec.get("name") == "bytes"), None)
            if isinstance(bytes_codec, dict):
                endian = str((bytes_codec.get("configuration") or {}).get("endian") or "little").strip().lower()
        if endian == "little":
            dtype = dtype.newbyteorder("<")
        elif endian == "big":
            dtype = dtype.newbyteorder(">")
        object.__setattr__(self, "dtype", dtype)

    def read_frame(self, storage_index: list[Any]) -> np.ndarray:
        if self.chunk_shape[-2:] != self.shape[-2:]:
            raise NotImplementedError(f"Spatial chunking is not supported for {self.array_path}")
        leading_dims = len(self.shape) - 2
        if len(storage_index) != leading_dims:
            raise ValueError(f"Invalid storage_index for {self.array_path}: {storage_index}")
        chunk_path = self.array_path / "c"
        for index, chunk_size in zip(storage_index, self.chunk_shape[:leading_dims]):
            chunk_path /= str(int(index) // int(chunk_size))
        for _ in range(2):
            chunk_path /= "0"
        raw = chunk_path.read_bytes()
        frame = np.frombuffer(raw, dtype=self.dtype).reshape(self.shape[-2:])
        return frame


def try_open_v3_zarr_array(path: Path):
    metadata_path = path / "zarr.json"
    if not metadata_path.is_file():
        return None
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(metadata, dict):
        return None
    if str(metadata.get("zarr_format") or "").strip() != "3":
        try:
            if int(metadata.get("zarr_format")) != 3:
                return None
        except (TypeError, ValueError):
            return None
    if str(metadata.get("node_type") or "").strip().lower() != "array":
        return None

    return V3ZarrArrayView(path=path, metadata=metadata)


@dataclass(frozen=True)
class V3ZarrArrayView:
    path: Path
    metadata: dict[str, Any]

    def __post_init__(self) -> None:
        shape = tuple(int(value) for value in self.metadata.get("shape") or [])
        if len(shape) < 2:
            raise ValueError(f"Invalid Zarr v3 shape for {self.path}: {shape}")
        chunk_grid = self.metadata.get("chunk_grid")
        if not isinstance(chunk_grid, dict):
            raise ValueError(f"Invalid Zarr v3 chunk grid for {self.path}")
        configuration = chunk_grid.get("configuration")
        if not isinstance(configuration, dict):
            raise ValueError(f"Invalid Zarr v3 chunk configuration for {self.path}")
        chunk_shape = tuple(int(value) for value in configuration.get("chunk_shape") or [])
        if len(chunk_shape) != len(shape):
            raise ValueError(f"Chunk shape mismatch for {self.path}: {chunk_shape} vs {shape}")
        object.__setattr__(self, "shape", shape)
        object.__setattr__(self, "ndim", len(shape))
        object.__setattr__(self, "chunk_shape", chunk_shape)
        data_type = str(self.metadata.get("data_type") or "").strip()
        if not data_type:
            raise ValueError(f"Missing Zarr v3 data type for {self.path}")
        dtype = np.dtype(data_type)
        codecs = self.metadata.get("codecs")
        if isinstance(codecs, list):
            bytes_codec = next((codec for codec in codecs if isinstance(codec, dict) and codec.get("name") == "bytes"), None)
        else:
            bytes_codec = None
        endian = "little"
        if isinstance(bytes_codec, dict):
            endian = str((bytes_codec.get("configuration") or {}).get("endian") or "little").strip().lower()
        if endian == "little":
            dtype = dtype.newbyteorder("<")
        elif endian == "big":
            dtype = dtype.newbyteorder(">")
        object.__setattr__(self, "dtype", dtype)

    def __getitem__(self, item):
        if not isinstance(item, tuple):
            item = (item,)
        leading_dims = len(self.shape) - 2
        if len(item) != leading_dims:
            raise ValueError(f"Expected {leading_dims} leading indices for {self.path}, got {len(item)}")
        if self.chunk_shape[-2:] != self.shape[-2:]:
            raise NotImplementedError(f"Spatial chunking is not supported for {self.path}")
        chunk_coords = [int(index) // int(chunk_size) for index, chunk_size in zip(item, self.chunk_shape[:-2])]
        chunk_path = self.path / "c"
        for coord in chunk_coords:
            chunk_path /= str(coord)
        for _ in range(2):
            chunk_path /= "0"
        raw = chunk_path.read_bytes()
        frame = np.frombuffer(raw, dtype=self.dtype).reshape(self.chunk_shape[-2:])
        return frame


def find_first_zarr_array_dir(path: Path) -> Path | None:
    if not path.exists() or not path.is_dir():
        return None
    try:
        children = sorted((entry for entry in path.iterdir() if entry.is_dir()), key=lambda entry: entry.name.lower())
    except OSError:
        return None
    for child in children:
        if is_zarr_array_dir(child):
            return child
    for child in children:
        nested = find_first_zarr_array_dir(child)
        if nested is not None:
            return nested
    return None


def is_zarr_array_dir(path: Path) -> bool:
    if (path / ".zarray").is_file():
        return True
    metadata_path = path / "zarr.json"
    if not metadata_path.is_file():
        return False
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(metadata, dict):
        return False
    node_type = str(metadata.get("node_type") or "").strip().lower()
    if node_type:
        return node_type == "array"
    attributes = metadata.get("attributes")
    if isinstance(attributes, dict) and any(key in attributes for key in ("ome", "multiscales", "omero")):
        return False
    if "shape" in metadata or "chunk_grid" in metadata or "data_type" in metadata:
        return True
    return not metadata


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
        return compose_channel_composite([normalize_frame(channel_frame) for channel_frame in channel_frames])
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
        min_dimension = min(height, width)
        compact_overlay = min_dimension < 512
        desired_scale = preferred_overlay_scale(width=width, height=height)
        margin = preferred_overlay_margin(width=width, height=height, scale=desired_scale)
        line_gap = preferred_overlay_gap(width=width, height=height, scale=desired_scale)
        top_row_count = 1 + (1 if overlay_channel_labels else 0)
        bottom_row_count = (1 if overlay_position_label else 0) + (1 if overlay_project_label else 0)
        layout_scale = fit_overlay_layout_scale(
            frame_height=height,
            desired_scale=desired_scale,
            top_row_count=top_row_count,
            bottom_row_count=bottom_row_count,
            margin=margin,
            line_gap=line_gap,
        )
        margin = preferred_overlay_margin(width=width, height=height, scale=layout_scale)
        line_gap = preferred_overlay_gap(width=width, height=height, scale=layout_scale)
        label_scale = layout_scale
        title_scale = layout_scale
        available_width = max(16, width - (margin * 2))
        frame_label = f"F{index + 1}" if compact_overlay else f"FRAME {index + 1}"
        frame_text, frame_scale = fit_overlay_text(
            frame_label,
            available_width=available_width,
            desired_scale=layout_scale,
        )
        draw_text_with_box(canvas, frame_text, x=margin, y=margin, scale=frame_scale)
        next_top_y = margin + boxed_text_height(frame_scale) + line_gap
        if overlay_project_label:
            project_candidate = overlay_project_label[:18] if compact_overlay else overlay_project_label[:48]
            project_text, project_scale = fit_overlay_text(
                project_candidate,
                available_width=available_width,
                desired_scale=title_scale,
            )
            project_y = bottom_aligned_text_y(height - margin, project_scale)
            draw_text_with_box(
                canvas,
                project_text,
                x=margin,
                y=project_y,
                scale=project_scale,
            )
            next_bottom_y = max(margin, box_top_y(project_y) - line_gap)
        else:
            project_scale = title_scale
            next_bottom_y = height - margin
        if overlay_position_label:
            position_candidate = f"P{overlay_position_label[:10]}" if compact_overlay else f"POS {overlay_position_label[:28]}"
            position_text, position_scale = fit_overlay_text(
                position_candidate,
                available_width=available_width,
                desired_scale=title_scale,
            )
            position_y = bottom_aligned_text_y(next_bottom_y, position_scale)
            draw_text_with_box(
                canvas,
                position_text,
                x=margin,
                y=max(margin, position_y),
                scale=position_scale,
            )
        if overlay_channel_labels:
            panel_width = max(1, canvas.shape[1] // len(overlay_channel_labels))
            for channel_index, label in enumerate(overlay_channel_labels):
                channel_text = f"CH{channel_index + 1}" if compact_overlay else label
                channel_text, channel_scale = fit_overlay_text(
                    channel_text,
                    available_width=max(12, panel_width - (margin * 2)),
                    desired_scale=label_scale,
                )
                draw_text_with_box(
                    canvas,
                    channel_text,
                    x=channel_index * panel_width + margin,
                    y=next_top_y,
                    scale=channel_scale,
                )
        annotated.append(canvas)
    return annotated


def preferred_overlay_scale(*, width: int, height: int) -> int:
    min_dimension = min(width, height)
    if min_dimension < 640:
        return 1
    return max(1, min(3, min_dimension // 384))


def preferred_overlay_margin(*, width: int, height: int, scale: int) -> int:
    min_dimension = min(width, height)
    return max(3, min(12, min_dimension // 96, int(scale) * 3))


def preferred_overlay_gap(*, width: int, height: int, scale: int) -> int:
    min_dimension = min(width, height)
    return max(3, min(8, min_dimension // 128, int(scale) * 3))


def fit_overlay_layout_scale(
    *,
    frame_height: int,
    desired_scale: int,
    top_row_count: int,
    bottom_row_count: int,
    margin: int,
    line_gap: int,
) -> int:
    for scale in range(max(1, int(desired_scale)), 0, -1):
        row_height = boxed_text_height(scale)
        top_height = overlay_stack_height(top_row_count, row_height=row_height, line_gap=line_gap)
        bottom_height = overlay_stack_height(bottom_row_count, row_height=row_height, line_gap=line_gap)
        if top_height + bottom_height + (margin * 2) + line_gap <= frame_height:
            return scale
    return 1


def overlay_stack_height(row_count: int, *, row_height: int, line_gap: int) -> int:
    if row_count <= 0:
        return 0
    return (row_count * row_height) + ((row_count - 1) * line_gap)


def fit_overlay_text(text: str, *, available_width: int, desired_scale: int) -> tuple[str, int]:
    scale = fit_text_scale(
        text,
        available_width=max(1, available_width),
        desired_scale=max(1, desired_scale),
    )
    return truncate_text_to_width(text, available_width=available_width, scale=scale), scale


def truncate_text_to_width(text: str, *, available_width: int, scale: int) -> str:
    clean = str(text or "").strip()
    if not clean or text_pixel_width(clean, scale=scale) <= available_width:
        return clean
    result = clean
    while result and text_pixel_width(result, scale=scale) > available_width:
        result = result[:-1].rstrip()
    return result


def boxed_text_height(scale: int) -> int:
    return (7 * max(1, int(scale))) + 4


def box_top_y(text_y: int) -> int:
    return max(0, int(text_y) - 2)


def bottom_aligned_text_y(bottom_edge: int, scale: int) -> int:
    return max(0, int(bottom_edge) - 2 - (7 * max(1, int(scale))))


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


def compose_channel_composite(channel_frames: list[np.ndarray]) -> np.ndarray:
    if not channel_frames:
        raise ValueError("Cannot compose an empty channel composite")
    target_height = max(frame.shape[0] for frame in channel_frames)
    target_width = max(frame.shape[1] for frame in channel_frames)
    padded = [
        pad_frame(frame, target_height=target_height, target_width=target_width).astype(np.float32, copy=False)
        for frame in channel_frames
    ]
    if len(padded) == 1:
        return padded[0].astype(np.uint8)
    composite = np.maximum.reduce(padded)
    return np.clip(composite, 0, 255).astype(np.uint8)


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
    frame_count: int,
    source_width: int,
    source_height: int,
    encoded_width: int,
    encoded_height: int,
    channel_labels: list[str],
    project_label: str,
    runtime_config: RawPreviewRuntimeConfig,
) -> Artifact:
    fps_value = max(1, runtime_config.fps)
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
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "frame_count": frame_count,
        "width": int(encoded_width),
        "height": int(encoded_height),
        "fps": fps_value,
        "frame_mode": runtime_config.frame_mode,
        "max_frames_setting": runtime_config.max_frames,
        "max_dimension": int(runtime_config.max_dimension),
        "duration_seconds": duration_seconds,
        "file_size_bytes": file_size_bytes,
        "bitrate_kbps": bitrate_kbps,
        "binning_factor": max(1, runtime_config.binning_factor),
        "crf": int(runtime_config.crf),
        "preset": str(runtime_config.preset or "medium"),
        "ffmpeg_command": str(runtime_config.ffmpeg_command or ""),
        "preview_quality_config_version": 1,
        "format": "mp4",
        "absolute_path": str(video_path),
        "channel_labels": channel_labels,
        "project_label": project_label,
        "source_width": int(source_width),
        "source_height": int(source_height),
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
