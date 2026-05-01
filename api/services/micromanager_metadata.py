from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from api.services.ome_zarr_metadata import read_ome_zarr_metadata


MICROMANAGER_METADATA_TEXT_FILES = (
    "metadata.txt",
    "Metadata.txt",
    "Acquisition metadata.txt",
    "AcquisitionMetadata.txt",
)
MICROMANAGER_DISPLAY_SETTINGS_FILES = (
    "DisplaySettings.json",
    "DisplaySettings.txt",
    "displaysettings.json",
    "displaysettings.txt",
)


def read_micromanager_metadata(dataset_dir: Path) -> dict[str, Any]:
    metadata = read_micromanager_metadata_text(dataset_dir)
    ome_zarr_metadata = read_ome_zarr_metadata(dataset_dir)
    if ome_zarr_metadata:
        metadata = merge_metadata_dicts(metadata, ome_zarr_metadata)
    display_settings = read_micromanager_display_settings(dataset_dir)
    if display_settings:
        metadata = merge_metadata_dicts(
            metadata,
            {
                "display_settings": display_settings,
                "dimensions": extract_display_settings_dimensions(display_settings),
            },
        )
    metadata["dimensions"] = extract_acquisition_dimensions(metadata)
    return metadata


def read_micromanager_metadata_text(dataset_dir: Path) -> dict[str, Any]:
    metadata_path = find_first_micromanager_file(dataset_dir, MICROMANAGER_METADATA_TEXT_FILES)
    if metadata_path is None:
        return {}
    try:
        text_value = metadata_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            text_value = metadata_path.read_text(encoding="latin-1")
        except OSError:
            return {}
    except OSError:
        return {}

    stripped = text_value.strip()
    if not stripped:
        return {}
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            parsed["source_file"] = metadata_path.name
            parsed["source_path"] = str(metadata_path)
            return parsed
    except json.JSONDecodeError:
        return {"raw_text": stripped[:2000], "source_file": metadata_path.name, "source_path": str(metadata_path)}
    return {}


def read_micromanager_display_settings(dataset_dir: Path) -> dict[str, Any]:
    settings_path = find_first_micromanager_file(dataset_dir, MICROMANAGER_DISPLAY_SETTINGS_FILES)
    if settings_path is None:
        return {}
    try:
        payload = json.loads(settings_path.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        try:
            payload = json.loads(settings_path.read_text(encoding="latin-1"))
        except (OSError, json.JSONDecodeError):
            return {}
    except (OSError, json.JSONDecodeError):
        return {}
    normalized = normalize_micromanager_display_settings(payload)
    if normalized:
        normalized["source_file"] = settings_path.name
        normalized["source_path"] = str(settings_path)
        return normalized
    return {}


def normalize_micromanager_display_settings(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    if isinstance(payload.get("map"), dict):
        normalized = normalize_micromanager_property_map(payload["map"])
        normalized["encoding"] = payload.get("encoding")
        normalized["format"] = payload.get("format")
        normalized["major_version"] = payload.get("major_version")
        normalized["minor_version"] = payload.get("minor_version")
        return {key: value for key, value in normalized.items() if value not in (None, [], {})}
    return normalize_micromanager_property_map(payload)


def find_first_micromanager_file(dataset_dir: Path, file_names: tuple[str, ...]) -> Path | None:
    for file_name in file_names:
        candidate = dataset_dir / file_name
        if candidate.exists() and candidate.is_file():
            return candidate
    for file_name in file_names:
        try:
            candidate = next((path for path in dataset_dir.rglob(file_name) if path.is_file()), None)
        except (OSError, StopIteration):
            candidate = None
        if candidate is not None:
            return candidate
    return None


def find_micromanager_display_settings_path(dataset_dir: Path) -> Path | None:
    return find_first_micromanager_file(dataset_dir, MICROMANAGER_DISPLAY_SETTINGS_FILES)


def build_compact_micromanager_metadata(
    *,
    dataset_dir: Path,
    relative_path: str,
    source_label: str,
    parsed_metadata: dict[str, Any],
    data_format: str,
    source_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "source": source_label,
        "dataset_dir_abs": str(dataset_dir),
        "dataset_rel_from_root": relative_path,
        "data_format": data_format,
        "dimensions": extract_acquisition_dimensions(parsed_metadata),
    }

    if source_metadata:
        ingest_trace: dict[str, Any] = {}
        for key in ("file_count", "last_modified_at", "session_label", "session_date", "group_key", "group_label"):
            value = source_metadata.get(key)
            if value not in (None, "", [], {}):
                ingest_trace[key] = value
        if ingest_trace:
            metadata["ingest"] = ingest_trace

    return metadata


def normalize_micromanager_property_map(node: Any) -> Any:
    if isinstance(node, dict):
        if "scalar" in node and len(node) <= 2:
            return normalize_micromanager_property_map(node["scalar"])
        if "array" in node and len(node) <= 2:
            return [normalize_micromanager_property_map(item) for item in node.get("array") or []]
        if "map" in node and len(node) <= 2:
            return normalize_micromanager_property_map(node["map"])
        normalized: dict[str, Any] = {}
        for key, value in node.items():
            if key == "type":
                continue
            normalized[key] = normalize_micromanager_property_map(value)
        return normalized
    if isinstance(node, list):
        return [normalize_micromanager_property_map(item) for item in node]
    return node


def extract_display_settings_dimensions(display_settings: dict[str, Any]) -> dict[str, Any]:
    channel_settings = display_settings.get("ChannelSettings")
    channel_names: list[str] = []
    channel_details: list[dict[str, Any]] = []
    if isinstance(channel_settings, list):
        for index, item in enumerate(channel_settings):
            if not isinstance(item, dict):
                continue
            channel_name = first_non_empty_text(
                item.get("Channel"),
                item.get("ChannelName"),
                item.get("Name"),
                item.get("Label"),
            )
            if channel_name:
                channel_names.append(channel_name)
            exposure_ms = first_numeric_text(
                item.get("Exposure-ms"),
                item.get("ExposureMs"),
                item.get("Exposure time"),
                item.get("Exposure Time"),
                item.get("Exposure Time (ms)"),
                item.get("Exposure"),
            )
            led_power = first_numeric_text(
                item.get("LED power"),
                item.get("LED Power"),
                item.get("IlluminationPower"),
                item.get("LaserPower"),
                item.get("Power"),
                item.get("Intensity"),
            )
            interval_ms = first_numeric_text(
                item.get("Interval-ms"),
                item.get("IntervalMs"),
                item.get("Frame interval"),
                item.get("FrameInterval-ms"),
                item.get("Interval"),
            )
            frames = safe_int(item.get("Frames")) or safe_int(item.get("FrameCount"))
            channel_details.append(
                {
                    "index": index,
                    "channel": channel_name,
                    "channel_group": first_non_empty_text(item.get("ChannelGroup")),
                    "visible": item.get("Visible"),
                    "color": item.get("Color"),
                    "histogram_bit_depth": item.get("HistogramBitDepth"),
                    "uniform_component_scaling": item.get("UniformComponentScaling"),
                    "use_camera_bit_depth": item.get("UseCameraBitDepth"),
                    "exposure_ms": exposure_ms,
                    "led_power": led_power,
                    "interval_ms": interval_ms,
                    "frames": frames,
                }
            )

    playback_fps = coerce_number(display_settings.get("PlaybackFPS"))
    dimensions: dict[str, Any] = {
        "channel_count": len(channel_settings) if isinstance(channel_settings, list) else 0,
        "channel_names": channel_names,
        "channel_settings": channel_details,
        "playback_fps": playback_fps,
        "display_mode": first_non_empty_text(display_settings.get("ColorMode")),
        "uniform_channel_scaling": display_settings.get("UniformChannelScaling"),
        "autostretch": display_settings.get("Autostretch"),
        "roi_autoscale": display_settings.get("ROIAutoscale"),
    }
    return {key: value for key, value in dimensions.items() if value not in (None, [], {})}


def merge_metadata_dicts(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base or {})
    for key, value in (overlay or {}).items():
        if key in {"dimensions"} and isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_metadata_dicts(merged[key], value)
        elif key in {"display_settings"} and isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_metadata_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def extract_acquisition_dimensions(metadata_json: dict[str, Any]) -> dict[str, Any]:
    dimensions: dict[str, Any] = {}
    if isinstance(metadata_json, dict):
        raw_dimensions = metadata_json.get("dimensions")
        if isinstance(raw_dimensions, dict):
            dimensions.update(raw_dimensions)
    summary = metadata_json.get("Summary") if isinstance(metadata_json, dict) else None
    channel_names = []
    if isinstance(summary, dict):
        raw_channel_names = summary.get("ChNames")
        if isinstance(raw_channel_names, list):
            channel_names = [str(name).strip() for name in raw_channel_names if str(name).strip()]
            if channel_names:
                dimensions["channel_names"] = channel_names
    channel_settings = dimensions.get("channel_settings")
    if not isinstance(channel_settings, list):
        channel_settings = []
    if isinstance(summary, dict):
        if channel_names:
            dimensions["channel_count"] = len(channel_names)
        elif "channel_count" not in dimensions:
            dimensions["channel_count"] = safe_int(summary.get("Channels")) or 0
        dimensions["position_count"] = safe_int(summary.get("Positions")) or int(dimensions.get("position_count") or 0)
        dimensions["slice_count"] = safe_int(summary.get("Slices")) or int(dimensions.get("slice_count") or 0)
        dimensions["frame_count"] = safe_int(summary.get("Frames")) or int(dimensions.get("frame_count") or 0)
        interval_ms = first_numeric_text(
            summary.get("Interval-ms"),
            summary.get("IntervalMs"),
            summary.get("FrameInterval-ms"),
            summary.get("Frame interval"),
            summary.get("Interval"),
        )
        if interval_ms is not None:
            dimensions["interval_ms"] = interval_ms
            dimensions["interval_seconds"] = round(float(interval_ms) / 1000.0, 6)
        dimensions["width_px"] = safe_int(summary.get("Width")) or int(dimensions.get("width_px") or 0)
        dimensions["height_px"] = safe_int(summary.get("Height")) or int(dimensions.get("height_px") or 0)
        dimensions["pixel_type"] = summary.get("PixelType") or dimensions.get("pixel_type")
    merged_channel_settings = merge_channel_settings(
        base_settings=channel_settings,
        metadata_json=metadata_json if isinstance(metadata_json, dict) else {},
        channel_names=channel_names if isinstance(channel_names, list) else [],
    )
    if merged_channel_settings:
        dimensions["channel_settings"] = merged_channel_settings
    return {key: value for key, value in dimensions.items() if value not in (None, [], {})}


def safe_int(value: object) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def first_non_empty_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def coerce_number(value: Any) -> float | int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        numeric = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return int(numeric) if numeric.is_integer() else numeric


def first_numeric_text(*values: Any) -> float | int | None:
    for value in values:
        numeric = coerce_number(value)
        if numeric is not None:
            return numeric
    return None


def merge_channel_settings(
    *,
    base_settings: list[dict[str, Any]],
    metadata_json: dict[str, Any],
    channel_names: list[str],
) -> list[dict[str, Any]]:
    channel_details: dict[int, dict[str, Any]] = {}

    for index, item in enumerate(base_settings):
        if isinstance(item, dict):
            channel_details[index] = dict(item)

    if not channel_details and channel_names:
        for index, channel_name in enumerate(channel_names):
            channel_details[index] = {"index": index, "channel": channel_name}

    for key, value in metadata_json.items():
        if key in {"Summary", "display_settings", "dimensions", "source_file", "source_path"}:
            continue
        if not isinstance(value, dict):
            continue
        channel_index = infer_channel_index(key)
        if channel_index is None:
            channel_index = infer_channel_index_from_payload(value)
        if channel_index is None:
            continue
        detail = channel_details.setdefault(channel_index, {"index": channel_index})
        if channel_index < len(channel_names) and not detail.get("channel"):
            detail["channel"] = channel_names[channel_index]
        merge_channel_detail_from_payload(detail, value)

    ordered = [channel_details[index] for index in sorted(channel_details)]
    return ordered


CHANNEL_INDEX_PATTERNS = (
    re.compile(r"(?:^|[^a-z0-9])channel[_\-. ]*(\d+)", flags=re.IGNORECASE),
    re.compile(r"(?:^|[^a-z0-9])ch[_\-. ]*(\d+)", flags=re.IGNORECASE),
    re.compile(r"(?:^|[^a-z0-9])w(\d+)", flags=re.IGNORECASE),
    re.compile(r"(?:^|[^a-z0-9])img[_\-. ]*channel(\d+)", flags=re.IGNORECASE),
)


def infer_channel_index(text: str) -> int | None:
    normalized = str(text or "").strip()
    if not normalized:
        return None
    for pattern in CHANNEL_INDEX_PATTERNS:
        match = pattern.search(normalized)
        if match:
            try:
                return max(0, int(match.group(1)))
            except (TypeError, ValueError):
                return None
    return None


def infer_channel_index_from_payload(payload: dict[str, Any]) -> int | None:
    for key in ("ChannelIndex", "channel_index", "Channel", "channel"):
        value = payload.get(key)
        if isinstance(value, (int, float)) and int(value) >= 0:
            return int(value)
        if isinstance(value, str):
            maybe = infer_channel_index(value)
            if maybe is not None:
                return maybe
    return None


def merge_channel_detail_from_payload(detail: dict[str, Any], payload: dict[str, Any]) -> None:
    for key, value in payload.items():
        normalized = str(key or "").strip().lower()
        if normalized in {"type"}:
            continue
        if normalized in {"channel", "channelname", "name", "label"}:
            text = first_non_empty_text(value)
            if text:
                detail["channel"] = text
            continue
        if "exposure" in normalized:
            numeric = first_numeric_text(value)
            if numeric is not None:
                detail["exposure_ms"] = numeric
            continue
        if any(token in normalized for token in ("led", "power", "illumination", "laser", "intensity")):
            numeric = first_numeric_text(value)
            if numeric is not None:
                detail["led_power"] = numeric
            elif normalized in {"channel", "label"}:
                continue
            else:
                text = first_non_empty_text(value)
                if text:
                    detail.setdefault("led_power_text", text)
            continue
        if "interval" in normalized:
            numeric = first_numeric_text(value)
            if numeric is not None:
                detail["interval_ms"] = numeric
                detail["interval_seconds"] = round(float(numeric) / 1000.0, 6)
            continue
        if normalized in {"frames", "framecount", "frame_count"}:
            numeric = safe_int(value)
            if numeric is not None:
                detail["frames"] = numeric
            continue
        if isinstance(value, dict):
            merge_channel_detail_from_payload(detail, value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    merge_channel_detail_from_payload(detail, item)
