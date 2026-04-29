from __future__ import annotations

import json
from pathlib import Path
from typing import Any


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
    if not isinstance(summary, dict):
        return {key: value for key, value in dimensions.items() if value not in (None, [], {})}
    channel_names = summary.get("ChNames")
    if isinstance(channel_names, list):
        dimensions["channel_count"] = len(channel_names)
        dimensions["channel_names"] = channel_names
    elif "channel_count" not in dimensions:
        dimensions["channel_count"] = safe_int(summary.get("Channels")) or 0
    dimensions["position_count"] = safe_int(summary.get("Positions")) or int(dimensions.get("position_count") or 0)
    dimensions["slice_count"] = safe_int(summary.get("Slices")) or int(dimensions.get("slice_count") or 0)
    dimensions["frame_count"] = safe_int(summary.get("Frames")) or int(dimensions.get("frame_count") or 0)
    dimensions["width_px"] = safe_int(summary.get("Width")) or int(dimensions.get("width_px") or 0)
    dimensions["height_px"] = safe_int(summary.get("Height")) or int(dimensions.get("height_px") or 0)
    dimensions["pixel_type"] = summary.get("PixelType") or dimensions.get("pixel_type")
    if isinstance(channel_names, list) and channel_names:
        dimensions["channel_names"] = channel_names
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
