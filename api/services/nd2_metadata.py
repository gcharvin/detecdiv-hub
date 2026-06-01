from __future__ import annotations

import re
from dataclasses import is_dataclass, asdict
from pathlib import Path
from typing import Any


def read_nd2_dataset_metadata(dataset_dir: Path) -> dict[str, Any]:
    nd2_files = find_nd2_files(dataset_dir)
    if not nd2_files:
        return {}

    primary_file = pick_primary_nd2_file(nd2_files)
    try:
        import nd2
    except ImportError as exc:
        raise RuntimeError("ND2 metadata extraction requires the nd2[legacy] package.") from exc

    with nd2.ND2File(primary_file) as nd2_file:
        sizes = {str(key): int(value) for key, value in dict(nd2_file.sizes).items()}
        shape = [int(value) for value in tuple(nd2_file.shape)]
        channels = extract_channels(nd2_file)
        positions = extract_positions(nd2_file)
        loops = extract_loops(nd2_file)
        text_info = getattr(nd2_file, "text_info", None)
        text_channel_settings = extract_text_channel_settings(text_info)
        channel_settings = merge_nd2_channel_settings(
            channels=channels,
            text_channel_settings=text_channel_settings,
            loops=loops,
        )
        voxel_size = safe_public_object(nd2_file.voxel_size())
        file_stat = primary_file.stat()
        dimensions = {
            "frame_count": sizes.get("T", 0),
            "position_count": sizes.get("P", len(positions)),
            "channel_count": sizes.get("C", len(channels)),
            "slice_count": sizes.get("Z", 1),
            "width": sizes.get("X", 0),
            "height": sizes.get("Y", 0),
            "channel_names": [item["name"] for item in channels if item.get("name")],
        }
        time_loop = next((loop for loop in loops if loop.get("type") == "TimeLoop"), None)
        if isinstance(time_loop, dict):
            interval_ms = safe_number(time_loop.get("period_ms"))
            if interval_ms is not None:
                dimensions["interval_ms"] = interval_ms
                dimensions["interval_seconds"] = round(float(interval_ms) / 1000.0, 6)
        if channel_settings:
            dimensions["channel_settings"] = channel_settings
        metadata = {
            "source_file": primary_file.name,
            "source_path": str(primary_file),
            "file_size_bytes": int(file_stat.st_size),
            "is_legacy": bool(getattr(nd2_file, "is_legacy", False)),
            "version": safe_string(getattr(nd2_file, "version", None)),
            "ndim": int(getattr(nd2_file, "ndim", len(shape))),
            "shape": shape,
            "sizes": sizes,
            "dtype": str(getattr(nd2_file, "dtype", "")),
            "nbytes": int(getattr(nd2_file, "nbytes", 0) or 0),
            "dimensions": dimensions,
            "channels": channels,
            "loops": loops,
            "positions": positions,
            "voxel_size": voxel_size,
            "text_info": compact_text_info(text_info),
        }
        if len(nd2_files) > 1:
            metadata["files"] = [
                {"name": path.name, "path": str(path), "size_bytes": int(path.stat().st_size)}
                for path in nd2_files
            ]
        return metadata


def find_nd2_files(dataset_dir: Path) -> list[Path]:
    try:
        return sorted(
            (entry for entry in dataset_dir.iterdir() if entry.is_file() and entry.name.lower().endswith(".nd2")),
            key=lambda path: path.name.lower(),
        )
    except OSError:
        return []


def pick_primary_nd2_file(nd2_files: list[Path]) -> Path:
    return max(nd2_files, key=lambda path: path.stat().st_size)


def extract_channels(nd2_file: Any) -> list[dict[str, Any]]:
    channels = []
    metadata = getattr(nd2_file, "metadata", None)
    for index, item in enumerate(getattr(metadata, "channels", None) or []):
        channel = getattr(item, "channel", None)
        microscope = getattr(item, "microscope", None)
        volume = getattr(item, "volume", None)
        channels.append(
            {
                "index": int(getattr(channel, "index", index) or index),
                "name": safe_string(getattr(channel, "name", None)) or f"channel_{index + 1}",
                "color": safe_public_object(getattr(channel, "color", None)),
                "emission_lambda_nm": safe_number(getattr(channel, "emissionLambdaNm", None)),
                "excitation_lambda_nm": safe_number(getattr(channel, "excitationLambdaNm", None)),
                "microscope": compact_dict(
                    {
                        "objective_magnification": safe_number(
                            getattr(microscope, "objectiveMagnification", None)
                        ),
                        "objective_name": safe_string(getattr(microscope, "objectiveName", None)),
                        "objective_numerical_aperture": safe_number(
                            getattr(microscope, "objectiveNumericalAperture", None)
                        ),
                        "zoom_magnification": safe_number(getattr(microscope, "zoomMagnification", None)),
                        "modality_flags": safe_public_object(getattr(microscope, "modalityFlags", None)),
                    }
                ),
                "volume": compact_dict(
                    {
                        "axes_calibration": safe_public_object(getattr(volume, "axesCalibration", None)),
                        "bits_per_component": safe_number(
                            getattr(volume, "bitsPerComponentSignificant", None)
                        ),
                        "voxel_count": safe_public_object(getattr(volume, "voxelCount", None)),
                    }
                ),
            }
        )
    return channels


def merge_nd2_channel_settings(
    *,
    channels: list[dict[str, Any]],
    text_channel_settings: list[dict[str, Any]],
    loops: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    interval_ms = None
    time_loop = next((loop for loop in loops if loop.get("type") == "TimeLoop"), None)
    if isinstance(time_loop, dict):
        interval_ms = safe_number(time_loop.get("period_ms"))

    settings = []
    for index, channel in enumerate(channels):
        channel_name = str(channel.get("name") or f"Channel {index + 1}")
        detail = {
            "index": index,
            "channel": channel_name,
        }
        text_detail = find_channel_text_detail(text_channel_settings, channel_name=channel_name, index=index)
        for key in (
            "exposure_ms",
            "binning",
            "binning_factor",
            "led_power",
            "active_laser_line_nm",
            "laser_lines",
        ):
            value = text_detail.get(key) if text_detail else None
            if value not in (None, "", [], {}):
                detail[key] = value
        if interval_ms is not None:
            detail["interval_ms"] = interval_ms
            detail["interval_seconds"] = round(float(interval_ms) / 1000.0, 6)
        microscope = channel.get("microscope") if isinstance(channel.get("microscope"), dict) else {}
        for src_key, dst_key in (
            ("objective_name", "objective_name"),
            ("objective_magnification", "objective_magnification"),
            ("objective_numerical_aperture", "objective_numerical_aperture"),
        ):
            value = microscope.get(src_key) if isinstance(microscope, dict) else None
            if value not in (None, "", [], {}):
                detail[dst_key] = value
        settings.append(compact_dict(detail))
    return settings


def find_channel_text_detail(
    text_channel_settings: list[dict[str, Any]],
    *,
    channel_name: str,
    index: int,
) -> dict[str, Any]:
    for item in text_channel_settings:
        if int(item.get("index", -1)) == index:
            return item
    normalized_name = normalize_text_key(channel_name)
    for item in text_channel_settings:
        if normalize_text_key(str(item.get("channel") or "")) == normalized_name:
            return item
    return {}


def extract_text_channel_settings(text_info: Any) -> list[dict[str, Any]]:
    if not isinstance(text_info, dict):
        return []
    description = str(text_info.get("description") or "")
    if not description:
        return []
    plane_matches = list(re.finditer(r"Plane\s+#(?P<number>\d+):", description, flags=re.IGNORECASE))
    settings = []
    for match_index, match in enumerate(plane_matches):
        start = match.end()
        end = plane_matches[match_index + 1].start() if match_index + 1 < len(plane_matches) else len(description)
        block = description[start:end]
        plane_number = safe_number(match.group("number"))
        index = int(plane_number or (match_index + 1)) - 1
        detail = parse_nd2_plane_text_block(block)
        detail["index"] = index
        settings.append(compact_dict(detail))
    return settings


def parse_nd2_plane_text_block(block: str) -> dict[str, Any]:
    detail: dict[str, Any] = {}
    name_match = re.search(r"^\s*Name:\s*(?P<value>.+?)\s*$", block, flags=re.IGNORECASE | re.MULTILINE)
    if name_match:
        detail["channel"] = name_match.group("value").strip()
    exposure_match = re.search(
        r"Exposure:\s*(?P<value>[0-9]+(?:[\.,][0-9]+)?)\s*ms",
        block,
        flags=re.IGNORECASE,
    )
    if exposure_match:
        detail["exposure_ms"] = coerce_decimal_text(exposure_match.group("value"))
    binning_match = re.search(r"Binning:\s*(?P<x>\d+)\s*x\s*(?P<y>\d+)", block, flags=re.IGNORECASE)
    if binning_match:
        x_value = int(binning_match.group("x"))
        y_value = int(binning_match.group("y"))
        detail["binning"] = f"{x_value}x{y_value}"
        if x_value == y_value:
            detail["binning_factor"] = x_value

    laser_lines = []
    for line_match in re.finditer(
        r"Line:\s*(?P<line>\d+);\s*ExW:\s*(?P<wavelength>[0-9]+(?:[\.,][0-9]+)?);\s*Power:\s*(?P<power>[0-9]+(?:[\.,][0-9]+)?)(?P<active>\s*;\s*Active)?",
        block,
        flags=re.IGNORECASE,
    ):
        power = coerce_decimal_text(line_match.group("power"))
        wavelength = coerce_decimal_text(line_match.group("wavelength"))
        active = bool(line_match.group("active"))
        laser_lines.append(
            compact_dict(
                {
                    "line": int(line_match.group("line")),
                    "wavelength_nm": wavelength,
                    "power": power,
                    "active": active,
                }
            )
        )
    if laser_lines:
        detail["laser_lines"] = laser_lines
        active_lines = [item for item in laser_lines if item.get("active")]
        selected = active_lines[0] if active_lines else max(laser_lines, key=lambda item: float(item.get("power") or 0.0))
        if selected.get("power") is not None:
            detail["led_power"] = selected.get("power")
        if selected.get("wavelength_nm") is not None:
            detail["active_laser_line_nm"] = selected.get("wavelength_nm")
    return detail


def extract_positions(nd2_file: Any) -> list[dict[str, Any]]:
    for loop_index, loop in enumerate(getattr(nd2_file, "experiment", None) or []):
        if str(getattr(loop, "type", "")).lower() != "xyposloop":
            continue
        parameters = getattr(loop, "parameters", None)
        points = list(getattr(parameters, "points", None) or [])
        positions = []
        for index, point in enumerate(points):
            name = safe_string(getattr(point, "name", None)) or f"Pos{index + 1}"
            stage = getattr(point, "stagePositionUm", None)
            metadata_json = compact_dict(
                {
                    "source": "nd2_xy_position",
                    "nd2_position_index": index,
                    "nd2_loop_index": loop_index,
                    "stage_position_um": compact_dict(
                        {
                            "x": safe_number(getattr(stage, "x", None)),
                            "y": safe_number(getattr(stage, "y", None)),
                            "z": safe_number(getattr(stage, "z", None)),
                        }
                    ),
                    "pfs_offset": safe_number(getattr(point, "pfsOffset", None)),
                }
            )
            positions.append(
                {
                    "position_key": name,
                    "display_name": name,
                    "position_index": index,
                    "metadata_json": metadata_json,
                }
            )
        return positions
    position_count = int(dict(getattr(nd2_file, "sizes", {}) or {}).get("P", 0) or 0)
    return [
        {
            "position_key": f"Pos{index + 1}",
            "display_name": f"Pos{index + 1}",
            "position_index": index,
            "metadata_json": {"source": "nd2_sizes", "nd2_position_index": index},
        }
        for index in range(position_count)
    ]


def extract_loops(nd2_file: Any) -> list[dict[str, Any]]:
    loops = []
    for index, loop in enumerate(getattr(nd2_file, "experiment", None) or []):
        loop_type = safe_string(getattr(loop, "type", None))
        parameters = getattr(loop, "parameters", None)
        item = {
            "index": index,
            "type": loop_type,
            "count": safe_number(getattr(loop, "count", None)),
            "nesting_level": safe_number(getattr(loop, "nestingLevel", None)),
        }
        if loop_type == "TimeLoop":
            item["period_ms"] = safe_number(getattr(parameters, "periodMs", None))
            item["duration_ms"] = safe_number(getattr(parameters, "durationMs", None))
        elif loop_type == "XYPosLoop":
            item["position_count"] = len(getattr(parameters, "points", None) or [])
            item["is_setting_z"] = bool(getattr(parameters, "isSettingZ", False))
        elif loop_type == "ZStackLoop":
            item["parameters"] = safe_public_object(parameters)
        loops.append(compact_dict(item))
    return loops


def compact_text_info(text_info: Any) -> dict[str, Any]:
    if not isinstance(text_info, dict):
        return {}
    compact = {}
    for key in ("date", "optics", "capturing"):
        value = text_info.get(key)
        if value:
            compact[key] = safe_string(value, max_length=2000)
    description = text_info.get("description")
    if description:
        compact["description_excerpt"] = safe_string(description, max_length=4000)
    return compact


def compact_dict(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item not in (None, "", [], {})}


def safe_number(value: Any) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | float):
        return value
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric.is_integer():
        return int(numeric)
    return numeric


def coerce_decimal_text(value: str) -> int | float | None:
    return safe_number(str(value or "").replace(",", ".").strip())


def normalize_text_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def safe_string(value: Any, *, max_length: int | None = None) -> str | None:
    if value is None:
        return None
    text = str(value)
    if max_length is not None and len(text) > max_length:
        return text[:max_length] + "..."
    return text


def safe_public_object(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, dict):
        return compact_dict({str(key): safe_public_object(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return [safe_public_object(item) for item in value]
    if is_dataclass(value):
        return safe_public_object(asdict(value))
    if hasattr(value, "_asdict"):
        return safe_public_object(value._asdict())
    if hasattr(value, "__dict__"):
        public = {
            key: safe_public_object(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
        return compact_dict(public)
    return safe_string(value)
