from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def read_ome_zarr_metadata(dataset_dir: Path) -> dict[str, Any]:
    root_metadata = read_ome_zarr_group_metadata(dataset_dir)
    if not root_metadata:
        return {}

    positions = extract_ome_zarr_positions(dataset_dir, root_metadata)
    dimensions = extract_ome_zarr_dimensions(dataset_dir, root_metadata, positions=positions)

    metadata = dict(root_metadata)
    if positions:
        metadata["positions"] = positions
    if dimensions:
        metadata["dimensions"] = dimensions
    return metadata


def read_ome_zarr_group_metadata(group_dir: Path) -> dict[str, Any]:
    payload = read_json_payload(group_dir / "zarr.json")
    if payload:
        return flatten_zarr_metadata(payload, source_path=group_dir / "zarr.json")

    payload = read_json_payload(group_dir / ".zattrs")
    if payload:
        return flatten_zarr_metadata(payload, source_path=group_dir / ".zattrs")

    return {}


def flatten_zarr_metadata(payload: dict[str, Any], *, source_path: Path) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    metadata: dict[str, Any] = {}
    attributes = payload.get("attributes")
    if isinstance(attributes, dict):
        metadata.update(attributes)
        raw_payload = {key: value for key, value in payload.items() if key != "attributes"}
        if raw_payload:
            metadata["_zarr"] = raw_payload
    else:
        metadata.update(payload)

    metadata["source_file"] = source_path.name
    metadata["source_path"] = str(source_path)
    return metadata


def extract_ome_zarr_dimensions(
    dataset_dir: Path,
    metadata_json: dict[str, Any],
    *,
    positions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    dimensions: dict[str, Any] = {}

    primary_multiscales = first_dict(metadata_json.get("multiscales"))
    series_container_metadata, series_names = find_ome_series_container(dataset_dir, metadata_json)
    series_shape_axes = load_first_ome_series_shape_and_axes(dataset_dir, series_container_metadata, series_names)
    axes = normalize_axes(
        first_list(primary_multiscales.get("axes") if isinstance(primary_multiscales, dict) else None)
        or (series_shape_axes[1] if series_shape_axes is not None else [])
        or first_list(metadata_json.get("axes"))
    )
    if axes:
        dimensions["axes"] = axes

    channel_settings = extract_ome_zarr_channel_settings(metadata_json)
    if channel_settings:
        dimensions["channel_settings"] = channel_settings
        dimensions["channel_names"] = [
            str(item.get("channel") or "").strip()
            for item in channel_settings
            if str(item.get("channel") or "").strip()
        ]
        dimensions["channel_count"] = len(channel_settings)

    shape = (
        load_primary_array_shape(dataset_dir, primary_multiscales)
        if primary_multiscales
        else (series_shape_axes[0] if series_shape_axes is not None else None)
    )
    axis_sizes = map_axis_sizes(axes, shape)

    if "channel_count" not in dimensions:
        channel_count = axis_sizes.get("channel")
        if channel_count is not None and channel_count > 0:
            dimensions["channel_count"] = channel_count
            dimensions["channel_names"] = [f"Channel {index + 1}" for index in range(channel_count)]
            dimensions["channel_settings"] = [
                {"index": index, "channel": f"Channel {index + 1}"} for index in range(channel_count)
            ]
        elif primary_multiscales is not None:
            dimensions["channel_count"] = 1
            dimensions["channel_names"] = ["Channel 1"]
            dimensions["channel_settings"] = [{"index": 0, "channel": "Channel 1"}]

    if positions is not None:
        dimensions["position_count"] = len(positions)
    else:
        position_count = axis_sizes.get("position")
        if position_count is not None and position_count > 0:
            dimensions["position_count"] = position_count
        elif has_plate_or_well_positions(metadata_json):
            inferred_positions = extract_ome_zarr_positions(dataset_dir, metadata_json)
            if inferred_positions:
                dimensions["position_count"] = len(inferred_positions)
        elif primary_multiscales is not None:
            dimensions["position_count"] = 1

    time_count = axis_sizes.get("time")
    if time_count is not None and time_count > 0:
        dimensions["frame_count"] = time_count

    slice_count = axis_sizes.get("z")
    if slice_count is not None and slice_count > 0:
        dimensions["slice_count"] = slice_count

    width_px, height_px = infer_image_dimensions(axis_sizes)
    if width_px is not None:
        dimensions["width_px"] = width_px
    if height_px is not None:
        dimensions["height_px"] = height_px

    return {key: value for key, value in dimensions.items() if value not in (None, [], {})}


def extract_ome_zarr_positions(dataset_dir: Path, metadata_json: dict[str, Any]) -> list[dict[str, Any]]:
    positions = extract_ome_series_positions(dataset_dir, metadata_json)
    if positions:
        return positions

    positions = extract_plate_positions(dataset_dir, metadata_json)
    if positions:
        return positions

    positions = extract_well_positions(dataset_dir, metadata_json)
    if positions:
        return positions

    primary_multiscales = first_dict(metadata_json.get("multiscales"))
    axes = normalize_axes(
        first_list(primary_multiscales.get("axes") if isinstance(primary_multiscales, dict) else None)
        or first_list(metadata_json.get("axes"))
    )
    shape = load_primary_array_shape(dataset_dir, primary_multiscales) if primary_multiscales else None
    axis_sizes = map_axis_sizes(axes, shape)
    position_count = axis_sizes.get("position")
    if position_count is not None and position_count > 0:
        return [
            {
                "position_key": slugify_value(f"position_{index + 1}"),
                "display_name": f"Position {index + 1}",
                "position_index": index,
                "metadata_json": {
                    "source": "ome_zarr_axis",
                    "axis": "position",
                    "axis_index": index,
                },
            }
            for index in range(position_count)
        ]

    if primary_multiscales is not None:
        return [
            {
                "position_key": slugify_value(dataset_dir.name),
                "display_name": dataset_dir.name,
                "position_index": 0,
                "metadata_json": {
                    "source": "ome_zarr_image",
                    "path": ".",
                },
            }
        ]

    return []


def extract_ome_series_positions(dataset_dir: Path, metadata_json: dict[str, Any]) -> list[dict[str, Any]]:
    _, series = find_ome_series_container(dataset_dir, metadata_json)
    if not series:
        return []

    positions: list[dict[str, Any]] = []
    for index, item in enumerate(series):
        series_name = first_text(item.get("name"), item.get("path"), item.get("label")) if isinstance(item, dict) else first_text(item)
        if not series_name:
            continue
        positions.append(
            {
                "position_key": slugify_value(series_name),
                "display_name": series_name,
                "position_index": index,
                "metadata_json": {
                    "source": "ome_series",
                    "series_name": series_name,
                    "series_index": index,
                },
            }
        )
    return positions


def extract_plate_positions(dataset_dir: Path, metadata_json: dict[str, Any]) -> list[dict[str, Any]]:
    plate = metadata_json.get("plate")
    if not isinstance(plate, dict):
        return []

    wells = plate.get("wells")
    if not isinstance(wells, list):
        return []

    positions: list[dict[str, Any]] = []
    for well_index, well in enumerate(wells):
        if not isinstance(well, dict):
            continue
        well_path = first_text(
            well.get("path"),
            well.get("well_path"),
            well.get("name"),
            well.get("label"),
        )
        if not well_path:
            continue
        well_dir = dataset_dir / Path(well_path)
        well_metadata = read_ome_zarr_group_metadata(well_dir)
        well_info = well_metadata.get("well") if isinstance(well_metadata.get("well"), dict) else None
        images = well_info.get("images") if isinstance(well_info, dict) else None
        if not isinstance(images, list) or not images:
            positions.append(
                build_ome_zarr_position(
                    well_path=well_path,
                    image_path=None,
                    well=well,
                    image_index=0,
                    position_index=len(positions),
                    source="ome_zarr_plate_well",
                )
            )
            continue

        for image_index, image in enumerate(images):
            if not isinstance(image, dict):
                continue
            positions.append(
                build_ome_zarr_position(
                    well_path=well_path,
                    image_path=first_text(image.get("path"), image.get("name"), image.get("label")),
                    well=well,
                    image=image,
                    image_index=image_index,
                    position_index=len(positions),
                    source="ome_zarr_plate_fov",
                )
            )

    return positions


def extract_well_positions(dataset_dir: Path, metadata_json: dict[str, Any]) -> list[dict[str, Any]]:
    well = metadata_json.get("well")
    if not isinstance(well, dict):
        return []

    images = well.get("images")
    if not isinstance(images, list) or not images:
        return []

    positions: list[dict[str, Any]] = []
    for image_index, image in enumerate(images):
        if not isinstance(image, dict):
            continue
        positions.append(
            build_ome_zarr_position(
                well_path=dataset_dir.name,
                image_path=first_text(image.get("path"), image.get("name"), image.get("label")),
                well=well,
                image=image,
                image_index=image_index,
                position_index=len(positions),
                source="ome_zarr_well_fov",
            )
        )
    return positions


def build_ome_zarr_position(
    *,
    well_path: str,
    image_path: str | None,
    well: dict[str, Any],
    image_index: int,
    position_index: int,
    source: str,
    image: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = first_text(well.get("row"), well.get("row_name"), well.get("rowName"), well.get("well_row"))
    column = first_text(well.get("column"), well.get("column_name"), well.get("columnName"), well.get("well_column"))
    acquisition = first_text(well.get("acquisition"), image.get("acquisition") if isinstance(image, dict) else None)
    well_label = first_text(well.get("label"), well.get("name"))
    display_name = first_text(
        well_label,
        f"{row or ''}{column or ''}".strip() or None,
        well_path,
    )
    if image_path and image_path != "0":
        display_name = f"{display_name} / {image_path}" if display_name else image_path
    if not display_name:
        display_name = image_path or well_path or f"position_{position_index + 1}"

    position_key_parts = [part for part in (row, column, well_path, image_path) if part]
    if not position_key_parts:
        position_key_parts = [display_name]

    metadata_json: dict[str, Any] = {
        "source": source,
        "well_path": well_path,
        "image_index": image_index,
    }
    if row:
        metadata_json["row"] = row
    if column:
        metadata_json["column"] = column
    if well_label:
        metadata_json["well_label"] = well_label
    if acquisition:
        metadata_json["acquisition"] = acquisition
    if image_path:
        metadata_json["image_path"] = image_path
    if isinstance(image, dict):
        filtered_image = {
            key: value
            for key, value in image.items()
            if value not in (None, "", [], {})
        }
        if filtered_image:
            metadata_json["image"] = filtered_image

    return {
        "position_key": slugify_value("/".join(position_key_parts)),
        "display_name": display_name,
        "position_index": position_index,
        "metadata_json": metadata_json,
    }


def load_primary_array_shape(dataset_dir: Path, multiscales: dict[str, Any]) -> list[int] | None:
    datasets = multiscales.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        return None

    first_dataset = first_dict(datasets)
    if not first_dataset:
        return None

    dataset_path = first_text(first_dataset.get("path"))
    if not dataset_path:
        return None

    array_dir = dataset_dir / Path(dataset_path)
    payload = read_json_payload(array_dir / "zarr.json")
    if not payload:
        payload = read_json_payload(array_dir / ".zarray")
    if not isinstance(payload, dict):
        return None

    shape = payload.get("shape")
    if not isinstance(shape, list):
        return None

    coerced_shape: list[int] = []
    for value in shape:
        try:
            coerced_shape.append(int(value))
        except (TypeError, ValueError):
            return None
    return coerced_shape


def load_first_ome_series_shape_and_axes(
    dataset_dir: Path,
    metadata_json: dict[str, Any] | None,
    series: list[str | dict[str, Any]] | None = None,
) -> tuple[list[int] | None, list[dict[str, Any]] | None] | None:
    if metadata_json is None:
        return None

    if series is None:
        _, series = find_ome_series_container(dataset_dir, metadata_json)
    if not isinstance(series, list) or not series:
        return None

    first_series = series[0]
    series_name = first_text(first_series.get("name"), first_series.get("path"), first_series.get("label")) if isinstance(first_series, dict) else first_text(first_series)
    if not series_name:
        return None

    series_dir = dataset_dir / Path(series_name)
    array_dir = find_first_ome_array_dir(series_dir)
    if array_dir is None:
        return None

    payload = read_json_payload(array_dir / "zarr.json")
    if not payload:
        payload = read_json_payload(array_dir / ".zarray")
    if not isinstance(payload, dict):
        return None

    shape = payload.get("shape")
    if not isinstance(shape, list):
        return None

    coerced_shape: list[int] = []
    for value in shape:
        try:
            coerced_shape.append(int(value))
        except (TypeError, ValueError):
            return None

    axes: list[dict[str, Any]] = []
    dimension_names = payload.get("dimension_names")
    if isinstance(dimension_names, list):
        axes = normalize_axes(dimension_names)
    return coerced_shape, axes or None


def find_ome_series_container(
    dataset_dir: Path,
    metadata_json: dict[str, Any],
) -> tuple[dict[str, Any] | None, list[str | dict[str, Any]] | None]:
    root_ome = metadata_json.get("ome")
    if isinstance(root_ome, dict) and isinstance(root_ome.get("series"), list) and root_ome.get("series"):
        return metadata_json, root_ome.get("series")

    try:
        child_dirs = sorted((entry for entry in dataset_dir.iterdir() if entry.is_dir()), key=lambda entry: entry.name.lower())
    except OSError:
        return None, None

    for child in child_dirs:
        child_metadata = read_ome_zarr_group_metadata(child)
        child_ome = child_metadata.get("ome")
        if isinstance(child_ome, dict) and isinstance(child_ome.get("series"), list) and child_ome.get("series"):
            return child_metadata, child_ome.get("series")
    return None, None


def find_first_ome_array_dir(series_dir: Path) -> Path | None:
    if not series_dir.exists() or not series_dir.is_dir():
        return None
    try:
        children = sorted((entry for entry in series_dir.iterdir() if entry.is_dir()), key=lambda entry: entry.name.lower())
    except OSError:
        return None
    for child in children:
        if (child / "zarr.json").is_file() or (child / ".zarray").is_file():
            return child
    return None


def map_axis_sizes(axes: list[dict[str, Any]], shape: list[int] | None) -> dict[str, int]:
    if not axes or not shape or len(axes) != len(shape):
        return {}

    axis_sizes: dict[str, int] = {}
    for axis, size in zip(axes, shape):
        role = normalize_axis_role(axis)
        if role and role not in axis_sizes:
            axis_sizes[role] = size
    return axis_sizes


def infer_image_dimensions(axis_sizes: dict[str, int]) -> tuple[int | None, int | None]:
    width_px = axis_sizes.get("x")
    height_px = axis_sizes.get("y")
    return width_px, height_px


def extract_ome_zarr_channel_settings(metadata_json: dict[str, Any]) -> list[dict[str, Any]]:
    omero = metadata_json.get("omero")
    if isinstance(omero, dict):
        channels = omero.get("channels")
    else:
        channels = None
    if not isinstance(channels, list):
        return []

    channel_settings: list[dict[str, Any]] = []
    for index, channel in enumerate(channels):
        if not isinstance(channel, dict):
            continue
        channel_name = first_text(channel.get("label"), channel.get("name"), channel.get("channel"))
        window = channel.get("window") if isinstance(channel.get("window"), dict) else None
        detail: dict[str, Any] = {
            "index": index,
            "channel": channel_name or f"Channel {index + 1}",
        }
        if channel.get("color") not in (None, ""):
            detail["color"] = str(channel["color"]).strip()
        if isinstance(window, dict):
            normalized_window = {
                key: numeric_value
                for key in ("min", "max", "start", "end")
                if (numeric_value := coerce_number(window.get(key))) is not None
            }
            if normalized_window:
                detail["window"] = normalized_window
                for key, value in normalized_window.items():
                    detail[f"window_{key}"] = value
        channel_settings.append(detail)
    return channel_settings


def read_json_payload(path: Path) -> dict[str, Any]:
    try:
        text_value = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            text_value = path.read_text(encoding="latin-1")
        except OSError:
            return {}
    except OSError:
        return {}

    stripped = text_value.strip()
    if not stripped:
        return {}
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def first_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                return item
    elif isinstance(value, dict):
        return value
    return {}


def first_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def first_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def normalize_axes(axes: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for index, axis in enumerate(axes):
        if isinstance(axis, str):
            name = axis.strip()
            if not name:
                continue
            normalized.append({"name": name, "type": infer_axis_role(name) or "space"})
            continue
        if not isinstance(axis, dict):
            continue
        name = first_text(axis.get("name"), axis.get("label"), axis.get("key"))
        if not name:
            continue
        axis_type = infer_axis_role(name) or infer_axis_role(axis.get("type")) or "space"
        normalized_axis = dict(axis)
        normalized_axis["name"] = name
        normalized_axis["type"] = axis_type
        normalized.append(normalized_axis)
    return normalized


def normalize_axis_role(axis: dict[str, Any]) -> str | None:
    role = infer_axis_role(axis.get("type"))
    if role:
        return role
    return infer_axis_role(axis.get("name"))


def infer_axis_role(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"c", "ch", "channel", "channels"}:
        return "channel"
    if text in {"t", "time"}:
        return "time"
    if text in {"z"}:
        return "z"
    if text in {"x", "y"}:
        return text
    if text in {"position", "positions", "scene", "scenes", "fov", "fieldofview", "field_of_view", "well"}:
        return "position"
    return text if text in {"space", "channel", "time"} else None


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


def slugify_value(value: str) -> str:
    out: list[str] = []
    last_was_sep = False
    for ch in str(value or "").lower():
        if ch.isalnum():
            out.append(ch)
            last_was_sep = False
        elif not last_was_sep:
            out.append("_")
            last_was_sep = True
    slug = "".join(out).strip("_")
    return slug or "item"


def has_plate_or_well_positions(metadata_json: dict[str, Any]) -> bool:
    plate = metadata_json.get("plate")
    if isinstance(plate, dict) and isinstance(plate.get("wells"), list):
        return True
    well = metadata_json.get("well")
    return isinstance(well, dict) and isinstance(well.get("images"), list)
