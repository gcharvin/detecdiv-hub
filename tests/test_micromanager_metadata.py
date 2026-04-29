import json

from api.services.micromanager_ingest import extract_acquisition_dimensions
from api.services.micromanager_metadata import read_micromanager_metadata


def test_read_micromanager_metadata_extracts_channels_and_playback(tmp_path):
    payload = {
        "encoding": "UTF-8",
        "format": "Micro-Manager Property Map",
        "major_version": 2,
        "minor_version": 0,
        "map": {
            "ChannelSettings": {
                "type": "PROPERTY_MAP",
                "array": [
                    {
                        "Channel": {"type": "STRING", "scalar": "Brightfield 10x"},
                        "Visible": {"type": "BOOLEAN", "scalar": True},
                    },
                    {
                        "Channel": {"type": "STRING", "scalar": "GFP"},
                        "Visible": {"type": "BOOLEAN", "scalar": True},
                    },
                    {
                        "Channel": {"type": "STRING", "scalar": "mCherry"},
                        "Visible": {"type": "BOOLEAN", "scalar": False},
                    },
                ],
            },
            "PlaybackFPS": {"type": "DOUBLE", "scalar": 12.5},
            "ColorMode": {"type": "STRING", "scalar": "GRAYSCALE"},
        },
    }
    (tmp_path / "DisplaySettings.json").write_text(json.dumps(payload), encoding="utf-8")

    metadata = read_micromanager_metadata(tmp_path)

    assert metadata["display_settings"]["source_file"] == "DisplaySettings.json"
    assert metadata["dimensions"]["channel_names"] == ["Brightfield 10x", "GFP", "mCherry"]
    assert metadata["dimensions"]["channel_count"] == 3
    assert metadata["dimensions"]["playback_fps"] == 12.5
    assert metadata["dimensions"]["display_mode"] == "GRAYSCALE"


def test_read_micromanager_metadata_falls_back_to_nested_metadata_file(tmp_path):
    nested_dir = tmp_path / "Pos0"
    nested_dir.mkdir()
    payload = {
        "Summary": {
            "ChNames": ["Brightfield 10x"],
            "Channels": 1,
            "Positions": 8,
            "Frames": 288,
        },
        "Metadata-Pos0/img_channel000_position000_time000000000_z000.tif": {
            "Exposure-ms": 10.0,
            "PositionName": "Pos0",
        },
    }
    (nested_dir / "metadata.txt").write_text(json.dumps(payload), encoding="utf-8")

    metadata = read_micromanager_metadata(tmp_path)

    assert metadata["source_file"] == "metadata.txt"
    assert metadata["source_path"].endswith("Pos0\\metadata.txt") or metadata["source_path"].endswith("Pos0/metadata.txt")
    assert metadata["Summary"]["ChNames"] == ["Brightfield 10x"]
    assert metadata["dimensions"]["channel_names"] == ["Brightfield 10x"]
    assert metadata["dimensions"]["channel_count"] == 1
    assert metadata["dimensions"]["position_count"] == 8
    assert metadata["dimensions"]["frame_count"] == 288


def test_extract_acquisition_dimensions_preserves_display_settings_channels():
    metadata = {
        "dimensions": {
            "channel_names": ["Brightfield 10x", "GFP"],
            "channel_count": 2,
            "playback_fps": 10.0,
        },
        "Summary": {
            "ChNames": ["Brightfield 10x", "GFP"],
            "Positions": 8,
            "Frames": 24,
        },
    }

    dimensions = extract_acquisition_dimensions(metadata)

    assert dimensions["channel_names"] == ["Brightfield 10x", "GFP"]
    assert dimensions["channel_count"] == 2
    assert dimensions["position_count"] == 8
    assert dimensions["frame_count"] == 24
    assert dimensions["playback_fps"] == 10.0
