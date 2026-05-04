import json

from api.services.micromanager_metadata import extract_acquisition_dimensions, read_micromanager_metadata


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


def test_read_micromanager_metadata_extracts_channel_acquisition_details(tmp_path):
    display_payload = {
        "encoding": "UTF-8",
        "format": "Micro-Manager Property Map",
        "major_version": 2,
        "minor_version": 0,
        "map": {
            "ChannelSettings": {
                "type": "PROPERTY_MAP",
                "array": [
                    {"Channel": {"type": "STRING", "scalar": "Brightfield 10x"}},
                    {"Channel": {"type": "STRING", "scalar": "GFP"}},
                    {"Channel": {"type": "STRING", "scalar": "mCherry"}},
                ],
            }
        },
    }
    metadata_payload = {
        "Summary": {
            "ChNames": ["Brightfield 10x", "GFP", "mCherry"],
            "Channels": 3,
            "Positions": 20,
            "Frames": 288,
            "Interval-ms": 1500,
        },
        "Metadata-Pos0/img_channel000_position000_time000000000_z000.tif": {
            "Exposure-ms": 100.0,
            "LED Power": 15,
        },
        "Metadata-Pos0/img_channel001_position000_time000000000_z000.tif": {
            "Exposure-ms": 75.0,
            "LED Power": 25,
        },
        "Metadata-Pos0/img_channel002_position000_time000000000_z000.tif": {
            "Exposure-ms": 60.0,
            "LED Power": 35,
        },
    }

    (tmp_path / "DisplaySettings.json").write_text(json.dumps(display_payload), encoding="utf-8")
    (tmp_path / "metadata.txt").write_text(json.dumps(metadata_payload), encoding="utf-8")

    metadata = read_micromanager_metadata(tmp_path)

    dimensions = metadata["dimensions"]
    assert dimensions["frame_count"] == 288
    assert dimensions["interval_ms"] == 1500
    assert dimensions["interval_seconds"] == 1.5
    assert dimensions["channel_names"] == ["Brightfield 10x", "GFP", "mCherry"]
    assert len(dimensions["channel_settings"]) == 3
    assert dimensions["channel_settings"][0]["exposure_ms"] == 100.0
    assert dimensions["channel_settings"][1]["exposure_ms"] == 75.0
    assert dimensions["channel_settings"][2]["led_power"] == 35


def test_read_micromanager_metadata_extracts_ome_zarr_channels_and_positions(tmp_path):
    root_payload = {
        "zarr_format": 3,
        "node_type": "group",
        "attributes": {
            "ome": {"version": "0.5"},
            "multiscales": [
                {
                    "version": "0.5",
                    "axes": [
                        {"name": "c", "type": "channel"},
                        {"name": "y", "type": "space"},
                        {"name": "x", "type": "space"},
                    ],
                    "datasets": [{"path": "0"}],
                }
            ],
            "omero": {
                "channels": [
                    {
                        "label": "DAPI",
                        "color": "0000FF",
                        "window": {"min": 0, "max": 255, "start": 0, "end": 255},
                    },
                    {
                        "label": "GFP",
                        "color": "00FF00",
                        "window": {"min": 0, "max": 255, "start": 0, "end": 255},
                    },
                ]
            },
            "plate": {
                "wells": [
                    {
                        "path": "A/1",
                        "row": "A",
                        "column": "1",
                    }
                ]
            },
        },
    }
    well_payload = {
        "attributes": {
            "well": {
                "version": "0.5",
                "images": [
                    {"path": "0", "acquisition": 0},
                    {"path": "1", "acquisition": 0},
                ],
            }
        }
    }

    (tmp_path / "zarr.json").write_text(json.dumps(root_payload), encoding="utf-8")
    well_dir = tmp_path / "A" / "1"
    well_dir.mkdir(parents=True)
    (well_dir / "zarr.json").write_text(json.dumps(well_payload), encoding="utf-8")

    metadata = read_micromanager_metadata(tmp_path)

    assert metadata["source_file"] == "zarr.json"
    assert metadata["dimensions"]["channel_count"] == 2
    assert metadata["dimensions"]["channel_names"] == ["DAPI", "GFP"]
    assert metadata["dimensions"]["position_count"] == 2
    assert len(metadata["positions"]) == 2
    assert metadata["positions"][0]["metadata_json"]["row"] == "A"
    assert metadata["positions"][0]["metadata_json"]["column"] == "1"


def test_read_micromanager_metadata_extracts_ome_series_positions(tmp_path):
    root_payload = {
        "zarr_format": 3,
        "node_type": "group",
        "attributes": {
            "ome": {
                "version": "0.5",
                "series": ["Yam_1", "Yam_2", "Yak8_1"],
            }
        },
    }
    series_payload = {
        "zarr_format": 3,
        "node_type": "array",
        "shape": [192, 3, 3, 1024, 1024],
        "dimension_names": ["t", "c", "z", "y", "x"],
    }

    (tmp_path / "zarr.json").write_text(json.dumps(root_payload), encoding="utf-8")
    for series_name in ["Yam_1", "Yam_2", "Yak8_1"]:
        array_dir = tmp_path / series_name / "0"
        array_dir.mkdir(parents=True)
        (array_dir / "zarr.json").write_text(json.dumps(series_payload), encoding="utf-8")

    metadata = read_micromanager_metadata(tmp_path)

    dimensions = metadata["dimensions"]
    assert dimensions["position_count"] == 3
    assert dimensions["channel_count"] == 3
    assert dimensions["frame_count"] == 192
    assert dimensions["slice_count"] == 3
    assert dimensions["width_px"] == 1024
    assert dimensions["height_px"] == 1024
    assert [position["display_name"] for position in metadata["positions"]] == ["Yam_1", "Yam_2", "Yak8_1"]


def test_read_micromanager_metadata_parses_legacy_timelapse_id_file(tmp_path):
    (tmp_path / "sample-ID.txt").write_text(
        """
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%% Time-Lapse Assay ID File %%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

Created : 10-Apr-2018 15:50:51
Path : C:\\movies\\Basile\\180410\\sample\\
Filename : sample
Comments : legacy dataset

%%%%%%%%%%%%%%%%%%%%%% Time Lapse General Settings %%%%%%%%%%%%%
Movie type : Flow-Cell
Interval (s): 480
Number of frames: 450
Data saving mode :
Jpg 16 Bits

%%%%%%%%%%%%%%%%%%%%%% Time Lapse Channels %%%%%%%%%%%%%%%%%%%%%
Number of channels: 2
----- Channel1 --------
Imaging : 2 : TL_PH : Phase contrast
Filter Cube : 6 : Empty (PH+BF)
Exposure Time (s) : 0.05
Fluo excitation manager : 30%
Video Resolution : 1000  1000
----- Channel2 --------
Imaging : 3 : Fluo : fluorescence
Filter Cube : 2 : GFP
Exposure Time (s) : 0.35
Fluo excitation manager : 40%
Video Resolution : 500  500

%%%%%%%%%%%%%%%%%%%%%% Position List %%%%%%%%%%%%%%%%%%%%%%%%%%%
Number of positions: 2
----- Position1 --------
Name : PosA
ROI : 1     1  1002  1004
Number of Channels imaged : 2
Indices : 1
----- Position2 --------
Name : -
ROI : 1     1  1002  1004
Number of Channels imaged : 2
Indices : 1
""".strip(),
        encoding="utf-8",
    )

    metadata = read_micromanager_metadata(tmp_path)

    assert metadata["source_file"] == "sample-ID.txt"
    assert metadata["Summary"]["Channels"] == 2
    assert metadata["Summary"]["Positions"] == 2
    assert metadata["Summary"]["Frames"] == 450
    assert metadata["dimensions"]["interval_seconds"] == 480
    assert metadata["dimensions"]["interval_ms"] == 480000
    assert metadata["dimensions"]["channel_count"] == 2
    assert metadata["dimensions"]["position_count"] == 2
    assert metadata["dimensions"]["channel_settings"][0]["exposure_ms"] == 50
    assert metadata["dimensions"]["channel_settings"][1]["led_power"] == 40
    assert metadata["positions"][0]["display_name"] == "PosA"
    assert metadata["positions"][0]["roi"] == [1, 1, 1002, 1004]
