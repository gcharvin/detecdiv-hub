from pathlib import Path

from api.services.micromanager_metadata import build_compact_micromanager_metadata


def test_build_compact_micromanager_metadata_keeps_only_summary_fields(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    parsed_metadata = {
        "Summary": {
            "Prefix": "2026_04_16_YAM853_switch_1_0001",
            "Frames": 12,
            "Channels": 2,
            "Positions": 8,
        },
        "DisplaySettings": {
            "ChannelSettings": [
                {"Channel": "DAPI", "Exposure-ms": 25, "LED power": 18},
                {"Channel": "FITC", "Exposure-ms": 50, "LED power": 22},
            ],
            "ColorMode": "Composite",
        },
        "LargePayload": {"nested": {"deep": "value"}},
    }

    summary = build_compact_micromanager_metadata(
        dataset_dir=dataset_dir,
        relative_path="raw/2026_04_16_YAM853_switch_1_0001",
        source_label="micromanager_ingest",
        parsed_metadata=parsed_metadata,
        data_format="tiff_sequence",
        source_metadata={
            "file_count": 42,
            "last_modified_at": "2026-04-30T10:00:00+00:00",
            "session_label": "2026_04_16_YAM853",
            "session_date": "2026-04-16T00:00:00+00:00",
            "group_key": "group-1",
            "group_label": "Group 1",
        },
    )

    assert summary["source"] == "micromanager_ingest"
    assert summary["dataset_dir_abs"] == str(dataset_dir)
    assert summary["dataset_rel_from_root"] == "raw/2026_04_16_YAM853_switch_1_0001"
    assert summary["data_format"] == "tiff_sequence"
    assert summary["dimensions"]["channel_count"] == 2
    assert summary["dimensions"]["frame_count"] == 12
    assert summary["dimensions"]["position_count"] == 8
    assert summary["dimensions"]["channel_names"] == ["DAPI", "FITC"]
    assert summary["dimensions"]["channel_settings"][0]["channel"] == "DAPI"
    assert summary["ingest"]["file_count"] == 42
    assert "Summary" not in summary
    assert "DisplaySettings" not in summary
    assert "LargePayload" not in summary
    assert "display_settings_uri" not in summary
