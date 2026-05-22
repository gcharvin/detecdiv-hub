import sys
from types import SimpleNamespace
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.routes_raw_datasets import raw_preview_metadata_matches_config


def test_raw_preview_metadata_matches_current_quality_config():
    config = SimpleNamespace(
        fps=10,
        frame_mode="full",
        max_frames=0,
        max_dimension=2048,
        binning_factor=2,
        crf=24,
        preset="medium",
    )

    assert raw_preview_metadata_matches_config(
        {
            "fps": 10,
            "frame_mode": "full",
            "max_frames_setting": 0,
            "max_dimension": 2048,
            "binning_factor": 2,
            "crf": 24,
            "preset": "medium",
        },
        config,
    )


def test_raw_preview_metadata_without_encoder_config_is_not_current_quality_sample():
    config = SimpleNamespace(
        fps=10,
        frame_mode="full",
        max_frames=0,
        max_dimension=2048,
        binning_factor=2,
        crf=24,
        preset="medium",
    )

    assert not raw_preview_metadata_matches_config(
        {
            "fps": 10,
            "frame_mode": "full",
            "max_frames_setting": 0,
            "binning_factor": 2,
        },
        config,
    )


def test_raw_preview_metadata_from_another_preset_is_not_current_quality_sample():
    config = SimpleNamespace(
        fps=10,
        frame_mode="full",
        max_frames=0,
        max_dimension=2048,
        binning_factor=2,
        crf=24,
        preset="medium",
    )

    assert not raw_preview_metadata_matches_config(
        {
            "fps": 10,
            "frame_mode": "full",
            "max_frames_setting": 0,
            "max_dimension": 2048,
            "binning_factor": 2,
            "crf": 24,
            "preset": "slow",
        },
        config,
    )
