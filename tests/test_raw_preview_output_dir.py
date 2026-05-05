import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.routes_raw_datasets import raw_dataset_position_summary_view
from worker.raw_preview_video import resolve_preview_output_dir


def test_resolve_preview_output_dir_defaults_inside_dataset_folder():
    raw_dataset = SimpleNamespace(
        external_key="raw_2026_04_16_yam853_switch_1_0001_041732f6ff",
        acquisition_label="2026_04_16_YAM853_switch_1_0001",
        id="00000000-0000-0000-0000-000000000000",
    )

    output_dir = resolve_preview_output_dir(
        raw_dataset=raw_dataset,
        dataset_path=Path(r"X:\Florian\Microscopy\2026_04_16_YAM853_switch_1_0001"),
        runtime_config=SimpleNamespace(
            fps=6,
            frame_mode="full",
            max_frames=0,
            max_dimension=768,
            binning_factor=4,
            crf=24,
            preset="medium",
            include_existing=False,
            artifact_root=None,
            ffmpeg_command="",
        ),
    )

    assert output_dir == Path(
        r"X:\Florian\Microscopy\2026_04_16_YAM853_switch_1_0001\.detecdiv-previews\raw_2026_04_16_yam853_switch_1_0001_041732f6ff"
    )


def test_resolve_preview_output_dir_honors_configured_root():
    raw_dataset = SimpleNamespace(
        external_key="raw_2026_04_16_yam853_switch_1_0001_041732f6ff",
        acquisition_label="2026_04_16_YAM853_switch_1_0001",
        id="00000000-0000-0000-0000-000000000000",
    )

    output_dir = resolve_preview_output_dir(
        raw_dataset=raw_dataset,
        dataset_path=Path(r"X:\Florian\Microscopy\2026_04_16_YAM853_switch_1_0001"),
        runtime_config=SimpleNamespace(
            fps=6,
            frame_mode="full",
            max_frames=0,
            max_dimension=768,
            binning_factor=4,
            crf=24,
            preset="medium",
            include_existing=False,
            artifact_root=r"X:\tmp\previews",
            ffmpeg_command="",
        ),
    )

    assert output_dir == Path(r"X:\tmp\previews\raw_2026_04_16_yam853_switch_1_0001_041732f6ff")


def test_raw_dataset_position_summary_includes_preview_version():
    summary = raw_dataset_position_summary_view(
        SimpleNamespace(
            id="11111111-1111-1111-1111-111111111111",
            raw_dataset_id="22222222-2222-2222-2222-222222222222",
            position_key="Pos0",
            display_name="Pos0",
            position_index=0,
            status="indexed",
            preview_status="ready",
            metadata_json={},
            created_at=datetime(2026, 4, 29, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 29, tzinfo=timezone.utc),
            preview_artifact=SimpleNamespace(
                id="33333333-3333-3333-3333-333333333333",
                job_id="44444444-4444-4444-4444-444444444444",
                artifact_kind="raw_position_preview_mp4",
                uri=r"X:\Florian\Microscopy\dataset\.detecdiv-previews\raw_dataset\Pos0.mp4",
                metadata_json={"absolute_path": "/data/Florian/Microscopy/dataset/.detecdiv-previews/raw_dataset/Pos0.mp4"},
                created_at=datetime(2026, 4, 29, 16, 29, tzinfo=timezone.utc),
                job=SimpleNamespace(finished_at=datetime(2026, 4, 29, 16, 30, tzinfo=timezone.utc)),
            ),
            preview_artifact_id=None,
        )
    )

    assert summary.preview_artifact is not None
    assert summary.preview_artifact.job_finished_at == datetime(2026, 4, 29, 16, 30, tzinfo=timezone.utc)
