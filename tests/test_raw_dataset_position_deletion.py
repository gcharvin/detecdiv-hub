from api.models import RawDataset, RawDatasetLocation, StorageRoot
from api.services.raw_dataset_position_deletion import (
    recalculate_raw_dataset_total_bytes,
    update_raw_dataset_after_position_deletion,
)


def test_position_deletion_catalog_refresh_updates_size_and_metadata_count(tmp_path):
    dataset_dir = tmp_path / "W80_BY_W303_Q10_1"
    dataset_dir.mkdir()
    (dataset_dir / "remaining.tif").write_bytes(b"12345")

    storage_root = StorageRoot(
        name="test-raw-root",
        root_type="raw_root",
        host_scope="server",
        path_prefix=str(tmp_path),
    )
    location = RawDatasetLocation(
        storage_root=storage_root,
        relative_path=dataset_dir.name,
        access_mode="readwrite",
        is_preferred=True,
    )
    raw_dataset = RawDataset(
        acquisition_label="W80_BY_W303_Q10_1",
        data_format="tiff_sequence",
        total_bytes=468_370_000_000,
        metadata_json={
            "dimensions": {"position_count": 82, "frame_count": 1000},
            "Summary": {"Positions": 82},
        },
        locations=[location],
    )

    total_bytes = recalculate_raw_dataset_total_bytes(raw_dataset)
    update_raw_dataset_after_position_deletion(
        raw_dataset,
        total_bytes=total_bytes,
        remaining_position_count=6,
    )

    assert raw_dataset.total_bytes == 5
    assert raw_dataset.metadata_json["dimensions"]["position_count"] == 6
    assert raw_dataset.metadata_json["dimensions"]["frame_count"] == 1000
    assert raw_dataset.metadata_json["Summary"]["Positions"] == 6
    assert raw_dataset.last_size_scan_at is not None
