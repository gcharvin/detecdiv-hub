from uuid import uuid4

from api.models import RawDataset, RawDatasetLocation, RawDatasetPosition, StorageRoot
from api.services.raw_dataset_position_deletion import (
    POSITION_STATUS_DELETION_QUEUED,
    RawDatasetPositionDeletionPreviewData,
    parse_position_ids,
    queue_raw_dataset_position_deletion,
    recalculate_raw_dataset_total_bytes,
    update_raw_dataset_after_position_deletion,
)


class FakeScalarResult:
    def __init__(self, values):
        self.values = values

    def __iter__(self):
        return iter(self.values)


class FakeSession:
    def __init__(self, scalars_result=None):
        self.added = []
        self.flushed = False
        self.scalars_result = scalars_result or []

    def add(self, item):
        self.added.append(item)

    def flush(self):
        self.flushed = True

    def scalars(self, _stmt):
        return FakeScalarResult(self.scalars_result)


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


def test_position_deletion_queue_uses_worker_job_contract():
    raw_dataset = RawDataset(acquisition_label="W80_BY_W303_Q10_1", data_format="tiff_sequence")
    raw_dataset.id = uuid4()
    position_ids = [uuid4(), uuid4()]
    positions = [
        RawDatasetPosition(
            id=position_id,
            raw_dataset_id=raw_dataset.id,
            position_key=f"pos{index}",
            display_name=f"Pos{index}",
            status="indexed",
        )
        for index, position_id in enumerate(position_ids, start=1)
    ]
    preview = RawDatasetPositionDeletionPreviewData(
        raw_dataset=raw_dataset,
        position_ids=position_ids,
        reclaimable_bytes=1234,
        preview_json={},
    )
    requested_by = type("User", (), {"user_key": "guillaume"})()
    session = FakeSession(scalars_result=positions)

    job = queue_raw_dataset_position_deletion(
        session,
        preview=preview,
        requested_by_user=requested_by,
    )

    assert session.added == [job]
    assert session.flushed is True
    assert job.raw_dataset_id == raw_dataset.id
    assert job.requested_mode == "server"
    assert job.params_json["job_kind"] == "raw_dataset_position_deletion"
    assert job.params_json["position_ids"] == [str(position_id) for position_id in position_ids]
    assert job.params_json["position_count"] == 2
    assert job.params_json["reclaimable_bytes"] == 1234
    assert [position.status for position in positions] == [
        POSITION_STATUS_DELETION_QUEUED,
        POSITION_STATUS_DELETION_QUEUED,
    ]
    assert [position.display_name for position in positions] == ["Pos1", "Pos2"]


def test_parse_position_ids_ignores_invalid_values():
    valid_id = uuid4()

    assert parse_position_ids([str(valid_id), "not-a-uuid", None]) == [valid_id]
    assert parse_position_ids("not-a-list") == []
