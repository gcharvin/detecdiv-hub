import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.services import raw_dataset_position_deletion as deletion


class FakeScalars:
    def __init__(self, values):
        self.values = values

    def __iter__(self):
        return iter(self.values)


class FakeSession:
    def __init__(self, positions):
        self.positions = positions

    def scalars(self, _stmt):
        return FakeScalars(self.positions)


def test_position_deletion_queue_preview_does_not_measure_reclaimable(monkeypatch):
    position = SimpleNamespace(
        id=uuid4(),
        raw_dataset_id=uuid4(),
        position_key="Pos1",
        display_name="Pos1",
        position_index=1,
        metadata_json={"relative_path": "Pos1"},
        preview_artifact=None,
        preview_artifact_id=None,
    )
    raw_dataset = SimpleNamespace(
        id=position.raw_dataset_id,
        acquisition_label="big_dataset",
        total_bytes=123,
        locations=[],
    )

    def fail_if_measured(_path):
        raise AssertionError("queue preview should not stat position source paths")

    monkeypatch.setattr(deletion, "resolve_position_source_paths", lambda _raw_dataset, _relative_path: [Path("/data/big/Pos1")])
    monkeypatch.setattr(deletion, "count_position_source_bytes", fail_if_measured)
    monkeypatch.setattr(deletion, "preview_linked_projects_for_raw_dataset", lambda _session, raw_dataset: [])

    preview = deletion.build_raw_dataset_position_deletion_preview(
        FakeSession([position]),
        raw_dataset=raw_dataset,
        position_ids=[position.id],
        measure_reclaimable=False,
    )

    assert preview.reclaimable_bytes == 0
    assert preview.preview_json["positions"][0]["source_bytes"] == 0
