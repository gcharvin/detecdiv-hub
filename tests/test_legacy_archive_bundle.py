from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from api.models import Project, ProjectLocation, RawDataset, RawDatasetLocation, StorageRoot, User
import api.services.raw_dataset_lifecycle as lifecycle
from api.services.raw_dataset_lifecycle import (
    LegacyArchiveBundleScope,
    inspect_legacy_archive_bundle_scope,
    is_legacy_shared_project,
    path_relation,
    transition_raw_dataset_to_archive,
    update_bundle_raw_datasets_after_archive,
    update_bundle_raw_datasets_after_restore,
    update_bundle_projects_after_archive,
    update_bundle_projects_after_restore,
)


class DummySession:
    def __init__(self, projects):
        self.projects = projects

    def scalars(self, _stmt):
        return iter(self.projects)

    def add(self, item):
        self.projects.append(item)

    def flush(self):
        return None


class SequenceScalarResult:
    def __init__(self, values):
        self.values = values

    def unique(self):
        return self

    def __iter__(self):
        return iter(self.values)


class SequenceSession:
    def __init__(self, *results):
        self.results = iter(results)

    def scalars(self, _stmt):
        return SequenceScalarResult(next(self.results))


def test_path_relation_distinguishes_archive_bundle_overlaps():
    root = Path("/data/basile2/190204/legacy")

    assert path_relation(root=root, candidate=root) == "exact"
    assert path_relation(root=root, candidate=root / "legacy-pos1") == "descendant"
    assert path_relation(root=root, candidate=root.parent) == "ancestor"
    assert path_relation(root=root, candidate=Path("/data/basile2/other")) == "unrelated"


def test_legacy_shared_project_accepts_boolean_or_serialized_boolean():
    project = Project(project_name="legacy", metadata_json={"storage_is_shared_with_raw_dataset": True})
    assert is_legacy_shared_project(project) is True

    project.metadata_json = {"storage_is_shared_with_raw_dataset": "true"}
    assert is_legacy_shared_project(project) is True

    project.metadata_json = {"storage_is_shared_with_raw_dataset": False}
    assert is_legacy_shared_project(project) is False


def test_nested_raw_dataset_resolves_to_parent_bundle_with_legacy_project():
    owner_id = uuid4()
    storage_root = StorageRoot(id=1, name="data", root_type="raw", host_scope="server", path_prefix="/data")
    parent = RawDataset(id=uuid4(), owner_user_id=owner_id, acquisition_label="parent", metadata_json={})
    child = RawDataset(id=uuid4(), owner_user_id=owner_id, acquisition_label="child", metadata_json={})
    parent_location = RawDatasetLocation(
        id=1, raw_dataset_id=parent.id, storage_root_id=storage_root.id, relative_path="run", is_preferred=True
    )
    child_location = RawDatasetLocation(
        id=2, raw_dataset_id=child.id, storage_root_id=storage_root.id, relative_path="run/Pos0", is_preferred=True
    )
    parent_location.storage_root = storage_root
    parent_location.raw_dataset = parent
    child_location.storage_root = storage_root
    child_location.raw_dataset = child
    parent.locations = [parent_location]
    child.locations = [child_location]

    project = Project(
        id=uuid4(), project_name="legacy", status="active", metadata_json={"storage_is_shared_with_raw_dataset": True}
    )
    project_location = ProjectLocation(
        id=1, project_id=project.id, storage_root_id=storage_root.id, relative_path="run/Pos0", is_preferred=True
    )
    project_location.storage_root = storage_root
    project_location.project = project
    session = SequenceSession([parent_location, child_location], [project_location])

    scope = inspect_legacy_archive_bundle_scope(session, raw_dataset=child)

    assert scope.root_path == Path("/data/run")
    assert [item.id for item in scope.raw_datasets] == [parent.id, child.id]
    assert [item.id for item in scope.projects] == [project.id]
    assert scope.blockers == []


def test_archive_and_restore_update_every_bundle_project():
    project = Project(
        id=uuid4(),
        project_name="legacy",
        lifecycle_tier="hot",
        archive_status="none",
        metadata_json={},
    )
    session = DummySession([project])
    raw_dataset_id = uuid4()

    update_bundle_projects_after_archive(
        session,
        project_ids=[str(project.id)],
        archive_uri="/archive/legacy.zip",
        archive_compression="zip",
        source_deleted=True,
        bundle_raw_dataset_id=raw_dataset_id,
    )

    assert project.lifecycle_tier == "cold"
    assert project.archive_status == "archived"
    assert project.archive_uri == "/archive/legacy.zip"
    assert project.metadata_json["archive_bundle_raw_dataset_id"] == str(raw_dataset_id)

    update_bundle_projects_after_restore(session, project_ids=[str(project.id)])

    assert project.lifecycle_tier == "hot"
    assert project.archive_status == "restored"


def test_archive_and_restore_update_every_bundled_raw_dataset():
    parent = RawDataset(
        id=uuid4(), acquisition_label="parent", total_bytes=200, lifecycle_tier="hot", archive_status="none"
    )
    child = RawDataset(
        id=uuid4(), acquisition_label="child", total_bytes=100, lifecycle_tier="hot", archive_status="none"
    )
    session = DummySession([parent, child])
    raw_dataset_ids = [str(parent.id), str(child.id)]

    update_bundle_raw_datasets_after_archive(
        session,
        raw_dataset_ids=raw_dataset_ids,
        archive_uri="/archive/run.zip",
        archive_compression="zip",
        source_deleted=True,
    )

    assert [(item.lifecycle_tier, item.archive_status) for item in (parent, child)] == [
        ("cold", "archived"),
        ("cold", "archived"),
    ]
    assert parent.archive_uri == child.archive_uri == "/archive/run.zip"

    update_bundle_raw_datasets_after_restore(session, raw_dataset_ids=raw_dataset_ids)

    assert [(item.lifecycle_tier, item.archive_status) for item in (parent, child)] == [
        ("hot", "restored"),
        ("hot", "restored"),
    ]


def test_archive_request_queues_every_exact_legacy_project(monkeypatch):
    project = Project(
        id=uuid4(),
        project_name="legacy",
        lifecycle_tier="hot",
        archive_status="none",
        metadata_json={"storage_is_shared_with_raw_dataset": True},
    )
    raw_dataset = RawDataset(
        id=uuid4(),
        acquisition_label="legacy",
        total_bytes=123,
        lifecycle_tier="hot",
        archive_status="none",
        metadata_json={},
    )
    user = User(id=uuid4(), user_key="Basile", display_name="Basile")
    session = DummySession([])
    monkeypatch.setattr(lifecycle, "find_active_lifecycle_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        lifecycle,
        "resolve_raw_archive_runtime_config",
        lambda *_args, **_kwargs: SimpleNamespace(
            archive_root="/archive",
            archive_compression="zip",
            delete_hot_source=True,
        ),
    )
    monkeypatch.setattr(
        lifecycle,
        "inspect_legacy_archive_bundle_scope",
        lambda *_args, **_kwargs: LegacyArchiveBundleScope(
            root_path=Path("/data/basile2/legacy"),
            projects=[project],
            blockers=[],
        ),
    )

    event = transition_raw_dataset_to_archive(
        session,
        raw_dataset=raw_dataset,
        requested_by_user=user,
        archive_uri=None,
        archive_compression=None,
        mark_archived=None,
    )

    queued_job = next(item for item in session.projects if item.__class__.__name__ == "Job")
    assert queued_job.priority == 150
    assert event.metadata_json["bundle_project_ids"] == [str(project.id)]
    assert queued_job.params_json["bundle_root_path"].replace("\\", "/") == "/data/basile2/legacy"
    assert queued_job.params_json["bundle_project_ids"] == [str(project.id)]
    assert project.archive_status == "archive_queued"
    assert project.archive_uri == "/archive"


def test_archive_request_uses_parent_raw_dataset_for_nested_bundle(monkeypatch):
    parent = RawDataset(
        id=uuid4(), acquisition_label="parent", total_bytes=200, lifecycle_tier="hot", archive_status="none", metadata_json={}
    )
    child = RawDataset(
        id=uuid4(), acquisition_label="child", total_bytes=100, lifecycle_tier="hot", archive_status="none", metadata_json={}
    )
    user = User(id=uuid4(), user_key="Basile", display_name="Basile")
    session = DummySession([])
    monkeypatch.setattr(lifecycle, "find_active_lifecycle_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        lifecycle,
        "resolve_raw_archive_runtime_config",
        lambda *_args, **_kwargs: SimpleNamespace(
            archive_root="/archive", archive_compression="zip", delete_hot_source=True
        ),
    )
    monkeypatch.setattr(
        lifecycle,
        "inspect_legacy_archive_bundle_scope",
        lambda *_args, **_kwargs: LegacyArchiveBundleScope(
            root_path=Path("/data/run"), projects=[], blockers=[], raw_datasets=[parent, child]
        ),
    )

    event = transition_raw_dataset_to_archive(
        session,
        raw_dataset=child,
        requested_by_user=user,
        archive_uri=None,
        archive_compression=None,
        mark_archived=None,
    )

    queued_job = next(item for item in session.projects if item.__class__.__name__ == "Job")
    assert queued_job.raw_dataset_id == parent.id
    assert queued_job.params_json["bundle_raw_dataset_ids"] == [str(parent.id), str(child.id)]
    assert event.raw_dataset_id == parent.id
    assert parent.archive_status == "archive_queued"
    assert child.archive_status == "archive_queued"
