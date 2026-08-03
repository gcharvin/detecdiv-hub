from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from api.models import Project, RawDataset, User
import api.services.raw_dataset_lifecycle as lifecycle
from api.services.raw_dataset_lifecycle import (
    LegacyArchiveBundleScope,
    is_legacy_shared_project,
    path_relation,
    transition_raw_dataset_to_archive,
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
    assert event.metadata_json["bundle_project_ids"] == [str(project.id)]
    assert queued_job.params_json["bundle_root_path"].replace("\\", "/") == "/data/basile2/legacy"
    assert queued_job.params_json["bundle_project_ids"] == [str(project.id)]
    assert project.archive_status == "archive_queued"
    assert project.archive_uri == "/archive"
