from types import SimpleNamespace
from uuid import uuid4

import pytest

from api.models import Artifact, Job, RawDataset
import worker.storage_lifecycle as lifecycle


class DummySession:
    def __init__(self):
        self.added = []

    def add(self, item):
        self.added.append(item)

    def flush(self):
        return None


def build_archive_job(*, archive_path, source_path) -> Job:
    return Job(
        id=uuid4(),
        raw_dataset_id=uuid4(),
        params_json={
            "job_kind": "archive_raw_dataset",
            "archive_uri": str(archive_path),
            "archive_compression": "zip",
            "mark_archived": True,
            "bundle_root_path": str(source_path),
            "bundle_raw_dataset_ids": [],
            "bundle_project_ids": [],
        },
    )


def test_duplicate_archive_reuses_valid_completed_artifact_when_source_is_gone(tmp_path, monkeypatch):
    archive_path = tmp_path / "raw.zip"
    archive_path.write_bytes(b"completed archive")
    source_path = tmp_path / "missing-source"
    job = build_archive_job(archive_path=archive_path, source_path=source_path)
    raw_dataset = RawDataset(
        id=job.raw_dataset_id,
        acquisition_label="raw",
        lifecycle_tier="cold",
        archive_status="archive_queued",
        archive_compression="zip",
    )
    artifact = Artifact(
        id=uuid4(),
        job_id=uuid4(),
        artifact_kind="raw_dataset_archive",
        uri=str(archive_path),
        metadata_json={
            "compression": "zip",
            "sha256": "verified-sha256",
            "archive_bytes": archive_path.stat().st_size,
            "source_path": str(source_path),
            "source_deleted": True,
            "preserved_preview_dirs": [],
        },
    )
    completed = []
    monkeypatch.setattr(lifecycle, "get_settings", lambda: SimpleNamespace(default_archive_compression="zip", default_archive_root=""))
    monkeypatch.setattr(lifecycle, "load_raw_dataset_for_job", lambda *_args, **_kwargs: raw_dataset)
    monkeypatch.setattr(lifecycle, "resolve_requested_by_user", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(lifecycle, "pick_preferred_raw_location", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(lifecycle, "find_valid_completed_archive_artifact", lambda *_args, **_kwargs: artifact)
    monkeypatch.setattr(lifecycle, "complete_raw_dataset_archive", lambda *_args, **kwargs: completed.append(kwargs))
    monkeypatch.setattr(
        lifecycle,
        "create_archive",
        lambda **_kwargs: pytest.fail("A duplicate request must not recreate the archive"),
    )

    result = lifecycle.execute_raw_dataset_archive(DummySession(), job=job)

    assert result["already_archived"] is True
    assert result["reused_artifact_id"] == str(artifact.id)
    assert result["archive_uri"] == str(archive_path)
    assert completed[0]["archive_uri"] == str(archive_path)
    assert completed[0]["source_deleted"] is True


def test_existing_unverified_archive_is_never_overwritten(tmp_path, monkeypatch):
    archive_path = tmp_path / "unknown.zip"
    archive_path.write_bytes(b"unknown archive")
    source_path = tmp_path / "source"
    source_path.mkdir()
    job = build_archive_job(archive_path=archive_path, source_path=source_path)
    raw_dataset = RawDataset(
        id=job.raw_dataset_id,
        acquisition_label="raw",
        lifecycle_tier="hot",
        archive_status="archive_queued",
        archive_compression="zip",
    )
    monkeypatch.setattr(lifecycle, "get_settings", lambda: SimpleNamespace(default_archive_compression="zip", default_archive_root=""))
    monkeypatch.setattr(lifecycle, "load_raw_dataset_for_job", lambda *_args, **_kwargs: raw_dataset)
    monkeypatch.setattr(lifecycle, "resolve_requested_by_user", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(lifecycle, "pick_preferred_raw_location", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(lifecycle, "find_valid_completed_archive_artifact", lambda *_args, **_kwargs: None)

    with pytest.raises(FileExistsError, match="already exists"):
        lifecycle.execute_raw_dataset_archive(DummySession(), job=job)


def test_failure_finalizer_preserves_prior_valid_archive(tmp_path, monkeypatch):
    archive_path = tmp_path / "raw.zip"
    archive_path.write_bytes(b"completed archive")
    job = build_archive_job(archive_path=archive_path, source_path=tmp_path / "missing")
    raw_dataset = RawDataset(
        id=job.raw_dataset_id,
        acquisition_label="raw",
        lifecycle_tier="cold",
        archive_status="archive_queued",
    )
    artifact = Artifact(
        id=uuid4(),
        job_id=uuid4(),
        artifact_kind="raw_dataset_archive",
        uri=str(archive_path),
        metadata_json={
            "sha256": "verified-sha256",
            "archive_bytes": archive_path.stat().st_size,
            "source_deleted": True,
        },
    )
    reconciled = []
    monkeypatch.setattr(lifecycle, "load_raw_dataset_for_job", lambda *_args, **_kwargs: raw_dataset)
    monkeypatch.setattr(lifecycle, "resolve_requested_by_user", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        lifecycle,
        "find_latest_valid_completed_archive_artifact",
        lambda *_args, **_kwargs: artifact,
    )
    monkeypatch.setattr(
        lifecycle,
        "reconcile_existing_completed_archive",
        lambda *_args, **kwargs: reconciled.append(kwargs),
    )
    monkeypatch.setattr(
        lifecycle,
        "fail_raw_dataset_lifecycle_job",
        lambda *_args, **_kwargs: pytest.fail("A prior valid archive must not be downgraded"),
    )

    lifecycle.finalize_storage_lifecycle_failure(
        DummySession(),
        job=job,
        error_text="Archive destination already exists",
    )

    assert reconciled[0]["artifact"] is artifact
    assert reconciled[0]["raw_dataset"] is raw_dataset
