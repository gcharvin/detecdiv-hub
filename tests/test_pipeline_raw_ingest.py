from __future__ import annotations

from pathlib import Path

from api.models import StorageRoot
from api.services.pipeline_raw_ingest import resolve_server_raw_storage_root


class DummyScalarResult:
    def __init__(self, items):
        self._items = list(items)

    def all(self):
        return list(self._items)


class DummySession:
    def __init__(self, storage_roots):
        self._storage_roots = list(storage_roots)

    def scalars(self, _statement):
        return DummyScalarResult(self._storage_roots)


def test_pipeline_raw_ingest_accepts_registered_project_root(tmp_path: Path) -> None:
    project_root = tmp_path / "data" / "Alexander" / "2023_1"
    dataset_dir = project_root / "190423_nh_sir2_tween" / "190423_nh_sir2_tween_4"
    dataset_dir.mkdir(parents=True)
    storage_root = StorageRoot(
        name="data_alexander_2023_1",
        root_type="project_root",
        host_scope="server",
        path_prefix=str(project_root),
    )

    resolved = resolve_server_raw_storage_root(
        DummySession([storage_root]), dataset_dir=dataset_dir
    )

    assert resolved is storage_root


def test_pipeline_raw_ingest_uses_most_specific_registered_server_root(tmp_path: Path) -> None:
    broad_root = tmp_path / "data"
    project_root = broad_root / "Alexander" / "2023_1"
    dataset_dir = project_root / "dataset"
    dataset_dir.mkdir(parents=True)
    broad = StorageRoot(
        name="raw-data",
        root_type="raw_root",
        host_scope="server",
        path_prefix=str(broad_root),
    )
    specific = StorageRoot(
        name="project-data",
        root_type="project_root",
        host_scope="server",
        path_prefix=str(project_root),
    )

    resolved = resolve_server_raw_storage_root(
        DummySession([broad, specific]), dataset_dir=dataset_dir
    )

    assert resolved is specific


def test_pipeline_raw_ingest_rejects_paths_outside_registered_server_roots(tmp_path: Path) -> None:
    registered_root = tmp_path / "registered"
    dataset_dir = tmp_path / "unregistered" / "dataset"
    registered_root.mkdir()
    dataset_dir.mkdir(parents=True)
    storage_root = StorageRoot(
        name="registered",
        root_type="project_root",
        host_scope="server",
        path_prefix=str(registered_root),
    )

    resolved = resolve_server_raw_storage_root(
        DummySession([storage_root]), dataset_dir=dataset_dir
    )

    assert resolved is None
