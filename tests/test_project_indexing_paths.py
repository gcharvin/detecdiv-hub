from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from api.services.project_indexing import (
    build_rebased_raw_path_candidates,
    classify_project_candidate,
    find_existing_project,
    find_preferred_project_location,
    get_or_create_storage_root,
    is_legacy_matlab_timelapse_project_dir,
    legacy_matlab_position_dir,
    raw_root_candidates,
    source_matching_storage_root_candidates,
)
from api.services.raw_dataset_ingest import detect_raw_dataset_format
from api.services.raw_dataset_ingest import resolve_raw_dataset_acquisition_label


@dataclass
class DummyStorageRoot:
    name: str
    root_type: str
    host_scope: str
    path_prefix: str
    created_at: datetime | None = None


class DummyScalarResult:
    def __init__(self, items):
        self._items = list(items)

    def __iter__(self):
        return iter(self._items)


class DummySession:
    def __init__(self, storage_roots):
        self._storage_roots = list(storage_roots)
        self.added = []

    def scalars(self, _stmt):
        return DummyScalarResult(self._storage_roots)

    def add(self, item):
        self.added.append(item)

    def flush(self):
        return None


class DummyProjectSession:
    def __init__(self, results):
        self._results = iter(results)
        self.scalar_calls = 0

    def scalars(self, _stmt):
        self.scalar_calls += 1
        return DummyScalarResult(next(self._results))


def test_raw_root_candidates_use_only_explicit_raw_storage_roots(tmp_path: Path):
    existing_raw_root = tmp_path / "raw"
    existing_raw_root.mkdir()
    existing_project_root = tmp_path / "projects"
    existing_project_root.mkdir()

    session = DummySession(
        [
            DummyStorageRoot(
                name="projects",
                root_type="project_root",
                host_scope="server",
                path_prefix=str(existing_project_root),
            ),
            DummyStorageRoot(
                name="raw",
                root_type="raw_root",
                host_scope="server",
                path_prefix=str(existing_raw_root),
            ),
            DummyStorageRoot(
                name="missing",
                root_type="raw_root",
                host_scope="server",
                path_prefix=str(tmp_path / "missing_raw"),
            ),
        ]
    )

    roots = raw_root_candidates(session)

    assert roots == [existing_raw_root]


def test_build_rebased_raw_path_candidates_rebases_against_explicit_root(tmp_path: Path):
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    session = DummySession(
        [
            DummyStorageRoot(
                name="raw",
                root_type="raw_root",
                host_scope="server",
                path_prefix=str(raw_root),
            )
        ]
    )

    source_path = r"X:\Florian\Microscopy\2026_04_16_YAM853_switch_1_0001\Pos0\frame_0001.tif"
    candidates = build_rebased_raw_path_candidates(session, source_path=source_path, project_dir=tmp_path)

    assert str(raw_root / "Florian" / "Microscopy" / "2026_04_16_YAM853_switch_1_0001" / "Pos0" / "frame_0001.tif") in candidates
    assert not any("//10.20.11.250" in candidate for candidate in candidates)


def test_build_rebased_raw_path_candidates_uses_matching_server_owner_root(tmp_path: Path):
    owner_root = tmp_path / "Abhilasha"
    raw_dataset = owner_root / "21_05_2027.ome.zarr"
    raw_dataset.mkdir(parents=True)
    session = DummySession(
        [
            DummyStorageRoot(
                name="Abhilashsa_data",
                root_type="project_root",
                host_scope="server",
                path_prefix=str(owner_root),
            )
        ]
    )

    source_path = r"Z:\Abhilasha\21_05_2027.ome.zarr"
    candidates = build_rebased_raw_path_candidates(session, source_path=source_path, project_dir=tmp_path)

    assert str(raw_dataset) in candidates
    assert source_matching_storage_root_candidates(session, source_path) == [owner_root]


def test_legacy_matlab_timelapse_project_dir_is_detected(tmp_path: Path):
    dataset_dir = tmp_path / "sample"
    dataset_dir.mkdir()
    (dataset_dir / "sample-project.mat").write_text("mat", encoding="utf-8")
    (dataset_dir / "sample-ID.txt").write_text("Time-Lapse Assay ID File", encoding="utf-8")
    (dataset_dir / "sample-pos1").mkdir()
    (dataset_dir / "sample-pos2").mkdir()

    assert is_legacy_matlab_timelapse_project_dir(dataset_dir)
    assert detect_raw_dataset_format(dataset_dir, {}) == "legacy_matlab_jpg_timelapse"


def test_legacy_matlab_timelapse_detects_internal_prefix_without_project_mat(tmp_path: Path):
    dataset_dir = tmp_path / "20120302"
    dataset_dir.mkdir()
    (dataset_dir / "BK-project.mat").write_text("backup", encoding="utf-8")
    (dataset_dir / "test_classic_chip-ID.txt").write_text("Time-Lapse Assay ID File", encoding="utf-8")
    (dataset_dir / "test_classic_chip-pos1").mkdir()
    (dataset_dir / "test_classic_chip-pos2").mkdir()

    assert is_legacy_matlab_timelapse_project_dir(dataset_dir)
    assert detect_raw_dataset_format(dataset_dir, {}) == "legacy_matlab_jpg_timelapse"
    assert legacy_matlab_position_dir(dataset_dir, 2) == dataset_dir / "test_classic_chip-pos2"
    assert (
        resolve_raw_dataset_acquisition_label(
            dataset_dir=dataset_dir,
            data_format="legacy_matlab_jpg_timelapse",
            acquisition_label="20120302",
        )
        == "test_classic_chip"
    )


def test_classify_project_candidate_accepts_in_place_project_mat(tmp_path: Path):
    dataset_dir = tmp_path / "sample"
    dataset_dir.mkdir()
    mat_path = dataset_dir / "sample-project.mat"
    mat_path.write_text("mat", encoding="utf-8")
    (dataset_dir / "sample-ID.txt").write_text("Time-Lapse Assay ID File", encoding="utf-8")
    (dataset_dir / "sample-pos1").mkdir()

    candidate = classify_project_candidate(mat_path)

    assert candidate is not None
    assert candidate[0] == mat_path.resolve()
    assert candidate[1] == dataset_dir.resolve()


def test_classify_project_candidate_ignores_non_project_legacy_mat_files(tmp_path: Path):
    for file_name in ("temp-project.mat", "BK-project.mat", "bk-PROJECT.MAT"):
        dataset_dir = tmp_path / file_name.replace(".", "_")
        dataset_dir.mkdir()
        mat_path = dataset_dir / file_name
        mat_path.write_text("mat", encoding="utf-8")
        (dataset_dir / "sample-pos1").mkdir()

        assert classify_project_candidate(mat_path) is None


def test_get_or_create_storage_root_prefers_existing_path_prefix_over_new_name(tmp_path: Path):
    root_path = (tmp_path / "Antoine").resolve()
    older = DummyStorageRoot(
        name="Antoine_data",
        root_type="project_root",
        host_scope="server",
        path_prefix=str(root_path),
        created_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    newer = DummyStorageRoot(
        name="antoine_data",
        root_type="project_root",
        host_scope="server",
        path_prefix=str(root_path),
        created_at=datetime(2026, 5, 4, tzinfo=timezone.utc),
    )
    session = DummySession([older, newer])

    root = get_or_create_storage_root(
        session,
        root_path=str(root_path),
        storage_root_name="antoine_data",
        host_scope="server",
        root_type="project_root",
    )

    assert root is older
    assert session.added == []


def test_find_existing_project_prefers_physical_mat_identity_over_scan_root_key():
    same_mat_project = object()
    session = DummyProjectSession([[same_mat_project]])

    project = find_existing_project(
        session,
        project_mat_abs="/data/user/project/example.mat",
        project_key="different_root_example_0123456789",
    )

    assert project is same_mat_project
    assert session.scalar_calls == 1


def test_find_existing_project_falls_back_to_project_key_for_old_metadata():
    keyed_project = object()
    session = DummyProjectSession([[], [keyed_project]])

    project = find_existing_project(
        session,
        project_mat_abs="/data/user/project/example.mat",
        project_key="project_example_0123456789",
    )

    assert project is keyed_project
    assert session.scalar_calls == 2


def test_find_preferred_project_location_skips_query_for_new_project():
    session = DummyProjectSession([])

    assert find_preferred_project_location(session, project=None) is None
    assert session.scalar_calls == 0


def test_find_preferred_project_location_returns_catalog_authority():
    preferred_location = object()
    session = DummyProjectSession([[preferred_location]])

    project = type("DummyProject", (), {"id": "project-id"})()
    location = find_preferred_project_location(session, project=project)

    assert location is preferred_location
    assert session.scalar_calls == 1
