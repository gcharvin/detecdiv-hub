from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from api.services.project_indexing import build_rebased_raw_path_candidates, raw_root_candidates


@dataclass
class DummyStorageRoot:
    root_type: str
    path_prefix: str


class DummyScalarResult:
    def __init__(self, items):
        self._items = list(items)

    def __iter__(self):
        return iter(self._items)


class DummySession:
    def __init__(self, storage_roots):
        self._storage_roots = list(storage_roots)

    def scalars(self, _stmt):
        return DummyScalarResult(self._storage_roots)


def test_raw_root_candidates_use_only_explicit_raw_storage_roots(tmp_path: Path):
    existing_raw_root = tmp_path / "raw"
    existing_raw_root.mkdir()
    existing_project_root = tmp_path / "projects"
    existing_project_root.mkdir()

    session = DummySession(
        [
            DummyStorageRoot(root_type="project_root", path_prefix=str(existing_project_root)),
            DummyStorageRoot(root_type="raw_root", path_prefix=str(existing_raw_root)),
            DummyStorageRoot(root_type="raw_root", path_prefix=str(tmp_path / "missing_raw")),
        ]
    )

    roots = raw_root_candidates(session)

    assert roots == [existing_raw_root]


def test_build_rebased_raw_path_candidates_rebases_against_explicit_root(tmp_path: Path):
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    session = DummySession([DummyStorageRoot(root_type="raw_root", path_prefix=str(raw_root))])

    source_path = r"X:\Florian\Microscopy\2026_04_16_YAM853_switch_1_0001\Pos0\frame_0001.tif"
    candidates = build_rebased_raw_path_candidates(session, source_path=source_path, project_dir=tmp_path)

    assert str(raw_root / "Florian" / "Microscopy" / "2026_04_16_YAM853_switch_1_0001" / "Pos0" / "frame_0001.tif") in candidates
    assert not any("//10.20.11.250" in candidate for candidate in candidates)
