from pathlib import Path
from uuid import uuid4

import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.models import Project, ProjectLocation, StorageRoot
from api.services.project_deletion import build_deletion_preview, execute_project_deletion


class FakeSession:
    def __init__(self):
        self.added = []
        self.flushed = False

    def add(self, item):
        self.added.append(item)

    def flush(self):
        self.flushed = True


def test_project_deletion_removes_project_bakk_backup_files(tmp_path):
    project_file = tmp_path / "sample-project.mat"
    project_file.write_text("project", encoding="utf-8")
    mat_backup = tmp_path / "sample-project.mat.bakk"
    mat_backup.write_text("mat backup", encoding="utf-8")
    stem_backup = tmp_path / "sample-project.BAKK"
    stem_backup.write_text("stem backup", encoding="utf-8")

    storage_root = StorageRoot(
        id=1,
        name="test-project-root",
        root_type="project_root",
        host_scope="server",
        path_prefix=str(tmp_path),
    )
    location = ProjectLocation(
        storage_root=storage_root,
        relative_path="",
        project_file_name=project_file.name,
        access_mode="readwrite",
        is_preferred=True,
    )
    project = Project(
        id=uuid4(),
        project_name="sample",
        status="indexed",
        health_status="ok",
        visibility="private",
        metadata_json={},
        locations=[location],
    )

    preview = build_deletion_preview(
        FakeSession(),
        project=project,
        delete_project_files=True,
        delete_linked_raw_data=False,
    )

    assert sorted(
        Path(path).name.lower() for path in preview.preview_json["project"]["paths"]["project_backup_files"]
    ) == [
        "sample-project.bakk",
        "sample-project.mat.bakk",
    ]

    event = execute_project_deletion(FakeSession(), preview=preview)

    assert not project_file.exists()
    assert not mat_backup.exists()
    assert not stem_backup.exists()
    assert sorted(Path(path).name.lower() for path in event.result_json["deleted_project_backup_files"]) == [
        "sample-project.bakk",
        "sample-project.mat.bakk",
    ]
