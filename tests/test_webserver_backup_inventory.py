from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.routes_backup import _apply_vm_retention_policy, _list_vm_archive_items


def test_vm_archive_size_uses_total_folder_not_metadata_tar(tmp_path):
    backup_dir = tmp_path / "archives" / "webserver-labo-20260614T013644Z"
    disk_dir = backup_dir / "disks"
    disk_dir.mkdir(parents=True)
    (backup_dir / "webserver-labo-20260614T013644Z.tar.gz").write_bytes(b"small metadata tar")
    (backup_dir / "webserver-labo-20260614T013644Z.tar.gz.sha256").write_bytes(b"checksum")
    (disk_dir / "webserver-labo.qcow2").write_bytes(b"x" * 4096)

    items = _list_vm_archive_items(tmp_path / "archives")

    assert len(items) == 1
    assert items[0].bundle_size_bytes == len(b"small metadata tar")
    assert items[0].disk_size_bytes == 4096
    assert items[0].size_bytes > items[0].bundle_size_bytes


def test_vm_retention_policy_marks_recent_monthly_and_yearly(tmp_path):
    archive_root = tmp_path / "archives"
    for stamp in (
        "20260614T013644Z",
        "20260607T014309Z",
        "20260531T013134Z",
        "20260524T013940Z",
        "20260517T013750Z",
        "20260510T014037Z",
        "20250406T014037Z",
    ):
        backup_dir = archive_root / f"webserver-labo-{stamp}"
        disk_dir = backup_dir / "disks"
        disk_dir.mkdir(parents=True)
        (backup_dir / f"webserver-labo-{stamp}.tar.gz").write_bytes(b"metadata")
        (disk_dir / "webserver-labo.qcow2").write_bytes(b"disk")

    items = _apply_vm_retention_policy(_list_vm_archive_items(archive_root), keep_recent=5)
    by_name = {item.name: item.retention_labels for item in items}

    assert "recent" in by_name["webserver-labo-20260517T013750Z"]
    assert "monthly" in by_name["webserver-labo-20260614T013644Z"]
    assert "monthly" in by_name["webserver-labo-20260531T013134Z"]
    assert "yearly" in by_name["webserver-labo-20260614T013644Z"]
    assert "yearly" in by_name["webserver-labo-20250406T014037Z"]
    assert by_name["webserver-labo-20260510T014037Z"] == []
