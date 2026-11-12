"""Native Alexander 2024 unlink step for an admitted retirement controller."""

from contextlib import ExitStack
from datetime import datetime, timezone
from itertools import zip_longest
import json
import os
from pathlib import Path, PurePosixPath
import stat

from verify_retirement_manifest import (
    EXCLUDED, SOURCE, VERIFIED, file_row, native_rows, open_absolute_directory, open_directory, verify,
)


def directory_identity(fd):
    info = os.fstat(fd)
    return info.st_dev, info.st_ino


def manifest_identity(manifest):
    info = os.fstat(manifest.fileno())
    if not stat.S_ISREG(info.st_mode):
        raise ValueError('Manifest must be a regular file')
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def available_bytes(fd):
    info = os.fstatvfs(fd)
    return info.f_bavail * info.f_frsize


def record(receipt, event, **fields):
    row = {'event': event, 'at': datetime.now(timezone.utc).isoformat(), **fields}
    receipt.write((json.dumps(row, sort_keys=True) + '\n').encode('utf-8'))
    receipt.flush()
    os.fsync(receipt.fileno())


def verify_leaf(row, parent_fd, path, device):
    if file_row(parent_fd, path.name, path, device) != row:
        raise ValueError(f'Source file changed before unlink: {path}')


def withdraw_verified_tree(root, manifest, expected, receipt_path, require_admission):
    """Unlink a verified payload. Admission must raise whenever its lease is invalid."""
    if not receipt_path.is_absolute() or receipt_path.parent.resolve(strict=True) != receipt_path.parent:
        raise ValueError('Receipt must be in a canonical absolute directory')
    if receipt_path == root or root in receipt_path.parents:
        raise ValueError('Receipt must be outside the retirement tree')

    manifest_stamp = manifest_identity(manifest)
    require_admission()
    with open_absolute_directory(root) as verified_root:
        identity = directory_identity(verified_root)
        facts = verify(verified_root, manifest, expected)
        if manifest_identity(manifest) != manifest_stamp:
            raise ValueError('Manifest changed during verification')

        require_admission()
        manifest.seek(0)
        with receipt_path.open('xb') as receipt:
            with open_absolute_directory(receipt_path.parent) as receipt_dir:
                os.fsync(receipt_dir)
            before = available_bytes(verified_root)
            record(receipt, 'start', **facts, available_before=before)
            count = removed_bytes = 0
            for line in manifest:
                row = json.loads(line)
                if row['kind'] != 'file':
                    continue
                path = PurePosixPath(row['path'])
                if not path.parts or path.is_absolute() or any(part in EXCLUDED or part == '..' for part in path.parts):
                    raise ValueError(f'Unexpected path in verified manifest: {path}')
                if manifest_identity(manifest) != manifest_stamp:
                    raise ValueError('Manifest changed before unlink')

                with ExitStack() as stack:
                    parent_fd = stack.enter_context(open_absolute_directory(root))
                    if directory_identity(parent_fd) != identity:
                        raise ValueError('Retirement root changed after verification')
                    for component in path.parts[:-1]:
                        parent_fd = stack.enter_context(open_directory(component, parent_fd))
                        if os.fstat(parent_fd).st_dev != identity[0]:
                            raise ValueError(f'Unexpected mounted filesystem: {path.parent}')
                    require_admission()
                    verify_leaf(row, parent_fd, path, identity[0])
                    record(receipt, 'intent', file=row)
                    require_admission()
                    verify_leaf(row, parent_fd, path, identity[0])
                    os.unlink(path.name, dir_fd=parent_fd)
                    os.fsync(parent_fd)
                    record(receipt, 'unlinked', file=row)
                count += 1
                removed_bytes += row['bytes']

            if (count, removed_bytes) != expected[:2] or manifest_identity(manifest) != manifest_stamp:
                raise ValueError('Withdrawal receipt totals or manifest identity changed')
            manifest.seek(0)
            directories = (row for row in map(json.loads, manifest) if row['kind'] == 'directory')
            with open_absolute_directory(root) as remaining_root:
                if directory_identity(remaining_root) != identity:
                    raise ValueError('Retirement root changed after unlink')
                for actual, planned in zip_longest(native_rows(remaining_root), directories):
                    if actual != planned:
                        raise ValueError('Retirement tree contains unexpected or remaining entries')
            after = available_bytes(verified_root)
            result = {'files': count, 'logical_bytes': removed_bytes, 'available_before': before,
                      'available_after': after, 'available_delta': after - before}
            record(receipt, 'complete', **result)
    return result


def withdraw_year(year, manifest_path, receipt_path, require_admission):
    """Bind the unlink step to the verified native 2024 roots and manifests."""
    expected = VERIFIED[year]
    with Path(manifest_path).open('rb') as manifest:
        return withdraw_verified_tree(SOURCE / year, manifest, expected, Path(receipt_path), require_admission)
