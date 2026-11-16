"""Fixture-only integration of partial withdrawal guards; not production code.

This module rejects any root outside a dedicated temporary fixture directory.
It has no production entry point, authenticated admission, atomic writer freeze,
or crash recovery. Never use it on a real NAS or project tree.
"""

from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import tempfile

from plan_partial_retirement_manifest import _plan
from retirement_directory_inventory_v2 import check_unchanged_chain, signature, snapshot
from retirement_directory_transition import capture, validate_one_file_removal
from verify_retirement_manifest import file_row, native_rows, open_absolute_directory, open_directory
from withdraw_retirement_manifest import available_bytes, manifest_identity, record


def withdraw_fixture(root, manifest, expected, selected, receipt_path, require_admission):
    """Exercise guards using a temporary fixture; never accepts production paths."""
    root = Path(root)
    receipt_path = Path(receipt_path)
    temp_root = Path(tempfile.gettempdir()).resolve(strict=True)
    if (not root.is_absolute() or root.parent.parent != temp_root
            or not root.parent.name.startswith('alex2024-partial-fixture-')
            or root.name != 'fixture-data' or root.resolve(strict=True) != root
            or receipt_path.parent != root.parent or receipt_path.name != 'fixture-receipt.jsonl'):
        raise ValueError('Only a dedicated temporary fixture is allowed')
    root_stat = os.lstat(root.parent)
    if not root.parent.is_dir() or root.parent.is_symlink() or root_stat.st_uid != os.geteuid():
        raise ValueError('Unsafe fixture parent')
    manifest_stamp = manifest_identity(manifest)
    require_admission()
    with open_absolute_directory(root) as root_fd:
        baseline = snapshot(root_fd, manifest, expected)
        subsets = _plan(manifest, selected, expected)
        if manifest_identity(manifest) != manifest_stamp:
            raise ValueError('Manifest changed before fixture withdrawal')
        expected_rows = {row['path']: row for row in baseline['directories']}
        before = available_bytes(root_fd)
        manifest.seek(0)
        count = size = 0
        with receipt_path.open('xb') as receipt:
            record(receipt, 'start', selected=selected, manifest_sha256=expected[2], available_before=before)
            for line in manifest:
                row = json.loads(line)
                if row['kind'] != 'file':
                    continue
                path = PurePosixPath(row['path'])
                if not path.parts or path.parts[0] not in subsets:
                    continue
                if manifest_identity(manifest) != manifest_stamp:
                    raise ValueError('Manifest changed during fixture withdrawal')
                require_admission()
                check_unchanged_chain(root, path.parent, baseline)
                with ExitStack() as stack:
                    parent_fd = stack.enter_context(open_absolute_directory(root))
                    rel = PurePosixPath('.')
                    if {'path': str(rel), **signature(parent_fd)} != expected_rows[str(rel)]:
                        raise ValueError('Fixture root changed')
                    for component in path.parts[:-1]:
                        parent_fd = stack.enter_context(open_directory(component, parent_fd))
                        rel /= component
                        if {'path': str(rel), **signature(parent_fd)} != expected_rows.get(str(rel)):
                            raise ValueError('Fixture ancestor changed')
                    device = os.fstat(root_fd).st_dev
                    if file_row(parent_fd, path.name, path, device) != row:
                        raise ValueError('Fixture file changed')
                    prior = capture(parent_fd)
                    if prior['children'].get(path.name) is None:
                        raise ValueError('Fixture file is missing')
                    record(receipt, 'intent', file=row)
                    require_admission()
                    if file_row(parent_fd, path.name, path, device) != row or capture(parent_fd) != prior:
                        raise ValueError('Fixture changed after intent')
                    os.unlink(path.name, dir_fd=parent_fd)
                    os.fsync(parent_fd)
                    following = capture(parent_fd)
                    parent = validate_one_file_removal(prior, following, path.name, prior['children'][path.name])
                    expected_rows[str(rel)].update(parent)
                    encoded = json.dumps(baseline['directories'], sort_keys=True, separators=(',', ':')).encode()
                    baseline['directories_sha256'] = hashlib.sha256(encoded).hexdigest()
                    record(receipt, 'unlinked', file=row, parent=parent)
                count += 1
                size += row['bytes']
            if (count, size) != (sum(v['files'] for v in subsets.values()), sum(v['bytes'] for v in subsets.values())):
                raise ValueError('Fixture totals differ')
            manifest.seek(0)
            expected_remaining = (row for row in map(json.loads, manifest)
                                  if row['kind'] != 'file' or PurePosixPath(row['path']).parts[0] not in subsets)
            from itertools import zip_longest
            with open_absolute_directory(root) as final_fd:
                if any(a != b for a, b in zip_longest(native_rows(final_fd), expected_remaining)):
                    raise ValueError('Fixture remaining tree differs')
            after = available_bytes(root_fd)
            record(receipt, 'complete', files=count, logical_bytes=size,
                   available_before=before, available_after=after, available_delta=after-before)
    return {'files': count, 'logical_bytes': size, 'production_authorized': False}
