"""Read-only native directory baseline for a future 2024 retirement controller.

This module does not authorize or perform deletion.  Its unchanged-chain check
is valid only before a directory has been intentionally modified; a production
controller still needs a separately verified transition protocol.
"""

import hashlib
import json
import os
from pathlib import PurePosixPath
import stat
from contextlib import ExitStack

from verify_retirement_manifest import EXCLUDED, open_absolute_directory, open_directory, verify


def signature(fd):
    info = os.fstat(fd)
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError('Expected a directory')
    return {name: getattr(info, 'st_' + name) for name in
            ('dev', 'ino', 'mode', 'uid', 'gid', 'nlink', 'size', 'mtime_ns', 'ctime_ns')}


def snapshot(root_fd, manifest, expected):
    """Check the full pinned file manifest and record all directory metadata."""
    before = os.fstat(manifest.fileno())
    stamp = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
    manifest.seek(0)
    facts = verify(root_fd, manifest, expected)
    manifest.seek(0)
    declared = [row['path'] for row in map(json.loads, manifest) if row['kind'] == 'directory']
    device = os.fstat(root_fd).st_dev
    rows = []
    for parent, dirs, files, parent_fd in os.fwalk('.', dir_fd=root_fd, follow_symlinks=False,
                                                 onerror=lambda error: (_ for _ in ()).throw(error)):
        dirs[:] = sorted(set(dirs) - EXCLUDED)
        for name in dirs:
            info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode) or info.st_dev != device:
                raise ValueError('Unexpected directory or mounted filesystem')
        value = signature(parent_fd)
        if value['dev'] != device:
            raise ValueError('Unexpected mounted filesystem')
        rows.append({'path': str(PurePosixPath(parent)), **value})
    # A file may have changed after the first verification but before its
    # containing directory was inventoried.  Check the complete manifest a
    # second time, then recheck every directory signature by no-follow opens.
    manifest.seek(0)
    if verify(root_fd, manifest, expected) != facts:
        raise ValueError('Manifest verification changed during inventory')
    for row in rows:
        with ExitStack() as stack:
            fd = os.dup(root_fd)
            stack.callback(os.close, fd)
            for component in PurePosixPath(row['path']).parts:
                fd = stack.enter_context(open_directory(component, fd))
            if {'path': row['path'], **signature(fd)} != row:
                raise ValueError(f'Directory changed during inventory: {row["path"]}')
    after = os.fstat(manifest.fileno())
    if stamp != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise ValueError('Manifest changed during directory inventory')
    if [row['path'] for row in rows] != declared:
        raise ValueError('Directory set changed during inventory')
    encoded = json.dumps(rows, sort_keys=True, separators=(',', ':')).encode()
    return {'manifest': facts, 'directories': rows, 'directories_sha256': hashlib.sha256(encoded).hexdigest()}


def check_unchanged_chain(root, relative_parent, baseline):
    """Verify an unchanged chain against a validated baseline, without deletion."""
    parent = PurePosixPath(relative_parent)
    if parent.is_absolute() or '..' in parent.parts or any(x in EXCLUDED for x in parent.parts):
        raise ValueError('Unsafe relative parent path')
    expected = {row['path']: row for row in baseline['directories']}
    if len(expected) != len(baseline['directories']):
        raise ValueError('Duplicate directory baseline')
    encoded = json.dumps(baseline['directories'], sort_keys=True, separators=(',', ':')).encode()
    if hashlib.sha256(encoded).hexdigest() != baseline['directories_sha256']:
        raise ValueError('Directory baseline digest mismatch')
    with ExitStack() as stack:
        fd = stack.enter_context(open_absolute_directory(root))
        path = PurePosixPath('.')
        if {'path': str(path), **signature(fd)} != expected.get(str(path)):
            raise ValueError('Root directory changed')
        for component in parent.parts:
            fd = stack.enter_context(open_directory(component, fd))
            path /= component
            if {'path': str(path), **signature(fd)} != expected.get(str(path)):
                raise ValueError(f'Directory changed: {path}')
