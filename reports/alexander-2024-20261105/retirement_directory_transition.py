"""Read-only fixture prototype for validating one directory transition.

It never unlinks files and provides no production authorization or crash
recovery. A surrounding controller must supply authenticated baselines and
handle concurrent changes, leases, receipts and filesystem-specific behavior.
"""

import os
import stat


FIELDS = ('dev', 'ino', 'mode', 'uid', 'gid', 'nlink', 'size', 'mtime_ns', 'ctime_ns')
PARENT_STABLE = ('dev', 'ino', 'mode', 'uid', 'gid', 'nlink')


def metadata(info):
    return {name: getattr(info, 'st_' + name) for name in FIELDS}


def capture(fd):
    """Capture a stable parent and its direct children through an open fd."""
    before = os.fstat(fd)
    if not stat.S_ISDIR(before.st_mode):
        raise ValueError('Expected an open directory')
    children = {}
    with os.scandir(fd) as entries:
        for entry in entries:
            info = entry.stat(follow_symlinks=False)
            if info.st_dev != before.st_dev or not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
                raise ValueError('Unexpected child type or filesystem')
            children[entry.name] = metadata(info)
    after = os.fstat(fd)
    if metadata(before) != metadata(after):
        raise ValueError('Parent changed while capturing children')
    return {'parent': metadata(after), 'children': children}


def validate_one_file_removal(before, after, name, expected_file):
    """Accept only a captured one-file removal; does not perform the removal."""
    if not isinstance(name, str) or not name or name in ('.', '..') or '/' in name:
        raise ValueError('Invalid file name')
    old = before['children']
    new = after['children']
    if old.get(name) != expected_file or not stat.S_ISREG(expected_file['mode']):
        raise ValueError('Removed file does not match evidence')
    if any(before['parent'][key] != after['parent'][key] for key in PARENT_STABLE):
        raise ValueError('Parent identity or permissions changed')
    if set(old) - {name} != set(new):
        raise ValueError('Unexpected directory entry change')
    if any(old[key] != new[key] for key in new):
        raise ValueError('Another directory entry changed')
    return after['parent']
