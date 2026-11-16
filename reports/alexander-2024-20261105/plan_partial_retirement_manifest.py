"""Read-only grouping of explicitly selected 2024 manifest roots.

This does not qualify a root, validate current source or destination data,
authorize deletion, or implement the partial-retirement controller.
"""

import hashlib
import json
import os
from pathlib import PurePosixPath
import stat

from verify_retirement_manifest import EXCLUDED, VERIFIED


# Conservative exclusions from the current evidence; not an eligibility list.
RETAIN = frozenset(name.casefold() for name in (
    '290424_loxP15', '130224_dfob_rep_dslog', '060224_cres',
    '250124_diploids', '230124_creRDNfull_RMng', '061224_X5152moth',
    '22032024_DHY210_nts1ng', '250724_dbud4stest', '281124_X5152bud4s',
))


def _stamp(stream):
    info = os.fstat(stream.fileno())
    if not stat.S_ISREG(info.st_mode):
        raise ValueError('Manifest must be a regular file')
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _scan(stream, selected):
    stream.seek(0)
    whole = hashlib.sha256()
    subsets = {name: {'files': 0, 'bytes': 0, 'directories': 0, 'sha': hashlib.sha256()}
               for name in selected}
    files = size = 0
    for line in stream:
        if not line.endswith(b'\n'):
            raise ValueError('Incomplete manifest record')
        whole.update(line)
        row = json.loads(line)
        if not isinstance(row, dict) or row.get('kind') not in ('directory', 'file'):
            raise ValueError('Unexpected manifest record')
        path = PurePosixPath(row['path'])
        if path.is_absolute() or '..' in path.parts or any(x in EXCLUDED for x in path.parts):
            raise ValueError('Unsafe manifest path')
        if row['kind'] == 'file':
            if type(row.get('bytes')) is not int or row['bytes'] < 0 or not path.parts:
                raise ValueError('Invalid file record')
            files += 1
            size += row['bytes']
        if not path.parts or path.parts[0] not in subsets:
            continue
        target = subsets[path.parts[0]]
        target['sha'].update(line)
        if row['kind'] == 'file':
            target['files'] += 1
            target['bytes'] += row['bytes']
        else:
            target['directories'] += 1
    return (files, size, whole.hexdigest()), {
        name: {'files': item['files'], 'bytes': item['bytes'],
               'directories': item['directories'], 'manifest_subset_sha256': item['sha'].hexdigest()}
        for name, item in subsets.items()}


def _plan(stream, names, expected):
    if not names or len(names) != len(set(names)):
        raise ValueError('Expected distinct explicit roots')
    for name in names:
        if (not isinstance(name, str) or name in ('', '.', '..') or '/' in name or '\\' in name
                or name in EXCLUDED or name.casefold() in RETAIN):
            raise ValueError('Invalid or explicitly retained root')
    initial = _stamp(stream)
    full, first = _scan(stream, names)
    if full != expected or any(not item['files'] or not item['directories'] for item in first.values()):
        raise ValueError('Full manifest or selected root does not match')
    again, second = _scan(stream, names)
    if again != expected or first != second or _stamp(stream) != initial:
        raise ValueError('Manifest changed during planning')
    return first


def plan(year, stream, names):
    """Group selected roots only against an already-open pinned 2024 manifest."""
    if year not in VERIFIED:
        raise ValueError('Unsupported year')
    return {'year': year, 'read_only': True, 'eligible_for_unlink': False,
            'roots': _plan(stream, names, VERIFIED[year])}
