"""Read and validate an externally pinned 2024 directory baseline.

This is a read-only building block, not an admission or retirement controller.
The expected hash must come from independently authenticated evidence.
"""

import hashlib
import hmac
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat

from verify_retirement_manifest import SOURCE, VERIFIED, open_absolute_directory

MAX_BASELINE_BYTES = 128 * 1024 * 1024


def _validate_envelope(envelope, year, source, expected):
    if envelope.get('year') != year or envelope.get('source') != str(source):
        raise ValueError('Baseline year or source does not match')
    baseline = envelope.get('baseline')
    if not isinstance(baseline, dict):
        raise ValueError('Missing directory baseline')
    facts = baseline.get('manifest')
    if facts != {'files': expected[0], 'logical_bytes': expected[1], 'manifest_sha256': expected[2]}:
        raise ValueError('Baseline manifest evidence does not match')
    rows = baseline.get('directories')
    if not isinstance(rows, list) or not rows or rows[0].get('path') != '.':
        raise ValueError('Missing root directory')
    keys = {'path', 'dev', 'ino', 'mode', 'uid', 'gid', 'nlink', 'size', 'mtime_ns', 'ctime_ns'}
    for row in rows:
        if not isinstance(row, dict) or set(row) != keys or not isinstance(row['path'], str):
            raise ValueError('Invalid directory signature')
        directory = PurePosixPath(row['path'])
        if directory.is_absolute() or '..' in directory.parts or str(directory) != row['path']:
            raise ValueError('Invalid directory path')
        if any(not isinstance(row[k], int) or isinstance(row[k], bool) for k in keys - {'path'}):
            raise ValueError('Invalid directory metadata')
    paths = [row.get('path') for row in rows]
    if len(paths) != len(set(paths)):
        raise ValueError('Duplicate directory paths')
    data = json.dumps(rows, sort_keys=True, separators=(',', ':')).encode()
    if not hmac.compare_digest(hashlib.sha256(data).hexdigest(), baseline.get('directories_sha256', '')):
        raise ValueError('Directory digest does not match')
    return baseline


def load_2024_baseline(path, year, expected_sha256):
    """Return a baseline only when path, year and external digest all match."""
    if year not in VERIFIED or not isinstance(expected_sha256, str) or not re.fullmatch('[0-9a-f]{64}', expected_sha256):
        raise ValueError('Invalid year or externally pinned digest')
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('Expected a canonical absolute path')
    with open_absolute_directory(path.parent) as parent:
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_BASELINE_BYTES:
                raise ValueError('Baseline must be a bounded regular file')
            with os.fdopen(fd, 'rb', closefd=False) as stream:
                data = stream.read(MAX_BASELINE_BYTES + 1)
            after = os.fstat(fd)
            stamp = lambda x: (x.st_dev, x.st_ino, x.st_size, x.st_mtime_ns, x.st_ctime_ns)
            if len(data) != before.st_size or stamp(before) != stamp(after):
                raise ValueError('Baseline changed while reading')
        finally:
            os.close(fd)
    if not hmac.compare_digest(hashlib.sha256(data).hexdigest(), expected_sha256):
        raise ValueError('External baseline digest does not match')
    return _validate_envelope(json.loads(data), year, SOURCE / year, VERIFIED[year])
