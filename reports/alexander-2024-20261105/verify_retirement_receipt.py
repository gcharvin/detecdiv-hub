"""Read-only verifier for a complete, ordered 2024 withdrawal receipt.

This checks the receipt against the pinned native manifest. It neither proves
the remaining tree is empty nor authorizes deletion or catalog changes.
"""

import hashlib
import json
import os
import stat

from verify_retirement_manifest import VERIFIED


def _read_row(stream):
    line = stream.readline()
    if not line or not line.endswith(b'\n'):
        raise ValueError('Missing or incomplete receipt record')
    value = json.loads(line)
    if not isinstance(value, dict):
        raise ValueError('Invalid receipt record')
    return value


def _verify(manifest, receipt, expected):
    def stamp(stream):
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('Evidence must be a regular file')
        return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)

    initial_manifest = stamp(manifest)
    initial_receipt = stamp(receipt)
    files_expected, bytes_expected, digest_expected = expected
    digest = hashlib.sha256()
    count = size = 0
    manifest.seek(0)
    for line in manifest:
        if not line.endswith(b'\n'):
            raise ValueError('Incomplete manifest record')
        digest.update(line)
        row = json.loads(line)
        if row.get('kind') == 'file':
            count += 1
            size += row['bytes']
    if (count, size, digest.hexdigest()) != expected:
        raise ValueError('Manifest does not match pinned copy evidence')

    receipt.seek(0)
    start = _read_row(receipt)
    if start.get('event') != 'start' or (start.get('files'), start.get('logical_bytes'),
                                          start.get('manifest_sha256')) != expected:
        raise ValueError('Receipt start does not match manifest evidence')
    available_before = start.get('available_before')
    if type(available_before) is not int or available_before < 0:
        raise ValueError('Invalid initial space measurement')
    manifest.seek(0)
    for line in manifest:
        row = json.loads(line)
        if row.get('kind') != 'file':
            continue
        intent = _read_row(receipt)
        unlinked = _read_row(receipt)
        if intent.get('event') != 'intent' or unlinked.get('event') != 'unlinked':
            raise ValueError('Missing or out-of-order per-file receipt')
        if intent.get('file') != row or unlinked.get('file') != row:
            raise ValueError('Per-file receipt does not match manifest')
    complete = _read_row(receipt)
    if complete.get('event') != 'complete' or receipt.read(1):
        raise ValueError('Receipt is incomplete or contains extra records')
    if (complete.get('files'), complete.get('logical_bytes'), complete.get('available_before')) != (
            files_expected, bytes_expected, available_before):
        raise ValueError('Receipt totals do not match')
    after = complete.get('available_after')
    if type(after) is not int or after < 0 or complete.get('available_delta') != after - available_before:
        raise ValueError('Invalid final space measurement')
    if stamp(manifest) != initial_manifest or stamp(receipt) != initial_receipt:
        raise ValueError('Evidence changed during receipt verification')
    return {'files': count, 'logical_bytes': size, 'manifest_sha256': digest_expected,
            'available_before': available_before, 'available_after': after,
            'available_delta': after - available_before}


def verify_2024_receipt(year, manifest, receipt):
    """Verify already-open binary streams against the pinned 2024 manifest."""
    if year not in VERIFIED:
        raise ValueError('Unsupported year')
    return _verify(manifest, receipt, VERIFIED[year])
