"""Read-only verification of selected roots against a complete 2024 manifest.

Successful verification neither qualifies dependencies nor authorizes unlink.
It is not an atomic snapshot and does not acquire Hub leases.
"""

import os
from pathlib import Path
import stat

from plan_partial_retirement_manifest import _plan
from verify_retirement_manifest import SOURCE, VERIFIED, open_absolute_directory, verify


def _stamp(fd):
    info = os.fstat(fd)
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _verify(root_fd, manifest, expected, expected_subsets):
    if not expected_subsets or not isinstance(expected_subsets, dict):
        raise ValueError('Explicit authenticated subset evidence is required')
    if not stat.S_ISREG(os.fstat(manifest.fileno()).st_mode):
        raise ValueError('Manifest must be a regular file')
    root_before = _stamp(root_fd)
    manifest_before = _stamp(manifest.fileno())
    manifest.seek(0)
    facts = verify(root_fd, manifest, expected)
    selected = _plan(manifest, sorted(expected_subsets), expected)
    if selected != expected_subsets:
        raise ValueError('Subset manifest evidence does not match')
    if _stamp(root_fd) != root_before or _stamp(manifest.fileno()) != manifest_before:
        raise ValueError('Root or manifest changed during verification')
    return {'read_only': True, 'eligible_for_unlink': False,
            'full_manifest': facts, 'selected_roots': selected}


def verify_2024_source(year, manifest, expected_subsets):
    """Open the fixed native year root without following symlinks and verify."""
    if year not in VERIFIED:
        raise ValueError('Unsupported year')
    with open_absolute_directory(Path(SOURCE / year)) as root_fd:
        return _verify(root_fd, manifest, VERIFIED[year], expected_subsets)
