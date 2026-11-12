"""Validate the native Alexander 2024 source tree against its verified copy manifest."""

import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat


SOURCE = PurePosixPath('/volume1/DATA/Alexander/data')
VERIFIED = {
    '2024_1': (732579, 5177814875236, '8fd64d094fe29707e0ddd0b1480e9b861786d3adde677087ca7a6e95e252f61e'),
    '2024_2': (974112, 5856516249162, '004fb3404b4af8fe09b6063a09e3a162488f689499ddb51087513daf3d9f4fad'),
}
EXCLUDED = {'@eaDir', '#recycle', '#snapshot', '.snapshot', '@Recycle'}


@contextmanager
def open_directory(path, parent_fd=None):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
    try:
        yield fd
    finally:
        os.close(fd)


@contextmanager
def open_absolute_directory(path):
    if not path.is_absolute() or '..' in path.parts or path.anchor != '/':
        raise ValueError(f'Expected an absolute native path: {path}')
    with ExitStack() as stack:
        fd = stack.enter_context(open_directory('/'))
        for component in path.parts[1:]:
            fd = stack.enter_context(open_directory(component, fd))
        yield fd


def file_row(parent_fd, name, relative, device):
    info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode) or info.st_dev != device:
        raise ValueError(f'Unexpected file type or filesystem: {relative}')
    return {
        'kind': 'file', 'path': str(relative), 'bytes': info.st_size, 'mtime_ns': info.st_mtime_ns,
        'inode': info.st_ino, 'uid': info.st_uid, 'gid': info.st_gid, 'mode': info.st_mode,
        'nlink': info.st_nlink,
    }


def fail_walk(error):
    raise error


def native_rows(root_fd):
    device = os.fstat(root_fd).st_dev
    for parent, directories, files, parent_fd in os.fwalk(
        '.', follow_symlinks=False, onerror=fail_walk, dir_fd=root_fd,
    ):
        directory = PurePosixPath(parent)
        if os.fstat(parent_fd).st_dev != device:
            raise ValueError(f'Unexpected mounted filesystem: {directory}')
        directories[:] = sorted(set(directories) - EXCLUDED)
        for name in directories:
            info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode) or info.st_dev != device:
                raise ValueError(f'Unexpected directory type or filesystem: {directory / name}')
        yield {'kind': 'directory', 'path': str(directory)}
        for name in sorted(set(files) - EXCLUDED):
            yield file_row(parent_fd, name, directory / name, device)


def verify(root_fd, manifest, expected):
    digest = hashlib.sha256()
    files = logical_bytes = 0
    for current in native_rows(root_fd):
        line = manifest.readline()
        if not line or json.loads(line) != current:
            raise ValueError(f'Native manifest mismatch at {current["path"]}')
        digest.update(line)
        if current['kind'] == 'file':
            files += 1
            logical_bytes += current['bytes']
    if manifest.read(1):
        raise ValueError('Native tree is missing entries from the verified manifest')
    if (files, logical_bytes, digest.hexdigest()) != expected:
        raise ValueError('Manifest digest, file count or byte count does not match verified copy evidence')
    return {'files': files, 'logical_bytes': logical_bytes, 'manifest_sha256': digest.hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('year', choices=VERIFIED)
    parser.add_argument('manifest', type=Path)
    args = parser.parse_args()
    with open_absolute_directory(SOURCE / args.year) as root_fd, args.manifest.open('rb') as manifest:
        result = verify(root_fd, manifest, VERIFIED[args.year])
    print(json.dumps({'read_only': True, 'year': args.year, **result}))


if __name__ == '__main__':
    main()
