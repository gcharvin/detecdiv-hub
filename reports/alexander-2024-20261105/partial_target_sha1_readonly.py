"""Read-only, bounded SHA1 comparison for selected 2024 acquisition roots.

This actor does not copy, change catalog state, or authorize retirement. A
fresh native source recheck and all dependency/admission gates remain required.
"""

from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time

WORK = Path(__file__).resolve().parent
BASE = WORK.parent
PIN = 'a0576fffa3c1d43d232f54567c6c9f7153aed40b436ace98f26079af72432b99'
RETAIN = frozenset(x.casefold() for x in (
    '290424_loxP15', '130224_dfob_rep_dslog', '060224_cres', '250124_diploids',
    '230124_creRDNfull_RMng', '061224_X5152moth', '22032024_DHY210_nts1ng',
    '250724_dbud4stest', '281124_X5152bud4s',
))
IGNORED = {'@eaDir', '#recycle', '#snapshot', '.snapshot', '@Recycle', '.rsync-partial'}


def utc():
    return datetime.now(timezone.utc).isoformat()


def state(value):
    tmp = WORK / 'state.tmp'
    with tmp.open('w', encoding='utf8') as stream:
        json.dump(value, stream, sort_keys=True)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    tmp.replace(WORK / 'state.json')


def inventory(root, progress=None):
    """Fingerprint native path metadata as seen from compute; read-only."""
    root = Path(root)
    if not root.is_dir() or root.is_symlink():
        raise ValueError('Expected a real directory')
    device = root.lstat().st_dev
    digest = hashlib.sha256()
    files = size = directories = 0
    last = time.monotonic()

    def fail(error):
        raise error

    for parent, dirs, names in os.walk(root, followlinks=False, onerror=fail):
        parent = Path(parent)
        dirs[:] = sorted(d for d in dirs if d not in IGNORED)
        info = parent.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_dev != device:
            raise ValueError('Unexpected directory or filesystem')
        for name in dirs:
            child = (parent / name).lstat()
            if not stat.S_ISDIR(child.st_mode) or child.st_dev != device:
                raise ValueError('Unexpected directory child')
        row = {'kind': 'directory', 'path': str(parent.relative_to(root)),
               'inode': info.st_ino, 'mode': info.st_mode, 'uid': info.st_uid, 'gid': info.st_gid,
               'mtime_ns': info.st_mtime_ns, 'ctime_ns': info.st_ctime_ns}
        digest.update((json.dumps(row, sort_keys=True) + '\n').encode())
        directories += 1
        for name in sorted(n for n in names if n not in IGNORED):
            path = parent / name
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_dev != device:
                raise ValueError('Unexpected file type or filesystem')
            row = {'kind': 'file', 'path': str(path.relative_to(root)), 'bytes': info.st_size,
                   'inode': info.st_ino, 'mode': info.st_mode, 'uid': info.st_uid, 'gid': info.st_gid,
                   'mtime_ns': info.st_mtime_ns, 'ctime_ns': info.st_ctime_ns}
            digest.update((json.dumps(row, sort_keys=True) + '\n').encode())
            files += 1
            size += info.st_size
            if progress and time.monotonic() - last >= 20:
                progress(files, size, directories)
                last = time.monotonic()
    return {'files': files, 'bytes': size, 'directories': directories, 'sha256': digest.hexdigest()}


def safe_child(base, year, name):
    if year not in ('2024_1', '2024_2') or not isinstance(name, str) or name in ('', '.', '..'):
        raise ValueError('Invalid root')
    if '/' in name or '\\' in name or name.casefold() in RETAIN:
        raise ValueError('Retained or unsafe root')
    for part in (base / year, base / year / name):
        info = part.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError('Root must be a directory, not a symlink')
    return base / year / name


def main(expected_request_sha):
    payload = (WORK / 'request.json').read_bytes()
    assert hashlib.sha256(payload).hexdigest() == expected_request_sha
    request = json.loads(payload)
    assert request['read_only'] and not request['eligible_for_unlink']
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == request['script_sha256']
    assert len(request['roots']) == 17 and len({(x['year'], x['name']) for x in request['roots']}) == 17
    original = BASE / 'copy_raws.py'
    assert hashlib.sha256(original.read_bytes()).hexdigest() == PIN
    spec = importlib.util.spec_from_file_location('pinned_original_copy', original)
    copy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(copy)
    copy.require_mounts()
    source_base = Path('/data/Alexander/data')
    target_base = Path('/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105')
    overall = {'phase': 'running', 'started_at': utc(), 'read_only': True,
               'eligible_for_unlink': False, 'roots': []}
    state(overall)
    try:
        for index, item in enumerate(request['roots'], 1):
            year, name = item['year'], item['name']
            source = safe_child(source_base, year, name)
            target = safe_child(target_base, year, name)
            overall['current'] = {'index': index, 'year': year, 'name': name, 'phase': 'metadata_before',
                                  'heartbeat_at': utc()}
            state(overall)
            def collect(root, phase):
                overall['current'].update(phase=phase, heartbeat_at=utc())
                state(overall)
                def heartbeat(files, size, directories):
                    overall['current'].update(files=files, bytes=size, directories=directories, heartbeat_at=utc())
                    state(overall)
                return inventory(root, heartbeat)

            before_source = collect(source, 'source_metadata_before')
            before_target = collect(target, 'target_metadata_before')
            for measured in (before_source, before_target):
                assert (measured['files'], measured['bytes'], measured['directories']) == (
                    item['files'], item['bytes'], item['directories'])
            args = copy.rsync(source, target, 0, verify=True)
            assert '--dry-run' in args and '--checksum-choice=sha1' in args and '--delete' in args
            log = WORK / ('root-' + str(index).zfill(2) + '-sha1.log')
            with log.open('xb') as output:
                proc = subprocess.Popen(['nice', '-n', '19', 'ionice', '-c', '3', *args],
                                        stdout=output, stderr=subprocess.STDOUT)
                try:
                    while proc.poll() is None:
                        overall['current'].update(phase='sha1', pid=proc.pid, heartbeat_at=utc())
                        state(overall)
                        time.sleep(15)
                    code = proc.returncode
                finally:
                    if proc.poll() is None:
                        proc.terminate()
                        proc.wait(timeout=15)
                output.flush()
                os.fsync(output.fileno())
            assert code == 0 and log.stat().st_size == 0, 'SHA1 comparison failed or differs'
            overall['current'].update(phase='metadata_after', heartbeat_at=utc())
            state(overall)
            after_source = collect(source, 'source_metadata_after')
            after_target = collect(target, 'target_metadata_after')
            assert before_source == after_source and before_target == after_target, 'Metadata changed during SHA1'
            overall['roots'].append({'year': year, 'name': name, 'files': item['files'], 'bytes': item['bytes'],
                                     'directories': item['directories'], 'sha1_compare_exit': code,
                                     'log_bytes': log.stat().st_size, 'source_metadata': before_source,
                                     'target_metadata': before_target, 'finished_at': utc()})
            state(overall)
        overall.update(phase='success', finished_at=utc())
    except Exception as exc:
        overall.update(phase='failed_safe', finished_at=utc(), error=type(exc).__name__ + ': ' + str(exc))
        state(overall)
        raise
    state(overall)
    with (WORK / 'result.json').open('x', encoding='utf8') as stream:
        json.dump(overall, stream, sort_keys=True)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


if __name__ == '__main__':
    assert len(sys.argv) == 2
    main(sys.argv[1])
