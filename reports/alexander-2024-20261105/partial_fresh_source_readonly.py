"""Compare fresh native 2024 inventories to pinned manifests, read-only.

One-shot compute actor. It never copies, modifies or deletes source data and
never grants retirement eligibility. A later controller must recheck freshness.
"""

from datetime import datetime, timezone
import hashlib
import importlib.util
from itertools import zip_longest
import json
import os
from pathlib import Path, PurePosixPath
import shlex
import subprocess
import sys
import time

WORK = Path(__file__).resolve().parent
BASE = WORK.parent
PIN = 'a0576fffa3c1d43d232f54567c6c9f7153aed40b436ace98f26079af72432b99'
VERIFIED = {
    '2024_1': (732579, 5177814875236, '8fd64d094fe29707e0ddd0b1480e9b861786d3adde677087ca7a6e95e252f61e'),
    '2024_2': (974112, 5856516249162, '004fb3404b4af8fe09b6063a09e3a162488f689499ddb51087513daf3d9f4fad'),
}
RETAIN = frozenset(x.casefold() for x in (
    '290424_loxP15', '130224_dfob_rep_dslog', '060224_cres', '250124_diploids',
    '230124_creRDNfull_RMng', '061224_X5152moth', '22032024_DHY210_nts1ng',
    '250724_dbud4stest', '281124_X5152bud4s',
))


def utc():
    return datetime.now(timezone.utc).isoformat()


def state(value):
    temporary = WORK / 'state.tmp'
    with temporary.open('w', encoding='utf8') as stream:
        json.dump(value, stream, sort_keys=True)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(WORK / 'state.json')


def main(expected_request_sha):
    raw = (WORK / 'request.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == expected_request_sha
    request = json.loads(raw)
    assert request['read_only'] is True and request['eligible_for_unlink'] is False
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == request['script_sha256']
    original = BASE / 'copy_raws.py'
    assert hashlib.sha256(original.read_bytes()).hexdigest() == PIN
    spec = importlib.util.spec_from_file_location('pinned_original_copy', original)
    original_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(original_module)
    overall = {'phase': 'running', 'started_at': utc(), 'read_only': True,
               'eligible_for_unlink': False, 'years': {}}
    state(overall)
    try:
        for year in ('2024_1', '2024_2'):
            plan = request['years'].get(year)
            if not plan:
                continue
            assert (plan['files'], plan['bytes'], plan['manifest_sha256']) == VERIFIED[year]
            assert plan['roots'] and all('/' not in x and '\\' not in x and x not in ('', '.', '..')
                                         and x.casefold() not in RETAIN for x in plan['roots'])
            manifest = BASE / year / 'source-before.jsonl'
            command = ['ssh', '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes',
                       '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3',
                       '-i', '/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519',
                       'Gilles@10.20.11.250', shlex.join(['nice', '-n', '19', 'python3', '-u', '-c',
                       original_module.NATIVE, '/volume1/DATA/Alexander/data/' + year])]
            overall['current_year'] = year
            overall['years'][year] = {'phase': 'running', 'files': 0, 'bytes': 0, 'heartbeat_at': utc()}
            state(overall)
            whole = hashlib.sha256()
            groups = {root: {'files': 0, 'bytes': 0, 'directories': 0, 'sha': hashlib.sha256()}
                      for root in plan['roots']}
            count = size = 0
            last = time.monotonic()
            with manifest.open('rb') as reference, (WORK / (year + '-ssh-errors.log')).open('xb') as errors:
                before = os.fstat(reference.fileno())
                stamp = lambda x: (x.st_dev, x.st_ino, x.st_size, x.st_mtime_ns, x.st_ctime_ns)
                proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=errors)
                try:
                    for old, current in zip_longest(reference, proc.stdout):
                        if old is None or current is None or json.loads(old) != json.loads(current):
                            with (WORK / (year + '-first-mismatch.json')).open('x', encoding='utf8') as detail:
                                json.dump({'expected': json.loads(old) if old else None,
                                           'current': json.loads(current) if current else None}, detail)
                                detail.write('\n')
                                detail.flush()
                                os.fsync(detail.fileno())
                            raise ValueError('Native source inventory differs from pinned manifest')
                        whole.update(old)
                        row = json.loads(old)
                        path = PurePosixPath(row['path'])
                        assert not path.is_absolute() and '..' not in path.parts
                        if row['kind'] == 'file':
                            count += 1
                            size += row['bytes']
                        if path.parts and path.parts[0] in groups:
                            item = groups[path.parts[0]]
                            item['sha'].update(old)
                            if row['kind'] == 'file':
                                item['files'] += 1
                                item['bytes'] += row['bytes']
                            else:
                                item['directories'] += 1
                        if time.monotonic() - last >= 20:
                            overall['years'][year].update(files=count, bytes=size, heartbeat_at=utc(), child_pid=proc.pid)
                            state(overall)
                            last = time.monotonic()
                    assert proc.wait() == 0, 'Native source inventory process failed'
                finally:
                    if proc.poll() is None:
                        proc.terminate()
                        proc.wait(timeout=15)
                assert stamp(os.fstat(reference.fileno())) == stamp(before), 'Pinned manifest changed'
            assert (count, size, whole.hexdigest()) == (plan['files'], plan['bytes'], plan['manifest_sha256'])
            actual = {name: {'files': item['files'], 'bytes': item['bytes'], 'directories': item['directories'],
                             'manifest_subset_sha256': item['sha'].hexdigest()} for name, item in groups.items()}
            assert actual == plan['roots'], 'Selected subset evidence mismatch'
            overall['years'][year] = {'phase': 'success', 'files': count, 'bytes': size,
                                      'manifest_sha256': whole.hexdigest(), 'roots': actual, 'finished_at': utc()}
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
