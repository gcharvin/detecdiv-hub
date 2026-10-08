"""Detached, resumable COPY/VERIFY phase. Never delete sources or switch the DB.

Run on detecdiv-server with its existing per-user CIFS mount. The immutable
manifest and catalog snapshot are private operational files, not credentials.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import base64
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time


YEARS = ('2023_1', '2023_2', '2023_3')
SOURCE = Path('/data/Alexander/data')
DESTINATION = Path('/homes2/maliavko/DetecdivHub/raw')
IGNORED = ('@eaDir', '#recycle', '#snapshot')

NATIVE_INVENTORY = r'''
import json,os,stat,sys
from pathlib import Path
root=Path(sys.argv[1])
assert str(root) in ['/volume1/DATA/Alexander/data/'+y for y in ('2023_1','2023_2','2023_3')]
assert root.is_dir() and root.resolve()==root
ignored={'@eaDir','#recycle','#snapshot'}
def fail(exc): raise exc
for parent,dirs,files in os.walk(root,followlinks=False,onerror=fail):
 dirs[:]=sorted(d for d in dirs if d not in ignored)
 for name in dirs:
  p=Path(parent)/name
  if p.is_symlink(): raise RuntimeError('Source symlink requires explicit review: '+str(p))
 print(json.dumps({'kind':'directory','path':str(Path(parent).relative_to(root))},sort_keys=True))
 for name in sorted(files):
  if name in ignored: continue
  p=Path(parent)/name; s=p.lstat()
  if not stat.S_ISREG(s.st_mode): raise RuntimeError('Non-regular payload requires review: '+str(p))
  print(json.dumps({'kind':'file','path':str(p.relative_to(root)),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns},sort_keys=True))
'''


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def require_mounts(mountinfo: str | None = None) -> None:
    lines = (mountinfo if mountinfo is not None else Path('/proc/self/mountinfo').read_text()).splitlines()
    expected = {
        '/data': ('//10.20.11.250/DATA', None),
        '/homes2/maliavko': ('//10.20.8.250/home', 'maliavko'),
    }
    targets = {}
    for line in lines:
        left, right = line.split(' - ', 1)
        a, b = left.split(), right.split()
        targets[a[4]] = (a, b)
    for target, (source, identity) in expected.items():
        assert target in targets, f'Missing NAS mount: {target}'
        a, b = targets[target]
        assert b[0] == 'cifs' and b[1] == source, f'Wrong NAS: {target}'
        if identity:
            assert 'rw' in a[5].split(',') and 'username='+identity in b[2].split(','), 'Wrong write identity or read-only home'
    # The most specific mount, including an ancestor of raw/, must be trusted.
    for point, expected_target in [(SOURCE/year, '/data') for year in YEARS]+[(DESTINATION, '/homes2/maliavko')]:
        matches=[target for target in targets if point.as_posix()==target or point.as_posix().startswith(target.rstrip('/')+'/')]
        assert matches and max(matches,key=len)==expected_target, 'Unexpected nested mount'


def validate_manifest(manifest: dict) -> None:
    assert manifest['schema_version'] == 1
    assert manifest['batch_name'] == 'alexander-2023-20261008'
    assert manifest['years'] == list(YEARS)
    assert manifest['source_root'] == SOURCE.as_posix() and manifest['destination_root'] == DESTINATION.as_posix()
    assert manifest['source_deletion_allowed'] is False and manifest['database_cutover_allowed'] is False
    assert 1024 <= manifest['bandwidth_kib'] <= 102400
    assert manifest['minimum_free_bytes'] >= 2_000_000_000_000


def rsync_command(source: Path, destination: Path, bandwidth: int, *, verify: bool) -> list[str]:
    if verify:
        command = ['rsync', '--recursive', '--checksum', '--checksum-choice=sha1',
                   '--dry-run', '--itemize-changes', '--out-format=%i %n%L']
    else:
        command = ['rsync', '--recursive', '--times', '--omit-dir-times',
                   '--no-owner', '--no-group', '--no-perms', '--partial-dir=.rsync-partial',
                   '--info=progress2', '--stats', f'--bwlimit={bandwidth}']
    command += [f'--exclude={name}' for name in (*IGNORED, '.rsync-partial')]
    return command + ['--', source.as_posix()+'/', destination.as_posix()+'/']


def inventory(year: str, output: Path, progress=None) -> dict:
    launcher = "import base64;exec(base64.b64decode('" + base64.b64encode(NATIVE_INVENTORY.encode()).decode() + "'))"
    command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', '-o', 'IdentitiesOnly=yes',
               '-i', '/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519', 'Gilles@10.20.11.250',
               shlex.join(['nice', '-n', '19', 'python3', '-c', launcher, '/volume1/DATA/Alexander/data/'+year])]
    digest = hashlib.sha256()
    size = count = 0
    last_progress = time.monotonic()
    with output.open('wb') as record, output.with_suffix('.errors.log').open('wb') as errors:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=errors)
        try:
            for line in process.stdout:
                row = json.loads(line)
                record.write(line)
                digest.update(line)
                if row['kind'] == 'file':
                    size += row['bytes']
                    count += 1
                if progress is not None and time.monotonic()-last_progress >= 15:
                    progress({'files':count,'bytes':size,'child_pid':process.pid})
                    last_progress=time.monotonic()
            result = process.wait()
            assert result == 0, f'Native inventory failed; see {errors.name}'
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait()
    return {'sha256':digest.hexdigest(), 'bytes':size, 'files':count}


def run_process(command: list[str], output: Path, heartbeat) -> int:
    with output.open('ab') as log:
        process = subprocess.Popen(['nice', '-n', '19', 'ionice', '-c', '3', *command], stdout=log, stderr=subprocess.STDOUT)
        try:
            while process.poll() is None:
                heartbeat(process.pid)
                time.sleep(15)
            return process.returncode
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait()


def main() -> None:
    import fcntl
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    manifest = json.loads(args.manifest.read_text())
    validate_manifest(manifest)
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == manifest['runner_sha256'], 'Runner changed after manifest approval'
    assert Path('/etc/hostname').read_text().strip() == 'GC-CALCUL-306'
    work = args.manifest.resolve().parent
    snapshot=json.loads((work/'catalog-before.json').read_text())
    assert hashlib.sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest()==manifest['catalog_snapshot_sha256']
    assert sorted(r['id'] for r in snapshot['raws'])==sorted(manifest['raw_ids'])
    lock = (work/'transfer.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    state_path = work/'state.json'
    state = json.loads(state_path.read_text()) if state_path.exists() else {
        'batch_name':manifest['batch_name'], 'batch_id':manifest['batch_id'],
        'manifest_sha256':hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
        'started_at':utc(), 'years':{}, 'phase':'preflight',
        'source_deleted':False, 'database_cutover_performed':False}
    assert state['manifest_sha256'] == hashlib.sha256(args.manifest.read_bytes()).hexdigest()
    partial_parent = DESTINATION/('.incoming-'+manifest['batch_name'])

    def save() -> None:
        state['heartbeat_at'] = utc()
        state['runner_pid'] = os.getpid()
        atomic_json(state_path, state)

    def heartbeat(pid: int) -> None:
        state['child_pid'] = pid
        save()

    def inventory_progress(value: dict) -> None:
        state['inventory_progress']=value
        save()

    try:
        require_mounts()
        assert DESTINATION.is_dir() and not DESTINATION.is_symlink()
        for year in YEARS:
            assert (SOURCE/year).is_dir() and not (SOURCE/year).is_symlink()
            assert not (DESTINATION/year).exists(), 'A final target already exists; inspect before reuse'
        partial_parent.mkdir(mode=0o700, exist_ok=True)
        assert not partial_parent.is_symlink()
        for year in YEARS:
            row = state['years'].setdefault(year, {})
            if row.get('phase') == 'verified_in_staging':
                continue
            require_mounts()
            state['phase'] = row['phase'] = 'inventory_source'
            state['current_year'] = year
            save()
            before = inventory(year, work/(year+'-before.jsonl'),inventory_progress)
            row['source_inventory'] = before
            free = os.statvfs(DESTINATION)
            assert free.f_bavail*free.f_frsize >= before['bytes']+manifest['minimum_free_bytes'], 'Insufficient free capacity with margin'
            target = partial_parent/year
            target.mkdir(mode=0o700, exist_ok=True)
            assert not target.is_symlink()
            state['phase'] = row['phase'] = 'copying'
            row['copy_started_at'] = utc()
            save()
            result = run_process(rsync_command(SOURCE/year,target,manifest['bandwidth_kib'],verify=False), work/(year+'-copy.log'), heartbeat)
            assert result == 0, f'Copy failed: rsync exit {result}'
            require_mounts()
            state['phase'] = row['phase'] = 'verifying_full_content'
            save()
            verify_log = work/(year+'-verify.log')
            # A new verification receipt must not reuse a prior failed run's output.
            with verify_log.open('wb'):
                pass
            result = run_process(rsync_command(SOURCE/year,target,manifest['bandwidth_kib'],verify=True), verify_log, heartbeat)
            assert result == 0 and verify_log.stat().st_size == 0, 'Content verification found differences or errors'
            state['phase'] = row['phase'] = 'checking_source_stability'
            save()
            after = inventory(year, work/(year+'-after.jsonl'),inventory_progress)
            assert before == after, 'Source changed while copying; do not publish or cut over'
            require_mounts()
            row.update({'phase':'verified_in_staging','verified_at':utc(),
                        'staging_path':str(target),'content_verification':'full rsync SHA1 source/destination comparison',
                        'source_stability_verified':True})
            save()
        state['phase'] = 'verified_ready_for_catalog_cutover'
        state['finished_at'] = utc()
        save()
    except Exception as exc:
        state['phase'] = 'failed_safe'
        state['error'] = str(exc)
        save()
        raise


if __name__ == '__main__':
    main()
