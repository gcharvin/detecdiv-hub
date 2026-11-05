"""Copy 2024_1, then 2024_2; verify every byte and source stability.

No source removal, project write, publication or catalog cutover is implemented.
This is an immutable one-shot runner: inspect partial state before any recovery.
"""
import base64
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

YEARS = ('2024_1', '2024_2')
WORK = Path(__file__).resolve().parent
SOURCE = Path('/data/Alexander/data')
HOME = Path('/homes2/maliavko/DetecdivHub')
STAGING = HOME/'raw/.incoming-alexander-2024-20261105'
IGNORED = ('@eaDir', '#recycle', '#snapshot', '.snapshot', '@Recycle')
NATIVE = r'''
import json,os,stat,sys
from pathlib import Path
root=Path(sys.argv[1])
assert str(root) in ['/volume1/DATA/Alexander/data/2024_1','/volume1/DATA/Alexander/data/2024_2']
assert root.is_dir() and root.resolve()==root
ignored={'@eaDir','#recycle','#snapshot','.snapshot','@Recycle'}
def fail(e):raise e
for parent,dirs,files in os.walk(root,followlinks=False,onerror=fail):
 dirs[:]=sorted(d for d in dirs if d not in ignored)
 for d in dirs:assert not (Path(parent)/d).is_symlink(),'Source directory symlink'
 print(json.dumps({'kind':'directory','path':str(Path(parent).relative_to(root))},sort_keys=True))
 for name in sorted(files):
  if name in ignored:continue
  p=Path(parent)/name;s=p.lstat();assert stat.S_ISREG(s.st_mode),'Non-regular source payload'
  print(json.dumps({'kind':'file','path':str(p.relative_to(root)),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,
   'inode':s.st_ino,'uid':s.st_uid,'gid':s.st_gid,'mode':s.st_mode,'nlink':s.st_nlink},sort_keys=True))
'''


def utc():
    return datetime.now(timezone.utc).isoformat()


def atomic(path, value):
    temporary = path.with_suffix(path.suffix+'.tmp')
    with temporary.open('w', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
    temporary.replace(path)


def require_mounts():
    targets = {}
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        left, right = line.split(' - ', 1)
        a, b = left.split(), right.split()
        targets[a[4]] = (a, b)
    for target, source in [('/data', '//10.20.11.250/DATA'), ('/homes2/maliavko', '//10.20.8.250/home')]:
        a, b = targets[target]
        assert b[0]=='cifs' and b[1]==source, 'Unexpected NAS mount'
        if target.startswith('/homes2'):
            assert 'rw' in a[5].split(',') and 'username=maliavko' in b[2].split(',')
    for point, mount in [(SOURCE/y, '/data') for y in YEARS]+[(STAGING, '/homes2/maliavko')]:
        ancestors = [p for p in targets if str(point)==p or str(point).startswith(p.rstrip('/')+'/')]
        assert max(ancestors, key=len)==mount, 'Unexpected nested mount'
        current = point
        while str(current) != mount:
            assert not current.is_symlink(), 'Unexpected path symlink'
            current = current.parent


def inventory(year, output, progress):
    assert year in YEARS and not output.exists(), 'Inspect existing inventory before recovery'
    command = ['ssh', '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes', '-o', 'ServerAliveInterval=15',
               '-o', 'ServerAliveCountMax=3', '-i', '/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519',
               'Gilles@10.20.11.250', shlex.join(['nice', '-n', '19', 'python3', '-u', '-c', NATIVE,
                                                            '/volume1/DATA/Alexander/data/'+year])]
    h = hashlib.sha256(); count = size = 0; last = time.monotonic()
    with output.open('xb') as record, output.with_suffix('.errors.log').open('xb') as errors:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=errors)
        try:
            for line in process.stdout:
                row = json.loads(line); record.write(line); h.update(line)
                if row['kind']=='file':
                    count += 1; size += row['bytes']
                if time.monotonic()-last>=15:
                    progress({'files':count, 'bytes':size, 'child_pid':process.pid}); last=time.monotonic()
            assert process.wait()==0, 'Native inventory failed; inspect its error log'
            record.flush(); os.fsync(record.fileno())
        finally:
            if process.poll() is None:
                process.terminate(); process.wait()
    return {'files':count, 'bytes':size, 'sha256':h.hexdigest()}


def rsync(source, destination, bandwidth, verify=False):
    if verify:
        # --delete is deliberately restricted to --dry-run, detecting destination extras.
        args = ['rsync', '--recursive', '--checksum', '--checksum-choice=sha1', '--dry-run',
                '--delete', '--itemize-changes', '--out-format=%i %n%L']
    else:
        args = ['rsync', '--recursive', '--times', '--omit-dir-times', '--no-owner', '--no-group',
                '--no-perms', '--partial-dir=.rsync-partial', '--info=progress2', '--stats',
                '--bwlimit='+str(bandwidth)]
    args += ['--exclude='+name for name in (*IGNORED, '.rsync-partial')]
    return args+['--', str(source)+'/', str(destination)+'/']


def run_process(args, output, heartbeat):
    assert not output.exists(), 'Never append to a previous copy/verification receipt'
    with output.open('xb') as log:
        process = subprocess.Popen(['nice', '-n', '19', 'ionice', '-c', '3', *args], stdout=log, stderr=subprocess.STDOUT)
        try:
            while process.poll() is None:
                heartbeat(process.pid); time.sleep(15)
            return process.returncode
        finally:
            if process.poll() is None:
                process.terminate(); process.wait()


def main():
    os.umask(0o077)
    request = json.loads((WORK/'request.json').read_text())
    assert request['years']==list(YEARS) and request['bandwidth_kib']==61440
    assert request['source_deletion_allowed'] is False and request['catalog_cutover_allowed'] is False
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==request['runner_sha256']
    assert Path('/etc/hostname').read_text().strip()=='GC-CALCUL-306'
    snapshot = (WORK/'catalog-before.json').read_bytes()
    assert hashlib.sha256(snapshot).hexdigest()==request['catalog_snapshot_sha256']
    lock = (WORK/'transfer.lock').open('a'); fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
    assert not (WORK/'state.json').exists(), 'One-shot runner already started; inspect its state'
    state = {'phase':'preflight', 'started_at':utc(), 'batch_id':request['batch_id'], 'years':{},
             'source_deleted':False, 'catalog_changed':False, 'runner_pid':os.getpid()}
    def save():
        state['heartbeat_at']=utc(); atomic(WORK/'state.json', state)
    def heartbeat(pid):
        state['child_pid']=pid; save()
    def progress(value):
        state['inventory_progress']=value; save()
    save()
    try:
        require_mounts()
        assert STAGING.is_dir() and not STAGING.is_symlink() and not list(STAGING.iterdir())
        free=os.statvfs(STAGING)
        assert free.f_bavail*free.f_frsize>=request['estimated_total_bytes']+2_000_000_000_000
        for year in YEARS:
            assert (SOURCE/year).is_dir() and not (HOME/'raw'/year).exists()
        for year in YEARS:
            directory=WORK/year; directory.mkdir(mode=0o700)
            state['current_year']=year
            row=state['years'][year]={'phase':'inventory_source', 'source':str(SOURCE/year), 'destination':str(STAGING/year)}
            state['phase']='inventory_source'; save()
            before=inventory(year,directory/'source-before.jsonl',progress)
            row['source_inventory']=before; save()
            expected=request['initial_measurements'][year]
            assert (before['files'],before['bytes'])==(expected['files'],expected['bytes']), 'Source differs from measured scope; review before copy'
            require_mounts(); free=os.statvfs(STAGING)
            assert free.f_bavail*free.f_frsize>=before['bytes']+2_000_000_000_000
            target=STAGING/year; target.mkdir(mode=0o700)
            state['phase']=row['phase']='copying'; row['copy_started_at']=utc(); save()
            rc=run_process(rsync(SOURCE/year,target,request['bandwidth_kib']),directory/'copy.log',heartbeat)
            assert rc==0, 'Copy failed: '+str(rc)
            require_mounts(); row['copy_finished_at']=utc()
            state['phase']=row['phase']='verifying_full_content'; save()
            log=directory/'verify-full-sha1.log'
            rc=run_process(rsync(SOURCE/year,target,request['bandwidth_kib'],verify=True),log,heartbeat)
            assert rc==0 and log.stat().st_size==0, 'Content difference/error; preserve source and inspect'
            row['full_sha1_passed']=True; row['sha1_finished_at']=utc()
            state['phase']=row['phase']='checking_source_stability'; save()
            after=inventory(year,directory/'source-after.jsonl',progress)
            assert before==after, 'Source changed; preserve all files and inspect'
            row.update(phase='verified_in_staging_project_host_catalog_gates_pending',source_stability_verified=True,finished_at=utc())
            require_mounts(); save()
        state.update(phase='both_years_verified_project_host_catalog_gates_pending',finished_at=utc()); save()
    except Exception as error:
        state.update(phase='failed_safe',error=type(error).__name__+': '+str(error)); save(); raise


if __name__=='__main__':
    main()
