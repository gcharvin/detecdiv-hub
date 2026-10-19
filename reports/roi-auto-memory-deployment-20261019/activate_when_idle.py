"""One activation pass, on the compute host; safe to retry from a timer.

The target admission lock closes claim/restart races. Busy pollers and any
poller with child processes are never signalled. No scientific job is created.
"""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from datetime import datetime, timezone

PLATFORM = sys.argv[1]
ROOT = Path.cwd()
STAGE = ROOT / '.deploy-staging/roi-auto-memory-20261019'
TARGET = 'detecdiv-server' if PLATFORM == 'linux' else 'windows-10-20-11-56'
ROLLOUT = 'roi-auto-memory-20261019'
KEY = 'roi_memory_rollout'
MANIFEST = json.loads((STAGE / 'manifest.json').read_text())
EXPECTED = MANIFEST['expected_fingerprint']
sys.path.insert(0, str(ROOT))
if PLATFORM == 'linux':
    manager_pid = subprocess.check_output(
        ['systemctl', 'show', 'detecdiv-worker-manager.service', '-p', 'MainPID', '--value'],
        text=True).strip()
    inherited = dict(item.split('=', 1) for item in
                     Path('/proc', manager_pid, 'environ').read_text().split('\0') if '=' in item)
    os.environ.update({k: v for k, v in inherited.items() if k.startswith('DETECDIV_HUB_')})
else:
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env', override=False)

from sqlalchemy import select, text
from api.db import SessionLocal
from api.models import ExecutionTarget, Job, WorkerInstance


def hash_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def apply_files():
    # Validate every file first; preserve the exact original bytes for rollback.
    for name, item in MANIFEST['files'].items():
        assert hash_file(ROOT / name) in (item['before'], item['after']), 'Live source changed: ' + name
        assert hash_file(STAGE / 'files' / name) == item['after'], 'Staged source changed: ' + name
        compile((STAGE / 'files' / name).read_bytes(), name, 'exec')
    backup = ROOT / '.deploy-backups' / ROLLOUT
    for name, item in MANIFEST['files'].items():
        dest = ROOT / name
        if hash_file(dest) == item['after']:
            continue
        if dest.exists():
            saved = backup / name
            saved.parent.mkdir(parents=True, exist_ok=True)
            if saved.exists():
                assert hash_file(saved) == item['before']
            else:
                saved.write_bytes(dest.read_bytes())
        temporary = dest.with_suffix('.roi-deploy-tmp')
        temporary.write_bytes((STAGE / 'files' / name).read_bytes())
        os.replace(temporary, dest)
    from api.services.runtime_version import get_runtime_version_info
    get_runtime_version_info.cache_clear()
    assert get_runtime_version_info()['code_fingerprint'] == EXPECTED


def restore_admission():
    with SessionLocal.begin() as s:
        s.execute(text("SET LOCAL lock_timeout = '10s'"))
        t = s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key == TARGET)
                      .with_for_update()).one()
        meta = dict(t.metadata_json or {})
        owner = meta.get(KEY)
        if owner:
            assert owner['id'] == ROLLOUT
            if owner['previous_present']:
                meta['drain_new_jobs'] = owner['previous_value']
            else:
                meta.pop('drain_new_jobs', None)
            meta.pop(KEY)
            t.metadata_json = meta


def linux_reload(worker):
    name = worker['instance']
    assert name.startswith('@') and name[1:].isdigit()
    unit = 'detecdiv-worker' + name + '.service'
    pid = int(subprocess.check_output(['systemctl', 'show', unit, '-p', 'MainPID', '--value'], text=True))
    assert pid == worker['pid'], 'PID changed: ' + name
    cg = subprocess.check_output(['systemctl', 'show', unit, '-p', 'ControlGroup', '--value'], text=True).strip()
    if (Path('/sys/fs/cgroup' + cg) / 'cgroup.procs').read_text().split() != [str(pid)]:
        return {'instance': name, 'deferred': 'child_processes_present'}
    assert os.stat('/proc/' + str(pid)).st_uid == os.getuid()
    assert os.path.realpath('/proc/' + str(pid) + '/cwd') == str(ROOT)
    assert subprocess.check_output(['systemctl', 'show', unit, '-p', 'Restart', '--value'], text=True).strip() == 'always'
    os.kill(pid, signal.SIGTERM)
    return {'instance': name, 'reloaded_pid': pid}


def windows_reload(worker):
    assert worker['instance'] == 'windows-10-20-11-56-main'
    # Task Scheduler can leave the Python wrapper alive. Verify the complete
    # managed process tree before stopping only these idle Python processes.
    ps = r'''
$ErrorActionPreference='Stop'
$repo='C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub'
$processes=@(Get-CimInstance Win32_Process)
$workerPid=WORKERPID
$worker=$processes | Where-Object ProcessId -eq $workerPid
if (!$worker -or $worker.Name -ne 'python.exe' -or $worker.CommandLine -notmatch '-m worker.run_worker') {throw 'Worker identity changed'}
$pattern=[regex]::Escape($repo)
$managed=@($processes | Where-Object { $_.Name -eq 'python.exe' -and ([string]$_.CommandLine).Replace([char]92,[char]47) -match $pattern -and $_.CommandLine -match '-m worker.run_worker' })
$ids=@($managed | ForEach-Object ProcessId)
if ($workerPid -notin $ids) {
 if ($worker.ParentProcessId -notin $ids) {throw 'Worker outside managed tree'}
 $ids+=@($workerPid)
}
do {
 $next=@($processes | Where-Object { $_.ParentProcessId -in $ids -and $_.ProcessId -notin $ids })
 $ids+=@($next | ForEach-Object ProcessId)
} while ($next.Count)
if (@($processes | Where-Object { $_.ProcessId -in $ids -and $_.Name -ne 'python.exe' }).Count) {throw 'Worker has child processes; defer'}
$managed=@($processes | Where-Object { $_.ProcessId -in $ids })
Set-Content -LiteralPath "$repo/.deploy-staging/roi-auto-memory-20261019/windows-task-stop-requested" -Value 'idle tree verified'
Stop-ScheduledTask -TaskName 'DetecDiv Hub Worker'
Start-Sleep -Seconds 2
foreach ($expected in $managed) {
 $current=Get-CimInstance Win32_Process -Filter "ProcessId = $($expected.ProcessId)"
 if ($current) {
  if ($current.CreationDate -ne $expected.CreationDate -or $current.CommandLine -ne $expected.CommandLine) {throw 'PID identity changed'}
  Stop-Process -Id $current.ProcessId -Force -ErrorAction Stop
 }
}
if (@(Get-CimInstance Win32_Process | Where-Object ProcessId -in $ids).Count) {throw 'Worker did not stop'}
'''.replace('WORKERPID', str(worker['pid']))
    try:
        subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', ps],
                       check=True, capture_output=True, text=True, timeout=40)
        apply_files()
    finally:
        marker = STAGE / 'windows-task-stop-requested'
        if marker.exists():
            subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-Command',
                            "Start-ScheduledTask -TaskName 'DetecDiv Hub Worker'"],
                           check=True, capture_output=True, text=True, timeout=15)
            marker.unlink()
    return {'instance': worker['instance'], 'reloaded_pid': worker['pid']}


def main():
    # Recover an interrupted prior pass before selecting any new action.
    restore_admission()
    with SessionLocal.begin() as s:
        s.execute(text("SET LOCAL lock_timeout = '2s'"))
        t = s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key == TARGET)
                      .with_for_update()).one()
        workers = s.scalars(select(WorkerInstance).where(WorkerInstance.execution_target_id == t.id)).all()
        assert workers, 'Missing worker registry'
        now = datetime.now(timezone.utc)
        pending = [w for w in workers if w.code_fingerprint != EXPECTED]
        if not pending:
            assert all(w.last_seen_at and (now - w.last_seen_at).total_seconds() < 30 for w in workers)
            return {'complete': True, 'fingerprint': EXPECTED}
        idle = [{'instance': w.worker_instance, 'pid': w.process_id} for w in pending
                if not w.current_job_id and w.last_seen_at and (now - w.last_seen_at).total_seconds() < 30]
        if PLATFORM == 'windows':
            active = list(s.scalars(select(Job.id).where(Job.execution_target_id == t.id,
                            Job.status.in_(('running', 'cancelling')))))
            if active:
                idle = []
        if not idle:
            return {'complete': False, 'deferred': [w.worker_instance for w in pending], 'reason': 'busy_or_stale'}
        meta = dict(t.metadata_json or {})
        if meta.get('drain_new_jobs'):
            return {'complete': False, 'reason': 'existing_admission_drain'}
        meta[KEY] = {'id': ROLLOUT, 'previous_present': 'drain_new_jobs' in meta,
                     'previous_value': meta.get('drain_new_jobs')}
        meta['drain_new_jobs'] = True
        t.metadata_json = meta
    actions = []
    try:
        if PLATFORM == 'linux':
            apply_files()
            for worker in idle:
                actions.append(linux_reload(worker))
            marker = STAGE / 'manager-activation.json'
            if not marker.exists():
                pid = int(subprocess.check_output(['systemctl', 'show', 'detecdiv-worker-manager.service',
                          '-p', 'MainPID', '--value'], text=True))
                assert os.stat('/proc/' + str(pid)).st_uid == os.getuid()
                assert subprocess.check_output(['systemctl', 'show', 'detecdiv-worker-manager.service',
                           '-p', 'Restart', '--value'], text=True).strip() == 'always'
                os.kill(pid, signal.SIGTERM)
                marker.write_text(json.dumps({'old_pid': pid, 'at': now.isoformat()}))
        else:
            actions.append(windows_reload(idle[0]))
    finally:
        restore_admission()
    return {'complete': False, 'actions': actions, 'fingerprint': EXPECTED,
            'awaiting_fresh_heartbeats': True}


if __name__ == '__main__':
    try:
        result = main()
    except Exception as exc:
        result = {'complete': False, 'error': type(exc).__name__ + ': ' + str(exc)}
    result['checked_at'] = datetime.now(timezone.utc).isoformat()
    (STAGE / 'activation-last.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result))
    if result.get('complete') and PLATFORM == 'linux':
        subprocess.run(['systemctl', '--user', 'stop', 'detecdiv-roi-memory-activation-20261019.timer'],
                       capture_output=True, timeout=10)
