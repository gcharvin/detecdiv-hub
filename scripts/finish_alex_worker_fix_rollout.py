"""Activate the prepared Alex worker fixes only on idle pollers.

Run once from the Windows administration workstation, or from a heartbeat.
Target admission is reserved only for the short idle reload, not while waiting
for analyses. No MATLAB release, scientific data, or running job is changed.
"""
import base64
import json
from pathlib import Path
import time

import deploy_detecdiv_release as release


ROLLOUT = "alex-terminal-gpu-20261014"
TARGETS = {
    "detecdiv-server": {"fingerprint": "c0f13002d996", "instances": ["@" + str(i) for i in range(1, 7)]},
    "windows-10-20-11-56": {"fingerprint": "8c31b3db2d99", "instances": ["windows-10-20-11-56-main"]},
}
WINDOWS = {
    "ssh_host": "10.20.11.56", "ssh_user": "detecdiv-ops",
    "ssh_key": str(Path.home() / ".ssh/id_ed25519_detecdiv_windows"),
}


def snapshot(config):
    return release.api_code(config, '''
import json
from sqlalchemy import select
from api.db import SessionLocal
from api.models import ExecutionTarget,WorkerInstance
with SessionLocal() as s:
 targets=s.scalars(select(ExecutionTarget)).all()
 workers=s.scalars(select(WorkerInstance)).all()
 keys={str(t.id):t.target_key for t in targets}
 print(json.dumps([{'target':keys[str(w.execution_target_id)],'instance':w.worker_instance,
 'job':str(w.current_job_id) if w.current_job_id else None,'pid':w.process_id,
 'fingerprint':w.code_fingerprint,'seen':w.last_seen_at.isoformat() if w.last_seen_at else None}
 for w in workers]))
''')


def pending_workers(rows, target):
    wanted = TARGETS[target]
    available = {w["instance"]: w for w in rows if w["target"] == target}
    missing = set(wanted["instances"]) - available.keys()
    if missing:
        raise RuntimeError(f"Missing worker instances on {target}: {sorted(missing)}")
    # Include additional autoscaled instances, which use the same source tree.
    return [w for w in available.values() if w["fingerprint"] != wanted["fingerprint"]]


def reserve(config, target):
    """Use the same target-row lock as claim_next_job to prevent new claims."""
    return release.api_code(config, f'''
import json
from datetime import datetime,timezone
from sqlalchemy import select
from api.db import SessionLocal
from api.models import ExecutionTarget,WorkerInstance
with SessionLocal() as s:
 t=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=={target!r}).with_for_update()).one()
 meta=dict(t.metadata_json or {{}}); owner=meta.get('alex_worker_fix_rollout') or {{}}
 assert not owner or owner.get('id')=={ROLLOUT!r},'Another rollout owns admission'
 ws=s.scalars(select(WorkerInstance).where(WorkerInstance.execution_target_id==t.id)).all()
 now=datetime.now(timezone.utc)
 idle=[{{'instance':w.worker_instance,'pid':w.process_id}} for w in ws
       if w.code_fingerprint!={TARGETS[target]['fingerprint']!r} and not w.current_job_id
       and w.last_seen_at and (now-w.last_seen_at).total_seconds()<30]
 if idle and (owner or not meta.get('drain_new_jobs')):
  previous=owner.get('previous') or {{'present':'drain_new_jobs' in meta,'value':meta.get('drain_new_jobs')}}
  meta['drain_new_jobs']=True; meta['alex_worker_fix_rollout']={{'id':{ROLLOUT!r},'previous':previous}}
  t.metadata_json=meta; s.commit(); result={{'idle':idle,'reserved':True}}
 else: result={{'idle':[],'reserved':bool(owner)}}
 print(json.dumps(result))
''')


def restore(config, target):
    return release.api_code(config, f'''
import json
from sqlalchemy import select
from api.db import SessionLocal
from api.models import ExecutionTarget
with SessionLocal() as s:
 t=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=={target!r}).with_for_update()).one()
 meta=dict(t.metadata_json or {{}}); owner=meta.get('alex_worker_fix_rollout') or {{}}
 if owner:
  assert owner['id']=={ROLLOUT!r},'Admission ownership changed'
  previous=owner['previous']
  if previous['present']: meta['drain_new_jobs']=previous['value']
  else: meta.pop('drain_new_jobs',None)
  meta.pop('alex_worker_fix_rollout',None); t.metadata_json=meta; s.commit()
 print(json.dumps({{'restored':True}}))
''')


def reload_linux(selected):
    code = f'''
import hashlib,json,os,signal,subprocess
from pathlib import Path
root=Path('/home/charvin-admin/repos/detecdiv-hub-webvm')
expected={{'pipeline_run_executor.py':'0464da6627d90d99a8cf1ccec89da9243254e0a08d32be205d13e5514c75d146',
 'job_resources.py':'6450a97cf1ae178e6a1a36b665807ddee10773b9fbd97ae98aec51d4f4862ef5'}}
for name,digest in expected.items(): assert hashlib.sha256((root/'worker'/name).read_bytes()).hexdigest()==digest
from api.services.runtime_version import get_runtime_version_info
assert get_runtime_version_info()['code_fingerprint']=={TARGETS['detecdiv-server']['fingerprint']!r},'Source changed since staging'
reloaded=[]
for worker in {selected!r}:
 name=worker['instance']; assert name.startswith('@') and name[1:].isdigit()
 unit='detecdiv-worker'+name+'.service'
 pid=int(subprocess.check_output(['systemctl','show',unit,'-p','MainPID','--value'],text=True))
 assert pid==int(worker['pid']),'Worker process changed; re-inspect before reload'
 cg=subprocess.check_output(['systemctl','show',unit,'-p','ControlGroup','--value'],text=True).strip()
 assert (Path('/sys/fs/cgroup'+cg)/'cgroup.procs').read_text().split()==[str(pid)],'Worker still has child processes'
 assert os.stat('/proc/'+str(pid)).st_uid==os.getuid()
 assert os.path.realpath('/proc/'+str(pid)+'/cwd')==str(root)
 assert 'python' in Path('/proc/'+str(pid)+'/comm').read_text().lower()
 assert subprocess.check_output(['systemctl','show',unit,'-p','Restart','--value'],text=True).strip()=='always'
 os.kill(pid,signal.SIGTERM); reloaded.append(name)
print(json.dumps({{'reloaded':reloaded}}))
'''
    encoded = base64.b64encode(code.encode()).decode()
    return release.remote_json({"ssh_host": "detecdiv-server"},
        f"cd /home/charvin-admin/repos/detecdiv-hub-webvm && .venv/bin/python -c \"import base64;exec(base64.b64decode('{encoded}'))\"")


def reload_windows(selected):
    assert len(selected) == 1 and selected[0]["instance"] == TARGETS["windows-10-20-11-56"]["instances"][0]
    apply_code = '''
import hashlib,json,shutil
from pathlib import Path
root=Path.cwd(); stage=root/'.deploy-staging/alex-terminal-gpu-20261014'
manifest=json.loads((stage/'manifest.json').read_text(encoding='utf-8'))
assert manifest['staging_checks_passed']==11
for relative,hashes in manifest['files'].items():
 source=stage/Path(relative).name; target=root/relative
 assert hashlib.sha256(source.read_bytes()).hexdigest()==hashes['after_sha256']
 assert hashlib.sha256(target.read_bytes()).hexdigest() in (hashes['before_sha256'],hashes['after_sha256']),'Live source changed since staging'
backup=root/'.deploy-backups/alex-terminal-gpu-20261014'
for relative,hashes in manifest['files'].items():
 target=root/relative; source=stage/Path(relative).name
 if hashlib.sha256(target.read_bytes()).hexdigest()==hashes['before_sha256']:
  saved=backup/relative; saved.parent.mkdir(parents=True,exist_ok=True)
  assert not saved.exists() or saved.read_bytes()==target.read_bytes()
  shutil.copy2(target,saved); shutil.copy2(source,target)
 assert hashlib.sha256(target.read_bytes()).hexdigest()==hashes['after_sha256']
from api.services.runtime_version import get_runtime_version_info
assert get_runtime_version_info()['code_fingerprint']==manifest['expected_fingerprint']
print(json.dumps({'applied':True}))
'''
    encoded_code = base64.b64encode(apply_code.encode()).decode()
    ps = f'''
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$repo='C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub'
Set-Location $repo
$workerPid={int(selected[0]['pid'])}
$processes=@(Get-CimInstance Win32_Process)
$worker=$processes | Where-Object ProcessId -eq $workerPid
if (!$worker -or $worker.Name -ne 'python.exe' -or $worker.CommandLine -notmatch 'worker.run_worker') {{throw 'Worker process changed'}}
$descendants=@($workerPid)
do {{
 $next=@($processes | Where-Object {{ $_.ParentProcessId -in $descendants -and $_.ProcessId -notin $descendants }})
 $descendants+=@($next | ForEach-Object ProcessId)
}} while ($next.Count)
if (@($processes | Where-Object {{ $_.ProcessId -in $descendants -and $_.Name -ne 'python.exe' }}).Count) {{throw 'Worker still has child processes'}}
$task='DetecDiv Hub Worker'
Stop-ScheduledTask -TaskName $task
try {{
 Start-Sleep -Seconds 2
 if (@(Get-CimInstance Win32_Process | Where-Object {{ $_.ProcessId -in $descendants }}).Count) {{throw 'Worker did not stop'}}
 & ./.venv/Scripts/python.exe -c "import base64;exec(base64.b64decode('{encoded_code}'))"
 if ($LASTEXITCODE -ne 0) {{throw 'Staged source validation failed'}}
}} finally {{Start-ScheduledTask -TaskName $task}}
'''
    encoded = base64.b64encode(ps.encode("utf-16-le")).decode()
    return release.remote_json(WINDOWS, f"powershell -NoProfile -EncodedCommand {encoded}")


def main():
    config = json.loads(release.DEFAULT_CONFIG.read_text(encoding="utf-8"))
    rows = snapshot(config)
    actions = []
    for target in TARGETS:
        if not pending_workers(rows, target):
            restore(config, target)  # Recover an interrupted final cleanup.
            continue
        reservation = reserve(config, target)
        if not reservation["reserved"]:
            continue
        try:
            selected = reservation["idle"]
            if selected:
                outcome = reload_linux(selected) if target == "detecdiv-server" else reload_windows(selected)
                actions.append({"target": target, **outcome})
                for _ in range(12):
                    time.sleep(3)
                    fresh = snapshot(config)
                    wanted = {w["instance"] for w in selected}
                    ready = {w["instance"] for w in fresh if w["target"] == target
                             and w["fingerprint"] == TARGETS[target]["fingerprint"]}
                    if wanted <= ready:
                        break
                else:
                    raise RuntimeError(f"Fresh worker fingerprint not confirmed on {target}")
        finally:
            restore(config, target)
    rows = snapshot(config)
    pending = {t: pending_workers(rows, t) for t in TARGETS}
    print(json.dumps({"rollout_id": ROLLOUT, "complete": not any(pending.values()),
                      "pending": pending, "actions": actions}, indent=2))


if __name__ == "__main__":
    main()
