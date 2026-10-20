from common import ops,OUT
import json,time

KEY='windows_worker_recovery_20261020'
reservation=ops.api('''
import json
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import ExecutionTarget,Job
with SessionLocal.begin() as s:
 s.execute(text("SET LOCAL lock_timeout = '10s'"))
 t=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='windows-10-20-11-56').with_for_update()).one()
 meta=dict(t.metadata_json or {})
 assert not meta.get('drain_new_jobs') and not meta.get('force_stop_rollout')
 meta['windows_worker_recovery_20261020']={'previous_present':'drain_new_jobs' in meta,'previous_value':meta.get('drain_new_jobs')}
 meta['drain_new_jobs']=True;t.metadata_json=meta
 print(json.dumps({'reserved':True,'target':t.target_key}))
''')
ops.save('reservation.json',reservation)
try:
 deployed=ops.remote('windows',r'''
import json,psutil,subprocess,base64,importlib.util,sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv('.env')
from sqlalchemy import select
from api.db import SessionLocal
from api.models import ExecutionTarget
with SessionLocal() as s:
 t=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='windows-10-20-11-56')).one()
 assert (t.metadata_json or {}).get('windows_worker_recovery_20261020') and t.metadata_json.get('drain_new_jobs')
for p in psutil.process_iter(['pid','name','cmdline']):
 try:
  cmd=' '.join(p.info['cmdline'] or [])
  assert 'worker.run_worker' not in cmd, 'Worker appeared'
  assert 'f4150ff1-f1bf-4c89-8483-1b7e26c8726e' not in cmd, 'Old job process remains'
  if (p.info['name'] or '').lower()=='matlab.exe':assert '-batch' not in cmd.lower(), 'Unattributed batch MATLAB'
 except psutil.NoSuchProcess:pass
backup=Path('.deploy-backups/windows-worker-recovery-20261020');backup.mkdir(parents=True,exist_ok=True)
assert not (backup/'worker-task.xml').exists(), 'Recovery already applied'
ps=r"""
$ErrorActionPreference='Stop';$ProgressPreference='SilentlyContinue';[Console]::OutputEncoding=[Text.Encoding]::UTF8
$task=Get-ScheduledTask -TaskName 'DetecDiv Hub Worker'
if ($task.State -ne 'Ready') {throw 'Task is no longer idle'}
Export-ScheduledTask -TaskName 'DetecDiv Hub Worker' | Set-Content -LiteralPath '.deploy-backups/windows-worker-recovery-20261020/worker-task.xml' -Encoding Unicode
$settings=$task.Settings
$settings.ExecutionTimeLimit='PT0S'
Set-ScheduledTask -TaskName 'DetecDiv Hub Worker' -Settings $settings | Out-Null
$updated=Get-ScheduledTask -TaskName 'DetecDiv Hub Worker'
if ($updated.Settings.ExecutionTimeLimit -ne 'PT0S') {throw 'Task time limit not removed'}
[pscustomobject]@{State=[string]$updated.State;Limit=$updated.Settings.ExecutionTimeLimit;Principal=$updated.Principal.UserId} | ConvertTo-Json -Compress
"""
r=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(ps.encode('utf-16le')).decode()],capture_output=True,encoding='utf-8',errors='replace',timeout=35)
assert r.returncode==0,r.stderr
task=json.loads(r.stdout)
script=Path('scripts/register_windows_worker_task.ps1');original=script.read_bytes()
(backup/'register_windows_worker_task.ps1').write_bytes(original)
source=original.decode('utf-8-sig')
line='$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)'
assert source.count(line)==1 and '-ExecutionTimeLimit' not in source
script.write_bytes(source.replace(line,line+' -ExecutionTimeLimit ([TimeSpan]::Zero)').encode('utf-8'))
sys.argv=['activate_when_idle.py','windows']
spec=importlib.util.spec_from_file_location('activation',Path('.deploy-staging/job-force-stop-20261020/activate_when_idle.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);m.apply_files()
print(json.dumps({'task':task,'applied_fingerprint':m.EXPECTED,'no_process_stopped':True}))
''')
 ops.save('deployment.json',deployed)
 print(deployed,flush=True)
 audited=ops.api('''
import json
from datetime import datetime,timezone
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Job,ExecutionTarget
from api.services.project_locks import release_project_locks_for_job
with SessionLocal.begin() as s:
 t=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='windows-10-20-11-56').with_for_update()).one()
 assert (t.metadata_json or {}).get('windows_worker_recovery_20261020') and t.metadata_json.get('drain_new_jobs')
 j=s.get(Job,'f4150ff1-f1bf-4c89-8483-1b7e26c8726e');assert j.status=='running'
 assert j.heartbeat_at.isoformat()=='2026-10-20T13:21:16.910078+00:00'
 now=datetime.now(timezone.utc)
 result=dict(j.result_json or {})
 result['ops_recovery']={'actor':'gilles','at':now.isoformat(),'reason':'Worker process disappeared; no matching batch MATLAB or Python job process remains. Existing guest-session MATLAB processes were preserved.','previous_status':j.status,'last_heartbeat_at':j.heartbeat_at.isoformat(),'evidence':'reports/windows-worker-recovery-20261020','requeued':False}
 j.result_json=result;j.status='failed';j.finished_at=now;j.updated_at=now
 j.error_text='Worker process disappeared and its compute processes are no longer running. The interrupted run was not restarted automatically; existing outputs and logs were preserved.'
 release_project_locks_for_job(s,job_id=j.id)
 print(json.dumps({'job_id':str(j.id),'status':j.status,'audit':result['ops_recovery']}))
''')
 ops.save('orphan-reconciliation.json',audited)
 print(audited,flush=True)
 started=ops.remote('windows',r'''
import subprocess,json,psutil
assert not any('worker.run_worker' in ' '.join(p.info['cmdline'] or []) for p in psutil.process_iter(['cmdline']))
r=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',"$ErrorActionPreference='Stop'; Start-ScheduledTask -TaskName 'DetecDiv Hub Worker'; Get-ScheduledTask -TaskName 'DetecDiv Hub Worker' | Select-Object TaskName,@{n='State';e={[string]$_.State}} | ConvertTo-Json -Compress"],capture_output=True,text=True,errors='replace',timeout=20)
assert r.returncode==0,r.stderr
print(r.stdout.strip())
''')
 ops.save('task-started.json',started)
 print(started,flush=True)
 for attempt in range(12):
  snapshot=ops.save('while-drained.json',ops.snapshot())
  w=next(w for w in snapshot['workers'] if w['target_key']=='windows-10-20-11-56')
  if w['process_id']!=31820 and w['code_fingerprint']=='a1fffd69452e' and w['current_job_id'] is None:
   from datetime import datetime,timezone
   if (datetime.now(timezone.utc)-datetime.fromisoformat(w['last_seen_at'])).total_seconds()<25:
    ops.save('fresh-worker.json',w);print(w,flush=True);break
  time.sleep(5)
 else:raise RuntimeError('Fresh updated worker heartbeat not observed')
finally:
 restored=ops.api('''
import json
from sqlalchemy import select
from api.db import SessionLocal
from api.models import ExecutionTarget
with SessionLocal.begin() as s:
 t=s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='windows-10-20-11-56').with_for_update()).one()
 meta=dict(t.metadata_json or {});owner=meta.pop('windows_worker_recovery_20261020',None)
 if owner:
  if owner['previous_present']:meta['drain_new_jobs']=owner['previous_value']
  else:meta.pop('drain_new_jobs',None)
  t.metadata_json=meta
 print(json.dumps({'admission_restored':True,'drain_new_jobs':meta.get('drain_new_jobs')}))
''')
 ops.save('admission-restored.json',restored)
 print(restored,flush=True)
