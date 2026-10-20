from common import ops,OUT
import json
job=json.loads((OUT/'windows-job.json').read_text())
result=ops.remote('windows',r'''
import json,psutil,subprocess,base64,hashlib
from pathlib import Path
from dotenv import load_dotenv
load_dotenv('.env')
from sqlalchemy import text
from api.db import SessionLocal
from api.services.runtime_version import get_runtime_version_info
ps=r"""
$ProgressPreference='SilentlyContinue';[Console]::OutputEncoding=[Text.Encoding]::UTF8
$t=Get-ScheduledTask -TaskName 'DetecDiv Hub Worker'
$info=Get-ScheduledTaskInfo -TaskName 'DetecDiv Hub Worker'
$proc=@(Get-CimInstance Win32_Process | Where-Object {$_.Name -eq 'MATLAB.exe'} | ForEach-Object {$owner=Invoke-CimMethod -InputObject $_ -MethodName GetOwner;[pscustomobject]@{Pid=$_.ProcessId;Session=$_.SessionId;User=$owner.User;Domain=$owner.Domain}})
[pscustomobject]@{State=[string]$t.State;Limit=$t.Settings.ExecutionTimeLimit;User=$t.Principal.UserId;LastRun=$info.LastRunTime.ToString('o');LastResult=$info.LastTaskResult;Matlab=$proc} | ConvertTo-Json -Depth 4 -Compress
"""
r=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(ps.encode('utf-16le')).decode()],capture_output=True,encoding='utf-8',errors='replace',timeout=35)
assert r.returncode==0,r.stderr
task=json.loads(r.stdout)
assert task['State']=='Ready',task
processes=[]
for p in psutil.process_iter(['pid','name','cmdline','create_time']):
 try:
  cmd=' '.join(p.info['cmdline'] or [])
  assert 'worker.run_worker' not in cmd, 'Worker appeared'
  assert 'f4150ff1-f1bf-4c89-8483-1b7e26c8726e' not in cmd, 'Old job process remains'
  if (p.info['name'] or '').lower()=='matlab.exe':
   assert '-batch' not in cmd.lower(), 'Unattributed batch MATLAB remains'
   processes.append({'pid':p.pid,'created_at_unix':p.create_time(),'cpu_seconds':sum(p.cpu_times()[:2])})
 except psutil.NoSuchProcess:pass
stage=Path('.deploy-staging/job-force-stop-20261020');manifest=json.loads((stage/'manifest.json').read_text())
for name,info in manifest['files'].items():
 p=Path(name);live=hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
 assert live in (info['before'],info['after']),name
 assert hashlib.sha256((stage/'files'/name).read_bytes()).hexdigest()==info['after'],name
with SessionLocal() as s:
 assert s.execute(text('select 1')).scalar()==1
print(json.dumps({'task':task,'preserved_matlab':processes,'db_connection':'ok','current_runtime':get_runtime_version_info(),'prepared_fingerprint':manifest['expected_fingerprint'],'old_stdout':{'modified':Path(STDOUT).stat().st_mtime,'size':Path(STDOUT).stat().st_size}},default=str))
'''.replace('STDOUT',repr(job['result_json']['worker_runtime']['stdout_log'])))
ops.save('final-preflight.json',result)
print(json.dumps(result,indent=2))
