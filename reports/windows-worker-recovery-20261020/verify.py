from common import ops,OUT
import json
from datetime import datetime,timezone

before=json.loads((OUT/'before.json').read_text())
preflight=json.loads((OUT/'final-preflight.json').read_text())
after=ops.save('after.json',ops.snapshot())
linux_id='137eec41-3bfc-4c7a-bbbf-03d5a948a329'
old=next(j for j in before['jobs'] if j['id']==linux_id)
new=next(j for j in after['jobs'] if j['id']==linux_id)
for field in ('status','params_sha256','allocation','started_at','execution_target_id'):assert old[field]==new[field],field
assert new['heartbeat_at']>old['heartbeat_at']
oldw=next(w for w in before['workers'] if w['current_job_id']==linux_id)
neww=next(w for w in after['workers'] if w['current_job_id']==linux_id)
assert oldw['process_id']==neww['process_id']
windows=next(w for w in after['workers'] if w['target_key']=='windows-10-20-11-56')
assert windows['code_fingerprint']=='a1fffd69452e' and windows['process_id']==45124
assert (datetime.now(timezone.utc)-datetime.fromisoformat(windows['last_seen_at'])).total_seconds()<30
fresh=json.loads((OUT/'fresh-worker.json').read_text())
assert windows['last_seen_at']>fresh['last_seen_at']
host=ops.remote('windows',r'''
import json,psutil,subprocess,base64
from pathlib import Path
expected=EXPECTED
preserved=[]
for item in expected:
 p=psutil.Process(item['pid']);assert p.create_time()==item['created_at_unix']
 cpu=sum(p.cpu_times()[:2]);assert cpu>=item['cpu_seconds']
 preserved.append({'pid':p.pid,'created_at_unix':p.create_time(),'cpu_before':item['cpu_seconds'],'cpu_after':cpu})
ps=r"""
$ErrorActionPreference='Stop';$ProgressPreference='SilentlyContinue';[Console]::OutputEncoding=[Text.Encoding]::UTF8
$t=Get-ScheduledTask -TaskName 'DetecDiv Hub Worker';$i=Get-ScheduledTaskInfo -TaskName 'DetecDiv Hub Worker'
$activation=Get-ScheduledTask -TaskName 'DetecDiv Hub Force Stop Activation 20261020' -ErrorAction SilentlyContinue
[pscustomobject]@{State=[string]$t.State;Limit=$t.Settings.ExecutionTimeLimit;Principal=$t.Principal.UserId;LastResult=$i.LastTaskResult;ActivationWatcherPresent=($null -ne $activation)} | ConvertTo-Json -Compress
"""
r=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(ps.encode('utf-16le')).decode()],capture_output=True,encoding='utf-8',errors='replace',timeout=20)
assert r.returncode==0,r.stderr
task=json.loads(r.stdout);assert task['State']=='Running' and task['Limit']=='PT0S'
p=psutil.Process(45124);assert 'worker.run_worker' in ' '.join(p.cmdline())
log=Path('logs/windows-worker.log');b=log.read_bytes();tail=b.decode('utf-16',errors='replace')[-5000:] if b.startswith(b'\xff\xfe') else b.decode('utf-8',errors='replace').replace('\x00','')[-5000:]
print(json.dumps({'task':task,'preserved_matlab':preserved,'worker_pid':p.pid,'worker_children':[{'pid':c.pid,'name':c.name()} for c in p.children(recursive=True)],'tunnel_alive':psutil.pid_exists(21408),'worker_log_tail':tail},default=str))
'''.replace('EXPECTED',repr(preflight['preserved_matlab'])))
ops.save('host-verification.json',host)
assert host['tunnel_alive']
print(json.dumps({'windows_worker':windows,'task':host['task'],'preserved_matlab':host['preserved_matlab'],'linux_job_preserved':True,'windows_jobs':[{'id':j['id'],'status':j['status'],'heartbeat_at':j['heartbeat_at']} for j in after['jobs'] if j['execution_target_id']=='a8eedb3f-85fc-47a8-b327-2aa766550f51'],'worker_log_tail':host['worker_log_tail'][-2200:]},indent=2))
