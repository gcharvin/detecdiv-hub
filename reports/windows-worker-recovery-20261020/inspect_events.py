exec((__import__('pathlib').Path(__file__).parent/'inspect_more.py').read_text().split('result=ops.remote')[0])
job=json.loads((OUT/'windows-job.json').read_text())
code=r'''
import json,subprocess,base64
from pathlib import Path
ps=r"""
$ProgressPreference='SilentlyContinue'
[Console]::OutputEncoding=[Text.Encoding]::UTF8
$ev=@(Get-WinEvent -LogName 'Microsoft-Windows-TaskScheduler/Operational' -FilterXPath "*[EventData[Data[@Name='TaskName']='\DetecDiv Hub Worker']]" -MaxEvents 35 | ForEach-Object {[pscustomobject]@{Time=$_.TimeCreated.ToString('o');Id=$_.Id;Message=$_.Message;Xml=$_.ToXml()}})
$task=Get-ScheduledTask -TaskName 'DetecDiv Hub Worker'
[pscustomobject]@{Events=$ev;Principal=$task.Principal;Settings=$task.Settings;Actions=$task.Actions} | ConvertTo-Json -Depth 5 -Compress
"""
r=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(ps.encode('utf-16le')).decode()],capture_output=True,encoding='utf-8',errors='replace',timeout=30)
files=[]
for p in list(Path('.').glob('*'))+list(Path('.deploy-staging').glob('*/*')):
 if p.is_file() and ('log' in p.name.lower() or p.suffix=='.ps1'):files.append({'path':str(p),'size':p.stat().st_size,'modified':p.stat().st_mtime})
runtime=RUNTIME
logs={}
for k in ('stdout_log','stderr_log'):
 p=Path(runtime[k]);b=p.read_bytes() if p.exists() else b''
 logs[k]={'exists':p.exists(),'size':len(b),'modified':p.stat().st_mtime if p.exists() else None,'tail':b[-6500:].decode('utf-8',errors='replace')}
print(json.dumps({'task':json.loads(r.stdout) if r.returncode==0 else {'error':r.stderr},'files':files,'job_logs':logs},default=str))
'''.replace('RUNTIME',repr(job['result_json']['worker_runtime']))
result=ops.save('events-and-logs.json',ops.remote('windows',code))
print(json.dumps({'events':[{'Time':e['Time'],'Id':e['Id'],'Message':e['Message']} for e in result['task'].get('Events',[])][:18],'settings':result['task'].get('Settings'),'files':result['files'],'job_logs':result['job_logs']},indent=2))
