import sys,json
from pathlib import Path
OUT=Path(__file__).resolve().parent
sys.path.insert(0,str(OUT.parent/'job-force-stop-deployment-20261020'))
import ops
ops.OUT=OUT
ops.save('before.json',ops.snapshot())
result=ops.remote('windows',r'''
import psutil,json,subprocess,base64,re
from pathlib import Path
from datetime import datetime,timezone
def redact(s):
 return re.sub(r'postgres(?:ql)?(?:\+psycopg)?://[^\s"\']+', '[DATABASE_URL_REDACTED]',s)
rows=[]
for p in psutil.process_iter(['pid','ppid','name','create_time','cmdline']):
 try:
  if (p.info['name'] or '').lower() in ('python.exe','pythonw.exe','matlab.exe','powershell.exe','ssh.exe'):
   info=p.info;info['cmdline']=redact(' '.join(info['cmdline'] or []));info['cpu_seconds']=sum(p.cpu_times()[:2]);info['memory_rss']=p.memory_info().rss
   rows.append(info)
 except psutil.Error:pass
ps=r"""
$tasks=@(Get-ScheduledTask | Where-Object {$_.TaskName -like '*DetecDiv*'} | ForEach-Object {
 $info=$_ | Get-ScheduledTaskInfo
 [pscustomobject]@{Name=$_.TaskName;State=[string]$_.State;Principal=$_.Principal;Settings=$_.Settings;Actions=$_.Actions;LastRunTime=$info.LastRunTime.ToString('o');LastTaskResult=$info.LastTaskResult;NextRunTime=$info.NextRunTime.ToString('o')}
})
$events=@(Get-WinEvent -FilterHashtable @{LogName='System';StartTime=(Get-Date).AddHours(-4)} -MaxEvents 250 -ErrorAction SilentlyContinue | Where-Object {$_.Id -in @(1074,6005,6006,6008,41)} | Select-Object TimeCreated,Id,Message)
$app=@(Get-WinEvent -FilterHashtable @{LogName='Application';StartTime=(Get-Date).AddHours(-3);Level=2} -MaxEvents 15 -ErrorAction SilentlyContinue | Select-Object TimeCreated,Id,ProviderName,Message)
[pscustomobject]@{Tasks=$tasks;SystemEvents=$events;ApplicationErrors=$app;Boot=(Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToString('o');Users=(query user 2>&1 | Out-String)} | ConvertTo-Json -Depth 6 -Compress
"""
r=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(ps.encode('utf-16le')).decode()],capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=45)
logs={}
for path in [Path('logs/windows-worker.log'),Path('.deploy-staging/job-force-stop-20261020/activation-last.json'),Path('C:/ProgramData/DetecDivHub/db-tunnel.log')]:
 if path.exists():
  logs[str(path)]={'modified':path.stat().st_mtime,'size':path.stat().st_size,'tail':redact(path.read_text(errors='replace')[-18000:])}
print(json.dumps({'checked_at':datetime.now(timezone.utc).isoformat(),'processes':rows,'tasks_and_events':json.loads(r.stdout) if r.returncode==0 else {'error':r.stderr[-1500:]},'logs':logs},default=str))
''')
ops.save('diagnosis.json',result)
print(json.dumps(result,indent=2))
