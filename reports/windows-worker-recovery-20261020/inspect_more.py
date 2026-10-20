import sys,json
from pathlib import Path
OUT=Path(__file__).resolve().parent
sys.path.insert(0,str(OUT.parent/'job-force-stop-deployment-20261020'))
import ops
ops.OUT=OUT
result=ops.remote('windows',r'''
import json,subprocess,base64,re
from pathlib import Path
ps=r"""
$ErrorActionPreference='Continue'
whoami /groups
Get-ScheduledTask -TaskName 'DetecDiv Hub Worker' | Format-List TaskName,State,Principal,Settings,Actions
Get-ScheduledTaskInfo -TaskName 'DetecDiv Hub Worker' | Format-List *
schtasks.exe /Query /TN 'DetecDiv Hub Worker' /XML
Get-WinEvent -FilterHashtable @{LogName='Microsoft-Windows-TaskScheduler/Operational';StartTime=(Get-Date).AddHours(-2)} -MaxEvents 30 | Select-Object TimeCreated,Id,Message | Format-List
"""
r=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(ps.encode('utf-16le')).decode()],capture_output=True,text=True,errors='replace',timeout=45)
files=[]
for parent in ['logs','.deploy-staging/job-force-stop-20261020','C:/ProgramData/DetecDivHub']:
 for p in Path(parent).glob('*'):
  if p.is_file() and ('log' in p.name.lower() or p.suffix in ('.json','.ps1')):files.append({'path':str(p),'size':p.stat().st_size,'modified':p.stat().st_mtime})
print(json.dumps({'stdout':r.stdout,'stderr':r.stderr,'code':r.returncode,'files':files}))
''')
ops.save('inspection-more.json',result)
print(json.dumps(result,indent=2))
print(ops.save('windows-job.json',ops.api('''
import json
from sqlalchemy import text
from api.db import SessionLocal
with SessionLocal() as s:
 j=dict(s.execute(text("select id,status,project_id,started_at,heartbeat_at,params_json,result_json from jobs where id='f4150ff1-f1bf-4c89-8483-1b7e26c8726e'")).mappings().one())
 print(json.dumps(j,default=str))
''')))
