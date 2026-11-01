import base64
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

PRIVATE='C:/ProgramData/DetecDivHub/homes2-connection-private'
TASK='DetecDiv Alexander Project1 Verify 20261101'

def run(mode):
    files={n:base64.b64encode((HERE/n).read_bytes()).decode() for n in ['verify_project1_windows.py','verify_project1_windows.m']}
    code=r'''
import json,base64,subprocess
from pathlib import Path
w=Path(PRIVATE);task=TASK
def ps(command):
 p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(command.encode('utf-16le')).decode()],capture_output=True,text=True,timeout=30)
 assert p.returncode==0,p.stderr
 return p.stdout.strip()
if MODE=='retry':
 data=json.loads((w/'project1-session-verification.json').read_text(encoding='utf-8-sig'));assert not data['success']
 ps("$ErrorActionPreference='Stop';$t=Get-ScheduledTask -TaskName '"+task+"';if($t.State -eq 'Running'){throw 'Diagnostic still running'};Unregister-ScheduledTask -TaskName '"+task+"' -Confirm:$false")
 for n in ['project1-session-verification.json','project1-verification-matlab.log']:
  p=w/n;target=w/('attempt1-'+n);assert not target.exists();p.rename(target)
 for n,b in FILES.items():(w/n).write_bytes(base64.b64decode(b))
 print(json.dumps({'rearmed':True,'failed_attempt_preserved':True,'production_project_saved':False}))
elif MODE=='stage':
 for n,data in FILES.items():
  p=w/n;assert not p.exists();p.write_bytes(base64.b64decode(data))
 print(json.dumps({'files_staged':list(FILES),'task_started':False}))
elif MODE=='start':
 command=r"""
$ErrorActionPreference='Stop'
$name='TASKNAME'
if(Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue){throw 'Diagnostic task already exists'}
$principal=New-ScheduledTaskPrincipal -UserId 'GMGM\Charvin-Admin' -LogonType Interactive -RunLevel Limited
$action=New-ScheduledTaskAction -Execute 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -Command "& ''C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub\.venv\Scripts\python.exe'' ''C:\ProgramData\DetecDivHub\homes2-connection-private\verify_project1_windows.py''"'
$settings=New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 30) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $name -Principal $principal -Action $action -Settings $settings | Out-Null
Start-ScheduledTask -TaskName $name
[ordered]@{task=$name;state=[string](Get-ScheduledTask -TaskName $name).State;workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State}|ConvertTo-Json -Compress
""".replace('TASKNAME',task)
 assert not (w/'project1-session-verification.json').exists()
 print(ps(command))
elif MODE=='status':
 r={'task':json.loads(ps("$t=Get-ScheduledTask -TaskName '"+task+"';$i=Get-ScheduledTaskInfo -TaskName '"+task+"';@{state=[string]$t.State;result=$i.LastTaskResult}|ConvertTo-Json -Compress"))}
 for n in ['project1-session-verification.json','project1-verification-matlab.log']:
  p=w/n
  if p.exists():r[n]=json.loads(p.read_text(encoding='utf-8-sig')) if n.endswith('.json') else p.read_text(errors='replace')[-7000:]
 print(json.dumps(r))
elif MODE=='cleanup':
 data=json.loads((w/'project1-session-verification.json').read_text(encoding='utf-8-sig'));assert data['success']
 print(ps("$ErrorActionPreference='Stop';$t=Get-ScheduledTask -TaskName '"+task+"';if($t.State -eq 'Running'){throw 'Diagnostic still running'};Unregister-ScheduledTask -TaskName '"+task+"' -Confirm:$false;@{taskRemoved=$true;workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State}|ConvertTo-Json -Compress"))
 for n in FILES:(w/n).unlink()
'''
    code=code.replace('PRIVATE',repr(PRIVATE)).replace('TASK',repr(TASK),1).replace('MODE',repr(mode)).replace('FILES',repr(files))
    # The first TASK substitution targets task=TASK; leave PowerShell placeholders intact.
    result=remote('windows',code,timeout=60)
    (HERE/f'windows-{mode}.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))

if __name__=='__main__':run(sys.argv[1])
