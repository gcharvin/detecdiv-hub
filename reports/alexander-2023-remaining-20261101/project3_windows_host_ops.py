"""Stage/run/read/clean only this read-only diagnostic under the worker token."""
import base64
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote
PRIVATE='C:/ProgramData/DetecDivHub/homes2-connection-private/project3-host-validation-20261103'
TASK='DetecDiv Alexander Project3 Host Verify 20261103'


def main(mode):
    files={n:base64.b64encode((HERE/n).read_bytes()).decode() for n in ['verify_project3_hosts.m','run_project3_host_validation.py']}
    files['project3-host-request.json']=base64.b64encode((HERE/'runtime/project3-host-request.json').read_bytes()).decode()
    code='PRIVATE='+repr(PRIVATE)+'\nTASK='+repr(TASK)+'\nMODE='+repr(mode)+'\nFILES='+repr(files)+'\n'+r'''
import json,base64,subprocess
from pathlib import Path
w=Path(PRIVATE);task=TASK
def ps(code):
 p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(code.encode('utf-16le')).decode()],capture_output=True,text=True,timeout=45)
 assert p.returncode==0,p.stderr
 return p.stdout.strip()
if MODE=='stage':
 assert w.parent.is_dir() and not w.exists();w.mkdir()
 for name,b in FILES.items():(w/name).write_bytes(base64.b64decode(b))
 print(json.dumps({'files_staged':list(FILES),'task_started':False,'private_directory':str(w)}))
elif MODE=='start':
 assert (w/'project3-host-request.json').exists() and not (w/'project3-host-windows-state.json').exists()
 command=r"""
$ErrorActionPreference='Stop'
$name='__TASK__'
if(Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue){throw 'Diagnostic task already exists'}
$principal=New-ScheduledTaskPrincipal -UserId 'GMGM\Charvin-Admin' -LogonType Interactive -RunLevel Limited
$action=New-ScheduledTaskAction -Execute 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -Command "& ''C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub\.venv\Scripts\python.exe'' -X utf8 ''C:\ProgramData\DetecDivHub\homes2-connection-private\project3-host-validation-20261103\run_project3_host_validation.py''"'
$settings=New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 3) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $name -Principal $principal -Action $action -Settings $settings | Out-Null
Start-ScheduledTask -TaskName $name
[ordered]@{task=$name;state=[string](Get-ScheduledTask -TaskName $name).State;workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State}|ConvertTo-Json -Compress
""".replace('__TASK__',task)
 print(ps(command))
elif MODE=='status':
 out={'task':json.loads(ps("$t=Get-ScheduledTask -TaskName '"+task+"';$i=Get-ScheduledTaskInfo -TaskName '"+task+"';@{state=[string]$t.State;result=$i.LastTaskResult}|ConvertTo-Json -Compress"))}
 for name in ['project3-host-windows-state.json','project3-host-windows-progress.json','project3-host-windows-verification.json','project3-host-windows-matlab.log']:
  f=w/name
  if f.exists():out[name]=json.loads(f.read_text(encoding='utf-8-sig')) if name.endswith('.json') else f.read_text(errors='replace')[-4000:]
 print(json.dumps(out))
elif MODE=='cleanup':
 data=json.loads((w/'project3-host-windows-state.json').read_text(encoding='utf-8-sig'));assert data['success'] and data['phase']=='host_reads_verified_in_staging'
 assert w.resolve()==Path('C:/ProgramData/DetecDivHub/homes2-connection-private/project3-host-validation-20261103').resolve()
 print(ps("$ErrorActionPreference='Stop';$t=Get-ScheduledTask -TaskName '"+task+"';if($t.State -eq 'Running'){throw 'Diagnostic still running'};Unregister-ScheduledTask -TaskName '"+task+"' -Confirm:$false;@{diagnosticTaskRemoved=$true;workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State}|ConvertTo-Json -Compress"))
 for name in ['verify_project3_hosts.m','run_project3_host_validation.py']:(w/name).unlink()
else:raise ValueError('Unknown diagnostic operation')
'''
    result=remote('windows',code,timeout=90)
    f=HERE/(('runtime/project3-host-windows-status.json') if mode=='status' else 'project3-host-windows-'+mode+'.json')
    f.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    if mode=='status':
        report=result.get('project3-host-windows-verification.json');summary={k:v for k,v in result.items() if k not in ['project3-host-windows-verification.json']}
        if report:summary['verification_summary']={'success':report['success'],'projects':len(report['projects'])}
        print(json.dumps(summary,indent=2))
    else:print(json.dumps(result,indent=2))


if __name__=='__main__':
    assert len(sys.argv)==2 and sys.argv[1] in ['stage','start','status','cleanup']
    main(sys.argv[1])
