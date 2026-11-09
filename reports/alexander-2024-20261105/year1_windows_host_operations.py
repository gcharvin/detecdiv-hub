"""Operate only the new limited-token year1 final-path diagnostic task."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote
PRIVATE='C:/ProgramData/DetecDivHub/homes2-connection-private/year1-final-hosts-20261109'
TASK='DetecDiv Alexander 2024 Year1 Final Hosts 20261109'

CODE=r'''
import json,base64,gzip,hashlib,subprocess
from pathlib import Path
w=Path(PRIVATE)
def ps(script):
 p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(script.encode('utf-16le')).decode()],capture_output=True,text=True,timeout=45)
 assert p.returncode==0,p.stderr[-1000:]
 return p.stdout.strip()
if MODE=='stage':
 assert w.parent.is_dir() and not w.exists();w.mkdir()
 raw=gzip.decompress(base64.b64decode(REQUEST_GZIP));assert hashlib.sha256(raw).hexdigest()==CONTROL['request_sha256']
 (w/'host-request.json').write_bytes(raw)
 for name,value in FILES.items():(w/name).write_bytes(base64.b64decode(value))
 assert hashlib.sha256((w/'verify_year1_final_hosts.m').read_bytes()).hexdigest()==CONTROL['script_sha256']
 assert hashlib.sha256((w/'run_year1_final_hosts.py').read_bytes()).hexdigest()==CONTROL['runner_sha256']
 (w/'controller-request.json').write_text(json.dumps(CONTROL)+'\n',encoding='utf-8')
 print(json.dumps({'staged':True,'private_directory':str(w),'task_started':False}))
elif MODE=='start':
 assert (w/'controller-request.json').is_file() and not (w/'host-windows-state.json').exists()
 script=r"""
$ErrorActionPreference='Stop'
$name='__TASK__'
if(Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue){throw 'Diagnostic already exists'}
$principal=New-ScheduledTaskPrincipal -UserId 'GMGM\Charvin-Admin' -LogonType Interactive -RunLevel Limited
$action=New-ScheduledTaskAction -Execute 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -Command "& ''C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub\.venv\Scripts\python.exe'' -X utf8 ''C:\ProgramData\DetecDivHub\homes2-connection-private\year1-final-hosts-20261109\run_year1_final_hosts.py''; exit $LASTEXITCODE"'
$settings=New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 5) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $name -Principal $principal -Action $action -Settings $settings | Out-Null
Start-ScheduledTask -TaskName $name
@{task=$name;state=[string](Get-ScheduledTask -TaskName $name).State;workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State}|ConvertTo-Json -Compress
""".replace('__TASK__',TASK)
 print(ps(script))
elif MODE=='status':
 out={'task':json.loads(ps("$t=Get-ScheduledTask -TaskName '"+TASK+"';$i=Get-ScheduledTaskInfo -TaskName '"+TASK+"';@{state=[string]$t.State;result=$i.LastTaskResult}|ConvertTo-Json -Compress"))}
 for name in ['host-windows-state.json','host-windows-progress.json','host-windows-verification.json']:
  p=w/name
  if p.exists():out[name]=json.loads(p.read_text(encoding='utf-8-sig'))
 p=w/'host-windows-matlab.log'
 if p.exists():
  with p.open('rb') as stream:stream.seek(max(0,p.stat().st_size-1600));out['log_tail']=stream.read().decode(errors='replace')
 print(json.dumps(out))
elif MODE=='cleanup':
 assert w.resolve()==Path(PRIVATE).resolve()
 state=json.loads((w/'host-windows-state.json').read_text());assert state['success'] and state['phase']=='final_available_data_reads_and_baselines_verified'
 print(ps("$ErrorActionPreference='Stop';$t=Get-ScheduledTask -TaskName '"+TASK+"';if($t.State -eq 'Running'){throw 'Diagnostic still running'};if($t.Principal.UserId -ne 'GMGM\\Charvin-Admin'){throw 'Unexpected principal'};Unregister-ScheduledTask -TaskName '"+TASK+"' -Confirm:$false;@{diagnosticTaskRemoved=$true;workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State}|ConvertTo-Json -Compress"))
 for name in ['verify_year1_final_hosts.m','run_year1_final_hosts.py']:(w/name).unlink()
else:raise ValueError('Unsupported operation')
'''

def operate(mode,control=None):
    assert mode in ['stage','start','status','cleanup']
    if mode!='status':assert not (HERE/('year1-windows-'+mode+'.json')).exists(),'Review prior operation receipt instead of replay'
    header='PRIVATE='+repr(PRIVATE)+'\nTASK='+repr(TASK)+'\nMODE='+repr(mode)+'\n'
    if mode=='stage':
        files={n:base64.b64encode((HERE/n).read_bytes()).decode() for n in ['verify_year1_final_hosts.m','run_year1_final_hosts.py']}
        raw=(HERE/'runtime/year1-host-request.json.gz').read_bytes()
        header+='REQUEST_GZIP='+repr(base64.b64encode(raw).decode())+'\nCONTROL='+repr(control)+'\nFILES='+repr(files)+'\n'
    result=remote('windows',header+CODE,timeout=180 if mode=='stage' else 90)
    destination=HERE/('runtime/year1-windows-status.json' if mode=='status' else 'year1-windows-'+mode+'.json')
    if mode!='status':assert not destination.exists(),'Existing operation receipt must be reviewed'
    destination.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    return result
