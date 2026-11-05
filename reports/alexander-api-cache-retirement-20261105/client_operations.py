"""Guarded one-shot client diagnostics, independent of all worker tasks."""
import base64
import json
from pathlib import Path
import sys
from media_operations import HERE,RUNTIME,load,PREVIOUS
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

PRIVATE='C:/ProgramData/DetecDivHub/homes2-connection-private/api-media-final-20261105'
TASK='DetecDiv API Homes2 Media Read 20261105'


def stage(resume_verified_linux=False):
    retired=json.loads((HERE/'cache-retirement-receipt-verification.json').read_text());assert retired['success']
    plan=json.loads((PREVIOUS/'runtime/api-homes2-cache-serving-plan-20261105.json').read_text())
    pairs={(r['year'],r['relative_path']):r for r in load('pairs-status.json')['receipt']};files=[]
    for year in ['2023_1','2023_2','2023_3']:
        for kind in ['artifact','display_settings']:
            ref=next(r for r in plan['references'] if r['year']==year and r['kind']==kind);row=pairs[year,ref['relative_path']]
            files.append({'year':year,'kind':kind,'path':ref['after'],'bytes':row['bytes'],'sha1':row['sha1']})
    request={'cache_retirement_receipt_sha256':retired['state']['receipt_sha256'],'files':files}
    payload={'read_client_media.py':base64.b64encode((HERE/'read_client_media.py').read_bytes()).decode(),'request.json':base64.b64encode(json.dumps(request).encode()).decode()}
    code='FILES='+repr(payload)+'\n'+r'''
import json,base64,subprocess,os
from pathlib import Path
os.umask(0o077);d=Path('/home/charvin-admin/storage-transfers/alexander-api-cache-retirement-20261105/client-checks');assert not d.exists();d.mkdir(mode=0o700)
for n,b in FILES.items():(d/n).write_bytes(base64.b64decode(b))
p=subprocess.run(['python3',str(d/'read_client_media.py')],capture_output=True,text=True,timeout=60);(d/'controller.log').write_text(p.stdout+p.stderr);assert p.returncode==0,'Inspect private client read log'
print(json.dumps(json.loads((d/'result.json').read_text())))
'''
    if resume_verified_linux:
        code=r'''
import json
from pathlib import Path
d=Path('/home/charvin-admin/storage-transfers/alexander-api-cache-retirement-20261105/client-checks')
assert d.is_dir() and (d/'result.json').is_file() and not (d/'failure.json').exists()
r=json.loads((d/'result.json').read_text());assert r['success'] and len(r['files'])==6
assert (d/'controller.log').read_text()==''
print(json.dumps(r))
'''
    linux=remote('linux',code,timeout=80)
    assert linux['success'] and linux['cache_retirement_receipt_sha256']==request['cache_retirement_receipt_sha256']
    (HERE/'linux-client-verification.json').write_text(json.dumps(linux,indent=2)+'\n',encoding='utf-8')
    code='FILES='+repr(payload)+'\nPRIVATE='+repr(PRIVATE)+'\nTASK='+repr(TASK)+'\n'+r'''
import json,subprocess,base64
from pathlib import Path
d=Path(PRIVATE);assert d.parent.is_dir() and not d.exists();d.mkdir()
for n,b in FILES.items():(d/n).write_bytes(base64.b64decode(b))
ps=r"""
$ErrorActionPreference='Stop';$name='__TASK__'
if(Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue){throw 'Client diagnostic task already exists'}
$p=New-ScheduledTaskPrincipal -UserId 'GMGM\Charvin-Admin' -LogonType Interactive -RunLevel Limited
$a=New-ScheduledTaskAction -Execute 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -Command "& ''C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub\.venv\Scripts\python.exe'' -X utf8 ''C:\ProgramData\DetecDivHub\homes2-connection-private\api-media-final-20261105\read_client_media.py''"'
$s=New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 10) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $name -Principal $p -Action $a -Settings $s|Out-Null
Start-ScheduledTask -TaskName $name
@{task=$name;state=[string](Get-ScheduledTask -TaskName $name).State}|ConvertTo-Json -Compress
""".replace('__TASK__',TASK)
p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(ps.encode('utf-16le')).decode()],capture_output=True,text=True,timeout=40);assert p.returncode==0,p.stderr
print(p.stdout.strip())
'''
    win=remote('windows',code,timeout=60);(HERE/'windows-client-launch.json').write_text(json.dumps(win,indent=2)+'\n',encoding='utf-8');print(json.dumps({'linux_verified':linux['success'],'windows':win}))


def status():
    code='PRIVATE='+repr(PRIVATE)+'\nTASK='+repr(TASK)+'\n'+r'''
import json,subprocess,base64
from pathlib import Path
d=Path(PRIVATE);ps="$t=Get-ScheduledTask -TaskName '"+TASK+"';$i=Get-ScheduledTaskInfo -TaskName '"+TASK+"';@{state=[string]$t.State;result=$i.LastTaskResult}|ConvertTo-Json -Compress"
p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(ps.encode('utf-16le')).decode()],capture_output=True,text=True,timeout=30);assert p.returncode==0,p.stderr
out={'task':json.loads(p.stdout)}
for n in ['result.json','failure.json']:
 if (d/n).exists():out[n]=json.loads((d/n).read_text(encoding='utf-8-sig'))
print(json.dumps(out))
'''
    data=remote('windows',code,timeout=45);(RUNTIME/'windows-client-status.json').write_text(json.dumps(data,indent=2)+'\n',encoding='utf-8')
    if data.get('result.json',{}).get('success') and data['task']['state']!='Running':
        assert data['task']['result']==0
        lin=json.loads((HERE/'linux-client-verification.json').read_text());win=data['result.json'];assert lin['files']==win['files'] and lin['cache_retirement_receipt_sha256']==win['cache_retirement_receipt_sha256']
        (HERE/'windows-client-verification.json').write_text(json.dumps(win,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(data))


def cleanup():
    assert json.loads((HERE/'windows-client-verification.json').read_text())['success']
    code='PRIVATE='+repr(PRIVATE)+'\nTASK='+repr(TASK)+'\n'+r'''
import json,subprocess,base64
from pathlib import Path
d=Path(PRIVATE);assert d.resolve()==Path('C:/ProgramData/DetecDivHub/homes2-connection-private/api-media-final-20261105').resolve()
assert json.loads((d/'result.json').read_text())['success']
ps="$ErrorActionPreference='Stop';$t=Get-ScheduledTask -TaskName '"+TASK+"';if($t.State -eq 'Running'){throw 'Client diagnostic still running'};Unregister-ScheduledTask -TaskName '"+TASK+"' -Confirm:$false;@{taskRemoved=$true;workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State}|ConvertTo-Json -Compress"
p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(ps.encode('utf-16le')).decode()],capture_output=True,text=True,timeout=30);assert p.returncode==0,p.stderr
(d/'read_client_media.py').unlink();print(p.stdout.strip())
'''
    result=remote('windows',code,timeout=45);(HERE/'windows-client-cleanup.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');print(json.dumps(result))


if __name__=='__main__':{'stage':stage,'resume-transport':lambda:stage(True),'status':status,'cleanup':cleanup}[sys.argv[1]]()
