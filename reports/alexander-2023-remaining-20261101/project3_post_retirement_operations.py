"""Run bounded read checks after the source manifest is completely retired."""
import base64
import hashlib
import json
import sys
from pathlib import Path
from project3_final_operations import HERE,RUNTIME,WORK,load,save,encoded
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

PRIVATE='C:/ProgramData/DetecDivHub/homes2-connection-private/project3-post-retirement-20261105'
TASK='DetecDiv Alexander 2023_3 Post Retirement 20261105'


def stage():
    proof=load('project3-source-retirement-receipt-verification.json');assert proof['success']
    assert proof['state']['files_unlinked']==708590 and proof['source_payload_empty_after_native_scan']
    request=load('project3-host-final-request.json',private=True)
    request.update(source_manifest_retired=True,source_retirement_receipt_sha256=proof['state']['receipt_sha256'],post_script_sha256=hashlib.sha256((HERE/'verify_project3_after_retirement.m').read_bytes()).hexdigest())
    files={n:base64.b64encode((HERE/n).read_bytes()).decode() for n in ['verify_project3_after_retirement.m','run_project3_after_retirement.py']}
    files['project3-host-request.json']=base64.b64encode(json.dumps(request).encode()).decode()
    code='FILES='+repr(files)+'\n'+r'''
import os,json,base64,subprocess,shlex
from pathlib import Path
os.umask(0o077);w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101');d=w/'project3-post-retirement-hosts-20261105'
s=json.loads((w/'project3-source-retirement-20261105/state.json').read_text());assert s['phase']=='source_manifest_retired_catalog_finalization_pending' and s['files_unlinked']==708590
assert not d.exists();d.mkdir(mode=0o700)
for n,b in FILES.items():(d/n).write_bytes(base64.b64decode(b))
launch='#!/bin/bash\ncd '+shlex.quote(str(d))+'\npython3 -u run_project3_after_retirement.py > controller.log 2>&1\nrc=$?\nprintf "%s\\n" "$rc" > controller.exit\nexit "$rc"\n'
(d/'launch.sh').write_text(launch);task='alex2023-3-post-retirement-linux-20261105'
assert subprocess.run(['tmux','has-session','-t',task],capture_output=True).returncode!=0
subprocess.run(['tmux','new-session','-d','-s',task,'bash '+shlex.quote(str(d/'launch.sh'))],check=True)
print(json.dumps({'started':True,'task':task,'private_directory':str(d)}))
'''
    lin=remote('linux',code,timeout=40)
    code='FILES='+repr(files)+'\nPRIVATE='+repr(PRIVATE)+'\nTASK='+repr(TASK)+'\n'+r'''
import base64,subprocess,json
from pathlib import Path
w=Path(PRIVATE);assert w.parent.is_dir() and not w.exists();w.mkdir()
for n,b in FILES.items():(w/n).write_bytes(base64.b64decode(b))
ps=r"""
$ErrorActionPreference='Stop'
$name='__TASK__'
if(Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue){throw 'Post-retirement diagnostic task already exists'}
$principal=New-ScheduledTaskPrincipal -UserId 'GMGM\Charvin-Admin' -LogonType Interactive -RunLevel Limited
$action=New-ScheduledTaskAction -Execute 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -Command "& ''C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub\.venv\Scripts\python.exe'' -X utf8 ''C:\ProgramData\DetecDivHub\homes2-connection-private\project3-post-retirement-20261105\run_project3_after_retirement.py''"'
$settings=New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 2) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $name -Action $action -Principal $principal -Settings $settings | Out-Null
Start-ScheduledTask -TaskName $name
@{task=$name;state=[string](Get-ScheduledTask -TaskName $name).State;workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State}|ConvertTo-Json -Compress
""".replace('__TASK__',TASK)
p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(ps.encode('utf-16le')).decode()],capture_output=True,text=True,timeout=45);assert p.returncode==0,p.stderr
print(p.stdout.strip())
'''
    win=remote('windows',code,timeout=70)
    result={'linux':lin,'windows':win,'source_retirement_receipt_sha256':request['source_retirement_receipt_sha256'],'script_sha256':request['post_script_sha256']}
    save('project3-post-retirement-hosts-launch.json',result);print(json.dumps(result))


def status():
    code=r'''
import json
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101/project3-post-retirement-hosts-20261105');out={}
for n in ['post-linux-state.json','post-linux-progress.json','post-linux-verification.json','controller.exit','controller.log','post-linux-matlab.log']:
 p=w/n
 if p.exists():
  if n.endswith('.json'):out[n]=json.loads(p.read_text())
  else:
   with p.open('rb') as f:f.seek(max(0,p.stat().st_size-2200));out[n]=f.read().decode(errors='replace')
print(json.dumps(out))
'''
    lin=remote('linux',code,timeout=35)
    code='PRIVATE='+repr(PRIVATE)+'\nTASK='+repr(TASK)+'\n'+r'''
import json,subprocess,base64
from pathlib import Path
w=Path(PRIVATE);out={}
ps="$t=Get-ScheduledTask -TaskName '"+TASK+"';$i=Get-ScheduledTaskInfo -TaskName '"+TASK+"';@{state=[string]$t.State;result=$i.LastTaskResult}|ConvertTo-Json -Compress"
p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(ps.encode('utf-16le')).decode()],capture_output=True,text=True,timeout=35);assert p.returncode==0,p.stderr
out['task']=json.loads(p.stdout)
for n in ['post-windows-state.json','post-windows-progress.json','post-windows-verification.json','post-windows-matlab.log']:
 f=w/n
 if f.exists():
  if n.endswith('.json'):out[n]=json.loads(f.read_text(encoding='utf-8-sig'))
  else:
   with f.open('rb') as stream:stream.seek(max(0,f.stat().st_size-2200));out[n]=stream.read().decode(errors='replace')
print(json.dumps(out))
'''
    win=remote('windows',code,timeout=50)
    data={'linux':lin,'windows':win};(RUNTIME/'project3-post-retirement-hosts-status.json').write_text(json.dumps(data,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'linux':lin.get('post-linux-state.json'),'windows':win.get('post-windows-state.json'),'windows_task':win['task']}))


def verify():
    data=load('project3-post-retirement-hosts-status.json',private=True);lin=data['linux'];win=data['windows']
    assert lin['controller.exit'].strip()=='0' and win['task']['result']==0 and win['task']['state']!='Running'
    assert lin['post-linux-state.json']['success'] and win['post-windows-state.json']['success']
    lr=lin['post-linux-verification.json'];wr=win['post-windows-verification.json']
    assert lr['success'] and wr['success'] and lr['projects']==wr['projects']
    assert lr['source_retirement_receipt_sha256']==wr['source_retirement_receipt_sha256']==load('project3-source-retirement-receipt-verification.json')['state']['receipt_sha256']
    result={'success':True,'projects':lr['projects'],'raw_reads_per_host':123,'roi_image_reads_per_host':11,'source_retirement_receipt_sha256':lr['source_retirement_receipt_sha256'],'project_saved':False}
    save('project3-post-retirement-hosts-completion-verification.json',result);print(json.dumps(result))


def cleanup():
    assert load('project3-post-retirement-hosts-completion-verification.json')['success']
    code='PRIVATE='+repr(PRIVATE)+'\nTASK='+repr(TASK)+'\n'+r'''
import json,subprocess,base64
from pathlib import Path
w=Path(PRIVATE);assert w.resolve()==Path('C:/ProgramData/DetecDivHub/homes2-connection-private/project3-post-retirement-20261105').resolve()
assert json.loads((w/'post-windows-state.json').read_text())['success']
ps="$ErrorActionPreference='Stop';$t=Get-ScheduledTask -TaskName '"+TASK+"';if($t.State -eq 'Running'){throw 'Post check still running'};Unregister-ScheduledTask -TaskName '"+TASK+"' -Confirm:$false;@{taskRemoved=$true;workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State}|ConvertTo-Json -Compress"
p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(ps.encode('utf-16le')).decode()],capture_output=True,text=True,timeout=35);assert p.returncode==0,p.stderr
for name in ['run_project3_after_retirement.py','verify_project3_after_retirement.m']:(w/name).unlink()
print(p.stdout.strip())
'''
    result=remote('windows',code,timeout=50);save('project3-post-retirement-windows-cleanup.json',result);print(json.dumps(result))


if __name__=='__main__':
    {'stage':stage,'status':status,'verify':verify,'cleanup':cleanup}[sys.argv[1]]()
