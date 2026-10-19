import base64
import json
import ops

source = base64.b64encode((ops.OUT / 'activate_when_idle.py').read_bytes()).decode()
for label in ops.TARGETS:
    ops.remote(label, f'''
import base64,json
from pathlib import Path
p=Path('.deploy-staging/roi-auto-memory-20261019/activate_when_idle.py')
p.write_bytes(base64.b64decode({source!r}))
print(json.dumps({{'helper_updated':True}}))
''')

result = ops.remote('linux', '''
import os,json,subprocess
from pathlib import Path
root=Path.cwd(); env={**os.environ,'XDG_RUNTIME_DIR':'/run/user/'+str(os.getuid())}
command=['systemd-run','--user','--unit=detecdiv-roi-memory-activation-20261019',
 '--on-active=30s','--on-unit-active=30s','--timer-property=AccuracySec=1s',
 '--property=WorkingDirectory='+str(root),str(root/'.venv/bin/python'),
 str(root/'.deploy-staging/roi-auto-memory-20261019/activate_when_idle.py'),'linux']
result=subprocess.run(command,env=env,capture_output=True,text=True)
assert result.returncode==0,result.stderr
state=subprocess.check_output(['systemctl','--user','show','detecdiv-roi-memory-activation-20261019.timer','-p','ActiveState','-p','NextElapseUSecMonotonic'],env=env,text=True)
print(json.dumps({'installed':True,'timer':state}))
''')
ops.save('linux-idle-activation-timer.json', result)
print('linux', result)

result = ops.remote('windows', r'''
import json,subprocess
from pathlib import Path
root=Path.cwd();stage=root/'.deploy-staging/roi-auto-memory-20261019'
wrapper=stage/'activate.ps1'
wrapper.write_text("""$ErrorActionPreference='Stop'
Set-Location 'C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub'
$output=& ./.venv/Scripts/python.exe ./.deploy-staging/roi-auto-memory-20261019/activate_when_idle.py windows
if ($LASTEXITCODE) {throw 'Activation check failed'}
$result=$output | Select-Object -Last 1 | ConvertFrom-Json
if ($result.complete) {Unregister-ScheduledTask -TaskName 'DetecDiv Hub ROI Memory Activation 20261019' -Confirm:$false}
""",encoding='utf-8')
ps="""
$ErrorActionPreference='Stop'
$action=New-ScheduledTaskAction -Execute 'powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub/.deploy-staging/roi-auto-memory-20261019/activate.ps1'
$trigger=New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 1)
$settings=New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 2) -StartWhenAvailable
Register-ScheduledTask -TaskName 'DetecDiv Hub ROI Memory Activation 20261019' -Action $action -Trigger $trigger -Settings $settings -User 'SYSTEM' -RunLevel Highest | Out-Null
$task=Get-ScheduledTask -TaskName 'DetecDiv Hub ROI Memory Activation 20261019'
@{name=$task.TaskName;state=[string]$task.State;principal=$task.Principal.UserId} | ConvertTo-Json -Compress
"""
result=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',ps],capture_output=True,text=True)
assert result.returncode==0,result.stderr
print(result.stdout.strip().splitlines()[-1])
''')
ops.save('windows-idle-activation-task.json', result)
print('windows', result)
