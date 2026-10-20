"""Activate tested changes only on idle pollers; arrange guarded later passes."""
import operations as ops

before = ops.save('before-worker-activation.json', ops.snapshot())
assert before['health']['code_fingerprint'] == '15fed3245ae8'

linux = ops.remote('linux', '''
import json,os,subprocess,time
from pathlib import Path
root=Path.cwd();stage=root/'.deploy-staging/hub-reconciled-20261020'
env={**os.environ,'XDG_RUNTIME_DIR':'/run/user/'+str(os.getuid())}
old='detecdiv-force-stop-activation-20261020'
# Only the old maintenance timer is superseded; compute services are untouched.
subprocess.run(['systemctl','--user','stop',old+'.timer'],env=env,check=True)
for _ in range(20):
 state=subprocess.check_output(['systemctl','--user','show',old+'.service','-p','ActiveState','--value'],env=env,text=True).strip()
 if state not in ['active','activating','deactivating']:break
 time.sleep(1)
else:raise RuntimeError('Previous maintenance pass is still running')
name='detecdiv-hub-reconciled-activation-20261020'
p=subprocess.run(['systemd-run','--user','--unit='+name,'--on-active=30s',
 '--on-unit-active=30s','--timer-property=AccuracySec=1s',
 '--property=WorkingDirectory='+str(root),str(root/'.venv/bin/python'),
 str(stage/'activate_when_idle.py'),'linux'],env=env,capture_output=True,text=True)
assert p.returncode==0,p.stderr
p=subprocess.run([str(root/'.venv/bin/python'),str(stage/'activate_when_idle.py'),'linux'],
 env=env,capture_output=True,text=True,timeout=90)
assert p.returncode==0,p.stderr
result=json.loads(p.stdout.strip().splitlines()[-1]);assert not result.get('error'),result
print(json.dumps({'maintenance_timer':name,'previous_timer_superseded':True,'activation':result}))
''', timeout=150)
ops.save('linux-first-activation.json', linux)
print('linux', linux, flush=True)

windows = ops.remote('windows', r'''
import json,subprocess
from pathlib import Path
root=Path.cwd();stage=root/'.deploy-staging/hub-reconciled-20261020'
wrapper=stage/'activate.ps1'
wrapper.write_text("""$ErrorActionPreference='Stop'
Set-Location 'C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub'
$output=& ./.venv/Scripts/python.exe ./.deploy-staging/hub-reconciled-20261020/activate_when_idle.py windows
if ($LASTEXITCODE) {throw 'Activation check failed'}
$result=$output | Select-Object -Last 1 | ConvertFrom-Json
if ($result.complete) {Unregister-ScheduledTask -TaskName 'DetecDiv Hub Reconciled Activation 20261020' -Confirm:$false}
""",encoding='utf-8')
ps="""
$ErrorActionPreference='Stop'
$action=New-ScheduledTaskAction -Execute 'powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub/.deploy-staging/hub-reconciled-20261020/activate.ps1'
$trigger=New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 1)
$settings=New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 2) -StartWhenAvailable
Register-ScheduledTask -TaskName 'DetecDiv Hub Reconciled Activation 20261020' -Action $action -Trigger $trigger -Settings $settings -User 'SYSTEM' -RunLevel Highest | Out-Null
Start-ScheduledTask -TaskName 'DetecDiv Hub Reconciled Activation 20261020'
@{installed=$true;workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State;tunnelState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Database Tunnel').State} | ConvertTo-Json -Compress
"""
p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',ps],capture_output=True,text=True)
assert p.returncode==0,p.stderr
print(p.stdout.strip().splitlines()[-1])
''')
ops.save('windows-activation-task.json', windows)
print('windows', windows, flush=True)
