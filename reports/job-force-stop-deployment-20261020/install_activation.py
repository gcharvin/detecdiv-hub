import ops

health = ops.api('''
import json,requests
r=requests.get('http://127.0.0.1:8000/health');r.raise_for_status()
v=r.json();assert v['database_status']=='ok'
print(json.dumps(v))
''')
expected = __import__('json').loads((ops.OUT / 'api-staging.json').read_text())['expected_fingerprint']
assert health['code_fingerprint'] == expected, health
ops.save('api-health.json', health)
print('API healthy:', expected, flush=True)

result = ops.remote('linux', f'''
import json,os,subprocess,time
from pathlib import Path
root=Path.cwd();stage=root/'.deploy-staging/{ops.ROLLOUT}'
env={{**os.environ,'XDG_RUNTIME_DIR':'/run/user/'+str(os.getuid())}}
old='detecdiv-roi-memory-activation-20261019'
subprocess.run(['systemctl','--user','stop',old+'.timer'],env=env,check=True)
for i in range(30):
 state=subprocess.check_output(['systemctl','--user','show',old+'.service','-p','ActiveState','--value'],env=env,text=True).strip()
 if state not in ('active','activating','deactivating'):break
 time.sleep(1)
else:raise RuntimeError('Previous activation pass still running')
command=['systemd-run','--user','--unit=detecdiv-force-stop-activation-20261020',
 '--on-active=30s','--on-unit-active=30s','--timer-property=AccuracySec=1s',
 '--property=WorkingDirectory='+str(root),str(root/'.venv/bin/python'),str(stage/'activate_when_idle.py'),'linux']
r=subprocess.run(command,env=env,capture_output=True,text=True);assert r.returncode==0,r.stderr
r=subprocess.run([str(root/'.venv/bin/python'),str(stage/'activate_when_idle.py'),'linux'],env=env,capture_output=True,text=True)
assert r.returncode==0,r.stderr
v=json.loads(r.stdout.strip().splitlines()[-1]);assert not v.get('error'),v
print(json.dumps({{'previous_timer_stopped':True,'activation':v}}))
''')
ops.save('linux-first-activation.json', result)
print('linux',result,flush=True)

result = ops.remote('windows', r'''
import json,subprocess
from pathlib import Path
root=Path.cwd();stage=root/'.deploy-staging/job-force-stop-20261020'
wrapper=stage/'activate.ps1'
wrapper.write_text("""$ErrorActionPreference='Stop'
Set-Location 'C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub'
$output=& ./.venv/Scripts/python.exe ./.deploy-staging/job-force-stop-20261020/activate_when_idle.py windows
if ($LASTEXITCODE) {throw 'Activation check failed'}
$result=$output | Select-Object -Last 1 | ConvertFrom-Json
if ($result.complete) {Unregister-ScheduledTask -TaskName 'DetecDiv Hub Force Stop Activation 20261020' -Confirm:$false}
""",encoding='utf-8')
ps="""
$ErrorActionPreference='Stop'
$old=Get-ScheduledTask -TaskName 'DetecDiv Hub ROI Memory Activation 20261019' -ErrorAction SilentlyContinue
if ($old) {
 for ($i=0;$i -lt 30;$i++) {
  $old=Get-ScheduledTask -TaskName $old.TaskName
  if ($old.State -ne 'Running') {break}
  Start-Sleep -Seconds 1
 }
 if ($old.State -eq 'Running') {throw 'Previous activation pass still running'}
 Unregister-ScheduledTask -TaskName $old.TaskName -Confirm:$false
}
$action=New-ScheduledTaskAction -Execute 'powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub/.deploy-staging/job-force-stop-20261020/activate.ps1'
$trigger=New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 1)
$settings=New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 2) -StartWhenAvailable
Register-ScheduledTask -TaskName 'DetecDiv Hub Force Stop Activation 20261020' -Action $action -Trigger $trigger -Settings $settings -User 'SYSTEM' -RunLevel Highest | Out-Null
Start-ScheduledTask -TaskName 'DetecDiv Hub Force Stop Activation 20261020'
@{installed=$true;previousTaskSuperseded=$true;workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State;tunnelState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Database Tunnel').State} | ConvertTo-Json -Compress
"""
r=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',ps],capture_output=True,text=True)
assert r.returncode==0,r.stderr
print(r.stdout.strip().splitlines()[-1])
''')
ops.save('windows-idle-activation-task.json', result)
print('windows',result,flush=True)
