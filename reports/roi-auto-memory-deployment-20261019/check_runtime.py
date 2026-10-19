import json
import ops

linux = ops.remote('linux', '''
import json,os,subprocess
from pathlib import Path
env={**os.environ,'XDG_RUNTIME_DIR':'/run/user/'+str(os.getuid())}
def run(args):
 r=subprocess.run(args,capture_output=True,text=True,env=env)
 return {'exit_code':r.returncode,'output':r.stdout[-5000:],'stderr':r.stderr[-1000:]}
stage=Path('.deploy-staging/roi-auto-memory-20261019')
print(json.dumps({'manager':run(['systemctl','show','detecdiv-worker-manager.service','-p','MainPID','-p','ActiveState']),
 'manager_activation':json.loads((stage/'manager-activation.json').read_text()),
 'timer':run(['systemctl','--user','show','detecdiv-roi-memory-activation-20261019.timer','-p','ActiveState','-p','LastTriggerUSec']),
 'activation':json.loads((stage/'activation-last.json').read_text()),
 'journal':run(['journalctl','--user','-u','detecdiv-roi-memory-activation-20261019.service','-n','5','--no-pager'])}))
''')
ops.save('linux-runtime-health.json', linux)
assert linux['manager']['exit_code'] == 0
assert 'ActiveState=active' in linux['manager']['output']
assert not linux['activation'].get('error')
assert str(linux['manager_activation']['old_pid']) not in linux['manager']['output']
print(json.dumps({'linux':linux}))

windows = ops.remote('windows', r'''
import json,subprocess
from pathlib import Path
ps="""
$task=Get-ScheduledTask -TaskName 'DetecDiv Hub ROI Memory Activation 20261019'
$info=$task | Get-ScheduledTaskInfo
$worker=Get-ScheduledTask -TaskName 'DetecDiv Hub Worker'
$tunnel=Get-ScheduledTask -TaskName 'DetecDiv Hub Database Tunnel'
$ps=@(Get-CimInstance Win32_Process)
$wrappers=@($ps | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match '-m worker.run_worker' })
$rows=@($wrappers | ForEach-Object { @{pid=$_.ProcessId;parent=$_.ParentProcessId;repoPath=([string]$_.CommandLine).Replace([char]92,[char]47).Contains('C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub')} })
@{state=[string]$task.State;principalSid=([xml](Export-ScheduledTask -TaskName $task.TaskName)).Task.Principals.Principal.UserId;lastRun=$info.LastRunTime.ToString('o');lastResult=$info.LastTaskResult;nextRun=$info.NextRunTime.ToString('o');workerState=[string]$worker.State;tunnelState=[string]$tunnel.State;pollers=$rows} | ConvertTo-Json -Depth 5 -Compress
"""
r=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',ps],capture_output=True,text=True)
assert r.returncode==0,r.stderr
result=json.loads(r.stdout.strip())
result['activation']=json.loads(Path('.deploy-staging/roi-auto-memory-20261019/activation-last.json').read_text())
print(json.dumps(result))
''')
ops.save('windows-runtime-health.json', windows)
assert windows['lastResult'] == 0
assert windows['tunnelState'] == 'Running'
assert windows['workerState'] in ['Running', 'Ready']
assert not windows['activation'].get('error')
roots = {p['pid'] for p in windows['pollers'] if p['repoPath']}
assert roots and all(p['repoPath'] or p['parent'] in roots for p in windows['pollers'])
print(json.dumps({'windows':windows}))

health = ops.transport.run([*ops.transport.ssh_args({'ssh_host':'webserver-labo'}),
                           'curl --fail --silent http://127.0.0.1:8000/health'])
ops.save('api-health.json', json.loads(health))
assert json.loads(health)['database_status']=='ok'
print('API health OK; no API code or container was changed')
