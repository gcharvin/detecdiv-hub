"""Operate a separate limited-session fixture task; no old diagnostic is queried."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

PRIVATE = 'C:/ProgramData/DetecDivHub/homes2-connection-private/raw-namespace-alias-probe-20261110'
TASK = 'DetecDiv Raw Namespace Alias Probe 20261110'

CODE = r'''
import base64,hashlib,json,subprocess
from pathlib import Path
w=Path(PRIVATE)
def ps(script):
 p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(script.encode('utf-16le')).decode()],capture_output=True,text=True,timeout=40)
 assert p.returncode==0,p.stderr[-1500:]
 return json.loads(p.stdout.strip())
if MODE=='stage':
 assert w.parent.is_dir() and not w.exists();w.mkdir()
 raw=base64.b64decode(SOURCE);assert hashlib.sha256(raw).hexdigest()==CONTROL['runner_sha256']
 (w/'probe_raw_namespace_windows.py').write_bytes(raw);(w/'request.json').write_text(json.dumps(CONTROL)+'\n')
 print(json.dumps({'staged':True,'runner_sha256':CONTROL['runner_sha256'],'task_started':False}))
elif MODE=='start':
 assert (w/'request.json').is_file() and not (w/'result.json').exists() and not (w/'error.json').exists()
 code=r"""
$ErrorActionPreference='Stop'
$name='__TASK__'
if(Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue){throw 'Fixture task already exists'}
$principal=New-ScheduledTaskPrincipal -UserId 'GMGM\Charvin-Admin' -LogonType Interactive -RunLevel Limited
$action=New-ScheduledTaskAction -Execute 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -Command "& ''C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub\.venv\Scripts\python.exe'' -X utf8 ''C:\ProgramData\DetecDivHub\homes2-connection-private\raw-namespace-alias-probe-20261110\probe_raw_namespace_windows.py''; exit $LASTEXITCODE"'
$settings=New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 3) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $name -Principal $principal -Action $action -Settings $settings | Out-Null
Start-ScheduledTask -TaskName $name
@{task=$name;state=[string](Get-ScheduledTask -TaskName $name).State;workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State}|ConvertTo-Json -Compress
""".replace('__TASK__',TASK)
 print(json.dumps(ps(code)))
elif MODE=='status':
 result={'task':ps("$t=Get-ScheduledTask -TaskName '"+TASK+"';$i=Get-ScheduledTaskInfo -TaskName '"+TASK+"';@{state=[string]$t.State;result=$i.LastTaskResult}|ConvertTo-Json -Compress")}
 for name in ['result.json','error.json']:
  if (w/name).exists():result[name]=json.loads((w/name).read_text(encoding='utf-8-sig'))
 print(json.dumps(result))
elif MODE=='cleanup':
 result=json.loads((w/'result.json').read_text());control=json.loads((w/'request.json').read_text())
 assert result['success'] and result['limited_token_verified'] and result['session_id']==1 and result['windows_identity'].casefold()=='gmgm\\charvin-admin'
 assert len(result['fixture_paths_verified'])==2 and all(r['sha256']==control['expected_sha256'] for r in result['fixture_paths_verified'])
 assert hashlib.sha256((w/'probe_raw_namespace_windows.py').read_bytes()).hexdigest()==control['runner_sha256']
 code=r"""
$ErrorActionPreference='Stop';$name='__TASK__';$t=Get-ScheduledTask -TaskName $name;$i=Get-ScheduledTaskInfo -TaskName $name
if($t.State -eq 'Running' -or $i.LastTaskResult -ne 0){throw 'Fixture not successfully terminal'}
$xml=[xml](Export-ScheduledTask -TaskName $name);$actual=[string]$xml.Task.Principals.Principal.UserId
$expectedSid=([Security.Principal.NTAccount]::new('GMGM\Charvin-Admin')).Translate([Security.Principal.SecurityIdentifier]).Value
if($actual -match '^S-1-'){$actualSid=$actual}else{$actualSid=([Security.Principal.NTAccount]::new($actual)).Translate([Security.Principal.SecurityIdentifier]).Value}
if($actualSid -ne $expectedSid -or $t.Principal.RunLevel -ne 'Limited' -or $t.Principal.LogonType -ne 'Interactive'){throw 'Unexpected fixture identity'}
if($t.Actions.Count -ne 1 -or $t.Actions[0].Arguments -notlike '*raw-namespace-alias-probe-20261110\probe_raw_namespace_windows.py*'){throw 'Unexpected fixture action'}
$before=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State
Unregister-ScheduledTask -TaskName $name -Confirm:$false
@{fixtureTaskRemoved=$true;workerStateBefore=$before;workerStateAfter=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State}|ConvertTo-Json -Compress
""".replace('__TASK__',TASK)
 value=ps(code);assert value['fixtureTaskRemoved'];(w/'probe_raw_namespace_windows.py').unlink()
 value.update(success=True,fixture_script_removed=True,requests_and_reports_preserved=True,worker_modified=False,dataset_paths_changed=False)
 print(json.dumps(value))
else:raise ValueError('Unknown operation')
'''


def operate(mode):
    assert mode in ['stage', 'start', 'status', 'cleanup']
    cleaned = HERE / 'raw-namespace-windows-cleanup.json'
    if mode == 'status' and cleaned.exists():
        return json.loads((HERE / 'runtime/raw-namespace-windows-status.json').read_text())
    output = HERE / ('runtime/raw-namespace-windows-status.json' if mode == 'status' else 'raw-namespace-windows-' + mode + '.json')
    assert mode == 'status' or not output.exists(), 'Inspect prior operation instead of replay'
    header = 'PRIVATE=' + repr(PRIVATE) + '\nTASK=' + repr(TASK) + '\nMODE=' + repr(mode) + '\n'
    if mode == 'stage':
        payload = (HERE / 'probe_raw_namespace_windows.py').read_bytes()
        control = {'runner_sha256': hashlib.sha256(payload).hexdigest(), 'expected_sha256': json.loads((HERE / 'raw-namespace-alias-linux-fixture-verification.json').read_text())['expected_sha256']}
        header += 'SOURCE=' + repr(base64.b64encode(payload).decode()) + '\nCONTROL=' + repr(control) + '\n'
    if mode == 'cleanup':
        assert (HERE / 'runtime/raw-namespace-windows-status.json').exists(), 'Archive result before cleanup'
    result = remote('windows', header + CODE, timeout=90)
    output.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    return result


if __name__ == '__main__':
    print(json.dumps(operate(sys.argv[1])))
