"""Remove only the completed year1 diagnostic task and its two pinned scripts."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import base64,hashlib,json,subprocess
from pathlib import Path
w=Path('C:/ProgramData/DetecDivHub/homes2-connection-private/year1-final-hosts-20261109')
state=json.loads((w/'host-windows-state.json').read_text());report=json.loads((w/'host-windows-verification.json').read_text())
assert state['success'] and state['matlab_exit_code']==0 and state['session_id']==1 and state['windows_identity'].casefold()=='gmgm\\charvin-admin'
assert report['success'] and len(report['projects'])==16 and sum(x['fovs'] for x in report['projects'])==159 and sum(x['rois'] for x in report['projects'])==50234 and sum(x['raw_sample_reads'] for x in report['projects'])==2059
assert hashlib.sha256((w/'host-windows-verification.json').read_bytes()).hexdigest()==state['report_sha256']
pins={'verify_year1_final_hosts.m':'2ca5f1177e4eb2edd2552255104c7ce72428880448fb3c7a9d2e0e1695a38d84','run_year1_final_hosts.py':'5b66b77513086002728a83f9bda6baf11d8e44cdeb77f59576bc293b881c7792'}
for name,h in pins.items():assert hashlib.sha256((w/name).read_bytes()).hexdigest()==h
script=r"""
$ErrorActionPreference='Stop'
$name='DetecDiv Alexander 2024 Year1 Final Hosts 20261109'
$task=Get-ScheduledTask -TaskName $name
$info=Get-ScheduledTaskInfo -TaskName $name
if($task.State -eq 'Running' -or $info.LastTaskResult -ne 0){throw 'Diagnostic not successfully terminal'}
if($task.Principal.UserId -ine 'GMGM\Charvin-Admin' -or $task.Principal.RunLevel -ne 'Limited'){throw 'Unexpected diagnostic principal'}
if($task.Actions.Count -ne 1 -or $task.Actions[0].Arguments -notlike '*homes2-connection-private\year1-final-hosts-20261109\run_year1_final_hosts.py*'){throw 'Unexpected diagnostic action'}
$workerBefore=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State
Unregister-ScheduledTask -TaskName $name -Confirm:$false
@{diagnosticTaskRemoved=$true;workerStateBefore=$workerBefore;workerStateAfter=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State}|ConvertTo-Json -Compress
"""
result=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',base64.b64encode(script.encode('utf-16le')).decode()],capture_output=True,text=True,timeout=40)
assert result.returncode==0,result.stderr[-1500:]
value=json.loads(result.stdout);assert value['diagnosticTaskRemoved']
for name in pins:(w/name).unlink()
value.update(success=True,diagnostic_scripts_removed=list(pins),requests_and_reports_preserved=True,worker_modified=False,project_saved=False,source_deleted=False,report_sha256=state['report_sha256'])
print(json.dumps(value))
'''

def main():
    assert not (HERE/'year1-windows-cleanup.json').exists(),'No replay after cleanup'
    assert (HERE/'year1-final-hosts-completion-verification.json').is_file()
    assert (HERE/'runtime/year1-windows-status.json').is_file(),'Archive reports before cleanup'
    result=remote('windows',CODE,timeout=90)
    with (HERE/'year1-windows-cleanup.json').open('x',encoding='utf-8') as stream:json.dump(result,stream,indent=2);stream.write('\n')
    print(json.dumps(result))

if __name__=='__main__':main()
