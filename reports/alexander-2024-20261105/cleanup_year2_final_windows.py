"""Remove only the successful dedicated year2 Windows diagnostic task."""

import json

from year2_final_host_operations import HERE, PRIVATE, RUNNER_SHA, SCRIPT_SHA, remote, windows_attempt

CODE = r'''
import base64, hashlib, json, subprocess
from pathlib import Path

work = Path('C:/ProgramData/DetecDivHub/homes2-connection-private/year2-final-hosts-20261111')
assert attempt in ('', '-rdp2', '-rdp2-homes')
work = work.with_name(work.name + attempt)
state = json.loads((work / 'host-windows-state.json').read_bytes())
report = work / 'host-windows-verification.json'
assert state['success'] and state['matlab_exit_code'] == 0 and state['session_id'] == 1
assert state['windows_identity'].casefold() == 'gmgm\\charvin-admin' and state['limited_token_verified']
assert hashlib.sha256(report.read_bytes()).hexdigest() == state['report_sha256'] == expected_report
for name, sha in pins.items():
    assert hashlib.sha256((work / name).read_bytes()).hexdigest() == sha
script = r"""
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$name = 'DetecDiv Alexander 2024 Year2 Final Hosts 20261111'
$task = Get-ScheduledTask -TaskName $name
$info = Get-ScheduledTaskInfo -TaskName $name
if ($task.State -ne 'Ready' -or $info.LastTaskResult -ne 0) { throw 'Task is not successfully terminal' }
$xml = [xml](Export-ScheduledTask -TaskName $name)
$actualUser = [string]$xml.Task.Principals.Principal.UserId
$expectedSid = ([Security.Principal.NTAccount]::new('GMGM\Charvin-Admin')).Translate([Security.Principal.SecurityIdentifier]).Value
if ($actualUser -match '^S-1-') { $actualSid = $actualUser } else {
    $actualSid = ([Security.Principal.NTAccount]::new($actualUser)).Translate([Security.Principal.SecurityIdentifier]).Value
}
if ($actualSid -ne $expectedSid -or $task.Principal.RunLevel -ne 'Limited' -or $task.Principal.LogonType -ne 'Interactive') {
    throw 'Unexpected task identity or privilege'
}
if ($task.Actions.Count -ne 1 -or $task.Actions[0].Execute -ne 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -or
    $task.Actions[0].Arguments -notlike '*homes2-connection-private\__WORK_NAME__\run_year2_final_hosts.py*') {
    throw 'Unexpected task action'
}
Unregister-ScheduledTask -TaskName $name -Confirm:$false
if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) { throw 'Diagnostic task remains registered' }
@{diagnosticTaskRemoved=$true} | ConvertTo-Json -Compress
"""
encoded = base64.b64encode(script.replace('__WORK_NAME__', work.name).encode('utf-16le')).decode()
result = subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-EncodedCommand', encoded],
                        capture_output=True, text=True, check=True, timeout=45)
receipt = json.loads(result.stdout)
assert receipt['diagnosticTaskRemoved']
for name, sha in pins.items():
    path = work / name
    assert hashlib.sha256(path.read_bytes()).hexdigest() == sha
    path.unlink()
receipt.update(success=True, report_sha256=expected_report, diagnostic_scripts_removed=list(pins),
               reports_and_request_retained=True, worker_modified=False)
print(json.dumps(receipt))
'''


def main():
    output = HERE / "year2-windows-cleanup.json"
    assert not output.exists()
    proof = json.loads((HERE / "year2-final-hosts-completion-verification.json").read_bytes())
    archived = json.loads((PRIVATE / "year2-final-host-status.json").read_bytes())["windows"]
    expected = proof["report_sha256_by_host"]["windows"]
    assert proof["success"] is True and archived["report_sha256"] == expected
    attempt, launch = windows_attempt()
    pins = {"verify_year2_final_hosts.m": launch.get("script_sha256", SCRIPT_SHA) if launch else SCRIPT_SHA,
            "run_year2_final_hosts.py": launch["runner_sha256"] if launch else RUNNER_SHA}
    prefix = f"expected_report={expected!r}\npins={pins!r}\nattempt={attempt!r}\n"
    result = remote("windows", prefix + CODE, timeout=60)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
