"""Run the year2 diagnostic in the observed limited RDP desktop."""

import argparse
import base64
import hashlib
import json

from year2_final_host_operations import HERE, PRIVATE, RUNNER_SHA, SCRIPT_SHA, fresh_admission, remote, write_receipt

CODE = r'''
import base64, hashlib, json, subprocess, time
from pathlib import Path

original = Path('C:/ProgramData/DetecDivHub/homes2-connection-private/year2-final-hosts-20261111')
rdp2 = original.with_name(original.name + '-rdp2')
work = original.with_name(original.name + ('-rdp2-homes' if use_homes else '-rdp2'))
control = json.loads((original / 'controller-request.json').read_bytes())
assert control['batch_id'] == batch_id and time.time() < control['own_lease_deadline_unix']
assert not (original / 'host-windows-state.json').exists() and not (original / 'host-windows-matlab.log').exists()
assert hashlib.sha256((original / 'run_year2_final_hosts.py').read_bytes()).hexdigest() == control['runner_sha256']
runner = base64.b64decode(encoded_runner, validate=True)
assert hashlib.sha256(runner).hexdigest() == runner_sha
files = {}
for filename, key in (('host-request.json', 'request_sha256'), ('verify_year2_final_hosts.m', 'script_sha256')):
    contents = (original / filename).read_bytes()
    assert hashlib.sha256(contents).hexdigest() == control[key]
    files[filename] = contents
if use_homes:
    previous = json.loads((rdp2 / 'host-windows-state.json').read_bytes())
    assert previous['phase'] == 'failed_safe_readonly' and previous['success'] is False
    assert previous['limited_token_verified'] and previous['session_id'] == 2
    assert not (rdp2 / 'host-windows-matlab.log').exists()
    script_bytes = base64.b64decode(encoded_script, validate=True)
    assert hashlib.sha256(script_bytes).hexdigest() == script_sha
    files['verify_year2_final_hosts.m'] = script_bytes
    control['script_sha256'] = script_sha
work.mkdir()
for filename, contents in dict(files, **{'run_year2_final_hosts.py': runner}).items():
    with (work / filename).open('xb') as stream:
        stream.write(contents)
control.update(runner_sha256=runner_sha, expected_windows_session_id=2)
with (work / 'controller-request.json').open('x', encoding='utf-8') as stream:
    json.dump(control, stream)
script = r"""
$ErrorActionPreference = 'Stop'
$name = 'DetecDiv Alexander 2024 Year2 Final Hosts 20261111'
$task = Get-ScheduledTask -TaskName $name
$info = Get-ScheduledTaskInfo -TaskName $name
if ($task.State -ne 'Ready' -or $info.LastTaskResult -ne __EXPECTED_RESULT__) { throw 'Unexpected diagnostic state' }
$principal = ([xml](Export-ScheduledTask -TaskName $name)).Task.Principals.Principal
$expected = ([Security.Principal.NTAccount]::new('GMGM\Charvin-Admin')).Translate([Security.Principal.SecurityIdentifier]).Value
$user = [string]$principal.UserId
$sid = if ($user -match '^S-1-') { $user } else {
    ([Security.Principal.NTAccount]::new($user)).Translate([Security.Principal.SecurityIdentifier]).Value
}
if ($sid -ne $expected -or $task.Principal.RunLevel -ne 'Limited' -or $task.Principal.LogonType -ne 'Interactive') {
    throw 'Unexpected diagnostic principal'
}
$engine = 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe'
$old = '\__PREVIOUS_WORK__\run_year2_final_hosts.py'
$new = '\__NEXT_WORK__\run_year2_final_hosts.py'
if ($task.Actions.Count -ne 1 -or $task.Actions[0].Execute -ne $engine -or
    -not $task.Actions[0].Arguments.Contains($old)) { throw 'Unexpected diagnostic action' }
$desktop = @(Get-Process -Name explorer -IncludeUserName -ErrorAction SilentlyContinue |
    Where-Object { $_.UserName -ieq 'GMGM\Charvin-Admin' } | Select-Object -ExpandProperty SessionId -Unique)
if ($desktop.Count -ne 1 -or $desktop[0] -ne 2) { throw 'Expected RDP desktop is unavailable or ambiguous' }
$action = New-ScheduledTaskAction -Execute $engine -Argument $task.Actions[0].Arguments.Replace($old, $new)
Set-ScheduledTask -TaskName $name -Action $action | Out-Null
Start-ScheduledTask -TaskName $name
@{task=$name;started=$true;session_id=2} | ConvertTo-Json -Compress
"""
script = (script.replace('__EXPECTED_RESULT__', '1' if use_homes else '267011')
          .replace('__PREVIOUS_WORK__', rdp2.name if use_homes else original.name)
          .replace('__NEXT_WORK__', work.name))
payload = base64.b64encode(script.encode('utf-16le')).decode()
result = subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-EncodedCommand', payload],
                        capture_output=True, text=True, timeout=45, check=True)
receipt = json.loads(result.stdout)
receipt.update(work=str(work), runner_sha256=runner_sha, script_sha256=control['script_sha256'],
               batch_id=batch_id, alternate_homes_path=use_homes)
print(json.dumps(receipt))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--homes", action="store_true")
    use_homes = parser.parse_args().homes
    receipt_name = "year2-final-host-windows-rdp2" + ("-homes" if use_homes else "") + "-launch.json"
    assert not (PRIVATE / receipt_name).exists()
    original = (HERE / "run_year2_final_hosts.py").read_bytes()
    assert hashlib.sha256(original).hexdigest() == RUNNER_SHA
    old = b"and session.value==1"
    assert original.count(old) == 1
    runner = original.replace(old, b"and session.value==c['expected_windows_session_id']")
    values = {"use_homes": use_homes}
    if use_homes:
        script = (HERE / "verify_year2_final_hosts.m").read_bytes()
        assert hashlib.sha256(script).hexdigest() == SCRIPT_SHA
        old_root = b"//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub"
        new_root = b"//10.20.8.250/homes/maliavko/DetecdivHub"
        assert runner.count(old_root) == script.count(old_root) == 1
        runner = runner.replace(old_root, new_root)
        script = script.replace(old_root, new_root)
        values.update(encoded_script=base64.b64encode(script).decode(), script_sha=hashlib.sha256(script).hexdigest())
    runner_sha = hashlib.sha256(runner).hexdigest()
    values.update(encoded_runner=base64.b64encode(runner).decode(), runner_sha=runner_sha,
                  batch_id=fresh_admission()["batch_id"])
    prefix = "\n".join(f"{key}={value!r}" for key, value in values.items())
    result = remote("windows", prefix + "\n" + CODE, timeout=65)
    assert result["started"] is True and result["session_id"] == 2 and result["runner_sha256"] == runner_sha
    write_receipt(receipt_name, result)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
