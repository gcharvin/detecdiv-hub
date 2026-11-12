"""Start and verify the two year2 readers after the exact publication succeeds."""

import argparse
import base64
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import time

from launch_year2_project_publication import (
    CATALOG, HERE, HOLDER, INTEGRITY_SHA, PRIVATE, SSH,
    read_json, remote, remote_json, scope_from_baseline, write_receipt,
)

REQUEST_SHA = "4824f2b1a22e26d048f4614077aee7843483d261d116e69956a74eb7e3e7b7fa"
SCRIPT_SHA = "76379c4ab7a121f432d9ef6908ac4ecbf8184a9ffece3501417c9d94cd86c0ff"
RUNNER_SHA = "db6f6c99a2117a44f85cce9e8891f3e47d1ac44846728bffad7d5334c5c691bb"
ENGINE_SHA = "d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7"
TASK = "DetecDiv Alexander 2024 Year2 Final Hosts 20261111"

PUBLICATION = r"""
import base64, hashlib, json
from pathlib import Path
root = Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
work = root / 'year2-final-publication-20261110'
assert (work / 'controller.exit').read_text().strip() == '0'
state = json.loads((work / 'state.json').read_text())
assert state['phase'] == 'filesystem_published_final_host_gates_pending' and state['success'] is True
payload = (work / 'result.json').read_bytes()
result = json.loads(payload)
assert result['success'] is True and result['filesystem_published'] is True
assert result['all_original_inventories_unchanged'] is True and result['source_deleted'] is False
assert result['catalog_changed'] is False
projects = result['projects']
assert len(projects) == 17 and all(row['native_owner'] == 'maliavko' for row in projects)
assert sum(row['files'] for row in projects) == 103243
assert sum(row['total_bytes'] for row in projects) == 1831201635230
renames = (work / 'renames.jsonl').read_bytes()
assert hashlib.sha256(renames).hexdigest() == result['renames_sha256']
rows = [json.loads(line) for line in renames.splitlines()]
assert len(rows) == 17 and all(row['no_overwrite'] and row['directory_no_follow'] for row in rows)
assert {(row['project_id'], row['name']) for row in projects} == {(row['project_id'], row['name']) for row in rows}
raw = json.loads((root / 'state.json').read_text())
assert raw['years']['2024_2']['full_sha1_passed'] and raw['years']['2024_2']['source_stability_verified']
assert (root / 'controller.exit').read_text().strip() == '0'
inputs = root / 'year2-final-host-inputs-20261110'
assert hashlib.sha256((inputs / 'request.json').read_bytes()).hexdigest() == request_sha
print(json.dumps({'projects': projects, 'result_sha256': hashlib.sha256(payload).hexdigest(),
                  'renames_sha256': result['renames_sha256'],
                  'gzip': base64.b64encode((inputs / 'request.json.gz').read_bytes()).decode()}))
"""

OPERATE = r'''
import base64, gzip, hashlib, json, os, shlex, subprocess, sys, time
from pathlib import Path

windows = os.name == 'nt'
host = 'windows' if windows else 'linux'
if windows:
    work = Path('C:/ProgramData/DetecDivHub/homes2-connection-private/year2-final-hosts-20261111')
    suffix = arguments.get('windows_attempt', '')
    assert suffix in ('', '-rdp2', '-rdp2-homes')
    work = work.with_name(work.name + suffix)
else:
    work = Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/year2-final-hosts-20261111')

def powershell(script):
    payload = base64.b64encode(script.encode('utf-16le')).decode()
    output = subprocess.run(
        ['powershell.exe', '-NoProfile', '-NonInteractive', '-EncodedCommand', payload],
        capture_output=True, check=True, text=True, timeout=45)
    return json.loads(output.stdout)

def fingerprint(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def check_inputs():
    control = json.loads((work / 'controller-request.json').read_text())
    assert time.time() < control['own_lease_deadline_unix']
    for name, key in (('host-request.json', 'request_sha256'), ('verify_year2_final_hosts.m', 'script_sha256'),
                      ('run_year2_final_hosts.py', 'runner_sha256')):
        assert fingerprint(work / name) == control[key]
    return control

mode = arguments['mode']
if mode == 'stage':
    control = arguments['control']
    files = {name: base64.b64decode(data, validate=True) for name, data in arguments['files'].items()}
    if windows:
        raw = gzip.decompress(base64.b64decode(arguments['gzip'], validate=True))
    else:
        raw = (work.parent / 'year2-final-host-inputs-20261110/request.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == control['request_sha256']
    work.mkdir(mode=0o700)
    for name, contents in dict(files, **{'host-request.json': raw}).items():
        with (work / name).open('xb') as stream:
            stream.write(contents)
    (work / 'controller-request.json').write_text(json.dumps(control) + '\n', encoding='utf-8')
    check_inputs()
    result = {'host': host, 'work': str(work), 'staged': True}
elif mode == 'start':
    control = check_inputs()
    assert control['batch_id'] == arguments['batch_id']
    assert not (work / ('host-' + host + '-state.json')).exists()
    if windows:
        result = powershell(r"""
$ErrorActionPreference = 'Stop'
$name = 'DetecDiv Alexander 2024 Year2 Final Hosts 20261111'
if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) { throw 'Task already exists' }
$principal = New-ScheduledTaskPrincipal -UserId 'GMGM\Charvin-Admin' -LogonType Interactive -RunLevel Limited
$action = New-ScheduledTaskAction -Execute 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -Command "& ''C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub\.venv\Scripts\python.exe'' -X utf8 ''C:\ProgramData\DetecDivHub\homes2-connection-private\year2-final-hosts-20261111\run_year2_final_hosts.py''; exit $LASTEXITCODE"'
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 5) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $name -Principal $principal -Action $action -Settings $settings | Out-Null
Start-ScheduledTask -TaskName $name
@{task=$name;state=[string](Get-ScheduledTask -TaskName $name).State} | ConvertTo-Json -Compress
""")
    else:
        assert not (work / 'controller.log').exists() and not (work / 'controller.exit').exists()
        command = shlex.join([sys.executable, '-u', str(work / 'run_year2_final_hosts.py')])
        command += ' > ' + shlex.quote(str(work / 'controller.log')) + ' 2>&1'
        command += '; result=$?; printf "%s\\n" "$result" > ' + shlex.quote(str(work / 'controller.exit'))
        session = 'alex2024-year2-final-hosts-20261111'
        subprocess.run(['tmux', 'new-session', '-d', '-s', session, 'bash', '-c', command], check=True)
        result = {'host': host, 'session': session, 'started': True}
elif mode == 'resume-never-run':
    assert windows
    control = check_inputs()
    assert control['batch_id'] == arguments['batch_id']
    assert not (work / 'host-windows-state.json').exists() and not (work / 'host-windows-matlab.log').exists()
    result = powershell(r"""
$ErrorActionPreference = 'Stop'
$name = 'DetecDiv Alexander 2024 Year2 Final Hosts 20261111'
$task = Get-ScheduledTask -TaskName $name
$info = Get-ScheduledTaskInfo -TaskName $name
if ($task.State -ne 'Ready' -or $info.LastTaskResult -ne 267011) { throw 'Diagnostic has already run or started' }
$expected = ([Security.Principal.NTAccount]::new('GMGM\Charvin-Admin')).Translate([Security.Principal.SecurityIdentifier]).Value
$xml = [xml](Export-ScheduledTask -TaskName $name)
$user = [string]$xml.Task.Principals.Principal.UserId
if ($user -match '^S-1-') { $sid = $user } else {
    $sid = ([Security.Principal.NTAccount]::new($user)).Translate([Security.Principal.SecurityIdentifier]).Value
}
if ($sid -ne $expected -or $task.Principal.RunLevel -ne 'Limited' -or $task.Principal.LogonType -ne 'Interactive') {
    throw 'Unexpected diagnostic principal'
}
if ($task.Actions.Count -ne 1 -or $task.Actions[0].Execute -ne 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -or
    $task.Actions[0].Arguments -notlike '*homes2-connection-private\year2-final-hosts-20261111\run_year2_final_hosts.py*') {
    throw 'Unexpected diagnostic action'
}
$desktop = @(Get-Process -Name explorer -IncludeUserName -ErrorAction SilentlyContinue |
    Where-Object { $_.SessionId -eq 1 -and $_.UserName -ieq 'GMGM\Charvin-Admin' })
if ($desktop.Count -eq 0) {
    @{task=$name;started=$false;interactiveSession1Available=$false} | ConvertTo-Json -Compress
} else {
    Start-ScheduledTask -TaskName $name
    @{task=$name;started=$true;interactiveSession1Available=$true} | ConvertTo-Json -Compress
}
""")
else:
    assert mode == 'status'
    result = {'host': host, 'work': str(work), 'work_exists': work.is_dir()}
    if windows:
        result['task'] = powershell(r"""
$ErrorActionPreference = 'Stop'
$name = 'DetecDiv Alexander 2024 Year2 Final Hosts 20261111'
$task = Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
if ($task) {
    $info = Get-ScheduledTaskInfo -TaskName $name
    @{exists=$true;state=[string]$task.State;result=$info.LastTaskResult;runLevel=[string]$task.Principal.RunLevel;logonType=[string]$task.Principal.LogonType} | ConvertTo-Json -Compress
} else { @{exists=$false} | ConvertTo-Json -Compress }
""")
    elif (work / 'controller.exit').exists():
        result['controller_exit'] = (work / 'controller.exit').read_text().strip()
    for key in ('state', 'progress', 'verification'):
        path = work / ('host-' + host + '-' + key + '.json')
        if path.exists():
            contents = path.read_bytes()
            result[key] = json.loads(contents.decode('utf-8-sig'))
            if key == 'verification':
                result['report_sha256'] = hashlib.sha256(contents).hexdigest()
print(json.dumps(result))
'''


def operate(host, mode, **values):
    payload = json.dumps(dict(values, mode=mode))
    return remote(host, "import json\narguments=json.loads(" + repr(payload) + ")\n" + OPERATE,
                  timeout=180 if mode == "stage" else 60)


def fresh_admission():
    initial = read_json(PRIVATE / "year2-publication-admission.json")
    live = remote_json(SSH, "webserver-labo", CATALOG, {
        "projects": scope_from_baseline(), "mode": "fresh", "batch_id": initial["batch_id"],
        "integrity_result_sha256": INTEGRITY_SHA,
    }, container=True)
    assert live["holder"] == HOLDER and live["batch_id"] == initial["batch_id"]
    assert sorted(live["leases"], key=lambda row: row["id"]) == sorted(initial["leases"], key=lambda row: row["id"])
    return live


def stage():
    assert not (PRIVATE / "year2-final-host-control.json").exists()
    publication = remote("linux", "request_sha=" + repr(REQUEST_SHA) + "\n" + PUBLICATION, timeout=60)
    scope = {(row["project_id"], row["name"]) for row in scope_from_baseline()}
    assert {(row["project_id"], row["name"]) for row in publication["projects"]} == scope
    assert hashlib.sha256(gzip.decompress(base64.b64decode(publication["gzip"], validate=True))).hexdigest() == REQUEST_SHA
    files = {}
    for name, sha in (("verify_year2_final_hosts.m", SCRIPT_SHA), ("run_year2_final_hosts.py", RUNNER_SHA)):
        payload = (HERE / name).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == sha
        files[name] = base64.b64encode(payload).decode()
    admission = fresh_admission()
    deadline = min(datetime.fromisoformat(row["expires_at"]).timestamp() for row in admission["leases"])
    assert time.time() < deadline
    control = {
        "batch_id": admission["batch_id"], "request_sha256": REQUEST_SHA, "script_sha256": SCRIPT_SHA,
        "runner_sha256": RUNNER_SHA, "own_lease_deadline_unix": deadline,
        "publication_result_sha256": publication["result_sha256"], "filesystem_publication_verified": True,
        "raw_year2_full_sha1_and_source_stability_verified": True, "terminal_incoming_integrity_result_sha256": INTEGRITY_SHA,
    }
    write_receipt("year2-final-host-control.json", control)
    for host in ("linux", "windows"):
        values = {"gzip": publication["gzip"]} if host == "windows" else {}
        result = operate(host, "stage", control=control, files=files, **values)
        write_receipt(f"year2-final-host-{host}-stage.json", result)
        print(json.dumps(result))


def start(host):
    control = read_json(PRIVATE / "year2-final-host-control.json")
    assert read_json(PRIVATE / f"year2-final-host-{host}-stage.json")["staged"] is True
    receipt = f"year2-final-host-{host}-launch.json"
    assert not (PRIVATE / receipt).exists()
    assert fresh_admission()["batch_id"] == control["batch_id"]
    result = operate(host, "start", batch_id=control["batch_id"])
    write_receipt(receipt, result)
    print(json.dumps(result))


def windows_attempt():
    for suffix in ("-rdp2-homes", "-rdp2"):
        receipt = PRIVATE / f"year2-final-host-windows{suffix}-launch.json"
        if receipt.is_file():
            return suffix, read_json(receipt)
    return "", None


def status():
    suffix, _ = windows_attempt()
    reports = {host: operate(host, "status", windows_attempt=suffix) for host in ("linux", "windows")}
    path = PRIVATE / "year2-final-host-status.json"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(reports, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
    return reports


def resume_windows():
    control = read_json(PRIVATE / "year2-final-host-control.json")
    receipt = "year2-final-host-windows-resume.json"
    assert not (PRIVATE / receipt).exists()
    assert fresh_admission()["batch_id"] == control["batch_id"]
    result = operate("windows", "resume-never-run", batch_id=control["batch_id"])
    if result["started"]:
        write_receipt(receipt, result)
    print(json.dumps(result))


def verify(reports):
    linux, windows = reports["linux"], reports["windows"]
    assert linux["controller_exit"] == "0"
    assert windows["task"] == {"exists": True, "state": "Ready", "result": 0,
                               "runLevel": "Limited", "logonType": "Interactive"}
    state = windows["state"]
    assert state["windows_identity"].casefold() == "gmgm\\charvin-admin"
    suffix, launch = windows_attempt()
    expected_session = launch["session_id"] if launch else 1
    assert state["session_id"] == expected_session and state["limited_token_verified"] is True
    by_host = {}
    for host, receipt in reports.items():
        state, report = receipt["state"], receipt["verification"]
        assert state["success"] is True and state["matlab_exit_code"] == 0 and state["engine_sha"] == ENGINE_SHA
        assert state["report_sha256"] == receipt["report_sha256"]
        assert report["success"] is True and report["host"] == host and report["project_namespace"] == "final"
        assert not report["project_saved"] and not report["source_deleted"] and not report["catalog_changed"]
        projects = report["projects"]
        assert len(projects) == 17
        assert [sum(project[key] for project in projects) for key in
                ("fovs", "rois", "raw_sample_reads", "blank_paths_preserved", "preexisting_placeholder_payloads")
                ] == [239, 43102, 3525, 412, 1]
        for project in projects:
            assert project["availability_baseline_preserved"] is True and project["project_saved"] is False
            assert len(project["field_results"]) == project["fovs"]
            assert all(field["engine_reader_verified"] and field["availability_baseline_preserved"]
                       for field in project["field_results"])
        by_host[host] = {project["project_id"]: project for project in projects}
    assert by_host["linux"] == by_host["windows"]
    assert {(p["project_id"], p["name"]) for p in by_host["linux"].values()} == {
        (p["project_id"], p["name"]) for p in scope_from_baseline()}
    gilles = next(project for project in by_host["linux"].values() if project["name"] == "281124_Alex")
    assert gilles["typed_paths"] == {"typed_json_verified": 9, "run_loaders_verified": 7,
                                    "external_files_verified": 3, "historical_run_resumption_validated": False}
    result = {"success": True, "projects": 17, "fovs": 239, "rois": 43102, "raw_samples_per_host": 3525,
              "engine_sha": ENGINE_SHA, "limited_windows_token_verified": True, "windows_session_id": expected_session,
              "windows_alternate_homes_path": suffix == "-rdp2-homes",
              "report_sha256_by_host": {host: receipt["report_sha256"] for host, receipt in reports.items()},
              "project_saved": False, "catalog_changed": False, "source_deleted": False}
    with (HERE / "year2-final-hosts-completion-verification.json").open("x", encoding="utf-8") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
    print(json.dumps(result))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("stage", "start-linux", "start-windows", "resume-windows", "status", "verify"))
    action = parser.parse_args().action
    if action == "stage":
        stage()
    elif action.startswith("start-"):
        start(action.removeprefix("start-"))
    elif action == "resume-windows":
        resume_windows()
    elif action == "verify":
        verify(status())
    else:
        for host, receipt in status().items():
            print(json.dumps({key: value for key, value in receipt.items() if key != "verification"}))


if __name__ == "__main__":
    main()
