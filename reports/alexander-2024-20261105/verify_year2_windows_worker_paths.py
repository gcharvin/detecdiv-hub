"""Read the proposed 17 Hub locations in the live Windows worker's logon."""

import argparse
import json

from year2_final_host_operations import PRIVATE, REQUEST_SHA, read_json, remote, write_receipt

PROBE = r"""
import ctypes,hashlib,json,os,subprocess,sys
from ctypes import wintypes
from pathlib import Path
from types import SimpleNamespace
root=Path('C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub')
os.chdir(root)
sys.path.insert(0,str(root))
from api.config import get_settings
from api.services.project_deletion import resolve_project_location_paths
from worker.path_mappings import parse_worker_path_mappings,map_worker_path
kernel=ctypes.WinDLL('kernel32',use_last_error=True)
advapi=ctypes.WinDLL('advapi32',use_last_error=True)
kernel.OpenProcess.argtypes=(wintypes.DWORD,wintypes.BOOL,wintypes.DWORD)
kernel.OpenProcess.restype=wintypes.HANDLE
kernel.CloseHandle.argtypes=(wintypes.HANDLE,)
advapi.OpenProcessToken.argtypes=(wintypes.HANDLE,wintypes.DWORD,ctypes.POINTER(wintypes.HANDLE))
advapi.GetTokenInformation.argtypes=(wintypes.HANDLE,ctypes.c_int,ctypes.c_void_p,wintypes.DWORD,ctypes.POINTER(wintypes.DWORD))

def authentication_id(pid):
    process=kernel.OpenProcess(0x1000,False,pid)
    if not process:raise ctypes.WinError(ctypes.get_last_error())
    token=wintypes.HANDLE()
    try:
        if not advapi.OpenProcessToken(process,8,ctypes.byref(token)):raise ctypes.WinError(ctypes.get_last_error())
        buffer=ctypes.create_string_buffer(128)
        size=wintypes.DWORD()
        if not advapi.GetTokenInformation(token,10,buffer,len(buffer),ctypes.byref(size)):
            raise ctypes.WinError(ctypes.get_last_error())
        assert size.value>=16
        return buffer.raw[8:16]
    finally:
        if token.value:kernel.CloseHandle(token)
        kernel.CloseHandle(process)

def verify(work):
    assert subprocess.check_output(['whoami'],text=True).strip().casefold()=='gmgm\\charvin-admin'
    assert ctypes.windll.shell32.IsUserAnAdmin()==0
    session=wintypes.DWORD()
    assert kernel.ProcessIdToSessionId(os.getpid(),ctypes.byref(session))
    assert session.value==2
    own=authentication_id(os.getpid())
    assert own==authentication_id(worker_pid)
    request=(work/'host-request.json').read_bytes()
    assert hashlib.sha256(request).hexdigest()==request_sha
    projects=json.loads(request)['projects']
    assert len(projects)==17
    mappings=parse_worker_path_mappings(get_settings().worker_path_mappings)
    assert map_worker_path('/homes2/maliavko/DetecdivHub',mappings).replace(chr(92),'/')==alternate
    rows=[]
    for p in projects:
        stem=p['name']
        location=SimpleNamespace(storage_root=SimpleNamespace(path_prefix='/homes2'),
            relative_path='maliavko/DetecdivHub/projects/'+stem,project_file_name=stem+'.mat')
        canonical,directory=resolve_project_location_paths(location)
        path=Path(map_worker_path(canonical,mappings))
        assert Path(map_worker_path(directory,mappings)).is_dir()
        with path.open('rb') as stream:digest=hashlib.file_digest(stream,'sha256').hexdigest()
        assert digest==p['main_sha256'], 'MAT digest mismatch: '+stem
        rows.append({'project_id':p['project_id'],'sha256':digest})
    assert own==authentication_id(worker_pid)
    return {'success':True,'projects':len(rows),'rows':rows,'worker_pid':worker_pid,
            'session_id':session.value,'limited':True,'worker_logon_matches':True,'windows_path':alternate}

work=Path(__file__).resolve().parent
try:
    result=verify(work)
except Exception as exc:
    result={'success':False,'error_type':type(exc).__name__,'message':str(exc)}
with (work/'hub-worker-path-result.json').open('x',encoding='utf-8') as out:json.dump(result,out)
sys.exit(0 if result['success'] else 1)
"""

REMOTE = r'''
import base64,hashlib,json,subprocess
from pathlib import Path
work=Path('C:/ProgramData/DetecDivHub/homes2-connection-private/year2-final-hosts-20261111-rdp2-homes')
file=work/'verify_hub_worker_paths.py'
output=work/'hub-worker-path-result.json'
task='DetecDiv Alexander Year2 Hub Worker Path Verification'
def ps(script):
    encoded=base64.b64encode(script.encode('utf-16le')).decode()
    run=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-EncodedCommand',encoded],
                       check=True,capture_output=True,text=True,timeout=30)
    return json.loads(run.stdout)
if mode=='start':
    assert not output.exists()
    with file.open('x',encoding='utf-8') as stream:stream.write(source_code)
    result=ps(r"""
$ErrorActionPreference='Stop'
$name='DetecDiv Alexander Year2 Hub Worker Path Verification'
if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) { throw 'Diagnostic already exists' }
$principal=New-ScheduledTaskPrincipal -UserId 'GMGM\Charvin-Admin' -LogonType Interactive -RunLevel Limited
$action=New-ScheduledTaskAction -Execute 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -Command "& ''C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub\.venv\Scripts\python.exe'' -X utf8 ''C:\ProgramData\DetecDivHub\homes2-connection-private\year2-final-hosts-20261111-rdp2-homes\verify_hub_worker_paths.py''; exit $LASTEXITCODE"'
Register-ScheduledTask -TaskName $name -Principal $principal -Action $action | Out-Null
Start-ScheduledTask -TaskName $name
@{started=$true} | ConvertTo-Json -Compress
""")
    result['script_sha256']=hashlib.sha256(file.read_bytes()).hexdigest()
elif mode=='cleanup':
    assert hashlib.sha256(file.read_bytes()).hexdigest()==expected_script
    assert json.loads(output.read_bytes())==expected_report
    result=ps(r"""
$ErrorActionPreference='Stop'
$name='DetecDiv Alexander Year2 Hub Worker Path Verification'
$task=Get-ScheduledTask -TaskName $name
$info=Get-ScheduledTaskInfo -TaskName $name
if ($task.State -ne 'Ready' -or $info.LastTaskResult -ne 0 -or $task.Actions.Count -ne 1 -or
    $task.Actions[0].Arguments -notlike '*year2-final-hosts-20261111-rdp2-homes\verify_hub_worker_paths.py*') {
    throw 'Unexpected diagnostic task state or action'
}
Unregister-ScheduledTask -TaskName $name -Confirm:$false
@{removed=$true} | ConvertTo-Json -Compress
""")
else:
    result=ps(r"""
$ErrorActionPreference='Stop'
$name='DetecDiv Alexander Year2 Hub Worker Path Verification'
$task=Get-ScheduledTask -TaskName $name
$info=Get-ScheduledTaskInfo -TaskName $name
@{task_state=[string]$task.State;exit=$info.LastTaskResult} | ConvertTo-Json -Compress
""")
    result['report']=json.loads(output.read_bytes()) if output.exists() else None
    result['script_sha256']=hashlib.sha256(file.read_bytes()).hexdigest()
print(json.dumps(result))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("start", "status", "cleanup"))
    mode = parser.parse_args().mode
    launch_file = "year2-windows-worker-path-launch.json"
    report_file = "year2-windows-worker-path-verification.json"
    values = {"mode": mode}
    if mode == "start":
        assert not (PRIVATE / launch_file).exists()
        worker = read_json(PRIVATE / "year2-windows-mapping-worker-restarted.json")
        values["source_code"] = (f"worker_pid={worker['pid']!r}\nrequest_sha={REQUEST_SHA!r}\n"
                                 "alternate='//10.20.8.250/homes/maliavko/DetecdivHub'\n" + PROBE)
    elif mode == "cleanup":
        assert not (PRIVATE / "year2-windows-worker-path-cleanup.json").exists()
        values.update(expected_script=read_json(PRIVATE / launch_file)["script_sha256"],
                      expected_report=read_json(PRIVATE / report_file))
    prefix = "\n".join(f"{key}={value!r}" for key, value in values.items())
    result = remote("windows", prefix + "\n" + REMOTE, timeout=45)
    if mode == "start":
        assert result["started"] is True
        write_receipt(launch_file, result)
    elif mode == "cleanup":
        assert result["removed"] is True
        write_receipt("year2-windows-worker-path-cleanup.json", result)
    elif result["task_state"] == "Ready" and result["exit"] == 0:
        receipt = read_json(PRIVATE / launch_file)
        report = result["report"]
        assert result["script_sha256"] == receipt["script_sha256"]
        assert report["success"] and report["projects"] == len(report["rows"]) == 17
        assert report["worker_logon_matches"] and report["limited"]
        output = PRIVATE / report_file
        if not output.exists():
            write_receipt(output.name, report)
        result["report"] = {key: value for key, value in report.items() if key != "rows"}
    print(json.dumps(result))


if __name__ == "__main__":
    main()
