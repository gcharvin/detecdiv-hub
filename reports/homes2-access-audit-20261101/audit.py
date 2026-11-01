"""Read-only operational audit; a temporary task probes the worker logon session."""
import base64
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys

REPO = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO / 'scripts' / 'ops'))
from register_alexander_2023_manifest import remote_json as _remote_json
from launch_alexander_2023_transfer import SNAPSHOT

SSH = 'C:/Windows/System32/OpenSSH/ssh.exe'
KEY = str(Path.home() / '.ssh/id_ed25519_detecdiv_windows')
TASK = 'DetecDiv homes2 access audit 20261101-1710'
REMOTE_RESULT = r'C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub\logs\homes2-access-audit-20261101-1710.json'

def remote_json(*args, **kwargs):
    try:
        return _remote_json(*args, **kwargs)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(exc.stderr) from None

def win(code):
    # Keep the command below Windows cmd.exe's length limit; payload travels on stdin.
    loader = "$ProgressPreference='SilentlyContinue'; $b=[Console]::In.ReadLine(); & ([ScriptBlock]::Create([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($b))))"
    encoded = base64.b64encode(loader.encode('utf-16le')).decode()
    p = subprocess.run([SSH, '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes', '-o', 'ConnectTimeout=10',
                        '-i', KEY, '-l', 'detecdiv-ops', '10.20.11.56',
                        'powershell.exe -NoProfile -NonInteractive -EncodedCommand ' + encoded],
                       input=base64.b64encode(code.encode()).decode()+'\n', text=True, capture_output=True, timeout=55)
    if p.returncode:
        raise RuntimeError(f'Windows diagnostic failed: {p.stdout}\n{p.stderr}')
    return p.stdout.lstrip('\ufeff').strip()

def save(name, data):
    (OUT / name).write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str) + '\n', encoding='utf-8')

def start(elevated=False):
    diagnostic = r"""
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$r=[ordered]@{utc=(Get-Date).ToUniversalTime().ToString('o');identity=[Security.Principal.WindowsIdentity]::GetCurrent().Name;session=(Get-Process -Id $PID).SessionId;pid=$PID;paths=@();connectionsBefore=@();connectionsAfter=@();mappings=@();errors=@()}
try {
 if ($r.identity -ne 'GMGM\Charvin-Admin' -or $r.session -ne 1) { throw 'Diagnostic not running under worker identity and session' }
 try { $r.connectionsBefore=@(Get-SmbConnection | Where-Object { $_.ServerName -eq '10.20.8.250' } | Select-Object ServerName,ShareName,UserName,Credential,NumOpens) } catch { $r.errors+=('Get-SmbConnection: '+$_.Exception.Message) }
 try { $r.mappings=@(Get-SmbMapping | Where-Object { $_.RemotePath -like '*10.20.8.250*' } | Select-Object LocalPath,RemotePath,Status) } catch { $r.errors+=('Get-SmbMapping: '+$_.Exception.Message) }
 foreach($path in @('\\10.20.8.250\home\DetecdivHub','\\10.20.8.250\home\DetecdivHub\raw\.incoming-alexander-2023-20261008\2023_2','\\10.20.8.250\homes\maliavko\DetecdivHub','\\10.20.8.250\homes\maliavko\DetecdivHub\raw\.incoming-alexander-2023-20261008\2023_2')) {
  $p=[ordered]@{path=$path;readable=$false;firstEntry=$null;error=$null}
  try { $e=[IO.Directory]::EnumerateFileSystemEntries($path).GetEnumerator(); try { if($e.MoveNext()){$p.firstEntry=[IO.Path]::GetFileName($e.Current)}; $p.readable=$true } finally {if($e -is [IDisposable]){$e.Dispose()}} } catch {$p.error=$_.Exception.Message}
  $r.paths+= $p
 }
 try { $r.connectionsAfter=@(Get-SmbConnection | Where-Object { $_.ServerName -eq '10.20.8.250' } | Select-Object ServerName,ShareName,UserName,Credential,NumOpens) } catch { $r.errors+=('Get-SmbConnection: '+$_.Exception.Message) }
 Add-Type -TypeDefinition 'using System; using System.Text; using System.Runtime.InteropServices; public class Homes2WNet { [DllImport("mpr.dll", CharSet=CharSet.Unicode)] public static extern int WNetGetUser(string resource, StringBuilder user, ref int length); }'
 $r.nativeSmbUsers=@()
 foreach($share in @('\\10.20.8.250\home','\\10.20.8.250\homes','X:','Y:')) { $name=New-Object Text.StringBuilder 1024; $length=1024; $status=[Homes2WNet]::WNetGetUser($share,$name,[ref]$length); $r.nativeSmbUsers+=@{share=$share;status=$status;user=$name.ToString()} }
} catch { $r.errors+= $_.Exception.Message }
$r | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath '__RESULT__' -Encoding UTF8
""".replace('__RESULT__', REMOTE_RESULT)
    payload = base64.b64encode(diagnostic.encode('utf-16le')).decode()
    controller = r"""
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$taskName='__TASK__'
$output='__RESULT__'
if ((Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) -or (Test-Path -LiteralPath $output)) { throw 'Diagnostic name already exists' }
$worker=Get-ScheduledTask -TaskName 'DetecDiv Hub Worker'
$principal=New-ScheduledTaskPrincipal -UserId 'GMGM\Charvin-Admin' -LogonType Interactive -RunLevel Limited
$action=New-ScheduledTaskAction -Execute 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -EncodedCommand __PAYLOAD__'
$settings=New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 2) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $taskName -Action $action -Principal $principal -Settings $settings | Out-Null
Start-ScheduledTask -TaskName $taskName
[ordered]@{diagnosticTask=$taskName;output=$output;state=[string](Get-ScheduledTask -TaskName $taskName).State;workerTaskState=[string]$worker.State} | ConvertTo-Json -Compress
""".replace('__TASK__', TASK).replace('__RESULT__', REMOTE_RESULT).replace('__PAYLOAD__', payload)
    if elevated:
        controller = controller.replace('-LogonType Interactive -RunLevel Limited', '-LogonType Interactive -RunLevel Highest')
    result = win(controller)
    print(result)
    save('windows-task-launch.json', json.loads(result))

def catalog():
    current = remote_json(SSH, 'webserver-labo', SNAPSHOT, {}, container=True)
    baseline = remote_json(SSH, 'detecdiv-server', "import json; print(open('/home/charvin-admin/storage-transfers/alexander-2023-20261008/catalog-before.json').read())", {})
    save('catalog-current.json', current)
    # Keep the existing private baseline on its host; record only the diff here.
    old = {r['id']: r for r in baseline['raws']}
    new = {r['id']: r for r in current['raws']}
    diffs = {}
    for key in old.keys() & new.keys():
        fields = [k for k in old[key] if old[key].get(k) != new[key].get(k)]
        if fields:
            diffs[key] = fields
    groups = {}
    for year in ('2023_1', '2023_2', '2023_3'):
        rows = [r for r in new.values() if any('/' + year + '/' in p or p.endswith('/' + year) for p in r['source_paths'])]
        groups[year] = {
            'raws': len(rows), 'owners': dict(Counter(r['owner_key'] for r in rows)),
            'preferred_roots': dict(Counter(l['root_prefix'] for r in rows for l in r['locations'] if l['is_preferred'])),
            'with_secondary_locations': sum(any(l['root_prefix'].startswith('/homes2') for l in r['locations']) for r in rows),
            'raw_ids': [r['id'] for r in rows],
        }
    summary = {'utc': datetime.now(timezone.utc).isoformat(), 'groups': groups,
               'new_matching_raw_ids': sorted(new.keys()-old.keys()), 'baseline_ids_no_longer_matching_source_prefix': sorted(old.keys()-new.keys()),
               'changed_fields': diffs, 'linked_projects': current['linked_projects'], 'active_jobs': current['active_jobs_at_plan']}
    save('catalog-comparison.json', summary)
    print(json.dumps({k:v for k,v in summary.items() if k != 'linked_projects'}, default=str))

def extra():
    code = r'''
import json
from datetime import datetime,timezone
from sqlalchemy import select,text,func
from api.db import SessionLocal
from api.models import Job,ProjectLock,WorkerInstance,ExecutionTarget,StorageMigrationBatch,StorageProvider,UserStorageAccount,User,BackupRun,SystemSetting,RawDataset
from api.config import get_settings
from api.services.path_resolution import compose_storage_path
now=datetime.now(timezone.utc)
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 raws=[r for r in db.scalars(select(RawDataset)) if any(compose_storage_path(l.storage_root.path_prefix,l.relative_path).startswith('/data/Alexander/data/2023_2/') for l in r.locations)]
 ids={str(r.id) for r in raws}; projects={str(l.project_id) for r in raws for l in r.project_links}
 jobs=[]
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  payload=json.dumps(j.params_json or {},default=str)
  affected=str(j.raw_dataset_id) in ids or str(j.project_id) in projects or any(x in payload for x in ids|projects) or any(x in payload for x in ['/data/Alexander/data/2023_2','/homes2/maliavko/DetecdivHub/raw'])
  jobs.append({'id':str(j.id),'status':j.status,'target_id':str(j.execution_target_id),'project_id':str(j.project_id),'raw_id':str(j.raw_dataset_id),'heartbeat_at':j.heartbeat_at,'started_at':j.started_at,'affected_2023_2':affected,'kind':(j.params_json or {}).get('job_kind')})
 locks=[{'project':str(l.project_id),'job':str(l.job_id),'expires':l.expires_at,'affected_2023_2':str(l.project_id) in projects} for l in db.scalars(select(ProjectLock).where(ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now]
 workers=[{'target':w.execution_target.target_key,'instance':w.worker_instance,'pid':w.process_id,'health':w.health,'current_job':str(w.current_job_id),'last_seen':w.last_seen_at,'fingerprint':w.code_fingerprint} for w in db.scalars(select(WorkerInstance))]
 batches=[{'id':str(b.id),'name':b.batch_name,'status':b.status,'updated':b.updated_at,'summary':b.summary_json,'items':[{'name':i.display_name,'status':i.status,'action':i.action} for i in b.items]} for b in db.scalars(select(StorageMigrationBatch).where(StorageMigrationBatch.batch_name=='alexander-2023-20261008'))]
 providers=[{'key':p.provider_key,'active':p.is_active,'mount':p.mount_root,'quota_mode':p.quota_mode} for p in db.scalars(select(StorageProvider)) if 'synology' in p.provider_key]
 accounts=[{'provider':a.provider.provider_key,'dsm_user':a.provider_user_key,'status':a.provisioning_status,'quota_status':a.quota_status,'quota_bytes':a.quota_bytes} for a in db.scalars(select(UserStorageAccount).join(User).where(User.user_key=='alexander'))]
 settings=get_settings(); entry=db.get(SystemSetting,'backup_settings'); backup=entry.value_json if entry else {}
 result={'utc':now,'jobs':jobs,'locks':locks,'workers':workers,'batches':batches,'providers':providers,'accounts':accounts,'backup':{'enabled':backup.get('backup_enabled',settings.backup_enabled),'runs':db.scalar(select(func.count()).select_from(BackupRun))}}
 print(json.dumps(result,default=str))
'''
    data = remote_json(SSH, 'webserver-labo', code, {}, container=True)
    save('live-runtime.json', data)
    print(json.dumps(data))

def collect():
    controller = r"""
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$task=Get-ScheduledTask -TaskName '__TASK__'
$info=Get-ScheduledTaskInfo -TaskName '__TASK__'
[ordered]@{state=[string]$task.State;lastTaskResult=$info.LastTaskResult;lastRun=$info.LastRunTime.ToUniversalTime().ToString('o');resultPresent=(Test-Path -LiteralPath '__RESULT__')} | ConvertTo-Json -Compress
if (Test-Path -LiteralPath '__RESULT__') { Get-Content -Raw -LiteralPath '__RESULT__' }
""".replace('__TASK__', TASK).replace('__RESULT__', REMOTE_RESULT)
    result = win(controller)
    (OUT / 'windows-session-result.txt').write_text(result+'\n', encoding='utf-8')
    status, receipt = result.split('\n', 1)
    save('windows-session-result.json', {'task': json.loads(status), 'receipt': json.loads(receipt)})
    print(result)

def cleanup():
    code = r"""
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$task=Get-ScheduledTask -TaskName '__TASK__'
if ($task.State -eq 'Running') { throw 'Diagnostic still running; do not remove it' }
if (-not (Test-Path -LiteralPath '__RESULT__')) { throw 'No diagnostic receipt; inspect task before cleanup' }
Unregister-ScheduledTask -TaskName '__TASK__' -Confirm:$false
Remove-Item -LiteralPath '__RESULT__' -Force
[ordered]@{taskRemoved=(-not [bool](Get-ScheduledTask -TaskName '__TASK__' -ErrorAction SilentlyContinue));resultRemoved=(-not (Test-Path -LiteralPath '__RESULT__'));workerState=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State} | ConvertTo-Json -Compress
""".replace('__TASK__', TASK).replace('__RESULT__', REMOTE_RESULT)
    result = win(code)
    save('windows-diagnostic-cleanup.json', json.loads(result))
    print(result)

if __name__ == '__main__':
    {'start': start, 'start-elevated': lambda: start(elevated=True), 'catalog': catalog, 'extra': extra, 'collect': collect, 'cleanup': cleanup}[sys.argv[1]]()
