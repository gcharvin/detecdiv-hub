"""Publish the verified 2023_3 project copies and validate their final paths.

This controller does not delete sources or change preferred locations. Each
mutation has an exclusive receipt. Partial publication must be reviewed rather
than replayed. Requests and full catalog snapshots stay in ignored runtime.
"""
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

HERE = Path(__file__).resolve().parent
RUNTIME = HERE / 'runtime'
WORK = '/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101'
HOLDER = 'alexander-2023-3-final-publication-20261105'
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import remote
from audit import SSH, remote_json


def save(name, data, private=False):
    directory = RUNTIME if private else HERE
    directory.mkdir(exist_ok=True)
    with (directory / name).open('x', encoding='utf-8') as stream:
        json.dump(data, stream, indent=2, ensure_ascii=False, default=str)
        stream.write('\n')


def load(name, private=False):
    return json.loads(((RUNTIME if private else HERE) / name).read_text(encoding='utf-8'))


def encoded(value):
    return base64.b64encode(json.dumps(value).encode()).decode()


def collect():
    result = remote('linux', r'''
import json,hashlib
from pathlib import Path
from datetime import datetime,timezone
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101')
s=json.loads((w/'2023_3/state.json').read_text())
assert s['phase']=='verified_in_staging_project_and_catalog_gates_pending'
assert s['full_sha1_passed'] and s['source_stability_verified']
assert not s['source_deleted'] and not s['catalog_changed']
assert not Path('/proc/'+str(s['runner_pid'])).exists()
assert not Path('/proc/'+str(s['child_pid'])).exists()
assert Path(s['verification_log']).stat().st_size==0
expected='4abdfbfff41264b9bc069c3968ac742ac06fa9b7c52127bbf4369a7d95306fd3'
hashes={}
for name in ['source-before.jsonl','source-after.jsonl']:
 hashes[name]=hashlib.sha256((w/'2023_3'/name).read_bytes()).hexdigest()
 assert hashes[name]==expected
assert s['source_inventory']=={'sha256':expected,'bytes':5967100516555,'files':708590}
r=json.loads((w/'project3-relink/result.json').read_text())
assert (w/'project3-relink/controller.exit').read_text().strip()=='0'
assert hashlib.sha256((w/'project3-relink/result.json').read_bytes()).hexdigest()=='f670e09a76189a230f869de6467c41506791122cec568ecf5a382867672c513d'
assert r['success']
assert (w/'project3-host-validation/controller.exit').read_text().strip()=='0'
assert json.loads((w/'project3-host-validation/project3-host-linux-state.json').read_text())['success']
assert json.loads((w/'project3-api-projection/state.json').read_text())['phase']=='verified_ready_for_api_reference_update'
assert (w/'project3-api-projection/controller.exit').read_text().strip()=='0'
print(json.dumps({'success':True,'checked_at':datetime.now(timezone.utc).isoformat(),'raw_state':s,'manifest_hashes':hashes,'verification_log_bytes':0,'source_deleted':False,'preferred_locations_changed':False}))
''', timeout=55)
    save('project3-raw-completion-verification.json', result)
    print(json.dumps(result))


DB_COMMON = r'''
import sys,json,uuid,hashlib
from datetime import datetime,timezone
from sqlalchemy import select,text,inspect
from api.db import SessionLocal
from api.models import User,Project,ProjectLock,RawDataset,Job,IndexingJob,StorageMigrationBatch
from api.services.path_resolution import compose_storage_path
OLD='/data/Alexander/data/2023_3'
NEW='/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_3'
def columns(x):return json.loads(json.dumps({p.key:getattr(x,p.key) for p in inspect(type(x)).column_attrs},default=str))
def digest(x):return hashlib.sha256(json.dumps(x,sort_keys=True,default=str).encode()).hexdigest()
def identity(p):return {'key':p.project_key,'owner':str(p.owner_user_id),'visibility':p.visibility,'notes':p.notes,'acl':[columns(x) for x in p.acl_entries],'groups':[columns(x) for x in p.group_memberships],'notes_entries':[columns(x) for x in p.note_entries],'raw_links':[columns(x) for x in p.raw_links]}
def source_paths(r):return [compose_storage_path(l.storage_root.path_prefix,l.relative_path) for l in r.locations if compose_storage_path(l.storage_root.path_prefix,l.relative_path).startswith(OLD+'/')]
def safety(db,ids,raws,holder=None):
 now=datetime.now(timezone.utc);rids={str(r.id) for r in raws}
 linked={x.project_id for r in raws for x in r.project_links}
 assert linked.issubset(ids),'Additional linked projects require review'
 markers=rids|{str(x) for x in ids}|{db.get(Project,x).project_key for x in ids}
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  data=json.dumps(j.params_json or {},default=str)
  assert j.project_id not in ids and str(j.raw_dataset_id) not in rids and OLD not in data and NEW not in data and not any(x in data for x in markers),'Affected job is active'
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running'])))),'Indexing is active'
 locks=[l for l in db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_(ids),ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now]
 assert all(holder is not None and l.holder_key==holder for l in locks),'Another project lease is active'
 return {'affected_jobs':0,'active_indexing':0,'own_locks':len(locks),'linked_projects':len(linked)}
'''


def admit():
    proof = load('project3-raw-completion-verification.json')
    assert proof['success']
    projects = load('2023_3-project-candidate-plan.json')['candidate_projects']
    host = load('project3-hosts-completion-verification.json')
    assert host['success'] and len(host['projects']) == 11
    code = DB_COMMON + r'''
from api.services.project_locks import acquire_client_edit_lease
v=json.load(sys.stdin);ids={uuid.UUID(p['project_id']) for p in v['projects']}
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 db.execute(text("SELECT pg_advisory_xact_lock(hashtext('alexander-2023-3-final-publication-20261105'))"))
 assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==v['holder'])) is None
 operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin',User.is_active.is_(True))).one()
 raws=[r for r in db.scalars(select(RawDataset)) if source_paths(r)]
 assert len(raws)==24 and sum(len(r.positions) for r in raws)==775
 assert all(r.owner.user_key=='alexander' for r in raws)
 safety(db,ids,raws)
 prior=db.get(StorageMigrationBatch,uuid.UUID('46b50e1d-bf24-4284-9d3d-fbe4df5de158'))
 before_prior={x['project_id']:x for x in prior.metadata_json['before_projects']}
 before=[];leases=[]
 for id in sorted(ids,key=str):
  p=db.get(Project,id,with_for_update=True);assert p.owner.user_key=='alexander'
  snap={'project_id':str(id),'name':p.project_name,'identity_sha256':digest(identity(p)),'metadata_sha256':digest(p.metadata_json),'locations_sha256':digest([columns(x) for x in p.locations])}
  assert snap==before_prior[str(id)],'Project changed since verified relink'
  before.append({'summary':snap,'columns':columns(p),'locations':[columns(x) for x in p.locations],'identity':identity(p)})
  l=acquire_client_edit_lease(db,project_id=id,owner=operator,holder_key=v['holder'],holder_host='codex-admin-workstation',ttl_seconds=14400,write_scope='storage_relocation',reason='Publish verified homes2 copy and test final paths; preserve original project',metadata_json={'source_deletion_allowed':False,'evidence':'reports/alexander-2023-remaining-20261101/'})
  leases.append({'id':str(l.id),'project_id':str(id),'expires_at':l.expires_at.isoformat()})
 raw_before=[{'columns':columns(r),'locations':[columns(l) for l in r.locations],'project_links':[columns(x) for x in r.project_links],'experiment_links':[columns(x) for x in r.experiment_links],'positions':[columns(x) for x in r.positions]} for r in sorted(raws,key=lambda x:str(x.id))]
 batch=StorageMigrationBatch(owner_user_id=raws[0].owner_user_id,batch_name=v['holder'],source_kind='detecdiv_project',source_path='/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis',storage_root_name='user-homes2',host_scope='detecdiv-server',root_type='project_root',strategy='publish_verified_project_copies_preserve_originals',status='final_publication_admitted',metadata_json={'holder':v['holder'],'before_projects':before,'before_raws':raw_before,'leases':leases,'raw_verification':v['proof']},summary_json={'projects':11,'raws':24,'positions':775,'source_deleted':False,'preferred_locations_changed':False})
 db.add(batch);db.flush()
 print(json.dumps({'admitted_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(batch.id),'holder':v['holder'],'leases':leases,'before_projects':before,'before_raws':raw_before,'raw_verification':v['proof']},default=str))
'''
    result = remote_json(SSH, 'webserver-labo', code, {'projects': projects, 'holder': HOLDER, 'proof': proof}, container=True)
    save('project3-final-admission.json', result, private=True)
    summary = {k: result[k] for k in ['admitted_at', 'batch_id', 'holder', 'leases']}
    summary.update(projects=11, raws=24, positions=775, source_deleted=False, preferred_locations_changed=False)
    save('project3-final-admission-summary.json', summary)
    print(json.dumps(summary))


def check_admission():
    a = load('project3-final-admission.json', private=True)
    result = remote_json(SSH, 'webserver-labo', DB_COMMON + r'''
v=json.load(sys.stdin);a=v['admission'];ids={uuid.UUID(x['project_id']) for x in a['leases']}
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']));assert b.status=='final_publication_admitted'
 raws=[r for r in db.scalars(select(RawDataset)) if source_paths(r)];assert len(raws)==24
 guards=safety(db,ids,raws,a['holder']);assert guards['own_locks']==11
 for row in a['leases']:
  l=db.get(ProjectLock,uuid.UUID(row['id']));assert l.status=='active' and l.holder_key==a['holder'] and l.expires_at>datetime.now(timezone.utc)
 for before in a['before_projects']:
  p=db.get(Project,uuid.UUID(before['summary']['project_id']))
  assert digest(identity(p))==before['summary']['identity_sha256']
  assert digest(p.metadata_json)==before['summary']['metadata_sha256']
  assert digest([columns(x) for x in p.locations])==before['summary']['locations_sha256']
 print(json.dumps({'success':True,'checked_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(b.id),'holder':a['holder'],'lease_deadline':min(x['expires_at'] for x in a['leases']),'guards':guards}))
''', {'admission': a}, container=True)
    return a, result


def publish():
    a, fresh = check_admission()
    projects = load('2023_3-project-candidate-plan.json')['candidate_projects']
    payload = {'admission': a, 'fresh': fresh, 'projects': projects, 'files': load('project3-relink-completion-verification.json')['files']}
    code = 'V=' + repr(encoded(payload)) + '\n' + r'''
import base64,json,os,stat,subprocess,ctypes,fcntl
from pathlib import Path
from datetime import datetime,timezone
v=json.loads(base64.b64decode(V));os.umask(0o077)
assert (datetime.now(timezone.utc)-datetime.fromisoformat(v['fresh']['checked_at'])).total_seconds()<120
assert datetime.now(timezone.utc)<datetime.fromisoformat(v['fresh']['lease_deadline'])
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101');d=w/'project3-final-publication'
assert not d.exists(),'Existing or partial publication requires review'
s=json.loads((w/'2023_3/state.json').read_text());assert s['full_sha1_passed'] and s['source_stability_verified']
root=Path('/homes2/maliavko/DetecdivHub/projects')
mount=json.loads(subprocess.check_output(['findmnt','-J','-T',str(root),'-o','SOURCE,TARGET,OPTIONS'],text=True))['filesystems'][0]
assert mount['target']=='/homes2/maliavko' and mount['source']=='//10.20.8.250/home'
assert 'username=maliavko' in mount['options'].split(',') and 'rw' in mount['options'].split(',')
for p in v['projects']:
 stem=p['name'];stage=root/('.incoming-'+stem+'-20261101');final=root/stem
 assert stage.is_dir() and not stage.is_symlink() and not os.path.lexists(final)
 assert Path(p['source_mat']).is_file() and Path(p['source_mat']).with_suffix('').is_dir()
native=r"""
import sys,json,stat
from pathlib import Path
v=json.load(sys.stdin);byid={p['project_id']:p for p in v['projects']}
for p in v['projects']:
 root=Path('/volume1/homes/maliavko/DetecdivHub/projects')/('.incoming-'+p['name']+'-20261101')
 s=root.lstat();assert stat.S_ISDIR(s.st_mode) and s.st_uid==1031
for r in v['files']:
 stem=byid[r['project_id']]['name'];a=Path('/volume1/homes/maliavko/DetecdivHub/projects')/('.incoming-'+stem+'-20261101')/r['relative_path']
 b=Path('/volume1/Sauvegarde/Alexander/analysis')/r['relative_path']
 sa=a.lstat();sb=b.lstat();assert stat.S_ISREG(sa.st_mode) and sa.st_uid==1031 and stat.S_ISREG(sb.st_mode)
 assert (sa.st_dev,sa.st_ino)!=(sb.st_dev,sb.st_ino)
print(json.dumps({'success':True,'independent_maliavko_mat_files':len(v['files'])}))
"""
import shlex
loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(native.encode()).decode())+'))'
args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',shlex.join(['python3','-c',loader])]
p=subprocess.run(args,input=json.dumps(v),capture_output=True,text=True,timeout=60);assert p.returncode==0,p.stderr
ownership=json.loads(p.stdout);assert ownership['success']
d.mkdir(mode=0o700);(d/'admission.json').write_text(json.dumps(v['admission'],indent=2,default=str)+'\n')
libc=ctypes.CDLL(None,use_errno=True);rename=libc.renameat2
rename.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint];rename.restype=ctypes.c_int
receipt=[]
with (w/'project3-final-publication.lock').open('a') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 with (d/'rename-receipt.jsonl').open('x') as out:
  for p in v['projects']:
   assert datetime.now(timezone.utc)<datetime.fromisoformat(v['fresh']['lease_deadline'])
   stem=p['name'];stage=root/('.incoming-'+stem+'-20261101');final=root/stem
   if rename(-100,os.fsencode(stage),-100,os.fsencode(final),1)!=0:
    error=ctypes.get_errno();raise OSError(error,os.strerror(error))
   assert final.is_dir() and not stage.exists() and Path(p['source_mat']).is_file()
   row={'project_id':p['project_id'],'from':str(stage),'to':str(final),'published_at':datetime.now(timezone.utc).isoformat(),'original_preserved':True}
   receipt.append(row);out.write(json.dumps(row)+'\n');out.flush();os.fsync(out.fileno())
result={'success':True,'batch_id':v['admission']['batch_id'],'published_at':datetime.now(timezone.utc).isoformat(),'projects':receipt,'native_ownership':ownership,'source_deleted':False,'catalog_changed':False}
(d/'publication.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
'''
    result = remote('linux', code, timeout=100)
    save('project3-final-publication.json', result)
    print(json.dumps(result))


def launch_linux():
    _, fresh = check_admission()
    assert load('project3-final-publication.json')['success']
    files = {n: base64.b64encode((HERE / n).read_bytes()).decode() for n in ['verify_project3_hosts.m', 'run_project3_host_validation.py']}
    code = 'V=' + repr(encoded({'files': files, 'fresh': fresh})) + '\n' + r'''
import base64,json,hashlib,os,subprocess,shlex
from pathlib import Path
from datetime import datetime,timezone
os.umask(0o077);v=json.loads(base64.b64decode(V))
assert (datetime.now(timezone.utc)-datetime.fromisoformat(v['fresh']['checked_at'])).total_seconds()<120
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101');d=w/'project3-host-final-validation'
assert not d.exists()
request=json.loads((w/'project3-host-validation/project3-host-request.json').read_text())
request['project_namespace']='final';request['script_sha256']=hashlib.sha256(base64.b64decode(v['files']['verify_project3_hosts.m'])).hexdigest()
assert len(request['projects'])==11
for p in request['projects']:
 assert (Path('/homes2/maliavko/DetecdivHub/projects')/p['name']/(p['name']+'.mat')).is_file()
d.mkdir(mode=0o700)
for n,b in v['files'].items():(d/n).write_bytes(base64.b64decode(b))
(d/'project3-host-request.json').write_text(json.dumps(request)+'\n')
launch='#!/bin/bash\ncd '+shlex.quote(str(d))+'\npython3 -u run_project3_host_validation.py > controller.log 2>&1\nrc=$?\nprintf "%s\\n" "$rc" > controller.exit\nexit "$rc"\n'
(d/'launch.sh').write_text(launch)
task='alex2023-3-host-final-linux-20261105'
assert subprocess.run(['tmux','has-session','-t',task],capture_output=True).returncode!=0
subprocess.run(['tmux','new-session','-d','-s',task,'bash '+shlex.quote(str(d/'launch.sh'))],check=True)
print(json.dumps({'started':True,'task':task,'script_sha256':request['script_sha256'],'request':request,'private_directory':str(d)}))
'''
    result = remote('linux', code, timeout=45)
    request = result.pop('request')
    save('project3-host-final-request.json', request, private=True)
    save('project3-host-final-linux-launch.json', result)
    print(json.dumps(result))


def status():
    result = remote('linux', r'''
import json
from pathlib import Path
d=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101/project3-host-final-validation')
out={}
for n in ['controller.exit','project3-host-linux-state.json','project3-host-linux-progress.json','project3-host-linux-verification.json','controller.log','project3-host-linux-matlab.log']:
 f=d/n
 if f.exists():
  if n.endswith('.json'):out[n]=json.loads(f.read_text())
  else:
   with f.open('rb') as stream:stream.seek(max(0,f.stat().st_size-4000));out[n]=stream.read().decode(errors='replace')
print(json.dumps(out))
''', timeout=35)
    (RUNTIME / 'project3-host-final-linux-status.json').write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    report = result.pop('project3-host-linux-verification.json', None)
    if report:
        result['verification_summary'] = {'success': report['success'], 'namespace': report['project_namespace'], 'projects': len(report['projects'])}
    print(json.dumps(result))


def watch():
    """Archive terminal host reports and release only this batch's own leases."""
    from project3_windows_host_ops import main as windows
    admission = load('project3-final-admission.json', private=True)
    deadline = min(datetime.fromisoformat(x['expires_at']).timestamp() for x in admission['leases'])
    terminal = None
    while time.time() < deadline:
        try:
            status()
            windows('status', final=True)
            lin = load('project3-host-final-linux-status.json', private=True)
            win = load('project3-host-windows-final-status.json', private=True)
            ls = lin.get('project3-host-linux-state.json', {})
            ws = win.get('project3-host-windows-state.json', {})
            if ls.get('phase') == 'failed_safe_readonly' or ws.get('phase') == 'failed_safe_readonly':
                terminal = {'success': False, 'linux_state': ls, 'windows_state': ws}
                break
            if (lin.get('controller.exit', '').strip() == '0' and ls.get('success')
                    and ws.get('success') and win.get('task', {}).get('state') != 'Running'):
                assert win['task']['result'] == 0
                assert ls['phase'] == ws['phase'] == 'host_reads_verified_in_final_folders'
                assert ws['windows_identity'].casefold() == 'gmgm\\charvin-admin' and ws['session_id'] == 1
                lr = lin['project3-host-linux-verification.json']
                wr = win['project3-host-windows-verification.json']
                assert lr['success'] and wr['success']
                assert lr['project_namespace'] == wr['project_namespace'] == 'final'
                assert lr['projects'] == wr['projects'], 'Host receipts disagree'
                assert len(lr['projects']) == 11
                assert sum(p['fovs'] for p in lr['projects']) == 123
                assert sum(p['rois'] for p in lr['projects']) == 53623
                assert sum(p['data_sidecars_present'] for p in lr['projects']) == 44944
                assert sum(p['processors_verified'] for p in lr['projects']) == 21
                assert sum(p['raw_sample_reads_verified'] for p in lr['projects']) == 1728
                terminal = {'success': True, 'linux_state': ls, 'windows_state': ws,
                            'projects': [{k: v for k, v in p.items() if k != 'samples'} for p in lr['projects']],
                            'linux_report_sha256': hashlib.sha256(json.dumps(lr, sort_keys=True).encode()).hexdigest(),
                            'windows_report_sha256': hashlib.sha256(json.dumps(wr, sort_keys=True).encode()).hexdigest()}
                save('project3-final-hosts-completion-verification.json', terminal)
                break
        except Exception as error:
            print(json.dumps({'watch_error': type(error).__name__+': '+str(error), 'source_deleted': False}), flush=True)
        time.sleep(60)
    if terminal is None:
        terminal = {'success': False, 'reason': 'own_lease_deadline_reached_inspect_host_tasks'}
    code = DB_COMMON + r'''
from api.services.project_locks import release_project_lock
v=json.load(sys.stdin);a=v['admission'];t=v['terminal']
with SessionLocal.begin() as db:
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']),with_for_update=True)
 assert b.status=='final_publication_admitted'
 released=[]
 for row in a['leases']:
  l=db.get(ProjectLock,uuid.UUID(row['id']),with_for_update=True)
  assert str(l.project_id)==row['project_id'] and l.holder_key==a['holder']
  if l.status=='active':release_project_lock(db,lock=l);released.append(str(l.id))
 b.status='final_folders_verified_catalog_cutover_pending' if t['success'] else 'final_host_validation_requires_review'
 meta=dict(b.metadata_json);meta['terminal_final_host_validation']=t;meta['own_leases_released']=released;b.metadata_json=meta
 print(json.dumps({'success':t['success'],'batch_id':str(b.id),'status':b.status,'own_leases_released':released,'checked_at':datetime.now(timezone.utc).isoformat(),'source_deleted':False,'catalog_changed':False}))
'''
    result = remote_json(SSH, 'webserver-labo', code, {'admission': admission, 'terminal': terminal}, container=True)
    save('project3-final-host-lease-finalization.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    {'collect': collect, 'admit': admit, 'publish': publish, 'launch-linux': launch_linux, 'status': status, 'watch': watch}[sys.argv[1]]()
