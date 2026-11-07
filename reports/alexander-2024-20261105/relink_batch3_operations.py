"""Prepare, lease, relink and finalize only five unpublished 2024 copies."""
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote
from audit import remote_json,SSH

HOLDER='alexander-2024-unpublished-relink-batch3-20261107'
PRIVATE=HERE/'runtime';PRIVATE.mkdir(exist_ok=True)

PLAN=r'''
import json,hashlib
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
assert not (root/'relink-batch3-20261107').exists(),'Existing relink work requires review, no replay'
types=root/'relink-value-types-20261107-batch3';assert (types/'controller.exit').read_text().strip()=='0'
typed_raw=(types/'result.json').read_bytes();typed=json.loads(typed_raw);assert typed['success']
assert hashlib.sha256(typed_raw).hexdigest()=='28975f64809372e11c0d6184d5bc3c730ed5f1ebbca0819c07636dc5e0c15bdd'
fig=root/'figure-path-audit-20261107-batch4';assert (fig/'controller.exit').read_text().strip()=='0'
figraw=(fig/'result.json').read_bytes();assert hashlib.sha256(figraw).hexdigest()=='f09d64f69e222fef9cb58f20bffed8cbc6c8e82cfc95fa98b3774002d2746e61'
assert all(p['files_with_unresolved_paths']==0 for p in json.loads(figraw)['projects'])
assert len(typed['files'])==9 and sum(len(f['gpu_values']) for f in typed['files'])==0
assert all(not f['function_handles'] and not f['nonpublic_persistent_properties'] for f in typed['files'])
typed_files={(f['project_id'],f['path']):f for f in typed['files']}
projects=[];recipes=[];audit_hashes=[]
backup=Path('/homes2/maliavko/DetecdivHub/.migration-backups/alexander-2024-20261105/relink-batch3-20261107');assert not backup.exists()
for folder in ['persistent-path-audit-20261107-batch5']:
 w=root/folder;assert (w/'controller.exit').read_text().strip()=='0'
 result=json.loads((w/'result.json').read_text());assert result['success'] and all(p['files_with_unresolved_paths']==0 for p in result['projects'])
 mapping={p['project_id']:p for p in json.loads((w/'request.json').read_text())['projects']}
 projects.extend(mapping.values());raw=(w/'receipt.jsonl').read_bytes();audit_hashes.append(hashlib.sha256(raw).hexdigest())
 for row in (json.loads(line) for line in raw.splitlines()):
  assert not row['unresolved_path_values'] and not row['file_saved']
  if not row['recognized_rebase_values']:continue
  p=mapping[row['project_id']];name=p['name'];rel=row['relative_path'];assert rel.endswith('.mat') and not Path(rel).name.startswith('im_')
  assert typed_files[row['project_id'],rel]['sha256_unchanged']==row['sha256_unchanged']
  copied=json.loads((root/'project-copies'/name/'verified-copy.json').read_text());assert copied['phase']=='copy_verified_identity_relink_and_hosts_pending'
  file=next(f for f in (json.loads(line) for line in (root/'project-copies'/name/'verify-receipt.jsonl').open()) if f['path']==rel)
  assert file['source_destination_hash_equal'] and file['owner']=='maliavko' and file['sha256']==row['sha256_unchanged']
  recipes.append({'project_id':row['project_id'],'stem':name,'relative_path':rel,'target':str(Path(p['stage'])/rel),'backup':str(backup/rel),
    'bytes':file['bytes'],'sha256':file['sha256'],'expected_path_changes':row['recognized_rebase_values']})
assert len(projects)==len({p['project_id'] for p in projects})==5 and len(recipes)==9
assert sum(r['expected_path_changes'] for r in recipes)==268978
for p in projects:
 p['verified_files']=[]
recipes.sort(key=lambda r:(r['relative_path']==r['stem']+'.mat',r['relative_path']))
print(json.dumps({'projects':projects,'files':recipes,'audit_receipt_hashes':audit_hashes,'type_inventory_sha256':hashlib.sha256(typed_raw).hexdigest(),
 'type_inventory_gpu_values':0,'type_inventory_function_handles':0,'type_inventory_nonpublic_properties':0,'source_deletion_allowed':False,'catalog_cutover_allowed':False}))
'''

ADMIT=r'''
import json,sys,uuid,hashlib
from datetime import datetime,timedelta,timezone
from sqlalchemy import select,text,inspect
from api.db import SessionLocal
from api.models import User,Project,Job,ProjectLock,IndexingJob,StorageMigrationBatch
v=json.load(sys.stdin);ids={uuid.UUID(p['project_id']) for p in v['projects']};now=datetime.now(timezone.utc)
def columns(x):return {p.key:getattr(x,p.key) for p in inspect(type(x)).column_attrs}
def digest(x):return hashlib.sha256(json.dumps(x,sort_keys=True,default=str).encode()).hexdigest()
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin')).one()
 assert not db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name=='alexander-2024-unpublished-relink-batch3-20261107'))
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  value=json.dumps(j.params_json or {},default=str)
  assert j.project_id not in ids and not any(str(i) in value for i in ids),'Affected project job active'
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running']))))
 snapshots=[];leases=[];deadlines=[];owner_id=None
 for project_id in sorted(ids,key=str):
  p=db.get(Project,project_id,with_for_update=True);assert p.owner.user_key=='alexander';owner_id=p.owner_user_id
  active=[l for l in db.scalars(select(ProjectLock).where(ProjectLock.project_id==project_id,ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now]
  assert not active,'Affected project already leased'
  snapshots.append({'project_id':str(project_id),'identity_sha256':digest({'key':p.project_key,'owner':str(p.owner_user_id),'visibility':p.visibility,'notes':p.notes,
   'acl':[columns(x) for x in p.acl_entries],'groups':[columns(x) for x in p.group_memberships],'note_entries':[columns(x) for x in p.note_entries],'raw_links':[columns(x) for x in p.raw_links]}),
   'metadata_sha256':digest(p.metadata_json),'locations_sha256':digest([columns(x) for x in p.locations])})
  l=ProjectLock(project_id=project_id,owner_user_id=operator.id,lock_kind='client_edit_lease',lock_scope='project',write_scope='project_relocation',status='active',
   holder_key=v['holder'],holder_host='codex-admin-workstation',reason='Relink verified unpublished copies; preserve originals',
   metadata_json={'source_projects_preserved':True,'preferred_locations_unchanged':True,'evidence':'reports/alexander-2024-20261105/'},expires_at=now+timedelta(hours=6),heartbeat_at=now)
  db.add(l);db.flush();leases.append(str(l.id));deadlines.append(l.expires_at.timestamp())
 b=StorageMigrationBatch(owner_user_id=owner_id,batch_name='alexander-2024-unpublished-relink-batch3-20261107',source_kind='detecdiv_project',
  source_path='/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis',storage_root_name='user-homes2',host_scope='detecdiv-server',root_type='project_root',
  strategy='relink_verified_unpublished_copies_keep_originals',status='relinking_unpublished_copies',
  metadata_json={'holder':v['holder'],'before_projects':snapshots,'lease_ids':leases,'parent_batch_id':'0382c23e-ad54-4ec9-9ad8-53cc79b64d97'},
  summary_json={'projects':5,'files_to_relink':9,'preferred_locations_unchanged':True,'source_deleted':False})
 db.add(b);db.flush()
 print(json.dumps({'admitted_at':now.isoformat(),'batch_id':str(b.id),'holder':v['holder'],'lease_ids':leases,'lease_deadline_unix':min(deadlines),'before_projects':snapshots,'projects':5,'files':9}))
'''


def launch():
    assert not (PRIVATE/'relink-batch3-admission.json').exists(),'Inspect prior admission instead of replay'
    plan=remote('linux',PLAN,timeout=90)
    (PRIVATE/'relink-batch3-plan.json').write_text(json.dumps(plan,indent=2)+'\n')
    admission=remote_json(SSH,'webserver-labo',ADMIT,{'projects':plan['projects'],'holder':HOLDER},container=True)
    (PRIVATE/'relink-batch3-admission.json').write_text(json.dumps(admission,indent=2)+'\n')
    py=(HERE/'prepare_relink_2024_batch3.py').read_bytes();mat=(HERE/'relink_2024_batch3.m').read_bytes()
    payload={'plan':plan,'admission':admission,'python':base64.b64encode(py).decode(),'matlab':base64.b64encode(mat).decode()}
    try:
        result=remote('linux','PAYLOAD='+repr(base64.b64encode(json.dumps(payload).encode()).decode())+'\n'+r'''
import json,base64,hashlib,os,subprocess,shlex,sys
from datetime import datetime,timezone
from pathlib import Path
os.umask(0o077);v=json.loads(base64.b64decode(PAYLOAD));a=v['admission'];p=v['plan']
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/relink-batch3-20261107');assert not w.exists()
assert datetime.now(timezone.utc).timestamp()<a['lease_deadline_unix'] and len(a['lease_ids'])==5
py=base64.b64decode(v['python']);mat=base64.b64decode(v['matlab']);w.mkdir(mode=0o700)
(w/'prepare_relink_2024_batch3.py').write_bytes(py);(w/'relink_2024_batch3.m').write_bytes(mat)
r=dict(p);r.update(lease_ids=a['lease_ids'],lease_deadline_unix=a['lease_deadline_unix'],batch_id=a['batch_id'],holder=a['holder'],runner_sha256=hashlib.sha256(py).hexdigest(),matlab_sha256=hashlib.sha256(mat).hexdigest())
(w/'controller-request.json').write_text(json.dumps(r,indent=2)+'\n');(w/'admission.json').write_text(json.dumps(a,indent=2)+'\n')
task='alex2024-relink-batch3-20261107';assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
args=['nice','-n','19',sys.executable,'-u',str(w/'prepare_relink_2024_batch3.py')]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'batch_id':a['batch_id'],'holder':a['holder'],'lease_deadline_unix':a['lease_deadline_unix'],
 'runner_sha256':r['runner_sha256'],'matlab_sha256':r['matlab_sha256'],'projects':5,'metadata_mats':9,'expected_path_changes':268978,'source_deleted':False,'catalog_cutover_allowed':False}))
''',timeout=90)
        (HERE/'relink-batch3-launch.json').write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps(result,indent=2))
    finally:
        with (PRIVATE/'relink-batch3-watcher.log').open('ab') as output:
            watcher=subprocess.Popen([sys.executable,'-X','utf8',str(Path(__file__).resolve()),'watch'],stdout=output,stderr=subprocess.STDOUT,
                                     creationflags=subprocess.CREATE_NO_WINDOW)
        (HERE/'relink-batch3-watcher-launch.json').write_text(json.dumps({'pid_at_launch':watcher.pid,'hidden':True,'only_own_leases':True,'holder':HOLDER,'batch_id':admission['batch_id']},indent=2)+'\n')


def watch():
    a=json.loads((PRIVATE/'relink-batch3-admission.json').read_text())
    while datetime.now(timezone.utc).timestamp()<a['lease_deadline_unix']+600:
        try:
            s=remote('linux',r'''
import json
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/relink-batch3-20261107')
def read(n):
 p=w/n;return json.loads(p.read_text()) if p.exists() else None
print(json.dumps({'exit':(w/'controller.exit').read_text().strip() if (w/'controller.exit').exists() else None,'state':read('state.json'),'result':read('result.json'),'work_exists':w.exists()}))
''',timeout=45)
            (PRIVATE/'relink-batch3-status.json').write_text(json.dumps(s,indent=2)+'\n')
            if s['exit'] is not None:finish(a,s);return
            if not s['work_exists'] and datetime.now(timezone.utc).timestamp()-datetime.fromisoformat(a['admitted_at']).timestamp()>300:
                finish(a,{'exit':'launch_not_started','state':{'phase':'launch_not_started'},'result':None});return
        except Exception as error:print(type(error).__name__+': '+str(error),flush=True)
        time.sleep(60)
    print('Own lease deadline passed. Inspect terminal state before further mutation.',flush=True)


def finish(admission,status):
    result=remote_json(SSH,'webserver-labo',r'''
import json,sys,uuid
from datetime import datetime,timezone
from api.db import SessionLocal
from api.models import ProjectLock,StorageMigrationBatch
from api.services.project_locks import release_project_lock
v=json.load(sys.stdin);a=v['admission'];s=v['status'];assert s['exit'] is not None
success=s['exit']=='0' and (s['result'] or {}).get('success') and (s['state'] or {}).get('phase')=='copied_paths_relinked_science_verified_raw_and_host_gates_pending'
with SessionLocal.begin() as db:
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']),with_for_update=True);assert b.metadata_json['holder']==a['holder']
 assert b.status=='relinking_unpublished_copies'
 released=[]
 for lease_id in a['lease_ids']:
  l=db.get(ProjectLock,uuid.UUID(lease_id),with_for_update=True);assert l.holder_key==a['holder'] and str(l.project_id) in {p['project_id'] for p in a['before_projects']}
  if l.status=='active':release_project_lock(db,lock=l);released.append(str(l.id))
 metadata=dict(b.metadata_json);metadata.update(relink_terminal_state=s['state'],relink_result=s['result'],own_leases_released=released);b.metadata_json=metadata
 b.status='copied_paths_relinked_pending_raw_and_host_gates' if success else 'failed_safe_unpublished_copies'
 b.summary_json={'projects':5,'files_relinked':len((s['result'] or {}).get('files',[])),'science_save_reopen_verified':bool(success),'preferred_locations_unchanged':True,'source_deleted':False}
 print(json.dumps({'success':bool(success),'batch_id':str(b.id),'status':b.status,'own_leases_released':released,'checked_at':datetime.now(timezone.utc).isoformat(),'source_deleted':False,'preferred_locations_unchanged':True}))
''',{'admission':admission,'status':status},container=True)
    (HERE/'relink-batch3-lease-finalization.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result),flush=True)


if __name__=='__main__':
    assert len(sys.argv)==2 and sys.argv[1] in ['launch','watch']
    launch() if sys.argv[1]=='launch' else watch()
