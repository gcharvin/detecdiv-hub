"""Prepare, lease, relink and finalize only three remaining unpublished main/JSON metadata copies."""
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

HOLDER='alexander-2024-unpublished-relink-gilles-recovery-20261107'
PRIVATE=HERE/'runtime';PRIVATE.mkdir(exist_ok=True)

PLAN=r'''
import json,hashlib
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');old=root/'relink-remaining-mains-20261107'
assert not (root/'relink-gilles-recovery-20261107').exists()
assert (old/'controller.exit').read_text().strip()==(old/'matlab.exit').read_text().strip()=='1'
assert 'Scientific values/types/annotations changed' in (old/'matlab.log').read_text()
assert len(list(old.glob('modified-*.json')))==2 and not list(old.glob('modified-json-*.json'))
r=json.loads((old/'controller-request.json').read_text());r['projects']=[p for p in r['projects'] if p['name']=='281124_Alex'];r['files']=[f for f in r['files'] if f['stem']=='281124_Alex']
assert len(r['projects'])==len(r['files'])==1 and r['files'][0]['expected_path_changes']==8335
assert r['files'][0]['sha256']=='3b3be83b9d83fc87adca63f9fa400278ac64e4db500a9dba2d9d31ac95a3364c'
for f in r['files']+r['json_files']:
 assert hashlib.sha256(Path(f['target']).read_bytes()).hexdigest()==f['sha256'],'Gilles copy already changed; no replay'
 assert hashlib.sha256((Path('/homes2/maliavko/DetecdivHub/.migration-backups/alexander-2024-20261105/relink-remaining-mains-20261107')/f['relative_path']).read_bytes()).hexdigest()==f['sha256']
assert not list(Path(r['projects'][0]['stage']).glob('*.homes2-relink-tmp.mat'))
assert len(r['json_files'])==9 and len(r['json_plan']['files'])==9 and sum(len(f['changes']) for f in r['json_plan']['files'])==20
assert r['type_inventory_gpu_values']==r['type_inventory_function_handles']==r['type_inventory_nonpublic_properties']==0
r.update(recovery_from=str(old),prior_failure_before_gilles_save=True,movie_and_qp_not_replayed=True,existing_backups_reused=True)
print(json.dumps(r))
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
 assert not db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name=='alexander-2024-unpublished-relink-gilles-recovery-20261107'))
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  value=json.dumps(j.params_json or {},default=str)
  assert j.project_id not in ids and not any(str(i) in value for i in ids),'Affected project job active'
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running']))))
 snapshots=[];leases=[];deadlines=[];owner_id=db.scalars(select(User.id).where(User.user_key=='alexander')).one()
 for project_id in sorted(ids,key=str):
  p=db.get(Project,project_id,with_for_update=True);assert p.owner.user_key==next(x['owner_key'] for x in v['projects'] if x['project_id']==str(project_id))
  active=[l for l in db.scalars(select(ProjectLock).where(ProjectLock.project_id==project_id,ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now]
  assert not active,'Affected project already leased'
  snapshots.append({'project_id':str(project_id),'identity_sha256':digest({'key':p.project_key,'owner':str(p.owner_user_id),'visibility':p.visibility,'notes':p.notes,
   'acl':[columns(x) for x in p.acl_entries],'groups':[columns(x) for x in p.group_memberships],'note_entries':[columns(x) for x in p.note_entries],'raw_links':[columns(x) for x in p.raw_links]}),
   'metadata_sha256':digest(p.metadata_json),'locations_sha256':digest([columns(x) for x in p.locations])})
  l=ProjectLock(project_id=project_id,owner_user_id=operator.id,lock_kind='client_edit_lease',lock_scope='project',write_scope='project_relocation',status='active',
   holder_key=v['holder'],holder_host='codex-admin-workstation',reason='Relink verified unpublished copies; preserve originals',
   metadata_json={'source_projects_preserved':True,'preferred_locations_unchanged':True,'evidence':'reports/alexander-2024-20261105/'},expires_at=now+timedelta(hours=6),heartbeat_at=now)
  db.add(l);db.flush();leases.append(str(l.id));deadlines.append(l.expires_at.timestamp())
 b=StorageMigrationBatch(owner_user_id=owner_id,batch_name='alexander-2024-unpublished-relink-gilles-recovery-20261107',source_kind='detecdiv_project',
  source_path='/homes2/maliavko/DetecdivHub/projects',storage_root_name='user-homes2',host_scope='detecdiv-server',root_type='project_root',
  strategy='relink_verified_unpublished_copies_keep_originals',status='relinking_unpublished_copies',
  metadata_json={'holder':v['holder'],'before_projects':snapshots,'lease_ids':leases,'parent_batch_id':'0382c23e-ad54-4ec9-9ad8-53cc79b64d97'},
  summary_json={'projects':1,'files_to_relink':10,'preferred_locations_unchanged':True,'source_deleted':False})
 db.add(b);db.flush()
 print(json.dumps({'admitted_at':now.isoformat(),'batch_id':str(b.id),'holder':v['holder'],'lease_ids':leases,'lease_deadline_unix':min(deadlines),'before_projects':snapshots,'projects':1,'files':10}))
'''


def launch():
    assert not (PRIVATE/'relink-gilles-recovery-admission.json').exists(),'Inspect prior admission instead of replay'
    plan=remote('linux',PLAN,timeout=90)
    (PRIVATE/'relink-gilles-recovery-plan.json').write_text(json.dumps(plan,indent=2)+'\n')
    admission=remote_json(SSH,'webserver-labo',ADMIT,{'projects':plan['projects'],'holder':HOLDER},container=True)
    (PRIVATE/'relink-gilles-recovery-admission.json').write_text(json.dumps(admission,indent=2)+'\n')
    py=(HERE/'prepare_relink_gilles_recovery.py').read_bytes();mat=(HERE/'relink_gilles_recovery.m').read_bytes()
    payload={'plan':plan,'admission':admission,'python':base64.b64encode(py).decode(),'matlab':base64.b64encode(mat).decode()}
    try:
        result=remote('linux','PAYLOAD='+repr(base64.b64encode(json.dumps(payload).encode()).decode())+'\n'+r'''
import json,base64,hashlib,os,subprocess,shlex,sys
from datetime import datetime,timezone
from pathlib import Path
os.umask(0o077);v=json.loads(base64.b64decode(PAYLOAD));a=v['admission'];p=v['plan']
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/relink-gilles-recovery-20261107');assert not w.exists()
assert datetime.now(timezone.utc).timestamp()<a['lease_deadline_unix'] and len(a['lease_ids'])==1
py=base64.b64decode(v['python']);mat=base64.b64decode(v['matlab']);w.mkdir(mode=0o700)
(w/'prepare_relink_gilles_recovery.py').write_bytes(py);(w/'relink_gilles_recovery.m').write_bytes(mat)
r=dict(p);r.update(lease_ids=a['lease_ids'],lease_deadline_unix=a['lease_deadline_unix'],batch_id=a['batch_id'],holder=a['holder'],runner_sha256=hashlib.sha256(py).hexdigest(),matlab_sha256=hashlib.sha256(mat).hexdigest())
(w/'controller-request.json').write_text(json.dumps(r,indent=2)+'\n');(w/'admission.json').write_text(json.dumps(a,indent=2)+'\n')
task='alex2024-relink-gilles-recovery-20261107';assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
args=['nice','-n','19',sys.executable,'-u',str(w/'prepare_relink_gilles_recovery.py')]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'batch_id':a['batch_id'],'holder':a['holder'],'lease_deadline_unix':a['lease_deadline_unix'],
 'runner_sha256':r['runner_sha256'],'matlab_sha256':r['matlab_sha256'],'projects':1,'metadata_mats':1,'metadata_json':9,'expected_path_changes':8355,'source_deleted':False,'catalog_cutover_allowed':False}))
''',timeout=90)
        (HERE/'relink-gilles-recovery-launch.json').write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps(result,indent=2))
    finally:
        with (PRIVATE/'relink-gilles-recovery-watcher.log').open('ab') as output:
            watcher=subprocess.Popen([sys.executable,'-X','utf8',str(Path(__file__).resolve()),'watch'],stdout=output,stderr=subprocess.STDOUT,
                                     creationflags=subprocess.CREATE_NO_WINDOW)
        (HERE/'relink-gilles-recovery-watcher-launch.json').write_text(json.dumps({'pid_at_launch':watcher.pid,'hidden':True,'only_own_leases':True,'holder':HOLDER,'batch_id':admission['batch_id']},indent=2)+'\n')


def watch():
    a=json.loads((PRIVATE/'relink-gilles-recovery-admission.json').read_text())
    while datetime.now(timezone.utc).timestamp()<a['lease_deadline_unix']+600:
        try:
            s=remote('linux',r'''
import json
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/relink-gilles-recovery-20261107')
def read(n):
 p=w/n;return json.loads(p.read_text()) if p.exists() else None
print(json.dumps({'exit':(w/'controller.exit').read_text().strip() if (w/'controller.exit').exists() else None,'state':read('state.json'),'result':read('result.json'),'work_exists':w.exists()}))
''',timeout=45)
            (PRIVATE/'relink-gilles-recovery-status.json').write_text(json.dumps(s,indent=2)+'\n')
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
 b.summary_json={'projects':1,'files_relinked':len((s['result'] or {}).get('files',[])),'science_save_reopen_verified':bool(success),'preferred_locations_unchanged':True,'source_deleted':False}
 print(json.dumps({'success':bool(success),'batch_id':str(b.id),'status':b.status,'own_leases_released':released,'checked_at':datetime.now(timezone.utc).isoformat(),'source_deleted':False,'preferred_locations_unchanged':True}))
''',{'admission':admission,'status':status},container=True)
    (HERE/'relink-gilles-recovery-lease-finalization.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result),flush=True)


if __name__=='__main__':
    assert len(sys.argv)==2 and sys.argv[1] in ['launch','watch']
    launch() if sys.argv[1]=='launch' else watch()
