"""Admit, launch and close leases for relinking unpublished 2023_3 copies."""
import base64
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import sys
import time

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from audit import remote_json,SSH
from activate_windows_mapping import remote
HOLDER='alexander-2023-3-unpublished-relink-20261103'


def launch():
    target=HERE/'runtime/project3-relink-admission.json';assert not target.exists()
    proof=json.loads((HERE/'project3-persistent-paths-verification.json').read_text(encoding='utf-8'))
    assert proof['files_audited']==98611 and len(proof['files_requiring_rebase'])==32
    projects=json.loads((HERE/'2023_3-project-candidate-plan.json').read_text(encoding='utf-8'))['candidate_projects']
    code=r'''
import json,sys,uuid,hashlib
from datetime import datetime,timezone
from sqlalchemy import select,text,inspect
from api.db import SessionLocal
from api.models import User,Project,Job,ProjectLock,RawDataset,IndexingJob,StorageMigrationBatch
from api.services.path_resolution import compose_storage_path
from api.services.project_locks import acquire_client_edit_lease
v=json.load(sys.stdin);ids={uuid.UUID(p['project_id']) for p in v['projects']};prefix='/data/Alexander/data/2023_3'
def columns(x):return {p.key:getattr(x,p.key) for p in inspect(type(x)).column_attrs}
def digest(x):return hashlib.sha256(json.dumps(x,sort_keys=True,default=str).encode()).hexdigest()
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin')).one()
 name='alexander-2023-3-unpublished-project-relink-20261103'
 assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==name)) is None
 raws=[r for r in db.scalars(select(RawDataset)) if any(compose_storage_path(l.storage_root.path_prefix,l.relative_path).startswith(prefix+'/') for l in r.locations)]
 rids={str(r.id) for r in raws};assert len(raws)==24
 markers=rids|{str(x) for x in ids}|{p['name'] for p in v['projects']}
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  data=json.dumps(j.params_json or {},default=str)
  assert j.project_id not in ids and str(j.raw_dataset_id) not in rids and prefix not in data and not any(x in data for x in markers),'Affected job is active'
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running']))))
 snapshots=[];leases=[];deadlines=[];owner_id=None
 for id in sorted(ids,key=str):
  p=db.get(Project,id,with_for_update=True);assert p.owner.user_key=='alexander';owner_id=p.owner_user_id
  snapshots.append({'project_id':str(id),'name':p.project_name,'identity_sha256':digest({'key':p.project_key,'owner':str(p.owner_user_id),'visibility':p.visibility,'notes':p.notes,'acl':[columns(x) for x in p.acl_entries],'groups':[columns(x) for x in p.group_memberships],'notes_entries':[columns(x) for x in p.note_entries],'raw_links':[columns(x) for x in p.raw_links]}),'metadata_sha256':digest(p.metadata_json),'locations_sha256':digest([columns(x) for x in p.locations])})
  lease=acquire_client_edit_lease(db,project_id=id,owner=operator,holder_key=v['holder'],holder_host='codex-admin-workstation',ttl_seconds=14400,write_scope='project_relocation',reason='Relink verified unpublished copies; preserve originals',metadata_json={'source_projects_preserved':True,'preferred_locations_unchanged':True,'evidence':'reports/alexander-2023-remaining-20261101/'})
  leases.append(str(lease.id));deadlines.append(lease.expires_at.timestamp())
 batch=StorageMigrationBatch(owner_user_id=owner_id,batch_name=name,source_kind='detecdiv_project',source_path='/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis',storage_root_name='user-homes2',host_scope='detecdiv-server',root_type='project_root',strategy='relink_verified_unpublished_copies_keep_originals',status='relinking_unpublished_copies',metadata_json={'holder':v['holder'],'before_projects':snapshots,'lease_ids':leases,'project_locations_changed':False,'raw_source_deleted':False,'audit_sha256':v['audit_sha256'],'receipt_sha256':v['receipt_sha256']},summary_json={'projects':11,'files_to_relink':32,'preferred_locations_unchanged':True,'source_deleted':False})
 db.add(batch);db.flush()
 print(json.dumps({'admitted_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(batch.id),'holder':v['holder'],'lease_ids':leases,'lease_deadline_unix':min(deadlines),'before_projects':snapshots,'project_count':11,'files':32,'preferred_locations_unchanged':True,'source_deleted':False}))
'''
    admission=remote_json(SSH,'webserver-labo',code,{'projects':projects,'holder':HOLDER,'audit_sha256':proof['audit_sha256'],'receipt_sha256':proof['receipt_sha256']},container=True)
    target.write_text(json.dumps(admission,indent=2)+'\n',encoding='utf-8')
    py=(HERE/'prepare_and_relink_project3.py').read_bytes();mat=(HERE/'relink_project3_copies.m').read_bytes()
    payload={'admission':admission,'projects':projects,'audit_sha256':proof['audit_sha256'],'receipt_sha256':proof['receipt_sha256'],
             'python':base64.b64encode(py).decode(),'matlab':base64.b64encode(mat).decode()}
    code='V='+repr(base64.b64encode(json.dumps(payload).encode()).decode())+'\n'+r'''
import base64,json,hashlib,subprocess,shlex,os
from pathlib import Path
from datetime import datetime,timezone
os.umask(0o077);v=json.loads(base64.b64decode(V));a=v['admission']
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101');directory=w/'project3-relink'
assert not directory.exists() and datetime.now(timezone.utc).timestamp()<a['lease_deadline_unix']
assert (w/'project3-persistent-paths.exit').read_text().strip()=='0'
assert hashlib.sha256((w/'project3-persistent-paths.json').read_bytes()).hexdigest()==v['audit_sha256']
receipt=w/'project3-persistent-paths-receipt.jsonl';assert hashlib.sha256(receipt.read_bytes()).hexdigest()==v['receipt_sha256']
rows={}
for line in receipt.open():
 r=json.loads(line);assert not r['unresolved_path_values'] and not r['file_saved'];key=(r['project_id'],r['relative_path']);assert key not in rows;rows[key]=r
projects={p['project_id']:p for p in v['projects']};expected=set();hashes={}
for id,p in projects.items():
 folder=w/'projects3'/p['name'];inventory=[json.loads(s) for s in (folder/'source-before.jsonl').read_text().splitlines()]
 for r in inventory:
  if r['kind']=='file' and r['path'].lower().endswith('.mat') and not Path(r['path']).name.startswith('im_'):expected.add((id,r['path']))
 for line in (folder/'verify-receipt.jsonl').open():
  r=json.loads(line)
  if (id,r['path']) in rows and rows[id,r['path']]['recognized_rebase_values']:
   assert r['source_and_destination_match'];hashes[id,r['path']]=r
assert set(rows)==expected and len(rows)==98611
selected=[(key,r) for key,r in rows.items() if r['recognized_rebase_values']];assert len(selected)==32
files=[]
backup=Path('/homes2/maliavko/DetecdivHub/.migration-backups/alexander-2023-remaining-20261101/2023_3-relink-20261103')
for (id,rel),r in selected:
 p=projects[id];stem=p['name'];h=hashes[id,rel];stage=Path('/homes2/maliavko/DetecdivHub/projects')/('.incoming-'+stem+'-20261101')
 files.append({'project_id':id,'stem':stem,'relative_path':rel,'target':str(stage/rel),'backup':str(backup/rel),'bytes':h['bytes'],'sha256':h['sha256'],'expected_path_changes':r['recognized_rebase_values']})
files.sort(key=lambda f:(f['relative_path']==f['stem']+'.mat',f['relative_path']))
directory.mkdir(mode=0o700)
py=base64.b64decode(v['python']);mat=base64.b64decode(v['matlab'])
(directory/'prepare_and_relink_project3.py').write_bytes(py);(directory/'relink_project3_copies.m').write_bytes(mat)
request={'projects':v['projects'],'files':files,'lease_ids':a['lease_ids'],'lease_deadline_unix':a['lease_deadline_unix'],'batch_id':a['batch_id'],'holder':a['holder'],'runner_sha256':hashlib.sha256(py).hexdigest(),'matlab_sha256':hashlib.sha256(mat).hexdigest(),'source_deletion_allowed':False,'catalog_cutover_allowed':False}
(directory/'controller-request.json').write_text(json.dumps(request,indent=2)+'\n')
task='alex2023-3-relink-20261103';assert subprocess.run(['tmux','has-session','-t',task],capture_output=True).returncode!=0
args=['nice','-n','19','/home/charvin-admin/repos/detecdiv-hub-webvm/.venv/bin/python','-u',str(directory/'prepare_and_relink_project3.py')]
shell=shlex.join(args)+' > '+shlex.quote(str(directory/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(directory/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'started':True,'task':task,'batch_id':a['batch_id'],'projects':11,'files_to_relink':32,'lease_deadline_unix':a['lease_deadline_unix'],'runner_sha256':request['runner_sha256'],'matlab_sha256':request['matlab_sha256'],'preferred_locations_unchanged':True,'source_deleted':False}))
'''
    result=remote('linux',code,timeout=60)
    (HERE/'project3-relink-launch.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result))


def watch():
    admission=json.loads((HERE/'runtime/project3-relink-admission.json').read_text(encoding='utf-8'))
    while datetime.now(timezone.utc).timestamp()<admission['lease_deadline_unix']+600:
        try:
            status=remote('linux',r'''
from pathlib import Path
import json
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101/project3-relink');e=w/'controller.exit';s=w/'state.json';r=w/'result.json'
print(json.dumps({'exit':e.read_text().strip() if e.exists() else None,'state':json.loads(s.read_text()) if s.exists() else None,'result':json.loads(r.read_text()) if r.exists() else None}))
''',timeout=40)
            (HERE/'runtime/project3-relink-status.json').write_text(json.dumps(status,indent=2)+'\n',encoding='utf-8')
            if status['exit'] is not None:
                finish(admission,status);return
        except Exception as error:print(type(error).__name__+': '+str(error),flush=True)
        time.sleep(60)
    print('Own lease deadline passed; next heartbeat must inspect terminal state before any cutover.',flush=True)


def finish(admission,status):
    code=r'''
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
 for id in a['lease_ids']:
  l=db.get(ProjectLock,uuid.UUID(id),with_for_update=True);assert l.holder_key==a['holder'] and str(l.project_id) in {p['project_id'] for p in a['before_projects']}
  if l.status=='active':release_project_lock(db,lock=l);released.append(str(l.id))
 m=dict(b.metadata_json);m['relink_terminal_state']=s['state'];m['relink_result']=s['result'];m['own_leases_released']=released;b.metadata_json=m
 b.status='copied_paths_relinked_pending_raw_and_host_gates' if success else 'failed_safe_unpublished_copies'
 b.summary_json={'projects':11,'files_relinked':len((s['result'] or {}).get('files',[])),'science_save_reopen_verified':bool(success),'preferred_locations_unchanged':True,'source_deleted':False}
 print(json.dumps({'success':bool(success),'batch_id':str(b.id),'status':b.status,'own_leases_released':released,'checked_at':datetime.now(timezone.utc).isoformat(),'source_deleted':False,'preferred_locations_unchanged':True}))
'''
    result=remote_json(SSH,'webserver-labo',code,{'admission':admission,'status':status},container=True)
    (HERE/'project3-relink-lease-finalization.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result),flush=True)


if __name__=='__main__':
    assert len(sys.argv)==2 and sys.argv[1] in ['launch','watch']
    launch() if sys.argv[1]=='launch' else watch()
