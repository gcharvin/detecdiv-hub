"""Publish verified 2023_2 copies and prefer them on the same 21 raw records.

The original raws, original locations, Sauvegarde MAT and all relationships
are retained. Project relocation and source reclamation are separate phases.
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from audit import remote_json, SSH

WORK = '/home/charvin-admin/storage-transfers/alexander-2023-cutover-20261101'
OLD = '/data/Alexander/data/2023_2'
STAGE = '/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_2'
FINAL = '/homes2/maliavko/DetecdivHub/raw/2023_2'

def save(name,data):
    (HERE/name).write_text(json.dumps(data,indent=2,default=str)+'\n',encoding='utf-8')
    return data

COMMON = r'''
import json,hashlib,uuid,sys
from datetime import datetime,timezone
from sqlalchemy import select,text,inspect
from api.db import SessionLocal
from api.models import RawDataset,RawDatasetLocation,Project,ProjectLock,Job,StorageRoot,StorageMigrationBatch,StorageMigrationItem,User,IndexingJob
from api.services.path_resolution import compose_storage_path
OLD='/data/Alexander/data/2023_2'
FINAL='/homes2/maliavko/DetecdivHub/raw/2023_2'
def digest(data):return hashlib.sha256(json.dumps(data,sort_keys=True,default=str,separators=(',',':')).encode()).hexdigest()
def scalar_columns(obj):return {a.key:getattr(obj,a.key) for a in inspect(type(obj)).column_attrs}
def location(l):
 return {'id':l.id,'root_id':l.storage_root_id,'root_name':l.storage_root.name,'root_prefix':l.storage_root.path_prefix,'host_scope':l.storage_root.host_scope,'relative_path':l.relative_path,'is_preferred':l.is_preferred,'access_mode':l.access_mode}
def snapshot(raw):
 return {'columns':scalar_columns(raw),'locations':sorted([location(l) for l in raw.locations],key=lambda x:x['id']),
         'project_links':sorted([scalar_columns(x) for x in raw.project_links],key=lambda x:x['id']),
         'experiment_links':sorted([scalar_columns(x) for x in raw.experiment_links],key=lambda x:x['id']),
         'positions':sorted([scalar_columns(x) for x in raw.positions],key=lambda x:str(x['id']))}
def source_paths(raw):
 return sorted({compose_storage_path(l.storage_root.path_prefix,l.relative_path) for l in raw.locations if compose_storage_path(l.storage_root.path_prefix,l.relative_path).startswith(OLD+'/')})
def safety(db,raws):
 ids={str(r.id) for r in raws}; pids={str(l.project_id) for r in raws for l in r.project_links}
 projects=list(db.scalars(select(Project).where(Project.id.in_([uuid.UUID(i) for i in pids]))))
 markers=ids|pids|{p.project_key for p in projects}
 blocked=[]
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  payload=json.dumps(j.params_json or {},sort_keys=True,default=str)
  if str(j.raw_dataset_id) in ids or str(j.project_id) in pids or any(m in payload for m in markers) or OLD in payload or FINAL in payload or '/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_2' in payload:
   blocked.append(str(j.id))
 assert not blocked, 'Affected jobs exist: '+','.join(blocked)
 now=datetime.now(timezone.utc)
 locks=[str(l.id) for l in db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_([uuid.UUID(i) for i in pids]),ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now]
 assert not locks, 'Affected project write leases exist'
 indexing=list(db.scalars(select(IndexingJob).where(IndexingJob.status.in_(['queued','running']))))
 assert not indexing, 'Indexing jobs are active; avoid concurrent catalog ingestion'
 return {'active_affected_jobs':0,'active_affected_locks':0,'active_indexing_jobs':0,'project_ids':sorted(pids)}
def other_locations_digest(db,ids):
 rows=[{'id':l.id,'raw_id':str(l.raw_dataset_id),'root_id':l.storage_root_id,'relative_path':l.relative_path,'preferred':l.is_preferred,'access':l.access_mode} for l in db.scalars(select(RawDatasetLocation).where(RawDatasetLocation.raw_dataset_id.notin_([uuid.UUID(i) for i in ids])).order_by(RawDatasetLocation.id))]
 return {'count':len(rows),'sha256':digest(rows)}
'''

def collect():
    data=remote_json(SSH,'detecdiv-server',r'''
import json
from pathlib import Path
r=Path('/home/charvin-admin/storage-transfers/alexander-2023-cutover-20261101')
state=json.loads((r/'state.json').read_text())
assert state['phase']=='metadata_and_prior_checksum_gates_passed'
assert state['source_inventory']['files']==state['destination_inventory']['files']==425112
assert state['source_inventory']['bytes']==state['destination_inventory']['bytes']==3583194799325
assert state['source_regular_files_unchanged'] and state['destination_paths_and_sizes_match']
assert state['destination_mtime_differences']['over_1000ns_count']==0
assert state['previous_full_content_verification_passed']
project=json.loads((r/'project-readonly-inspection.json').read_text())
assert (r/'project-inspection.exit').read_text().strip()=='0'
assert project['success'] and project['read_only'] and not project['project_saved']
assert project['fov_count']==12 and all(x['sourceExists'] and x['stagingExists'] for x in project['raw_path_checks'])
print(json.dumps({'inventory':state,'project':project}))
''',{})
    save('verification-gates.json',data)
    print(json.dumps({'phase':data['inventory']['phase'],'files':425112,'bytes':3583194799325,'project_reopened':True,'fovs':12,'rois':data['project']['roi_count']}))

def plan():
    gates=json.loads((HERE/'verification-gates.json').read_text())
    code=COMMON+r'''
payload=json.load(sys.stdin)
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin',User.is_active.is_(True))).one()
 root=db.scalars(select(StorageRoot).where(StorageRoot.name=='user-homes2')).one()
 assert root.path_prefix=='/homes2' and root.host_scope=='detecdiv-server'
 raws=[r for r in db.scalars(select(RawDataset)) if source_paths(r)]
 assert len(raws)==21 and all(r.owner.user_key=='alexander' for r in raws)
 guards=safety(db,raws)
 rows=[]
 for r in sorted(raws,key=lambda r:str(r.id)):
  paths=source_paths(r);assert len(paths)==1
  assert sum(l.is_preferred for l in r.locations)==1
  assert not any(l.storage_root.path_prefix.startswith('/homes2') for l in r.locations)
  suffix=paths[0][len(OLD)+1:]
  assert suffix and '..' not in suffix.split('/')
  state=snapshot(r)
  rows.append({'id':str(r.id),'label':r.acquisition_label,'source_path':paths[0],
               'relative_path':'maliavko/DetecdivHub/raw/2023_2/'+suffix,
               'new_path':FINAL+'/'+suffix,'before':state,'before_sha256':digest(state)})
 projects=[{'id':str(p.id),'snapshot':{'columns':scalar_columns(p),'locations':sorted([scalar_columns(l) for l in p.locations],key=lambda x:x['id'])}} for p in db.scalars(select(Project).where(Project.id.in_([uuid.UUID(i) for i in guards['project_ids']])))]
 original=db.scalars(select(StorageMigrationBatch).where(StorageMigrationBatch.batch_name=='alexander-2023-20261008')).one()
 assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name=='alexander-2023-2-cutover-20261101')) is None
 result={'schema_version':1,'created_at':datetime.now(timezone.utc).isoformat(),'operator_user_id':str(operator.id),'owner_user_id':str(raws[0].owner_user_id),
         'batch_id':str(uuid.uuid4()),'batch_name':'alexander-2023-2-cutover-20261101','original_batch_id':str(original.id),
         'root_id':root.id,'root_name':root.name,'guards':guards,'raws':rows,'projects':projects,
         'other_locations':other_locations_digest(db,[r['id'] for r in rows]),'verification_gates':payload,
         'strategy':'prefer_verified_copy_keep_sources_and_original_project','source_deletion_allowed':False,
         'project_saved':False,'project_compatibility':'Original primary raw paths and Sauvegarde project retained.'}
 result['plan_sha256']=digest(result)
 print(json.dumps(result,default=str))
'''
    result=remote_json(SSH,'webserver-labo',code,gates,container=True)
    save('cutover-plan.json',result)
    stage=r'''
import json,sys,os
from pathlib import Path
os.umask(0o077);plan=json.load(sys.stdin)
path=Path('/home/charvin-admin/storage-transfers/alexander-2023-cutover-20261101/cutover-plan.json')
with path.open('x') as f:json.dump(plan,f,indent=2);f.write('\n')
print(json.dumps({'plan_staged':True,'raws':len(plan['raws']),'plan_sha256':plan['plan_sha256']}))
'''
    print(json.dumps(remote_json(SSH,'detecdiv-server',stage,result)))

def publish():
    plan=json.loads((HERE/'cutover-plan.json').read_text())
    code=r'''
import json,sys,os,fcntl,hashlib,subprocess,ctypes
from pathlib import Path
from datetime import datetime,timezone
plan=json.load(sys.stdin);claimed=plan['plan_sha256'];unsigned={k:v for k,v in plan.items() if k!='plan_sha256'}
assert hashlib.sha256(json.dumps(unsigned,sort_keys=True,default=str,separators=(',',':')).encode()).hexdigest()==claimed
work=Path('/home/charvin-admin/storage-transfers/alexander-2023-cutover-20261101')
assert json.loads((work/'cutover-plan.json').read_text())==plan
state=json.loads((work/'state.json').read_text());assert state['phase']=='metadata_and_prior_checksum_gates_passed'
assert (datetime.now(timezone.utc)-datetime.fromisoformat(state['finished_at'])).total_seconds()<3600
stage=Path('/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_2')
final=Path('/homes2/maliavko/DetecdivHub/raw/2023_2')
original=Path('/data/Alexander/data/2023_2')
assert stage.is_dir() and not stage.is_symlink() and not final.exists()
mount=json.loads(subprocess.check_output(['findmnt','-J','-T',str(stage),'-o','SOURCE,TARGET,OPTIONS'],text=True))['filesystems'][0]
assert mount['target']=='/homes2/maliavko' and mount['source']=='//10.20.8.250/home' and 'username=maliavko' in mount['options'].split(',') and 'rw' in mount['options'].split(',')
for r in plan['raws']:
 suffix=r['source_path'].split('/data/Alexander/data/2023_2/',1)[1]
 assert (stage/suffix).is_dir() and Path(r['source_path']).is_dir()
assert not (work/'publication.json').exists()
with Path('/home/charvin-admin/storage-transfers/alexander-2023-20261008/transfer.lock').open('a') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 libc=ctypes.CDLL(None,use_errno=True)
 rename=libc.renameat2
 rename.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint]
 rename.restype=ctypes.c_int
 if rename(-100,os.fsencode(stage),-100,os.fsencode(final),1)!=0:
  error=ctypes.get_errno();raise OSError(error,os.strerror(error))
 assert final.is_dir() and not stage.exists() and original.is_dir()
 for r in plan['raws']:assert Path(r['new_path']).is_dir() and Path(r['source_path']).is_dir()
 result={'published_at':datetime.now(timezone.utc).isoformat(),'plan_sha256':claimed,'stage':str(stage),'final':str(final),'source_preserved':True,'database_cutover_performed':False,'raw_directories':len(plan['raws'])}
 with (work/'publication.json').open('x') as f:json.dump(result,f,indent=2);f.write('\n')
 print(json.dumps(result))
'''
    result=remote_json(SSH,'detecdiv-server',code,plan)
    save('publication.json',result)
    print(json.dumps(result))

def apply():
    plan=json.loads((HERE/'cutover-plan.json').read_text())
    publication=json.loads((HERE/'publication.json').read_text())
    windows=json.loads((HERE/'windows-final-path-verification.json').read_text())
    assert windows['success'] and len(windows['checks'])==21
    assert publication['plan_sha256']==plan['plan_sha256']
    code=COMMON+r'''
payload=json.load(sys.stdin);plan=payload['plan'];claimed=plan['plan_sha256']
assert digest({k:v for k,v in plan.items() if k!='plan_sha256'})==claimed
assert payload['publication']['plan_sha256']==claimed and payload['windows']['success']
assert not plan['source_deletion_allowed'] and not plan['project_saved']
assert (datetime.now(timezone.utc)-datetime.fromisoformat(plan['created_at'])).total_seconds()<3600
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 db.execute(text("SELECT pg_advisory_xact_lock(hashtext('alexander-2023-2-cutover-20261101'))"))
 root=db.scalars(select(StorageRoot).where(StorageRoot.id==plan['root_id']).with_for_update()).one()
 assert root.name=='user-homes2' and root.path_prefix=='/homes2' and root.host_scope=='detecdiv-server'
 ids=[uuid.UUID(r['id']) for r in plan['raws']]
 raws=list(db.scalars(select(RawDataset).where(RawDataset.id.in_(ids)).order_by(RawDataset.id).with_for_update()))
 assert len(raws)==21
 projects=list(db.scalars(select(Project).where(Project.id.in_([uuid.UUID(i) for i in plan['guards']['project_ids']])).order_by(Project.id).with_for_update()))
 guards=safety(db,raws)
 assert guards==plan['guards']
 expected={r['id']:r for r in plan['raws']}
 for r in raws:
  assert r.owner.user_key=='alexander' and digest(snapshot(r))==expected[str(r.id)]['before_sha256'], 'Catalog record changed since plan: '+str(r.id)
 for p in projects:
  before=next(x['snapshot'] for x in plan['projects'] if x['id']==str(p.id))
  assert digest({'columns':scalar_columns(p),'locations':sorted([scalar_columns(l) for l in p.locations],key=lambda x:x['id'])})==digest(before)
 assert other_locations_digest(db,[str(i) for i in ids])==plan['other_locations']
 assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==plan['batch_name'])) is None
 batch=StorageMigrationBatch(id=uuid.UUID(plan['batch_id']),owner_user_id=uuid.UUID(plan['owner_user_id']),batch_name=plan['batch_name'],
     source_kind='raw_root',source_path=OLD,storage_root_name='user-homes2',host_scope='detecdiv-server',root_type='raw_root',
     strategy='catalog_cutover_keep_sources',status='catalog_cutover_complete_sources_retained',
     metadata_json={'operator':'gilles','approved_in_chat':'Commit, push and continue the previously approved Alexander migration.',
       'plan_sha256':claimed,'original_batch_id':plan['original_batch_id'],'source_deletion_allowed':False,
       'project_saved':False,'compatibility_strategy':plan['project_compatibility'],'verification_receipts':'reports/alexander-2023-cutover-20261101/',
       'previous_full_sha1_receipt':'2023_2-verify-resume-20261015.exit=0; discrepancy log empty'},
     summary_json={'raw_records':21,'payload_files':425112,'payload_bytes':3583194799325,'source_deleted':False,'original_locations_retained':True,'project_relocation_pending':True})
 db.add(batch);db.flush()
 changes=[]
 for r in raws:
  row=expected[str(r.id)]
  preferred=[l.id for l in r.locations if l.is_preferred]
  original_ids=[l.id for l in r.locations]
  for l in r.locations:l.is_preferred=False
  new=RawDatasetLocation(raw_dataset_id=r.id,storage_root_id=root.id,relative_path=row['relative_path'],is_preferred=True,access_mode='read')
  db.add(new);db.flush()
  change={'raw_id':str(r.id),'old_preferred_location_ids':preferred,'original_location_ids':original_ids,'new_location_id':new.id,'new_path':row['new_path']}
  changes.append(change)
  db.add(StorageMigrationItem(batch_id=batch.id,item_type='raw_dataset',legacy_path=row['source_path'],legacy_key=r.external_key,
      display_name=r.acquisition_label,status='catalog_cutover_complete_sources_retained',action='prefer_verified_copy',metadata_json=change))
 original=db.get(StorageMigrationBatch,uuid.UUID(plan['original_batch_id']),with_for_update=True)
 original.status='in_progress'
 metadata=dict(original.metadata_json or {});metadata['catalog_cutover_2023_2_batch_id']=str(batch.id);original.metadata_json=metadata
 summary=dict(original.summary_json or {});summary['2023_2_catalog_cutover_raw_records']=21;summary['sources_retained']=True;original.summary_json=summary
 item=next(i for i in original.items if i.display_name=='2023_2')
 item.status='catalog_cutover_complete_sources_retained'
 details=dict(item.metadata_json or {});details.update({'cutover_batch_id':str(batch.id),'preferred_destination':FINAL,'original_location_retained':True,'source_deleted':False});item.metadata_json=details
 db.flush()
 result={'committed_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(batch.id),'plan_sha256':claimed,'raws_updated':len(changes),'source_deleted':False,'original_locations_retained':True,'project_saved':False,'guards':guards,'changes':changes}
 print(json.dumps(result))
'''
    result=remote_json(SSH,'webserver-labo',code,{'plan':plan,'publication':publication,'windows':windows},container=True)
    save('catalog-cutover-result.json',result)
    print(json.dumps({'batch_id':result['batch_id'],'raws_updated':result['raws_updated'],'source_deleted':False}))

def verify():
    plan=json.loads((HERE/'cutover-plan.json').read_text())
    applied=json.loads((HERE/'catalog-cutover-result.json').read_text())
    code=COMMON+r'''
from api.services.raw_dataset_lifecycle import pick_preferred_raw_location,resolve_raw_location_path
from api.routes_raw_datasets import raw_dataset_detail_view
payload=json.load(sys.stdin);plan=payload['plan'];applied=payload['applied']
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 rows=[];position_count=0
 for expected in plan['raws']:
  raw=db.get(RawDataset,uuid.UUID(expected['id']));assert raw is not None
  now=snapshot(raw);before=expected['before']
  for key in ('columns','project_links','experiment_links','positions'):
   assert digest(now[key])==digest(before[key]), key+' changed for '+expected['id']
  change=next(c for c in applied['changes'] if c['raw_id']==expected['id'])
  assert len(now['locations'])==len(before['locations'])+1
  for original in before['locations']:
   current=next(l for l in now['locations'] if l['id']==original['id'])
   assert {k:v for k,v in current.items() if k!='is_preferred'}=={k:v for k,v in original.items() if k!='is_preferred'}
   assert not current['is_preferred']
  preferred=pick_preferred_raw_location(raw)
  assert sum(l.is_preferred for l in raw.locations)==1
  assert preferred.id==change['new_location_id'] and preferred.access_mode=='read'
  assert str(resolve_raw_location_path(preferred))==expected['new_path']
  detail=raw_dataset_detail_view(raw)
  api_location=next(l for l in detail.locations if l.is_preferred)
  assert api_location.absolute_path==expected['new_path']
  position_count+=len(raw.positions)
  rows.append({'raw_id':str(raw.id),'new_location_id':preferred.id,'preferred_path':str(resolve_raw_location_path(preferred)),
               'original_location_ids_retained':change['original_location_ids'],'api_detail_valid':True})
 for expected in plan['projects']:
  p=db.get(Project,uuid.UUID(expected['id']))
  assert digest({'columns':scalar_columns(p),'locations':sorted([scalar_columns(l) for l in p.locations],key=lambda x:x['id'])})==digest(expected['snapshot'])
 other=other_locations_digest(db,[r['id'] for r in plan['raws']]);assert other==plan['other_locations']
 batch=db.get(StorageMigrationBatch,uuid.UUID(applied['batch_id']))
 assert batch.status=='catalog_cutover_complete_sources_retained' and len(batch.items)==21
 assert batch.metadata_json['source_deletion_allowed'] is False
 print(json.dumps({'verified_at':datetime.now(timezone.utc).isoformat(),'success':True,'raws_verified':len(rows),
   'raw_identity_ownership_metadata_and_links_preserved':True,'positions_preserved':position_count,
   'original_projects_unchanged':len(plan['projects']),'other_locations_unchanged':other,'api_details_valid':len(rows),
   'batch_id':str(batch.id),'source_deleted':False,'rows':rows}))
'''
    result=remote_json(SSH,'webserver-labo',code,{'plan':plan,'applied':applied},container=True)
    save('catalog-cutover-verification.json',result)
    rollback={'batch_id':applied['batch_id'],'plan_sha256':plan['plan_sha256'],'changes':applied['changes'],
              'strategy':'Restore old preferred flags transactionally after checking active jobs; retain the new copy and locations.'}
    save('rollback-location-references.json',rollback)
    print(json.dumps({k:v for k,v in result.items() if k!='rows'}))

if __name__=='__main__':
    {'collect':collect,'plan':plan,'publish':publish,'apply':apply,'verify':verify}[sys.argv[1]]()
