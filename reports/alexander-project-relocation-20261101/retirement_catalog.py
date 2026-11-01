"""Prepare/finalize only the user-approved retirement of verified raw duplicates."""
import base64
from datetime import datetime,timezone
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from catalog_operations import COMMON, WORK, save, remote, remote_json, SSH

APPROVAL='Oui, retirer ces doublons vérifiés de /data'


def prepare():
    project=json.loads((HERE/'project-catalog-cutover.json').read_text());assert project['success']
    windows=json.loads((HERE/'windows-status.json').read_text())['project-session-verification.json'];assert windows['success']
    state=remote('linux',f'''
import json
from pathlib import Path
print(json.dumps(json.loads(Path({WORK!r}+'/raw-retirement/state.json').read_text())))
''')
    assert state['phase']=='metadata_and_prior_checksum_gates_passed'
    save('raw-retirement-preflight.json',state)
    payload={'project':project,'inventory':state,'approval':APPROVAL}
    code=COMMON+r'''
from api.models import Artifact,ProjectLocation
from api.services.project_locks import acquire_client_edit_lease
payload=json.load(sys.stdin);approval=payload['approval'];project=payload['project']
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin')).one()
 p=db.get(Project,uuid.UUID(project['project_id']),with_for_update=True)
 raws=[r for r in db.scalars(select(RawDataset)) if source_paths(r)]
 assert len(raws)==21 and all(r.owner.user_key=='alexander' for r in raws)
 guards=safety(db,raws)
 assert next(l.id for l in p.locations if l.is_preferred)==project['new_location_id']
 assert p.metadata_json['project_mat_abs']==project['preferred_mat_path']
 assert any(l.id==2765 and not l.is_preferred and l.access_mode=='readonly' for l in p.locations)
 assert not list(db.scalars(select(Artifact.id).where(Artifact.uri.startswith(OLD+'/'))))
 assert not list(db.scalars(select(RawDataset.id).where(RawDataset.display_settings_uri.startswith(OLD+'/'))))
 assert not list(db.scalars(select(RawDataset.id).where(RawDataset.archive_uri.startswith(OLD+'/'))))
 rows=[]
 for raw in sorted(raws,key=lambda x:str(x.id)):
  original=[l for l in raw.locations if compose_storage_path(l.storage_root.path_prefix,l.relative_path).startswith(OLD+'/')]
  preferred=[l for l in raw.locations if l.is_preferred];assert len(original)==len(preferred)==1
  dest=compose_storage_path(preferred[0].storage_root.path_prefix,preferred[0].relative_path)
  assert dest.startswith(FINAL+'/') and preferred[0].id!=original[0].id and not original[0].is_preferred
  rows.append({'raw_id':str(raw.id),'source_location':location(original[0]),'preferred_location':location(preferred[0]),
   'source_path':source_paths(raw)[0],'destination_path':dest,'positions_digest':digest([scalar_columns(x) for x in raw.positions]),
   'links_digest':digest([scalar_columns(x) for x in raw.project_links]),'owner_user_id':str(raw.owner_user_id)})
 name='alexander-2023-2-source-retirement-20261101'
 assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==name)) is None
 lease=acquire_client_edit_lease(db,project_id=p.id,owner=operator,holder_key=name,holder_host='detecdiv-server',
  ttl_seconds=3600,write_scope='storage_relocation',reason='Verified source retirement',metadata_json={'approval':approval,'original_project_preserved':True})
 batch=StorageMigrationBatch(owner_user_id=raws[0].owner_user_id,batch_name=name,source_kind='raw_root',source_path=OLD,
  storage_root_name='user-homes2',host_scope='detecdiv-server',root_type='raw_root',strategy='retire_verified_duplicate_manifest',
  status='source_retirement_prepared',metadata_json={'operator':'gilles','approval_in_chat':approval,'inventory':payload['inventory'],
   'project_cutover':project,'project_lock_id':str(lease.id),'source_locations_before_retirement':rows,'guards':guards,
   'other_locations':other_locations_digest(db,[r['raw_id'] for r in rows])},
  summary_json={'raw_records':21,'manifest_files':425112,'manifest_bytes':3583194799325,'source_deleted':False,'project_original_preserved':True,'api_cache_bytes_retained':5874728850})
 db.add(batch);db.flush()
 for row in rows:
  db.add(StorageMigrationItem(batch_id=batch.id,item_type='raw_dataset',legacy_path=row['source_path'],display_name=row['source_path'].split('/')[-1],
   status='source_retirement_prepared',action='unlink_verified_source_manifest',metadata_json=row))
 print(json.dumps({'checked_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(batch.id),'lease_id':str(lease.id),
  'approval':approval,'project_catalog_switched':True,'linux_windows_project_validated':True,'project_original_preserved':True,
  'api_dependencies_resolved':True,'source_locations':rows,'guards':guards}))
'''
    result=remote_json(SSH,'webserver-labo',code,payload,container=True)
    save('retirement-execution-gates.json',result)
    # Gate and script are staged together only after the catalog transaction commits.
    files={'retirement-execution-gates.json':(HERE/'retirement-execution-gates.json').read_bytes(),
           'retire_verified_raws.py':(HERE/'retire_verified_raws.py').read_bytes()}
    encoded={n:base64.b64encode(b).decode() for n,b in files.items()}
    launch=remote('linux',f'''
from pathlib import Path
import json,base64,subprocess,shlex
w=Path({WORK!r});files={encoded!r}
for n,b in files.items():
 p=w/n;assert not p.exists();p.write_bytes(base64.b64decode(b))
cmd=shlex.join(['/usr/bin/python3',str(w/'retire_verified_raws.py')])+' > '+shlex.quote(str(w/'retirement-controller.log'))+' 2>&1'
subprocess.run(['tmux','new-session','-d','-s','alex-approved-raw-retirement-20261101',cmd],check=True)
print(json.dumps({{'started':True,'batch_id':{result['batch_id']!r},'source_root':'/data/Alexander/data/2023_2'}}))
''')
    save('raw-retirement-launch.json',launch);print(json.dumps(launch))


def status():
    result=remote('linux',f'''
from pathlib import Path
import json
w=Path({WORK!r});raw=w/'raw-retirement'
r={{n:(p.read_text(errors='replace') if n.endswith('.json') else p.read_text(errors='replace')[-6000:]) for n in ['retirement-state.json','unlink.errors.log'] if (p:=raw/n).exists()}}
p=w/'retirement-controller.log';r['controller_log']=p.read_text(errors='replace')[-4000:] if p.exists() else None
print(json.dumps(r))
''')
    if result.get('retirement-state.json'):
        save('raw-retirement-result.json',json.loads(result['retirement-state.json']))
    display=dict(result)
    if display.get('retirement-state.json'):
        value=json.loads(display['retirement-state.json'])
        for field in ['native_preflight','result']:
            if field in value:
                item=dict(value[field])
                for name in ['excluded_system_directories','retained_nonempty_directories']:
                    if name in item:item[name+'_count']=len(item.pop(name))
                value[field]=item
        display['retirement-state.json']=value
    print(json.dumps(display,indent=2))


def finalize():
    gate=json.loads((HERE/'retirement-execution-gates.json').read_text())
    result=json.loads((HERE/'raw-retirement-result.json').read_text())
    assert result['phase']=='source_manifest_retired_catalog_finalization_pending'
    assert result['files_unlinked']==425112 and result['bytes_unlinked']==3583194799325
    receipt_check=remote('linux',f'''
import json,hashlib
from pathlib import Path
w=Path({WORK!r})/'raw-retirement';state=json.loads((w/'retirement-state.json').read_text())
assert state['phase']=='source_manifest_retired_catalog_finalization_pending'
expected={{r['path']:r['bytes'] for line in (w/'source-current.jsonl').read_text().splitlines() if (r:=json.loads(line))['kind']=='file'}}
seen=set();size=0;complete=None;h=hashlib.sha256()
with (w/'unlink-receipt.jsonl').open('rb') as stream:
 for line in stream:
  h.update(line);r=json.loads(line)
  if r['event']=='unlinked':
   assert r['path'] not in seen and expected[r['path']]==r['bytes'];seen.add(r['path']);size+=r['bytes']
  elif r['event']=='complete':complete=r
assert seen==set(expected) and len(seen)==425112 and size==3583194799325
assert h.hexdigest()==state['receipt_sha256'] and complete==state['result'] and complete['remaining_source_payload_files']==0
print(json.dumps({{'success':True,'receipt_sha256':h.hexdigest(),'files':len(seen),'bytes':size,'exact_manifest_paths_verified':True,'remaining_source_payload_files':0}}))
''',timeout=60)
    save('unlink-receipt-verification.json',receipt_check)
    code=COMMON+r'''
from api.models import Artifact
from api.services.project_locks import release_project_lock
from api.routes_raw_datasets import raw_dataset_detail_view,get_raw_preview_artifact_content,get_raw_dataset_display_settings
from api.routes_projects import project_detail_view
payload=json.load(sys.stdin);gate=payload['gate'];result=payload['result']
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 batch=db.get(StorageMigrationBatch,uuid.UUID(gate['batch_id']),with_for_update=True)
 assert batch.status=='source_retirement_prepared'
 operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin')).one()
 changed=[];positions=0
 for expected in gate['source_locations']:
  r=db.get(RawDataset,uuid.UUID(expected['raw_id']),with_for_update=True)
  assert str(r.owner_user_id)==expected['owner_user_id']
  assert digest([scalar_columns(x) for x in r.positions])==expected['positions_digest']
  assert digest([scalar_columns(x) for x in r.project_links])==expected['links_digest']
  old=db.get(RawDatasetLocation,expected['source_location']['id']);assert location(old)==expected['source_location'] and not old.is_preferred
  new=db.get(RawDatasetLocation,expected['preferred_location']['id']);assert location(new)==expected['preferred_location'] and new.is_preferred
  db.delete(old);db.flush();db.expire(r,['locations'])
  detail=raw_dataset_detail_view(r);assert next(l.absolute_path for l in detail.locations if l.is_preferred)==expected['destination_path']
  assert len(r.locations)==1
  positions+=len(r.positions);changed.append({'raw_id':str(r.id),'retired_location_id':expected['source_location']['id'],'preferred_location_id':new.id})
 assert positions==337
 assert other_locations_digest(db,[r['raw_id'] for r in gate['source_locations']])==batch.metadata_json['other_locations']
 assert not list(db.scalars(select(Artifact.id).where(Artifact.uri.startswith(OLD+'/'))))
 artifacts=list(db.scalars(select(Artifact).where(Artifact.uri.startswith('/data/.detecdiv-hub-api-cache/alexander-2023_2/'))));assert len(artifacts)==335
 for a in artifacts:get_raw_preview_artifact_content(a.id,db,operator)
 settings=list(db.scalars(select(RawDataset).where(RawDataset.display_settings_uri.startswith('/data/.detecdiv-hub-api-cache/alexander-2023_2/'))));assert len(settings)==19
 for r in settings:get_raw_dataset_display_settings(r.id,db,operator)
 project=batch.metadata_json['project_cutover'];p=db.get(Project,uuid.UUID(project['project_id']))
 assert next(l.project_mat_path for l in project_detail_view(p).locations if l.is_preferred)==project['preferred_mat_path']
 assert any(l.id==2765 and not l.is_preferred for l in p.locations)
 lease=db.get(ProjectLock,uuid.UUID(gate['lease_id']));assert lease.holder_key==batch.batch_name and lease.status=='active'
 release_project_lock(db,lock=lease)
 batch.status='verified_source_retirement_complete'
 metadata=dict(batch.metadata_json);metadata['retirement_receipt']=result;batch.metadata_json=metadata
 summary=dict(batch.summary_json);summary.update(source_deleted=True,retired_source_locations=21,positions_preserved=positions,
  measured_free_bytes_increase=result['result']['measured_free_bytes_increase'],free_bytes_after=result['result']['free_bytes_after']);batch.summary_json=summary
 for item in batch.items:item.status='verified_source_retirement_complete'
 # Preserve earlier audit facts while recording the later, separately approved action.
 for previous in db.scalars(select(StorageMigrationBatch).where(StorageMigrationBatch.batch_name.in_(['alexander-2023-2-cutover-20261101','alexander-2023-20261008']))):
  m=dict(previous.metadata_json);m['subsequent_2023_2_retirement_batch_id']=str(batch.id);previous.metadata_json=m
 db.flush()
 print(json.dumps({'success':True,'completed_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(batch.id),
  'raw_records':21,'retired_source_locations':21,'positions_preserved':positions,'api_preview_handlers_verified':335,'api_settings_handlers_verified':19,
  'original_project_location_preserved':2765,'project_lease_released':True,'source_retirement':result['result'],'changes':changed}))
'''
    applied=remote_json(SSH,'webserver-labo',code,{'gate':gate,'result':result},container=True)
    save('raw-retirement-catalog-finalization.json',applied)
    print(json.dumps({k:v for k,v in applied.items() if k!='changes'},indent=2))


if __name__=='__main__':{'prepare':prepare,'status':status,'finalize':finalize}[sys.argv[1]]()
