"""Audited operational catalog changes for the explicitly approved migration."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from audit import remote_json, SSH
from activate_windows_mapping import remote

WORK = '/home/charvin-admin/storage-transfers/alexander-project-relocation-20261101'
sys.path.insert(0, str(HERE.parent/'alexander-2023-cutover-20261101'))
from cutover import COMMON


def save(name, value):
    (HERE / name).write_text(json.dumps(value, indent=2, default=str) + '\n', encoding='utf-8')
    return value


def cache():
    receipt = remote('linux', f'''
import json
from pathlib import Path
w=Path({WORK!r});state=json.loads((w/'api-cache-state.json').read_text())
assert state['phase']=='verified_ready_for_api_reference_update'
print(json.dumps({{'state':state,'files':[json.loads(x) for x in (w/'api-cache-receipt.jsonl').read_text().splitlines()]}}))
''')
    payload = {'receipt': receipt, 'dependencies': json.loads((HERE/'raw-retirement-dependencies.json').read_text())}
    code = r'''
import json,sys,uuid,hashlib
from pathlib import Path
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Artifact,RawDataset,User,StorageMigrationBatch,StorageMigrationItem
from api.routes_raw_datasets import get_raw_preview_artifact_content,get_raw_dataset_display_settings
payload=json.load(sys.stdin);receipt=payload['receipt'];deps=payload['dependencies']
old='/data/Alexander/data/2023_2/';cache='/data/.detecdiv-hub-api-cache/alexander-2023_2/'
files={r['cache_path']:r for r in receipt['files']}
assert len(files)==354 and sum(r['bytes'] for r in files.values())==5874728850
for path,row in files.items():
 p=Path(path);assert p.is_file() and p.stat().st_size==row['bytes']
 with p.open('rb') as f:assert f.read(16)
 if p.suffix=='.json':json.loads(p.read_text())
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin')).one()
 name='alexander-2023-2-api-cache-20261101'
 assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==name)) is None
 changes=[]
 for expected in deps['artifacts']:
  a=db.get(Artifact,uuid.UUID(expected['id']),with_for_update=True)
  assert a.uri==expected['uri'] and a.uri.startswith(old)
  target=cache+a.uri[len(old):];assert target in files
  changes.append({'kind':'artifact','id':str(a.id),'before':a.uri,'after':target})
  a.uri=target
 for expected in deps['raw_current_uris']:
  r=db.get(RawDataset,uuid.UUID(expected['id']),with_for_update=True)
  assert r.display_settings_uri==expected['display_settings_uri'] and r.archive_uri==expected['archive_uri']
  target=cache+r.display_settings_uri[len(old):];assert r.display_settings_uri.startswith(old) and target in files
  changes.append({'kind':'display_settings','id':str(r.id),'before':r.display_settings_uri,'after':target})
  r.display_settings_uri=target
 assert len(changes)==354
 db.flush()
 for c in changes:
  if c['kind']=='artifact':response=get_raw_preview_artifact_content(uuid.UUID(c['id']),db,operator)
  else:response=get_raw_dataset_display_settings(uuid.UUID(c['id']),db,operator)
  assert str(response.path)==c['after'] and Path(response.path).is_file()
 batch=StorageMigrationBatch(batch_name=name,source_kind='api_assets',source_path=old.rstrip('/'),storage_root_name='data-root',
  host_scope='detecdiv-server',root_type='api_cache',strategy='retain_verified_api_projection',status='verified_complete',
  metadata_json={'operator':'gilles','changes':changes,'full_content_verified_on_native_nas':True,
   'reason':'API container currently mounts /data but not /homes2; preserve existing content routes without service changes.'},
  summary_json={'artifacts':335,'display_settings':19,'files':354,'bytes':5874728850,'originals_deleted':False})
 db.add(batch);db.flush()
 print(json.dumps({'committed_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(batch.id),'artifacts_updated':335,
  'display_settings_updated':19,'api_content_handlers_verified':354,'retained_bytes':5874728850,'changes':changes}))
'''
    # Fail closed if dependency field names do not match the collected snapshot.
    assert len(payload['dependencies']['raw_current_uris']) == 19
    result = remote_json(SSH, 'webserver-labo', code, payload, container=True)
    save('api-cache-catalog-result.json', result)
    print(json.dumps({k:v for k,v in result.items() if k!='changes'}))


def status():
    result = remote('linux', f'''
from pathlib import Path
import json,subprocess
w=Path({WORK!r})
r={{n:p.read_text(errors='replace')[-9000:] for n in ['relink-progress.json','relink.log','relink.exit','linux-relink-verification.json'] if (p:=w/n).exists()}}
r['processes']=subprocess.run(['ps','-eo','pid,etime,pcpu,rss,args'],capture_output=True,text=True).stdout
r['processes']='\\n'.join(x for x in r['processes'].splitlines() if '/bin/glnxa64/MATLAB' in x)
print(json.dumps(r))
''')
    print(json.dumps(result, indent=2))


def project():
    linux=remote('linux',f'''
import json,hashlib,os
from pathlib import Path
w=Path({WORK!r});r=json.loads((w/'linux-relink-verification.json').read_text())
assert r['success'] and (w/'relink.exit').read_text().strip()=='0'
f=Path(r['copied_project_file']);d=f.parent/f.stem
size=sum(p.stat().st_size for parent,dirs,files in os.walk(d) for n in files if (p:=Path(parent)/n).is_file())
source=Path(r['source_project_file'])
assert source.stat().st_size==359993344
assert hashlib.sha256(source.read_bytes()).hexdigest()=='ef4bdb4155e4866ecef3d8fbf1c2466a644c85d7063d943488dab625613bdc96'
print(json.dumps({{'verification':r,'mat_bytes':f.stat().st_size,'directory_bytes':size,'mat_sha256':hashlib.sha256(f.read_bytes()).hexdigest(),'source_mat_sha256_unchanged':True}}))
''',timeout=60)
    save('linux-project-final-verification.json',linux)
    win=json.loads((HERE/'windows-status.json').read_text())['project-session-verification.json']
    assert win['success'] and win['verification']['fovs']==12 and win['verification']['rois']==5724
    for a,b in zip(linux['verification']['samples'],win['verification']['samples']):
        assert a['roi']==b['roi'] and a['imageSize']==b['imageSize'] and a['imageClass']==b['imageClass']
    payload={'linux':linux,'windows':win,'before':json.loads((HERE/'catalog-before.json').read_text())['project']}
    code=COMMON+r'''
from api.models import ProjectLocation
from api.routes_projects import project_detail_view
payload=json.load(sys.stdin);old=payload['before'];v=payload['linux'];stem='anof_26102023_yam76s_100ms'
container='/homes2/maliavko/DetecdivHub/projects/'+stem
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 p=db.get(Project,uuid.UUID(old['id']),with_for_update=True)
 raws=[db.get(RawDataset,l.raw_dataset_id) for l in p.raw_links];guards=safety(db,raws)
 assert p.project_name==old['name'] and p.owner.user_key==old['owner'] and p.metadata_json==old['metadata']
 assert sorted(str(l.raw_dataset_id) for l in p.raw_links)==sorted(old['raw_links'])
 assert len(p.locations)==1 and p.locations[0].id==2765 and p.locations[0].is_preferred
 assert p.locations[0].access_mode=='readonly'
 protected={'owner_user_id':str(p.owner_user_id),'project_key':p.project_key,'visibility':p.visibility,'notes':p.notes,
  'acl':digest([scalar_columns(x) for x in p.acl_entries]),'groups':digest([scalar_columns(x) for x in p.group_memberships]),
  'notes_entries':digest([scalar_columns(x) for x in p.note_entries]),'raw_links':digest([scalar_columns(x) for x in p.raw_links])}
 root=db.scalars(select(StorageRoot).where(StorageRoot.name=='user-homes2')).one();assert root.id==84 and root.path_prefix=='/homes2'
 before=scalar_columns(p)
 p.locations[0].is_preferred=False
 loc=ProjectLocation(project=p,storage_root=root,relative_path='maliavko/DetecdivHub/projects/'+stem,
  project_file_name=stem+'.mat',access_mode='readwrite',is_preferred=True)
 db.add(loc)
 meta=dict(p.metadata_json);meta.update(project_mat_abs=container+'/'+stem+'.mat',project_dir_abs=container+'/'+stem,
  project_rel_from_root='maliavko/DetecdivHub/projects/'+stem+'/'+stem)
 meta['project_relocation_20261101']={'original_project_retained':True,'original_location_id':2765,
  'linux_windows_reopen_verified':True,'copied_mat_sha256':v['mat_sha256'],'fovs':12,'rois':5724,
  'evidence':'reports/alexander-project-relocation-20261101/','saved_raw_root':FINAL}
 p.metadata_json=meta;p.fov_count=12;p.roi_count=5724;p.available_raw_count=1;p.missing_raw_count=0
 p.project_mat_bytes=v['mat_bytes'];p.project_dir_bytes=v['directory_bytes'];p.total_bytes=v['mat_bytes']+v['directory_bytes']
 p.last_size_scan_at=datetime.now(timezone.utc);p.health_status='ok'
 name='alexander-project-relocation-20261101'
 assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==name)) is None
 batch=StorageMigrationBatch(owner_user_id=p.owner_user_id,batch_name=name,source_kind='detecdiv_project',
  source_path=old['metadata']['project_mat_abs'],storage_root_name='user-homes2',host_scope='detecdiv-server',root_type='project_root',
  strategy='verified_independent_copy_relink_keep_original',status='verified_complete',
  metadata_json={'operator':'gilles','before_project_columns':json.loads(json.dumps(before,default=str)),'protected_identity_and_relationships':protected,
   'linux_verification':v,'windows_verification':payload['windows'],'original_source_location_id':2765},
  summary_json={'projects':1,'fovs':12,'rois':5724,'original_project_preserved':True,'project_mat_sha256':v['mat_sha256']})
 db.add(batch);db.flush()
 db.add(StorageMigrationItem(batch_id=batch.id,item_type='detecdiv_project',legacy_path=old['metadata']['project_mat_abs'],
  legacy_key=p.project_key,display_name=p.project_name,status='verified_complete',action='prefer_relinked_copy_keep_original',
  metadata_json={'project_id':str(p.id),'new_location_id':loc.id,'original_location_id':2765,'new_mat_path':container+'/'+stem+'.mat'}))
 db.flush();detail=project_detail_view(p)
 assert next(l for l in detail.locations if l.is_preferred).project_mat_path==container+'/'+stem+'.mat'
 after={'owner_user_id':str(p.owner_user_id),'project_key':p.project_key,'visibility':p.visibility,'notes':p.notes,
  'acl':digest([scalar_columns(x) for x in p.acl_entries]),'groups':digest([scalar_columns(x) for x in p.group_memberships]),
  'notes_entries':digest([scalar_columns(x) for x in p.note_entries]),'raw_links':digest([scalar_columns(x) for x in p.raw_links])}
 assert after==protected
 print(json.dumps({'success':True,'committed_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(batch.id),
  'project_id':str(p.id),'project_key':p.project_key,'new_location_id':loc.id,'original_location_id':2765,
  'preferred_mat_path':container+'/'+stem+'.mat','original_project_preserved':True,'protected_identity_and_relationships':protected,
  'mat_bytes':p.project_mat_bytes,'directory_bytes':p.project_dir_bytes,'total_bytes':p.total_bytes,'guards':guards}))
'''
    result=remote_json(SSH,'webserver-labo',code,payload,container=True)
    save('project-catalog-cutover.json',result);print(json.dumps(result,indent=2))


if __name__ == '__main__':
    {'cache': cache, 'status': status, 'project': project}[sys.argv[1]]()
