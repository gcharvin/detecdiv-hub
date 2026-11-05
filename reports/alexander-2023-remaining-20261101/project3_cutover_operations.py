"""Fresh admission and transactional 2023_3 project/raw/API cutover.

Original files and all source locations remain. Native source retirement is a
separate, receipt-gated operation. Full catalog snapshots are private.
"""
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

from project3_final_operations import HERE, RUNTIME, WORK, DB_COMMON, load, save, encoded
sys.path.insert(0, str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote, api
from audit import SSH, remote_json

HOLDER = 'alexander-2023-3-catalog-and-source-retirement-20261105'
CACHE = '/data/.detecdiv-hub-api-cache/alexander-2023_3/'
PROJECT_ROOT = '/homes2/maliavko/DetecdivHub/projects/'

COMMON = DB_COMMON + r'''
from api.models import StorageRoot,ProjectLocation,RawDatasetLocation,Artifact,StorageMigrationItem
from api.routes_projects import project_detail_view
from api.routes_raw_datasets import raw_dataset_detail_view,get_raw_preview_artifact_content,get_raw_dataset_display_settings
from pathlib import Path
CACHE='/data/.detecdiv-hub-api-cache/alexander-2023_3/'
PROJECT_ROOT='/homes2/maliavko/DetecdivHub/projects/'
def raw_snapshot(r):return {'columns':columns(r),'locations':[columns(l) for l in r.locations],'project_links':[columns(x) for x in r.project_links],'experiment_links':[columns(x) for x in r.experiment_links],'positions':[columns(x) for x in r.positions]}
def source_location_ids(r):return [l.id for l in r.locations if compose_storage_path(l.storage_root.path_prefix,l.relative_path).startswith(OLD+'/')]
def own_leases(db,a):
 now=datetime.now(timezone.utc)
 for row in a['leases']:
  l=db.get(ProjectLock,uuid.UUID(row['id']),with_for_update=True)
  assert l.status=='active' and l.holder_key==a['holder'] and str(l.project_id)==row['project_id'] and l.expires_at>now,'Own lease is not active'
def refs_current(db,plan):
 ids={x['kind']:set() for x in plan['references']};seen=set()
 for row in plan['references']:
  key=(row['kind'],row['id']);assert key not in seen;seen.add(key)
  obj=db.get(Artifact if row['kind']=='artifact' else RawDataset,uuid.UUID(row['id']))
  value=obj.uri if row['kind']=='artifact' else obj.display_settings_uri
  assert value==row['before'],'API reference changed since preparation'
  assert row['before']==OLD+'/'+row['relative_path']
  assert '..' not in Path(row['relative_path']).parts and not Path(row['relative_path']).is_absolute()
  ids[row['kind']].add(row['id'])
 current_artifacts={str(x.id) for x in db.scalars(select(Artifact).where(Artifact.uri.startswith(OLD+'/')))}
 current_settings={str(x.id) for x in db.scalars(select(RawDataset).where(RawDataset.display_settings_uri.startswith(OLD+'/')))}
 assert current_artifacts==ids['artifact'] and current_settings==ids['display_settings'],'Additional API dependencies require review'
 assert len(current_artifacts)==775 and len(current_settings)==23
'''


def collect():
    hosts = load('project3-final-hosts-completion-verification.json')
    closure = load('project3-final-host-lease-finalization.json')
    assert hosts['success'] and closure['success']
    assert len(closure['own_leases_released']) == 11
    result = remote('linux', r'''
import json,hashlib
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101')
d=w/'project3-final-native-verification-20261105'
assert (d/'controller.exit').read_text().strip()=='0'
s=json.loads((d/'state.json').read_text());assert s['phase']=='native_originals_and_final_copies_verified' and s['success']
raw=(d/'result.json').read_bytes();assert hashlib.sha256(raw).hexdigest()==s['result_sha256']
r=json.loads(raw);assert r['success'] and len(r['projects'])==11 and sum(x['files'] for x in r['projects'])==152277
api=w/'project3-api-projection';assert (api/'controller.exit').read_text().strip()=='0'
assert json.loads((api/'state.json').read_text())['phase']=='verified_ready_for_api_reference_update'
receipt=(api/'receipt.jsonl').read_bytes();assert hashlib.sha256(receipt).hexdigest()=='de91ecaabe749b5dff0b7b25b51734a30bc7787396ddb188479b43f0dc478711'
media=[json.loads(x) for x in receipt.splitlines()];assert len(media)==798 and all(x['primary_homes2_cache_equal'] for x in media)
plan=json.loads((w/'2023_3/api-media-readonly-plan-20261102.json').read_text())
print(json.dumps({'native':r,'native_result_sha256':s['result_sha256'],'media':media,'plan':plan}))
''', timeout=45)
    result.update(hosts=hosts, previous_admission=load('project3-final-admission.json', private=True), closure=closure)
    save('project3-cutover-input.json', result, private=True)
    save('project3-final-native-completion-verification.json', {'success': True, 'native_result_sha256': result['native_result_sha256'], **result['native']})
    print(json.dumps({'success': True, 'projects': 11, 'files': 152277, 'api_media': 798}))


def admit():
    v = load('project3-cutover-input.json', private=True)
    v['holder'] = HOLDER
    code = COMMON + r'''
from api.services.project_locks import acquire_client_edit_lease
v=json.load(sys.stdin);old=v['previous_admission'];ids={uuid.UUID(x['summary']['project_id']) for x in old['before_projects']}
assert v['hosts']['success'] and v['native']['success'] and len(v['media'])==798
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 db.execute(text("SELECT pg_advisory_xact_lock(hashtext('alexander-2023-3-catalog-and-source-retirement-20261105'))"))
 assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==v['holder'])) is None
 previous=db.get(StorageMigrationBatch,uuid.UUID(old['batch_id']),with_for_update=True)
 assert previous.status=='final_folders_verified_catalog_cutover_pending'
 assert previous.metadata_json['terminal_final_host_validation']['success']
 operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin',User.is_active.is_(True))).one()
 root=db.scalars(select(StorageRoot).where(StorageRoot.name=='user-homes2')).one()
 assert root.path_prefix=='/homes2' and root.host_scope=='detecdiv-server'
 raws=[r for r in db.scalars(select(RawDataset)) if source_paths(r)]
 assert len(raws)==24 and sum(len(r.positions) for r in raws)==775 and all(r.owner.user_key=='alexander' for r in raws)
 safety(db,ids,raws)
 before_raws={r['columns']['id']:r for r in old['before_raws']}
 for r in raws:assert digest(raw_snapshot(r))==digest(before_raws[str(r.id)]),'Raw catalog changed since publication admission'
 refs_current(db,v['plan'])
 media={x['path']:x for x in v['media']}
 for row in v['plan']['references']:
  path=Path(CACHE+row['relative_path']);assert path.is_file() and path.stat().st_size==media[row['relative_path']]['bytes'],'API projection is not available in the real container'
 leases=[]
 for before in old['before_projects']:
  id=uuid.UUID(before['summary']['project_id']);p=db.get(Project,id,with_for_update=True)
  assert digest(identity(p))==before['summary']['identity_sha256']
  assert digest(p.metadata_json)==before['summary']['metadata_sha256']
  assert digest([columns(x) for x in p.locations])==before['summary']['locations_sha256']
  lease=acquire_client_edit_lease(db,project_id=id,owner=operator,holder_key=v['holder'],holder_host='codex-admin-workstation',ttl_seconds=14400,write_scope='storage_relocation',reason='Verified final project/raw cutover and authorized source retirement',metadata_json={'source_manifest_sha256':'4abdfbfff41264b9bc069c3968ac742ac06fa9b7c52127bbf4369a7d95306fd3','original_projects_preserved':True})
  leases.append({'id':str(lease.id),'project_id':str(id),'expires_at':lease.expires_at.isoformat()})
 batch=StorageMigrationBatch(owner_user_id=raws[0].owner_user_id,batch_name=v['holder'],source_kind='raw_root',source_path=OLD,storage_root_name='user-homes2',host_scope='detecdiv-server',root_type='raw_root',strategy='verified_final_cutover_then_manifest_retirement',status='catalog_cutover_admitted',metadata_json={'operator':'gilles','standing_authorization':'User authorized verified source retirement on 2026-11-02 and reaffirmed on 2026-11-05; preserve original projects.','holder':v['holder'],'leases':leases,'before_projects':old['before_projects'],'before_raws':old['before_raws'],'api_plan':v['plan'],'native_project_verification_sha256':v['native_result_sha256'],'final_hosts':v['hosts'],'publication_batch_id':old['batch_id']},summary_json={'projects':11,'raws':24,'positions':775,'source_deleted':False})
 db.add(batch);db.flush()
 print(json.dumps({'success':True,'admitted_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(batch.id),'holder':v['holder'],'root_id':root.id,'leases':leases,'before_projects':old['before_projects'],'before_raws':old['before_raws'],'source_deleted':False}))
'''
    result = remote_json(SSH, 'webserver-labo', code, v, container=True)
    save('project3-cutover-admission.json', result, private=True)
    summary = {k: result[k] for k in ['success','admitted_at','batch_id','holder','root_id','leases','source_deleted']}
    save('project3-cutover-admission-summary.json', summary)
    print(json.dumps(summary))


def apply():
    v = load('project3-cutover-input.json', private=True)
    a = load('project3-cutover-admission.json', private=True)
    assert (datetime.now(timezone.utc)-datetime.fromisoformat(a['admitted_at'])).total_seconds() < 600
    code = COMMON + r'''
v=json.load(sys.stdin);a=v['admission'];inputs=v['inputs'];ids={uuid.UUID(x['project_id']) for x in a['leases']}
assert (datetime.now(timezone.utc)-datetime.fromisoformat(a['admitted_at'])).total_seconds()<600
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 db.execute(text("SELECT pg_advisory_xact_lock(hashtext('alexander-2023-3-catalog-and-source-retirement-20261105'))"))
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']),with_for_update=True);assert b.status=='catalog_cutover_admitted'
 root=db.get(StorageRoot,a['root_id']);assert root.path_prefix=='/homes2' and root.host_scope=='detecdiv-server'
 own_leases(db,a)
 raws=[db.get(RawDataset,uuid.UUID(r['columns']['id']),with_for_update=True) for r in a['before_raws']]
 assert safety(db,ids,raws,a['holder'])['own_locks']==11
 refs_current(db,inputs['plan']);raw_changes=[];project_changes=[]
 for r,before in zip(raws,a['before_raws']):
  assert digest(raw_snapshot(r))==digest(before)
  paths=source_paths(r);assert len(set(paths))==1
  suffix=paths[0][len(OLD)+1:];relative='maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_3/'+suffix
  assert suffix and '..' not in Path(suffix).parts
  assert not any(l.storage_root_id==root.id for l in r.locations)
  source_ids=source_location_ids(r);old_preferred=[l.id for l in r.locations if l.is_preferred]
  for l in r.locations:l.is_preferred=False
  loc=RawDatasetLocation(raw_dataset=r,storage_root=root,relative_path=relative,is_preferred=True,access_mode='read');db.add(loc);db.flush()
  change={'raw_id':str(r.id),'source_location_ids':source_ids,'old_preferred_location_ids':old_preferred,'new_location_id':loc.id,'preferred_path':NEW+'/'+suffix}
  raw_changes.append(change)
  db.add(StorageMigrationItem(batch_id=b.id,item_type='raw_dataset',legacy_path=paths[0],legacy_key=r.external_key,display_name=r.acquisition_label,status='catalog_cutover_complete_sources_retained',action='prefer_verified_homes2_raw',metadata_json=change))
 host_byid={p['project_id']:p for p in inputs['hosts']['projects']};native_byid={p['project_id']:p for p in inputs['native']['projects']}
 for before in a['before_projects']:
  p=db.get(Project,uuid.UUID(before['summary']['project_id']),with_for_update=True)
  assert digest(identity(p))==before['summary']['identity_sha256'] and digest(p.metadata_json)==before['summary']['metadata_sha256']
  assert digest([columns(x) for x in p.locations])==before['summary']['locations_sha256']
  h=host_byid[str(p.id)];fp=native_byid[str(p.id)];stem=h['name'];container=PROJECT_ROOT+stem
  assert p.project_name==stem and all(l.access_mode=='readonly' for l in p.locations)
  original_ids=[l.id for l in p.locations]
  for l in p.locations:l.is_preferred=False
  loc=ProjectLocation(project=p,storage_root=root,relative_path='maliavko/DetecdivHub/projects/'+stem,project_file_name=stem+'.mat',access_mode='readwrite',is_preferred=True);db.add(loc);db.flush()
  meta=dict(p.metadata_json or {});meta.update(project_mat_abs=container+'/'+stem+'.mat',project_dir_abs=container+'/'+stem,project_rel_from_root='maliavko/DetecdivHub/projects/'+stem+'/'+stem)
  meta['project_relocation_20261105']={'original_project_preserved':True,'original_location_ids':original_ids,'linux_windows_final_paths_verified':True,'main_mat_sha256':h['main_mat_sha256_unchanged'],'raw_root':NEW,'batch_id':str(b.id),'evidence':'reports/alexander-2023-remaining-20261101/'}
  p.metadata_json=meta;p.fov_count=h['fovs'];p.roi_count=h['rois'];p.processor_count=h['processors_verified']
  p.project_mat_bytes=fp['project_mat_bytes'];p.project_dir_bytes=fp['project_dir_bytes'];p.total_bytes=fp['total_bytes'];p.last_size_scan_at=datetime.now(timezone.utc);p.health_status='ok'
  assert digest(identity(p))==before['summary']['identity_sha256']
  project_changes.append({'project_id':str(p.id),'new_location_id':loc.id,'original_location_ids':original_ids,'preferred_mat_path':container+'/'+stem+'.mat','project_dir_abs':container+'/'+stem})
  db.add(StorageMigrationItem(batch_id=b.id,item_type='detecdiv_project',legacy_path=before['columns']['metadata_json'].get('project_mat_abs',before['summary']['name']+'.mat'),legacy_key=p.project_key,display_name=p.project_name,status='final_project_verified',action='prefer_relinked_copy_preserve_original',metadata_json=project_changes[-1]))
 for row in inputs['plan']['references']:
  obj=db.get(Artifact if row['kind']=='artifact' else RawDataset,uuid.UUID(row['id']),with_for_update=True)
  if row['kind']=='artifact':obj.uri=CACHE+row['relative_path']
  else:obj.display_settings_uri=CACHE+row['relative_path']
 db.flush()
 # Resolve through the real detail builders before committing the transaction.
 for row in raw_changes:
  r=db.get(RawDataset,uuid.UUID(row['raw_id']));db.expire(r,['locations'])
  detail=raw_dataset_detail_view(r);assert next(l for l in detail.locations if l.is_preferred).absolute_path==row['preferred_path']
 for row in project_changes:
  p=db.get(Project,uuid.UUID(row['project_id']));db.expire(p,['locations'])
  detail=project_detail_view(p);assert next(l for l in detail.locations if l.is_preferred).project_mat_path==row['preferred_mat_path']
 after_raws=[raw_snapshot(r) for r in raws]
 after_projects=[{'project_id':str(p.id),'columns':columns(p),'identity':identity(p),'locations':[columns(l) for l in p.locations]} for p in [db.get(Project,uuid.UUID(x['project_id'])) for x in project_changes]]
 result={'success':True,'committed_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(b.id),'raw_changes':raw_changes,'project_changes':project_changes,'api_references_changed':798,'source_deleted':False,'original_projects_preserved':True}
 b.status='catalog_cutover_complete_api_content_verification_pending'
 meta=dict(b.metadata_json);meta.update(cutover_result=result,after_raws=after_raws,after_projects=after_projects);b.metadata_json=meta
 b.summary_json={'projects':11,'raws':24,'positions':775,'api_references_changed':798,'source_deleted':False,'original_projects_preserved':True}
 print(json.dumps(result))
'''
    result = remote_json(SSH, 'webserver-labo', code, {'admission': a, 'inputs': v}, container=True)
    save('project3-catalog-cutover-result.json', result)
    print(json.dumps(result))


def launch_api(post=False):
    result = load('project3-catalog-cutover-result.json')
    inputs = load('project3-cutover-input.json', private=True)
    script = (HERE/'verify_project3_api_content.py').read_bytes()
    request = {'batch_id':result['batch_id'],'runner_sha256':hashlib.sha256(script).hexdigest(),
               'plan':inputs['plan'],'media':inputs['media']}
    suffix='-post-retirement' if post else ''
    if post:
        retired=load('project3-source-retirement-receipt-verification.json');assert retired['success']
        request.update(verification_phase='after_source_retirement',source_retirement_receipt_sha256=retired['state']['receipt_sha256'])
    payload={'directory':'/tmp/detecdiv-alexander-2023-3-content-20261105'+suffix,'request':request,'script':base64.b64encode(script).decode()}
    code = r'''
import json,base64,hashlib,os,subprocess,sys
from pathlib import Path
v=json.load(sys.stdin);os.umask(0o077)
d=Path(v['directory']);assert not d.exists()
assert Path('api/db.py').is_file()
d.mkdir(mode=0o700);script=base64.b64decode(v['script'])
assert hashlib.sha256(script).hexdigest()==v['request']['runner_sha256']
(d/'verify_project3_api_content.py').write_bytes(script);(d/'request.json').write_text(json.dumps(v['request'])+'\n')
env=dict(os.environ);env['PYTHONPATH']=os.getcwd()+os.pathsep+env.get('PYTHONPATH','')
with (d/'controller.log').open('xb') as log:
 p=subprocess.Popen([sys.executable,'-u',str(d/'verify_project3_api_content.py')],stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
print(json.dumps({'started':True,'pid':p.pid,'directory':str(d),'batch_id':v['request']['batch_id'],'runner_sha256':v['request']['runner_sha256'],'source_deleted':False}))
'''
    launched = remote_json(SSH,'webserver-labo',code,payload,container=True)
    save('project3-api-content'+suffix+'-launch.json', launched)
    print(json.dumps(launched))


def api_status(post=False):
    suffix='-post-retirement' if post else ''
    r = api('DIRECTORY='+repr('/tmp/detecdiv-alexander-2023-3-content-20261105'+suffix)+'\n'+r'''
import json,hashlib
from pathlib import Path
w=Path(DIRECTORY);out={}
p=w/'state.json'
if p.exists():out['state']=json.loads(p.read_text())
p=w/'controller.log'
if p.exists():
 with p.open('rb') as f:f.seek(max(0,p.stat().st_size-2500));out['log_tail']=f.read().decode(errors='replace')
if out.get('state',{}).get('success'):
 content=(w/'receipt.jsonl').read_bytes();assert hashlib.sha256(content).hexdigest()==out['state']['receipt_sha256']
 rows=[json.loads(x) for x in content.splitlines()];assert len(rows)==798 and all(x['actual_handler_stream_verified'] for x in rows)
 assert sum(x['bytes'] for x in rows)==12055348763
 out['receipt_verified']=True;out['receipt']=rows
print(json.dumps(out))
''')
    (RUNTIME/('project3-api-content'+suffix+'-status.json')).write_text(json.dumps(r,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in r.items() if k!='receipt'}))


def verify():
    proof = load('project3-api-content-status.json', private=True)
    assert proof.get('receipt_verified') and proof['state']['phase']=='all_real_api_content_handlers_verified'
    a = load('project3-cutover-admission.json', private=True)
    code = COMMON+r'''
v=json.load(sys.stdin);a=v['admission'];proof=v['proof'];ids={uuid.UUID(x['project_id']) for x in a['leases']}
assert proof['state']['success'] and proof['receipt_verified'] and len(proof['receipt'])==798
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']),with_for_update=True)
 assert b.status=='catalog_cutover_complete_api_content_verification_pending'
 own_leases(db,a)
 raws=[db.get(RawDataset,uuid.UUID(x['columns']['id']),with_for_update=True) for x in b.metadata_json['after_raws']]
 assert safety(db,ids,raws,a['holder'])['own_locks']==11
 for r,expected,before in zip(raws,b.metadata_json['after_raws'],a['before_raws']):
  now=raw_snapshot(r);assert digest(now)==digest(expected),'Catalog changed during API verification'
  for key in ['positions','project_links','experiment_links']:assert now[key]==before[key]
  assert {k:value for k,value in now['columns'].items() if k not in {'updated_at','display_settings_uri'}}=={k:value for k,value in before['columns'].items() if k not in {'updated_at','display_settings_uri'}}
 for expected in b.metadata_json['after_projects']:
  p=db.get(Project,uuid.UUID(expected['project_id']))
  assert columns(p)==expected['columns'] and identity(p)==expected['identity'] and [columns(l) for l in p.locations]==expected['locations']
  before=next(x for x in a['before_projects'] if x['summary']['project_id']==str(p.id))
  assert digest(identity(p))==before['summary']['identity_sha256']
 for row in proof['receipt']:
  obj=db.get(Artifact if row['kind']=='artifact' else RawDataset,uuid.UUID(row['id']))
  assert (obj.uri if row['kind']=='artifact' else obj.display_settings_uri)==CACHE+row['relative_path']
 assert not list(db.scalars(select(Artifact.id).where(Artifact.uri.startswith(OLD+'/'))))
 assert not list(db.scalars(select(RawDataset.id).where(RawDataset.display_settings_uri.startswith(OLD+'/'))))
 meta=dict(b.metadata_json);meta['api_content_verification']={k:value for k,value in proof.items() if k!='receipt'};b.metadata_json=meta
 b.status='catalog_and_api_verified_source_retirement_pending'
 result={'success':True,'verified_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(b.id),'projects_verified':11,'raws_verified':24,'positions_preserved':775,'actual_api_handlers_fully_streamed':798,'api_content_bytes_verified':12055348763,'identity_notes_acl_groups_links_preserved':True,'original_projects_preserved':True,'source_deleted':False}
 print(json.dumps(result))
'''
    result = remote_json(SSH,'webserver-labo',code,{'admission':a,'proof':proof},container=True)
    save('project3-catalog-cutover-verification.json',result)
    print(json.dumps(result))


if __name__ == '__main__':
    {'collect':collect,'admit':admit,'apply':apply,'launch-api':launch_api,'api-status':api_status,'verify':verify,'launch-api-post':lambda:launch_api(True),'api-status-post':lambda:api_status(True)}[sys.argv[1]]()
