"""Fresh lease admission, final-folder publication and project-only catalog cutover."""
import base64
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
PRIVATE=HERE/'runtime'
WORK='/home/charvin-admin/storage-transfers/alexander-2024-20261105'
HOLDER='alexander-2024-year1-final-publication-20261109'
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote
from audit import remote_json,SSH

def write(name,value,private=False):
    path=(PRIVATE if private else HERE)/name
    with path.open('x',encoding='utf-8') as stream:json.dump(value,stream,indent=2,default=str);stream.write('\n')

def read(name,private=False):return json.loads(((PRIVATE if private else HERE)/name).read_text(encoding='utf-8'))

COMMON=r'''
import json,sys,uuid,hashlib
from datetime import datetime,timezone
from sqlalchemy import select,text,inspect
from api.db import SessionLocal
from api.models import User,Project,ProjectLock,Job,IndexingJob,StorageMigrationBatch,StorageRoot,ProjectLocation,RawDataset
from api.services.path_resolution import compose_storage_path
def columns(obj):return {p.key:getattr(obj,p.key) for p in inspect(type(obj)).column_attrs}
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,default=str).encode()).hexdigest()
def identity(p):return {'key':p.project_key,'owner':str(p.owner_user_id),'visibility':p.visibility,'notes':p.notes,
 'acl':[columns(x) for x in p.acl_entries],'groups':[columns(x) for x in p.group_memberships],'note_entries':[columns(x) for x in p.note_entries],'raw_links':[columns(x) for x in p.raw_links]}
def safety(db,ids,holder=None):
 now=datetime.now(timezone.utc)
 raw_ids={str(r.id) for r in db.scalars(select(RawDataset)) if any(compose_storage_path(l.storage_root.path_prefix,l.relative_path).startswith('/data/Alexander/data/2024_1/') for l in r.locations)}
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  value=json.dumps(j.params_json or {},default=str)
  assert j.project_id not in ids and str(j.raw_dataset_id) not in raw_ids and not any(str(i) in value for i in ids) and '/data/Alexander/data/2024_1' not in value and '/raw/.incoming-alexander-2024-20261105/2024_1' not in value,'Affected project/raw job active'
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running'])))),'Indexing active'
 active=[l for l in db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_(ids),ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now]
 assert all(holder and l.holder_key==holder for l in active),'Other holder active'
 return active
def check_own(db,a):
 ids={uuid.UUID(x['project_id']) for x in a['leases']};active=safety(db,ids,a['holder']);assert len(active)==16
 for row in a['leases']:
  l=db.get(ProjectLock,uuid.UUID(row['id']));assert l.holder_key==a['holder'] and str(l.project_id)==row['project_id'] and l.status=='active' and l.expires_at>datetime.now(timezone.utc)
 return ids
def check_before(db,a):
 for old in a['before_projects']:
  p=db.get(Project,uuid.UUID(old['project_id']),with_for_update=True)
  assert digest(identity(p))==old['identity_sha256'] and digest(p.metadata_json)==old['metadata_sha256'] and digest([columns(x) for x in p.locations])==old['locations_sha256'],'Catalog changed during publication'
'''

def collect():
    fixture=read('year1-publication-rename-fixture-verification.json');assert fixture['success']
    cifs=read('year1-publication-cifs-fixture-verification.json');assert cifs['success'] and cifs['publisher_sha256']==fixture['publisher_sha256']
    assert hashlib.sha256((HERE/'publish_year1_folders.py').read_bytes()).hexdigest()==fixture['publisher_sha256']
    lint=read('year1-host-lint-v2-status.json',private=True);assert lint['controller.exit']=='0' and not any(x['id']=='SYNER' for x in lint['lint.json'])
    expected_script=read('year1-host-lint-v2-launch.json')['script_sha256'];assert hashlib.sha256((HERE/'verify_year1_final_hosts.m').read_bytes()).hexdigest()==expected_script
    result=remote('linux',r'''
import json,hashlib
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');w=root/'year1-prepublication-audit-20261109'
assert (w/'controller.exit').read_text().strip()=='0'
raw=(w/'result.json').read_bytes();result=json.loads(raw);assert result['success'] and result['read_only'] and len(result['projects'])==16 and not result['published']
assert sum(p['target_files'] for p in result['projects'])==105263
assert hashlib.sha256((w/'audit_year1_prepublication.py').read_bytes()).hexdigest()=='1e7b8955265c644c5c925b2282b8ec42f4279688cd568febcba6f73a8fdc78f0'
state=json.loads((root/'state.json').read_text());assert state['years']['2024_1']['full_sha1_passed'] and state['years']['2024_1']['source_stability_verified']
assert hashlib.sha256((root/'copy_raws.py').read_bytes()).hexdigest()=='a0576fffa3c1d43d232f54567c6c9f7153aed40b436ace98f26079af72432b99'
for filename in ['source-before.jsonl','source-after.jsonl']:assert hashlib.sha256((root/'2024_1'/filename).read_bytes()).hexdigest()=='8fd64d094fe29707e0ddd0b1480e9b861786d3adde677087ca7a6e95e252f61e'
assert (root/'2024_1/verify-full-sha1.log').stat().st_size==0
host=root/'year1-host-inputs-20261109';summary=json.loads((host/'preparation-summary.json').read_text());assert summary['success']
assert hashlib.sha256((host/'request.json').read_bytes()).hexdigest()==summary['request_sha256']
print(json.dumps({'success':True,'integrity_result_sha256':hashlib.sha256(raw).hexdigest(),'projects':result['projects'],'host_inputs':summary,'raw_year1':state['years']['2024_1']}))
''',timeout=60)
    write('year1-publication-inputs.json',result,True)
    write('year1-prepublication-integrity-completion-verification.json',{'success':True,'projects':16,'files':105263,'integrity_result_sha256':result['integrity_result_sha256'],'source_deleted':False,'published':False})
    return result

def admit():
    v=read('year1-publication-inputs.json',True);v['holder']=HOLDER
    code=COMMON+r'''
from api.services.project_locks import acquire_client_edit_lease
v=json.load(sys.stdin);ids={uuid.UUID(p['project_id']) for p in v['projects']};assert len(ids)==16
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"));db.execute(text("SELECT pg_advisory_xact_lock(hashtext('alexander-2024-year1-final-publication-20261109'))"))
 assert not db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==v['holder']))
 safety(db,ids);operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin',User.is_active.is_(True))).one()
 old={}
 for batch_id in ['bf1a270b-1e18-419a-b11b-b2241871f13b','90c03028-f0b9-4f86-86b6-a90912e3ef50','e72ca211-c526-4855-99ad-d2839da288a6','98f46050-bfe3-409f-a807-a2fb30c104ef','dd188133-e2b3-4279-b7ac-773a673e5d3b']:
  for snapshot in db.get(StorageMigrationBatch,uuid.UUID(batch_id)).metadata_json['before_projects']:
   if uuid.UUID(snapshot['project_id']) in ids:
    if snapshot['project_id'] in old:assert old[snapshot['project_id']]==snapshot
    old[snapshot['project_id']]=snapshot
 assert len(old)==16
 leases=[];before=[]
 for i in sorted(ids,key=str):
  p=db.get(Project,i,with_for_update=True);assert p.owner.user_key=='alexander' and p.project_name==next(x['name'] for x in v['projects'] if x['project_id']==str(i))
  now={'project_id':str(i),'identity_sha256':digest(identity(p)),'metadata_sha256':digest(p.metadata_json),'locations_sha256':digest([columns(x) for x in p.locations])}
  assert now==old[str(i)],'Project changed since verified relink; preserve and review'
  now.update(name=p.project_name,columns=columns(p),locations=[columns(x) for x in p.locations]);before.append(now)
  l=acquire_client_edit_lease(db,project_id=i,owner=operator,holder_key=v['holder'],holder_host='codex-admin-workstation',ttl_seconds=21600,write_scope='storage_relocation',reason='Publish verified year1 copies and validate final hosts; preserve originals',metadata_json={'source_deletion_allowed':False,'evidence':'reports/alexander-2024-20261105/'})
  leases.append({'id':str(l.id),'project_id':str(i),'expires_at':l.expires_at.isoformat()})
 b=StorageMigrationBatch(owner_user_id=p.owner_user_id,batch_name=v['holder'],source_kind='detecdiv_project',source_path='/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis',storage_root_name='user-homes2',host_scope='detecdiv-server',root_type='project_root',strategy='verified_final_project_publication_preserve_originals',status='filesystem_publication_admitted',metadata_json={'holder':v['holder'],'before_projects':before,'leases':leases,'integrity_result_sha256':v['integrity_result_sha256'],'host_inputs':v['host_inputs']},summary_json={'projects':16,'source_deleted':False,'preferred_locations_changed':False})
 db.add(b);db.flush();print(json.dumps({'batch_id':str(b.id),'holder':v['holder'],'before_projects':before,'leases':leases,'admitted_at':datetime.now(timezone.utc).isoformat()},default=str))
'''
    a=remote_json(SSH,'webserver-labo',code,v,container=True);write('year1-final-admission.json',a,True)
    write('year1-final-admission-summary.json',{k:a[k] for k in ['batch_id','holder','leases','admitted_at']})
    return a

def fresh():
    a=read('year1-final-admission.json',True)
    r=remote_json(SSH,'webserver-labo',COMMON+r'''
a=json.load(sys.stdin)
with SessionLocal() as db:
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']));assert b.metadata_json['holder']==a['holder']
 check_own(db,a);check_before(db,a)
 print(json.dumps({'checked_at':datetime.now(timezone.utc).isoformat(),'own_lease_deadline_unix':min(datetime.fromisoformat(x['expires_at']).timestamp() for x in a['leases'])}))
''',a,container=True)
    return dict(a,**r)

def publish():
    a=fresh();v=read('year1-publication-inputs.json',True);script=(HERE/'publish_year1_folders.py').read_bytes()
    code=r'''
import json,base64,hashlib,shlex,subprocess
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');audit=root/'year1-prepublication-audit-20261109';w=root/'year1-final-publication-20261109';assert not w.exists()
request=json.loads((audit/'request.json').read_text());proof=json.loads((audit/'result.json').read_text());assert hashlib.sha256((audit/'result.json').read_bytes()).hexdigest()==INPUTS['integrity_result_sha256']
projects=[]
for p in request['projects']:
 f=audit/p['name']/'target-full-hashes.jsonl';r=next(r for r in proof['projects'] if r['project_id']==p['project_id']);raw=f.read_bytes();assert hashlib.sha256(raw).hexdigest()==r['target_receipt_sha256']
 rows=[json.loads(line) for line in raw.splitlines()];assert rows[-1]['kind']=='complete'
 files=[x for x in rows if x['kind']=='verified_file'];assert len(files)==r['target_files']
 projects.append({'project_id':p['project_id'],'name':p['name'],'source_inventory_sha256':p['source_inventory_sha256'],'directories':p['directories'],'target_rows':files,'relinked_paths':[f['path'] for f in p['files'] if f['metadata_relinked']]})
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
w.mkdir(mode=0o700);(w/'publish_year1_folders.py').write_bytes(script)
(w/'request.json').write_text(json.dumps({'projects':projects,'admission':ADMISSION,'own_lease_deadline_unix':ADMISSION['own_lease_deadline_unix'],'integrity_result_sha256':INPUTS['integrity_result_sha256'],'runner_sha256':SHA,'source_deletion_allowed':False,'catalog_cutover_allowed':False})+'\n')
shell=shlex.join(['nice','-n','19','ionice','-c','3','/home/charvin-admin/repos/detecdiv-hub-webvm/.venv/bin/python','-u',str(w/'publish_year1_folders.py')])+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s','alex2024-year1-final-publication-20261109','bash','-c',shell],check=True)
print(json.dumps({'launched':True,'work':str(w),'runner_sha256':SHA,'projects':16,'source_deleted':False,'catalog_changed':False}))
'''
    result=remote('linux','ADMISSION='+repr(a)+'\nINPUTS='+repr(v)+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\nSHA='+repr(hashlib.sha256(script).hexdigest())+'\n'+code,timeout=90)
    write('year1-final-publication-launch.json',result);return result

def publication_status():
    r=remote('linux',r'''
import json,hashlib
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/year1-final-publication-20261109');r={}
for n in ['state.json','result.json','controller.exit']:
 p=w/n
 if p.exists():r[n]=json.loads(p.read_text()) if n.endswith('.json') else p.read_text().strip()
if (w/'result.json').exists():r['result_sha256']=hashlib.sha256((w/'result.json').read_bytes()).hexdigest()
print(json.dumps(r))
''',timeout=30)
    (PRIVATE/'year1-publication-status.json').write_text(json.dumps(r,indent=2)+'\n');return r

def start_hosts():
    a=fresh();published=publication_status();assert published['controller.exit']=='0' and published['result.json']['success']
    assert len(published['result.json']['projects'])==16
    v=read('year1-publication-inputs.json',True);files={n:base64.b64encode((HERE/n).read_bytes()).decode() for n in ['verify_year1_final_hosts.m','run_year1_final_hosts.py']}
    c={'request_sha256':v['host_inputs']['request_sha256'],'script_sha256':hashlib.sha256((HERE/'verify_year1_final_hosts.m').read_bytes()).hexdigest(),'runner_sha256':hashlib.sha256((HERE/'run_year1_final_hosts.py').read_bytes()).hexdigest(),
       'own_lease_deadline_unix':a['own_lease_deadline_unix'],'filesystem_publication_verified':True,'publication_result_sha256':published['result_sha256']}
    r=remote('linux','FILES='+repr(files)+'\nCONTROL='+repr(c)+'\n'+r'''
import json,base64,hashlib,shlex,subprocess
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');w=root/'year1-final-hosts-20261109';assert not w.exists();w.mkdir(mode=0o700)
raw=(root/'year1-host-inputs-20261109/request.json').read_bytes();assert hashlib.sha256(raw).hexdigest()==CONTROL['request_sha256'];(w/'host-request.json').write_bytes(raw)
for name,value in FILES.items():(w/name).write_bytes(base64.b64decode(value))
(w/'controller-request.json').write_text(json.dumps(CONTROL)+'\n')
shell=shlex.join(['/home/charvin-admin/repos/detecdiv-hub-webvm/.venv/bin/python','-u',str(w/'run_year1_final_hosts.py')])+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s','alex2024-year1-final-hosts-20261109','bash','-c',shell],check=True)
print(json.dumps({'launched':True,'work':str(w),'source_deleted':False}))
''',timeout=90)
    write('year1-linux-final-hosts-launch.json',r)
    packet=remote('linux',r'''
import json,base64
from pathlib import Path
p=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/year1-host-inputs-20261109/request.json.gz')
print(json.dumps({'gzip':base64.b64encode(p.read_bytes()).decode()}))
''',timeout=90)
    (PRIVATE/'year1-host-request.json.gz').write_bytes(base64.b64decode(packet['gzip']))
    from year1_windows_host_operations import operate
    operate('stage',c);operate('start');write('year1-host-controller-inputs.json',c,True)
    return {'linux_started':True,'windows_started':True}

def hosts_status():
    linux=remote('linux',r'''
import json
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/year1-final-hosts-20261109');r={}
for n in ['host-linux-state.json','host-linux-verification.json','controller.exit']:
 p=w/n
 if p.exists():r[n]=json.loads(p.read_text()) if n.endswith('.json') else p.read_text().strip()
print(json.dumps(r))
''',timeout=30)
    from year1_windows_host_operations import operate
    windows=operate('status');r={'linux':linux,'windows':windows};(PRIVATE/'year1-hosts-status.json').write_text(json.dumps(r,indent=2)+'\n');return r

def verify_hosts():
    r=read('year1-hosts-status.json',True);l=r['linux'];w=r['windows']
    assert l['controller.exit']=='0' and l['host-linux-state.json']['success'] and w['host-windows-state.json']['success']
    assert w['task']['state']!='Running' and w['host-windows-state.json']['windows_identity'].casefold()=='gmgm\\charvin-admin' and w['host-windows-state.json']['session_id']==1
    linux=l['host-linux-verification.json'];windows=w['host-windows-verification.json']
    assert linux['success'] and windows['success'] and linux['project_namespace']==windows['project_namespace']=='final'
    fields=['project_id','name','fovs','rois','raw_sample_reads','roi_representative_reads','images_present','images_previously_absent','results_present','data_present','blank_paths_preserved','historical_missing_raw_paths_preserved','historical_cache_size_mismatches_preserved','processors_verified','main_sha256_unchanged','availability_baseline_preserved','project_saved']
    byid={p['project_id']:p for p in windows['projects']};assert len(linux['projects'])==len(byid)==16
    for p in linux['projects']:
        assert all(p[k]==byid[p['project_id']][k] for k in fields)
        assert p['availability_baseline_preserved'] and not p['project_saved']
        assert all(x['engine_reader_verified'] and x['availability_baseline_preserved'] for x in p['field_results'])
    inputs=read('year1-publication-inputs.json',True)['host_inputs']
    assert sum(p['fovs'] for p in linux['projects'])==inputs['fovs']==159 and sum(p['rois'] for p in linux['projects'])==inputs['rois']==50234
    assert sum(p['raw_sample_reads'] for p in linux['projects'])==sum(p['raw_sample_reads'] for p in windows['projects'])==inputs['raw_samples']==2059
    proof={'success':True,'projects':16,'fovs':159,'rois':50234,'raw_sample_reads_per_host':2059,'linux_windows_available_data_and_baselines_equal':True,
      'project_saved':False,'source_deleted':False,'project_reports':linux['projects']}
    write('year1-final-hosts-completion-verification.json',proof);return proof

def cutover():
    a=fresh();hosts=read('year1-final-hosts-completion-verification.json');published=read('year1-publication-status.json',True)
    assert hosts['success'] and published['result.json']['success']
    code=COMMON+r'''
from pathlib import Path
from api.routes_projects import get_project
v=json.load(sys.stdin);a=v['admission'];hosts=v['hosts'];published=v['published'];assert hosts['success'] and len(hosts['project_reports'])==16
byid={x['project_id']:x for x in published['result.json']['projects']};changes=[]
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"));db.execute(text("SELECT pg_advisory_xact_lock(hashtext('alexander-2024-year1-final-publication-20261109'))"))
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']),with_for_update=True);assert b.status=='filesystem_publication_admitted' and b.metadata_json['holder']==a['holder']
 check_own(db,a);check_before(db,a)
 root=db.scalars(select(StorageRoot).where(StorageRoot.name=='user-homes2')).one();assert root.path_prefix=='/homes2' and root.host_scope=='detecdiv-server'
 operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin',User.is_active.is_(True))).one()
 for old in a['before_projects']:
  p=db.get(Project,uuid.UUID(old['project_id']),with_for_update=True);native=byid[str(p.id)];stem=old['name'];canonical='/homes2/maliavko/DetecdivHub/projects/'+stem
  assert native['name']==stem and native['native_owner']=='maliavko' and native['source_inventory_unchanged']
  path=Path(canonical)/(stem+'.mat');assert path.is_file() and path.stat().st_size==native['project_mat_bytes'] and (Path(canonical)/stem).is_dir(),'API container cannot see final project folder'
  assert not any(l.storage_root_id==root.id and l.relative_path=='maliavko/DetecdivHub/projects/'+stem for l in p.locations),'Target catalog location already exists'
  original_ids=[l.id for l in p.locations]
  for loc in p.locations:loc.is_preferred=False
  location=ProjectLocation(project=p,storage_root=root,relative_path='maliavko/DetecdivHub/projects/'+stem,project_file_name=stem+'.mat',access_mode='readwrite',is_preferred=True);db.add(location);db.flush()
  metadata=dict(p.metadata_json or {});assert 'project_relocation_20261109' not in metadata;metadata.update(project_mat_abs=canonical+'/'+stem+'.mat',project_dir_abs=canonical+'/'+stem,project_rel_from_root='maliavko/DetecdivHub/projects/'+stem+'/'+stem)
  metadata['project_relocation_20261109']={'batch_id':str(b.id),'original_projects_preserved':True,'original_location_ids':original_ids,'final_linux_windows_reads_and_baselines_verified':True,'evidence':'reports/alexander-2024-20261105/'}
  p.metadata_json=metadata;p.project_mat_bytes=native['project_mat_bytes'];p.project_dir_bytes=native['project_dir_bytes'];p.total_bytes=native['total_bytes'];p.last_size_scan_at=datetime.now(timezone.utc)
  restored=dict(metadata)
  for key in ['project_mat_abs','project_dir_abs','project_rel_from_root','project_relocation_20261109']:
   if key in (old['columns']['metadata_json'] or {}):restored[key]=old['columns']['metadata_json'][key]
   else:restored.pop(key,None)
  assert restored==(old['columns']['metadata_json'] or {}) and digest(identity(p))==old['identity_sha256'],'Nonpath catalog content changed'
  changes.append({'project_id':str(p.id),'name':stem,'original_location_ids':original_ids,'new_location_id':location.id,'preferred_mat_path':str(path),'originals_preserved':True})
 db.flush()
 for row in changes:
  p=db.get(Project,uuid.UUID(row['project_id']));db.expire(p,['locations']);view=get_project(p.id,db,operator)
  assert len([x for x in view.locations if x.is_preferred])==1 and next(x for x in view.locations if x.is_preferred).project_mat_path==row['preferred_mat_path']
  old=next(x for x in a['before_projects'] if x['project_id']==str(p.id))
  for before in old['locations']:
   after=next(columns(l) for l in p.locations if l.id==before['id']);assert {k:str(v) for k,v in after.items() if k not in ['is_preferred','updated_at']}=={k:str(v) for k,v in before.items() if k not in ['is_preferred','updated_at']}
 meta=dict(b.metadata_json);meta.update(filesystem_publication=published['result.json'],hosts=hosts,cutover_changes=changes);b.metadata_json=meta
 b.status='projects_preferred_on_homes2_sources_retained';b.summary_json={'projects':16,'source_deleted':False,'original_projects_preserved':True,'project_api_detail_handlers_verified':16,'raw_catalog_changed':False,'raw_api_media_uris_changed':False}
 parent=db.get(StorageMigrationBatch,uuid.UUID('0382c23e-ad54-4ec9-9ad8-53cc79b64d97'),with_for_update=True);summary=dict(parent.summary_json);checkpoint=dict(summary.get('autonomous_checkpoint',{}));checkpoint.update(preferred_year1_project_copies=16,project_final_linux_windows_reads_verified=True,source_deleted=False,actual_2024_bytes_reclaimed=0);summary['autonomous_checkpoint']=checkpoint;parent.summary_json=summary
 print(json.dumps({'success':True,'batch_id':str(b.id),'committed_at':datetime.now(timezone.utc).isoformat(),'project_changes':changes,'source_deleted':False,'original_projects_preserved':True,'raw_catalog_or_api_uris_changed':False}))
'''
    result=remote_json(SSH,'webserver-labo',code,{'admission':a,'hosts':hosts,'published':published},container=True)
    write('year1-project-catalog-cutover-result.json',result);return result

def finalize(terminal_phase):
    a=read('year1-final-admission.json',True)
    code=COMMON+r'''
v=json.load(sys.stdin);a=v['admission']
with SessionLocal.begin() as db:
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']),with_for_update=True);assert b.metadata_json['holder']==a['holder']
 assert {x['id'] for x in b.metadata_json['leases']}=={x['id'] for x in a['leases']}
 released=[]
 for row in a['leases']:
  l=db.get(ProjectLock,uuid.UUID(row['id']),with_for_update=True);assert l.holder_key==a['holder'] and str(l.project_id)==row['project_id']
  if l.status=='active':l.status='released';l.released_at=datetime.now(timezone.utc);l.metadata_json={**(l.metadata_json or {}),'released_by_year1_publication_controller':True};released.append(str(l.id))
 if b.status!='projects_preferred_on_homes2_sources_retained':b.status='publication_or_final_hosts_review_required_sources_retained'
 b.metadata_json={**b.metadata_json,'publication_controller_terminal_phase':v['terminal_phase'],'own_leases_released':released}
 print(json.dumps({'success':True,'batch_id':str(b.id),'own_leases_released':released,'terminal_batch_status':b.status,'source_deleted':False,'checked_at':datetime.now(timezone.utc).isoformat()}))
'''
    result=remote_json(SSH,'webserver-labo',code,{'admission':a,'terminal_phase':terminal_phase},container=True)
    write('year1-publication-lease-finalization.json',result);return result
