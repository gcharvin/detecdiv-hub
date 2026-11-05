"""Audited URI cutover and retirement of the three exact API cache manifests."""
import base64
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import sys

from media_operations import HERE,RUNTIME,PREVIOUS,load
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from audit import SSH,remote_json
from activate_windows_mapping import remote

HOLDER='alexander-2023-api-cache-retirement-20261105'
WORK='/home/charvin-admin/storage-transfers/alexander-api-cache-retirement-20261105'


def save(name,value,private=False):
    with ((RUNTIME if private else HERE)/name).open('x',encoding='utf-8') as f:json.dump(value,f,indent=2);f.write('\n')


def plan():return json.loads((PREVIOUS/'runtime/api-homes2-cache-serving-plan-20261105.json').read_text(encoding='utf-8'))


def inventory():
    script=(HERE/'cache_manifest_native.py').read_bytes()
    code=r'''
import json,sys,base64,hashlib,os,subprocess,shlex
from pathlib import Path
v=json.load(sys.stdin);os.umask(0o077)
w=Path('/home/charvin-admin/storage-transfers/alexander-api-cache-retirement-20261105');assert not w.exists();w.mkdir(mode=0o700)
script=base64.b64decode(v['script']);(w/'cache_manifest_native.py').write_bytes(script)
loader='import base64;exec(compile(base64.b64decode('+repr(base64.b64encode(script).decode())+'),"cache_manifest_native.py","exec"))'
cmd=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-i','/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519','Gilles@10.20.11.250',shlex.join(['nice','-n','19','python3','-u','-c',loader])]
with (w/'source-before.jsonl').open('xb') as out,(w/'inventory-errors.log').open('xb') as err:
 p=subprocess.run(cmd,input=(json.dumps({'mode':'inventory'})+'\n').encode(),stdout=out,stderr=err,timeout=90)
assert p.returncode==0,'Inspect native inventory errors; no source deletion was requested'
data=(w/'source-before.jsonl').read_bytes();rows=[json.loads(line) for line in data.splitlines()]
assert len(rows)==1926 and sum(x['bytes'] for x in rows)==28981723684
expected={(r['year'],r['relative_path']) for r in v['plan']['references']}
assert {(r['year'],r['path']) for r in rows}==expected
result={'success':True,'files':1926,'bytes':28981723684,'manifest_sha256':hashlib.sha256(data).hexdigest(),'native_script_sha256':hashlib.sha256(script).hexdigest(),'private_directory':str(w),'rows':rows,'cache_files_deleted':0}
(w/'inventory-verification.json').write_text(json.dumps({k:value for k,value in result.items() if k!='rows'},indent=2)+'\n')
print(json.dumps(result))
'''
    result=remote_json(SSH,'detecdiv-server',code,{'script':base64.b64encode(script).decode(),'plan':plan()})
    save('source-inventory.json',result,private=True)
    summary={k:v for k,v in result.items() if k!='rows'};save('source-inventory-verification.json',summary);print(json.dumps(summary))


COMMON=r'''
import json,sys,uuid,hashlib
from datetime import datetime,timezone
from pathlib import Path
from sqlalchemy import select,text,inspect
from api.db import SessionLocal
from api.models import Artifact,RawDataset,Project,ProjectLock,ProjectRawLink,User,Job,IndexingJob,StorageMigrationBatch
from api.services.project_locks import acquire_client_edit_lease,release_project_lock
PREFIX='/data/.detecdiv-hub-api-cache/alexander-2023_'
def columns(x):return json.loads(json.dumps({p.key:getattr(x,p.key) for p in inspect(type(x)).column_attrs},default=str))
def ordered(rows):return sorted([columns(x) for x in rows],key=lambda x:str(x['id']))
def digest(x):return hashlib.sha256(json.dumps(x,sort_keys=True).encode()).hexdigest()
def raw_snapshot(r):return {'columns':columns(r),'locations':ordered(r.locations),'positions':ordered(r.positions),'project_links':ordered(r.project_links),'experiment_links':ordered(r.experiment_links)}
def project_snapshot(p):return {'columns':columns(p),'locations':ordered(p.locations),'acl':ordered(p.acl_entries),'groups':ordered(p.group_memberships),'notes':ordered(p.note_entries),'raw_links':ordered(p.raw_links)}
def current_refs(db):
 return {('artifact',str(a.id)):a.uri for a in db.scalars(select(Artifact).where(Artifact.uri.startswith(PREFIX)))}|{('display_settings',str(r.id)):r.display_settings_uri for r in db.scalars(select(RawDataset).where(RawDataset.display_settings_uri.startswith(PREFIX)))}
def safety(db,rids,pids,holder=None):
 now=datetime.now(timezone.utc)
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  params=json.dumps(j.params_json or {},default=str)
  assert j.raw_dataset_id not in rids and j.project_id not in pids and PREFIX not in params and not any(str(id) in params for id in rids|pids),'Affected job is active'
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running']))))
 locks=[l for l in db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_(pids),ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now]
 assert all(holder and l.holder_key==holder for l in locks),'Another affected project lease is active'
 return locks
def check_leases(db,admission):
 for row in admission['leases']:
  l=db.get(ProjectLock,uuid.UUID(row['id']),with_for_update=True)
  assert l.status=='active' and l.holder_key==admission['holder'] and str(l.project_id)==row['project_id'] and l.expires_at>datetime.now(timezone.utc)
'''


def scoped_projects():
    result=json.loads((PREVIOUS/'project3-catalog-cutover-result.json').read_text())
    return sorted({x['project_id'] for x in result['project_changes']}|{'1bbe0337-ba93-4c15-a7e9-39ae6df47979','aee22015-d2a7-4c78-a715-bf957c722c72','0e485efc-6b23-4faa-a316-4faea283477d'})


def admit():
    pairs=load('pairs-status.json');assert pairs.get('receipt_verified') and pairs['state']['success']
    manifest=load('source-inventory.json');assert manifest['success']
    bypath={(x['year'],x['relative_path']):x for x in pairs['receipt']}
    for row in manifest['rows']:
        p=bypath[row['year'],row['path']]
        # The VM SSHFS view reports whole SFTP seconds. The NAS inventory and
        # unlink leaf retain and compare the exact native nanoseconds.
        assert p['bytes']==row['bytes'] and p['cache_mtime_ns']==row['mtime_ns']//1_000_000_000*1_000_000_000
    code=COMMON+r'''
v=json.load(sys.stdin);refs=v['plan']['references'];rids={uuid.UUID(x['raw_id']) for x in refs};pids={uuid.UUID(x) for x in v['project_ids']}
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 db.execute(text("SELECT pg_advisory_xact_lock(hashtext('alexander-2023-api-cache-retirement-20261105'))"))
 assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==v['holder'])) is None
 pids|={l.project_id for l in db.scalars(select(ProjectRawLink).where(ProjectRawLink.raw_dataset_id.in_(rids)))}
 safety(db,rids,pids)
 assert current_refs(db)=={(r['kind'],r['id']):r['before'] for r in refs},'Live cache reference set changed'
 operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin',User.is_active.is_(True))).one()
 raws=[db.get(RawDataset,id,with_for_update=True) for id in sorted(rids,key=str)];assert len(raws)==109 and all(r.owner.user_key=='alexander' for r in raws)
 before_raws=[raw_snapshot(r) for r in raws];before_projects=[];leases=[]
 for id in sorted(pids,key=str):
  p=db.get(Project,id,with_for_update=True);assert p is not None
  before_projects.append(project_snapshot(p))
  l=acquire_client_edit_lease(db,project_id=id,owner=operator,holder_key=v['holder'],holder_host='codex-admin-workstation',ttl_seconds=14400,write_scope='api_media_storage_relocation',reason='Switch verified API media to homes2 and retire exact temporary caches',metadata_json={'original_projects_preserved':True,'scientific_files_unchanged':True})
  leases.append({'id':str(l.id),'project_id':str(id),'expires_at':l.expires_at.isoformat()})
 artifacts=[columns(db.get(Artifact,uuid.UUID(r['id']))) for r in refs if r['kind']=='artifact']
 batch=StorageMigrationBatch(owner_user_id=raws[0].owner_user_id,batch_name=v['holder'],source_kind='api_media_cache',source_path='/data/.detecdiv-hub-api-cache/alexander-2023_{1,2,3}',storage_root_name='user-homes2',host_scope='webserver-labo',root_type='api_media',strategy='verified_home_serving_then_exact_cache_retirement',status='admitted_for_home_uri_cutover',metadata_json={'holder':v['holder'],'leases':leases,'before_raws':before_raws,'before_projects':before_projects,'before_artifacts':artifacts,'plan':v['plan'],'pairs_receipt_sha256':v['pairs_state']['receipt_sha256'],'native_manifest_sha256':v['manifest']['manifest_sha256'],'native_script_sha256':v['manifest']['native_script_sha256'],'standing_authorization':'Retire verified Alexander 2023 duplicates and live API caches after homes2 serving works; preserve originals.'},summary_json={'files':1926,'bytes':28981723684,'raws':109,'projects_leased':len(pids),'cache_deleted':False})
 db.add(batch);db.flush()
 print(json.dumps({'success':True,'checked_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(batch.id),'holder':v['holder'],'leases':leases,'raw_ids':sorted(str(x) for x in rids),'project_ids':sorted(str(x) for x in pids),'manifest_sha256':v['manifest']['manifest_sha256'],'cache_files_deleted':0}))
'''
    admission=remote_json(SSH,'webserver-labo',code,{'plan':plan(),'holder':HOLDER,'project_ids':scoped_projects(),'pairs_state':pairs['state'],'manifest':{k:v for k,v in manifest.items() if k!='rows'}},container=True)
    save('admission.json',admission,private=True);save('admission-summary.json',{k:v for k,v in admission.items() if k not in {'raw_ids','project_ids'}});print(json.dumps({k:v for k,v in admission.items() if k not in {'raw_ids','project_ids'}}))


def apply():
    admission=load('admission.json')
    code=COMMON+r'''
v=json.load(sys.stdin);a=v['admission'];assert (datetime.now(timezone.utc)-datetime.fromisoformat(a['checked_at'])).total_seconds()<600
pairs={(x['year'],x['relative_path']):x for x in v['pairs']};assert len(pairs)==1926
rids={uuid.UUID(x) for x in a['raw_ids']};pids={uuid.UUID(x) for x in a['project_ids']}
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']),with_for_update=True);assert b.status=='admitted_for_home_uri_cutover'
 check_leases(db,a);assert len(safety(db,rids,pids,a['holder']))==len(a['leases'])
 meta=b.metadata_json;refs=meta['plan']['references']
 assert current_refs(db)=={(r['kind'],r['id']):r['before'] for r in refs}
 for before in meta['before_raws']:assert raw_snapshot(db.get(RawDataset,uuid.UUID(before['columns']['id'])))==before
 for before in meta['before_projects']:assert project_snapshot(db.get(Project,uuid.UUID(before['columns']['id'])))==before
 for before in meta['before_artifacts']:assert columns(db.get(Artifact,uuid.UUID(before['id'])))==before
 for ref in refs:
  path=Path(ref['after']);prior=pairs[ref['year'],ref['relative_path']]
  assert path.is_file() and not path.is_symlink() and path.stat().st_size==prior['bytes'] and abs(path.stat().st_mtime_ns-prior['home_mtime_ns'])<=100,'Home media changed or is not visible in the live API'
  obj=db.get(Artifact if ref['kind']=='artifact' else RawDataset,uuid.UUID(ref['id']),with_for_update=True)
  if ref['kind']=='artifact':obj.uri=ref['after']
  else:obj.display_settings_uri=ref['after']
 db.flush();assert not current_refs(db)
 after_raws=[raw_snapshot(db.get(RawDataset,uuid.UUID(x['columns']['id']))) for x in meta['before_raws']]
 after_artifacts=[columns(db.get(Artifact,uuid.UUID(x['id']))) for x in meta['before_artifacts']]
 for old,new in zip(meta['before_raws'],after_raws):
  for key in ['locations','positions','project_links','experiment_links']:assert old[key]==new[key]
  assert {k:value for k,value in old['columns'].items() if k not in {'display_settings_uri','updated_at'}}=={k:value for k,value in new['columns'].items() if k not in {'display_settings_uri','updated_at'}}
 for old,new in zip(meta['before_artifacts'],after_artifacts):assert {k:value for k,value in old.items() if k not in {'uri','updated_at'}}=={k:value for k,value in new.items() if k not in {'uri','updated_at'}}
 updated=dict(meta);updated.update(after_raws=after_raws,after_artifacts=after_artifacts);b.metadata_json=updated;b.status='home_uris_switched_verification_pending'
 print(json.dumps({'success':True,'committed_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(b.id),'uri_references_changed':1926,'raws_preserved':109,'positions_notes_acl_groups_links_preserved':True,'projects_unchanged':len(meta['before_projects']),'cache_files_deleted':0}))
'''
    result=remote_json(SSH,'webserver-labo',code,{'admission':admission,'pairs':load('pairs-status.json')['receipt']},container=True);save('uri-cutover-result.json',result);print(json.dumps(result))


CHECK_AFTER=r'''
def check_after(db,b,a):
 check_leases(db,a);rids={uuid.UUID(x) for x in a['raw_ids']};pids={uuid.UUID(x) for x in a['project_ids']}
 assert len(safety(db,rids,pids,a['holder']))==len(a['leases'])
 assert not current_refs(db)
 for before in b.metadata_json['after_raws']:assert raw_snapshot(db.get(RawDataset,uuid.UUID(before['columns']['id'])))==before,'Raw catalog changed'
 for before in b.metadata_json['before_projects']:assert project_snapshot(db.get(Project,uuid.UUID(before['columns']['id'])))==before,'Project changed'
 for before in b.metadata_json['after_artifacts']:assert columns(db.get(Artifact,uuid.UUID(before['id'])))==before,'Artifact changed'
 for ref in b.metadata_json['plan']['references']:
  obj=db.get(Artifact if ref['kind']=='artifact' else RawDataset,uuid.UUID(ref['id']))
  assert (obj.uri if ref['kind']=='artifact' else obj.display_settings_uri)==ref['after']
'''


def verify():
    proof=load('handlers-status.json');assert proof.get('receipt_verified') and proof['state']['phase']=='all_home_handlers_verified'
    code=COMMON+CHECK_AFTER+r'''
v=json.load(sys.stdin);a=v['admission'];proof=v['proof']
assert proof['state']['success'] and proof['receipt_verified'] and proof['state']['files_verified']==1926
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']),with_for_update=True);assert b.status=='home_uris_switched_verification_pending'
 check_after(db,b,a)
 meta=dict(b.metadata_json);meta['home_handlers_verification']=proof;b.metadata_json=meta;b.status='home_serving_verified_cache_retirement_pending'
 print(json.dumps({'success':True,'verified_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(b.id),'real_home_handlers_verified':1926,'bytes_verified':28981723684,'catalog_identity_and_relationships_preserved':True,'cache_files_deleted':0}))
'''
    result=remote_json(SSH,'webserver-labo',code,{'admission':load('admission.json'),'proof':{k:v for k,v in proof.items() if k!='receipt'}},container=True);save('home-serving-verification.json',result);print(json.dumps(result))


def prepare():
    assert json.loads((HERE/'home-serving-verification.json').read_text())['success']
    fixtures=json.loads((HERE/'native-retirement-fixtures.json').read_text());assert fixtures['success'] and len(fixtures['isolated_cases_passed'])==10
    native=(HERE/'cache_manifest_native.py').read_bytes();controller=(HERE/'retire_caches.py').read_bytes()
    assert hashlib.sha256(native).hexdigest()==fixtures['native_script_sha256']
    code=COMMON+CHECK_AFTER+r'''
import os
v=json.load(sys.stdin);a=v['admission']
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']),with_for_update=True);assert b.status=='home_serving_verified_cache_retirement_pending'
 check_after(db,b,a)
 content=b.metadata_json['home_handlers_verification'];assert content['state']['success'] and content['receipt_verified']
 assert content['state']['files_verified']==1926 and content['state']['bytes_verified']==28981723684
 assert os.statvfs('/homes2/maliavko/DetecdivHub').f_flag&os.ST_RDONLY
 gate={'success':True,'checked_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(b.id),'standing_authorization':True,'cache_only':True,'home_pairs_fully_verified':1926,'actual_home_handlers_fully_verified':1926,'bytes_fully_verified':28981723684,'api_home_bind_readonly':True,'manifest_sha256':b.metadata_json['native_manifest_sha256'],'native_script_sha256':v['native_sha256'],'controller_sha256':v['controller_sha256'],'holder':a['holder'],'leases':a['leases'],'lease_deadline_unix':min(datetime.fromisoformat(l['expires_at']).timestamp() for l in a['leases'])}
 meta=dict(b.metadata_json);meta['retirement_gate']=gate;b.metadata_json=meta;b.status='cache_retirement_prepared'
 print(json.dumps(gate))
'''
    gate=remote_json(SSH,'webserver-labo',code,{'admission':load('admission.json'),'native_sha256':hashlib.sha256(native).hexdigest(),'controller_sha256':hashlib.sha256(controller).hexdigest()},container=True)
    save('retirement-gates.json',gate);print(json.dumps(gate))


def launch():
    gate=json.loads((HERE/'retirement-gates.json').read_text())
    assert (datetime.now(timezone.utc)-datetime.fromisoformat(gate['checked_at'])).total_seconds()<120
    files={n:base64.b64encode((HERE/n).read_bytes()).decode() for n in ['cache_manifest_native.py','retire_caches.py']}
    code=r'''
import json,sys,base64,hashlib,os,subprocess,shlex
from datetime import datetime,timezone
from pathlib import Path
v=json.load(sys.stdin);gate=v['gate'];os.umask(0o077)
assert (datetime.now(timezone.utc)-datetime.fromisoformat(gate['checked_at'])).total_seconds()<120
base=Path('/home/charvin-admin/storage-transfers/alexander-api-cache-retirement-20261105');w=base/'retirement';assert base.is_dir() and not w.exists();w.mkdir(mode=0o700)
for n,b in v['files'].items():(w/n).write_bytes(base64.b64decode(b))
assert hashlib.sha256((w/'cache_manifest_native.py').read_bytes()).hexdigest()==gate['native_script_sha256']
assert hashlib.sha256((w/'retire_caches.py').read_bytes()).hexdigest()==gate['controller_sha256']
(w/'gates.json').write_text(json.dumps(gate,indent=2)+'\n')
script='#!/bin/bash\ncd '+shlex.quote(str(w))+'\npython3 -u retire_caches.py > controller.log 2>&1\nrc=$?\nprintf "%s\\n" "$rc" > controller.exit\nexit "$rc"\n'
(w/'launch.sh').write_text(script);task='alexander-api-cache-retirement-20261105'
assert subprocess.run(['tmux','has-session','-t',task],capture_output=True).returncode!=0
subprocess.run(['tmux','new-session','-d','-s',task,'bash '+shlex.quote(str(w/'launch.sh'))],check=True)
print(json.dumps({'started':True,'task':task,'private_directory':str(w),'batch_id':gate['batch_id'],'files':1926,'bytes':28981723684}))
'''
    result=remote_json(SSH,'detecdiv-server',code,{'gate':gate,'files':files});save('retirement-launch.json',result);print(json.dumps(result))


def status():
    result=remote('linux',r'''
import json
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-api-cache-retirement-20261105/retirement');out={}
for name in ['state.json','controller.exit','controller.log','native-errors.log']:
 p=w/name
 if p.exists():
  if name.endswith('.json'):out[name]=json.loads(p.read_text())
  else:
   with p.open('rb') as f:f.seek(max(0,p.stat().st_size-2200));out[name]=f.read().decode(errors='replace')
print(json.dumps(out))
''',timeout=35)
    (RUNTIME/'retirement-status.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');print(json.dumps(result))


def receipt():
    result=remote('linux',r'''
import json,hashlib
from pathlib import Path
base=Path('/home/charvin-admin/storage-transfers/alexander-api-cache-retirement-20261105');w=base/'retirement'
assert (w/'controller.exit').read_text().strip()=='0'
s=json.loads((w/'state.json').read_text());assert s['phase']=='cache_manifest_retired_post_checks_pending'
expected={(r['year'],r['path']):r['bytes'] for line in (base/'source-before.jsonl').read_text().splitlines() if (r:=json.loads(line))}
seen=set();size=0;h=hashlib.sha256();complete=None
for line in (w/'unlink-receipt.jsonl').open('rb'):
 h.update(line);r=json.loads(line)
 if r['event']=='unlinked':
  key=(r['year'],r['path']);assert key not in seen and expected[key]==r['bytes'];seen.add(key);size+=r['bytes']
 elif r['event']=='complete':assert complete is None;complete=r
 else:assert r['event']=='preflight_passed'
assert seen==expected.keys() and len(seen)==1926 and size==28981723684
assert complete==s['result'] and h.hexdigest()==s['receipt_sha256']
assert (w/'native-errors.log').stat().st_size==0
print(json.dumps({'success':True,'state':s,'each_manifest_file_has_unique_unlink_receipt':True,'all_three_cache_payloads_empty':True}))
''',timeout=40)
    save('cache-retirement-receipt-verification.json',result);print(json.dumps(result))


def finish():
    retired=json.loads((HERE/'cache-retirement-receipt-verification.json').read_text());assert retired['success']
    post=load('post-retirement-status.json');assert post.get('receipt_verified') and post['state']['phase']=='post_retirement_home_handlers_verified'
    clients={host:json.loads((HERE/(host+'-client-verification.json')).read_text()) for host in ['linux','windows']}
    for value in clients.values():assert value['success'] and value['cache_retirement_receipt_sha256']==retired['state']['receipt_sha256'] and len(value['files'])==6
    assert clients['windows']['windows_identity'].casefold()=='gmgm\\charvin-admin' and clients['windows']['session_id']==1
    code=COMMON+CHECK_AFTER+r'''
v=json.load(sys.stdin);a=v['admission'];retired=v['retired'];post=v['post'];result=retired['state']['result']
assert retired['success'] and retired['each_manifest_file_has_unique_unlink_receipt'] and retired['all_three_cache_payloads_empty']
assert post['state']['success'] and post['receipt_verified'] and post['state']['files_verified']==1926 and post['state']['bytes_verified']==28981723684
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']),with_for_update=True);assert b.status=='cache_retirement_prepared'
 check_after(db,b,a)
 assert retired['state']['batch_id']==str(b.id) and retired['state']['manifest_sha256']==b.metadata_json['native_manifest_sha256']
 assert result['files_unlinked']==1926 and result['logical_bytes_unlinked']==28981723684
 released=[]
 for row in a['leases']:
  l=db.get(ProjectLock,uuid.UUID(row['id']),with_for_update=True);assert l.holder_key==a['holder'];release_project_lock(db,lock=l);released.append(str(l.id))
 meta=dict(b.metadata_json);meta.update(retirement_receipt=retired,post_retirement_home_handlers=post,client_reads=v['clients'],own_leases_released=released);b.metadata_json=meta
 b.status='api_cache_retired_verified'
 summary=dict(b.summary_json);summary.update(cache_deleted=True,files_unlinked=1926,logical_bytes_unlinked=28981723684,measured_free_bytes_increase=result['measured_free_bytes_increase'],free_bytes_after=result['free_bytes_after'],real_home_handlers_verified_after_retirement=1926,original_projects_preserved=True);b.summary_json=summary
 print(json.dumps({'success':True,'finished_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(b.id),'files_unlinked':1926,'logical_bytes_unlinked':28981723684,'measured_cache_free_bytes_increase':result['measured_free_bytes_increase'],'free_bytes_after':result['free_bytes_after'],'cumulative_2023_measured_free_bytes_increase':10282445193216+result['measured_free_bytes_increase'],'api_handlers_served_from_homes2':1926,'linux_and_actual_windows_clients_verified':True,'catalog_identity_and_relationships_preserved':True,'scientific_files_changed':False,'own_leases_released':released,'all_alexander_2023_primary_payload_retired':True}))
'''
    result=remote_json(SSH,'webserver-labo',code,{'admission':load('admission.json'),'retired':retired,'post':{k:v for k,v in post.items() if k!='receipt'},'clients':clients},container=True)
    save('final-verification.json',result)
    code=r'''
import json,sys
from pathlib import Path
r=json.load(sys.stdin);w=Path('/home/charvin-admin/storage-transfers/alexander-api-cache-retirement-20261105/retirement')
s=json.loads((w/'state.json').read_text());assert s['phase']=='cache_manifest_retired_post_checks_pending' and s['batch_id']==r['batch_id']
(w/'catalog-finalization.json').write_text(json.dumps(r,indent=2)+'\n');s.update(phase='api_cache_retired_verified',catalog_finalized_at=r['finished_at'])
p=w/'state.tmp';p.write_text(json.dumps(s,indent=2)+'\n');p.replace(w/'state.json')
print(json.dumps({'private_state_finalized':True}))
'''
    print(json.dumps(remote_json(SSH,'detecdiv-server',code,result)));print(json.dumps(result))


if __name__=='__main__':
    {'inventory':inventory,'admit':admit,'apply':apply,'verify':verify,'prepare':prepare,'launch':launch,'status':status,'receipt':receipt,'finish':finish}[sys.argv[1]]()
