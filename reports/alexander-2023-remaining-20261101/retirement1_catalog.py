"""Audited residual-source retirement; no catalog identity is replaced."""
import ast
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent/'homes2-access-audit-20261101'))
from audit import remote_json, SSH
from activate_windows_mapping import remote
sys.path.insert(0, str(HERE.parent/'alexander-2023-cutover-20261101'))
from cutover import COMMON as BASE_COMMON

COMMON = BASE_COMMON.replace('2023_2', '2023_1').replace(
    "FINAL='/homes2/maliavko/DetecdivHub/raw/2023_1'",
    "FINAL='/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_1'")
WORK = '/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101'
RAW_ID = '094444cd-1aa8-49d6-b6ab-d7c7df730b96'


def save(name, value):
    (HERE/name).write_text(json.dumps(value, indent=2, default=str)+'\n')
    return value


def prepare():
    alex = json.loads((HERE/'project1-catalog-finalization.json').read_text())
    gilles = json.loads((HERE/'gilles-json-catalog-finalization.json').read_text())
    target = json.loads((HERE/'residual-destination-revalidation.json').read_text())
    tests = json.loads((HERE/'source1-retirement-guard-tests.json').read_text())
    assert alex['success'] and gilles['success'] and target['success'] and tests['success']
    tree = ast.parse((HERE/'retire_remaining_2023_1.py').read_text(encoding='utf-8'))
    approval = ast.literal_eval(next(n.value for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'APPROVAL' for t in n.targets)))
    runner_sha = hashlib.sha256((HERE/'retire_remaining_2023_1.py').read_bytes()).hexdigest()
    payload = {'alex': alex, 'gilles': gilles, 'target': target, 'approval': approval, 'runner_sha256': runner_sha}
    code = COMMON+r'''
from pathlib import Path
from api.models import Artifact
from api.services.project_locks import acquire_client_edit_lease
v=json.load(sys.stdin)
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 operator=db.scalar(select(User).where(User.user_key=='gilles',User.role=='admin'))
 assert operator is not None
 a=db.get(Project,uuid.UUID(v['alex']['project_id']),with_for_update=True);g=db.get(Project,uuid.UUID(v['gilles']['project_id']),with_for_update=True)
 assert a.metadata_json['path_verification_20261101']['linux_windows_verified'] and g.metadata_json['raw_path_verification_20261101']['linux_windows_verified']
 gl=db.get(ProjectLock,uuid.UUID(v['gilles']['lease_retained']),with_for_update=True)
 assert gl.status=='active' and gl.holder_key=='alex-remaining-20261101-gilles-json' and gl.expires_at>datetime.now(timezone.utc)
 raws=list(db.scalars(select(RawDataset)));affected=[r for r in raws if source_paths(r) or any(compose_storage_path(l.storage_root.path_prefix,l.relative_path).startswith(FINAL+'/') for l in r.locations)]
 ids={str(r.id) for r in affected};pids={str(a.id),str(g.id)}|{str(l.project_id) for r in affected for l in r.project_links}
 assert len(affected)==76 and all(r.owner.user_key=='alexander' for r in affected)
 markers=ids|pids|{a.project_key,g.project_key}
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  data=json.dumps(j.params_json or {},default=str)
  assert str(j.project_id) not in pids and str(j.raw_dataset_id) not in ids and OLD not in data and FINAL not in data and not any(x in data for x in markers),'Affected job is active'
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running']))))
 for lock in db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_([uuid.UUID(x) for x in pids]),ProjectLock.status=='active')):
  if lock.expires_at is None or lock.expires_at>datetime.now(timezone.utc):assert lock.id==gl.id,'Another affected edit lease exists'
 legacy_artifacts=[x for x in db.scalars(select(Artifact)) if (x.uri or '').startswith(OLD+'/')]
 legacy_settings=[r for r in raws if (r.display_settings_uri or '').startswith(OLD+'/')]
 assert len(legacy_artifacts)==150 and all(x.uri.startswith(OLD+'/160623_test/') for x in legacy_artifacts)
 assert len(legacy_settings)==6 and all(r.display_settings_uri.startswith(OLD+'/160623_test/') for r in legacy_settings)
 assert not Path(OLD+'/160623_test').exists()
 assert not [r for r in raws if (r.archive_uri or '').startswith(OLD+'/')]
 cache='/data/.detecdiv-hub-api-cache/alexander-2023_1/'
 assets=[x for x in db.scalars(select(Artifact)) if (x.uri or '').startswith(cache)]
 settings=[r for r in raws if (r.display_settings_uri or '').startswith(cache)]
 assert len(assets)==710 and len(settings)==64
 assert all(Path(x.uri).is_file() for x in assets) and all(Path(r.display_settings_uri).is_file() for r in settings)
 r=db.get(RawDataset,uuid.UUID('094444cd-1aa8-49d6-b6ab-d7c7df730b96'),with_for_update=True)
 original=[l for l in r.locations if compose_storage_path(l.storage_root.path_prefix,l.relative_path).startswith(OLD+'/')]
 assert {l.id for l in original}=={3703,4288} and not any(l.is_preferred for l in original)
 assert [l.id for l in r.locations if l.is_preferred]==[5512]
 assert compose_storage_path(next(l for l in r.locations if l.is_preferred).storage_root.path_prefix,next(l for l in r.locations if l.is_preferred).relative_path).startswith(FINAL+'/')
 name='alexander-2023-1-residual-source-retirement-20261101';assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==name)) is None
 al=acquire_client_edit_lease(db,project_id=a.id,owner=operator,holder_key=name,holder_host='detecdiv-server',ttl_seconds=3600,write_scope='storage_relocation',reason='Retire verified 2023_1 residual source',metadata_json={'approval':v['approval']})
 baseline=json.loads(json.dumps(snapshot(r),default=str));other=other_locations_digest(db,[str(r.id)])
 batch=StorageMigrationBatch(owner_user_id=r.owner_user_id,batch_name=name,source_kind='raw_root',source_path=OLD,storage_root_name='user-homes2',host_scope='detecdiv-server',root_type='raw_root',strategy='retire_verified_residual_manifest',status='source_retirement_prepared',metadata_json={'operator':'gilles','approval':v['approval'],'raw_before':baseline,'other_locations':other,'alex_project':v['alex'],'gilles_project':v['gilles'],'project_leases':[str(al.id),str(gl.id)],'residual_target_verification':v['target'],'already_absent_source_files':662891,'already_missing_phantom_artifacts_unchanged':150,'already_missing_phantom_settings_unchanged':6},summary_json={'manifest_files':87364,'manifest_bytes':737182660223,'source_deleted':False,'api_cache_bytes_retained':11051646071})
 db.add(batch);db.flush();db.add(StorageMigrationItem(batch_id=batch.id,item_type='raw_dataset',legacy_path=OLD,display_name='2023_1 verified residual manifest',status='source_retirement_prepared',action='unlink_verified_residual_manifest',metadata_json={'raw_id':str(r.id),'source_location_ids':[3703,4288],'preferred_location_id':5512}))
 print(json.dumps({'checked_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(batch.id),'approval':v['approval'],'both_projects_linux_windows_validated':True,'api_dependencies_resolved':True,'original_projects_preserved':True,'gilles_project_retained_under_gilles':True,'retirement_project_leases_active':True,'project_leases':[str(al.id),str(gl.id)],'retirement_runner_sha256':v['runner_sha256'],'source_location_ids':[3703,4288],'preferred_location_id':5512}))
'''
    gate = remote_json(SSH, 'webserver-labo', code, payload, container=True)
    save('source1-retirement-execution-gates.json', gate)
    code = r'''
from pathlib import Path
import json,sys,hashlib,subprocess,shlex
gate=json.load(sys.stdin);w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101')
assert hashlib.sha256((w/'retire_remaining_2023_1.py').read_bytes()).hexdigest()==gate['retirement_runner_sha256']
p=w/'source1-retirement-execution-gates.json';assert not p.exists();p.write_text(json.dumps(gate,indent=2)+'\n')
script=w/'source1-retirement.sh';assert not script.exists();script.write_text(shlex.join(['/usr/bin/python3',str(w/'retire_remaining_2023_1.py')])+' > '+shlex.quote(str(w/'source1-retirement-controller.log'))+' 2>&1\n')
subprocess.run(['tmux','new-session','-d','-s','alex2023-1-source-retirement-20261101','sh',str(script)],check=True)
print(json.dumps({'started':True,'batch_id':gate['batch_id'],'year':'2023_1','files':87364,'bytes':737182660223}))
'''
    launch = remote_json(SSH, 'detecdiv-server', code, gate)
    save('source1-retirement-launch.json', launch); print(json.dumps(launch))


def status():
    r = remote('linux', r'''
from pathlib import Path
import json
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101');p=w/'2023_1/retirement-state.json'
r={'state':json.loads(p.read_text()) if p.exists() else None}
for n in ['source1-retirement-controller.log','2023_1/unlink.errors.log']:
 p=w/n
 if p.exists():
  with p.open('rb') as f:f.seek(max(0,p.stat().st_size-1800));r[n]=f.read().decode(errors='replace')
print(json.dumps(r))
''', timeout=50)
    save('source1-retirement-status.json', r)
    display = json.loads(json.dumps(r))
    if display['state']:
        for field in ['result', 'native_preflight']:
            if field in display['state']:
                for k in ['excluded_system_directories', 'retained_nonempty_directories']:
                    if k in display['state'][field]: display['state'][field][k] = len(display['state'][field][k])
    print(json.dumps(display))


def refresh_encoding():
    """Correct only the known prelaunch Windows source-decoding error."""
    gate = json.loads((HERE/'source1-retirement-execution-gates.json').read_text(encoding='utf-8'))
    tree = ast.parse((HERE/'retire_remaining_2023_1.py').read_text(encoding='utf-8'))
    approval = ast.literal_eval(next(n.value for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'APPROVAL' for t in n.targets)))
    assert gate['approval'] == approval.encode('utf-8').decode('cp1252')
    guard = remote('linux', r'''
from pathlib import Path
import json
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101')
assert not (w/'2023_1/retirement-state.json').exists() and not (w/'2023_1/retirement-request.json').exists()
assert "assert gate['approval'] == APPROVAL" in (w/'source1-retirement-controller.log').read_text()
print(json.dumps({'no_native_retirement_started':True}))
''', timeout=40)
    assert guard['no_native_retirement_started']
    code = COMMON+r'''
v=json.load(sys.stdin);gate=v['gate'];approval=v['approval']
with SessionLocal.begin() as db:
 b=db.get(StorageMigrationBatch,uuid.UUID(gate['batch_id']),with_for_update=True)
 assert b.status=='source_retirement_prepared' and b.metadata_json['approval']==gate['approval']
 r=db.get(RawDataset,uuid.UUID('094444cd-1aa8-49d6-b6ab-d7c7df730b96'),with_for_update=True)
 assert digest(snapshot(r))==digest(b.metadata_json['raw_before']) and other_locations_digest(db,[str(r.id)])==b.metadata_json['other_locations']
 for lease_id in gate['project_leases']:
  lease=db.get(ProjectLock,uuid.UUID(lease_id));assert lease.status=='active' and lease.expires_at>datetime.now(timezone.utc)
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  data=json.dumps(j.params_json or {},default=str)
  assert OLD not in data and FINAL not in data and str(j.raw_dataset_id)!=str(r.id) and str(j.project_id) not in {b.metadata_json['alex_project']['project_id'],b.metadata_json['gilles_project']['project_id']}
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running']))))
 m=dict(b.metadata_json);m['approval']=approval;m['prelaunch_encoding_correction']={'prior_gate_approval_sha256':hashlib.sha256(gate['approval'].encode()).hexdigest(),'correct_approval_sha256':hashlib.sha256(approval.encode()).hexdigest(),'native_retirement_had_not_started':True};b.metadata_json=m
 gate['approval']=approval;gate['checked_at']=datetime.now(timezone.utc).isoformat();gate['prelaunch_encoding_corrected']=True
 print(json.dumps(gate))
'''
    new = remote_json(SSH, 'webserver-labo', code, {'gate': gate, 'approval': approval}, container=True)
    save('attempt1-source1-retirement-execution-gates.json', gate)
    save('source1-retirement-execution-gates.json', new)
    code = r'''
from pathlib import Path
import json,sys,subprocess
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101');gate=json.load(sys.stdin)
assert not (w/'2023_1/retirement-state.json').exists() and not (w/'2023_1/retirement-request.json').exists()
for n in ['source1-retirement-execution-gates.json','source1-retirement-controller.log']:
 p=w/n;q=w/('attempt1-'+n);assert not q.exists();p.rename(q)
(w/'source1-retirement-execution-gates.json').write_text(json.dumps(gate,indent=2)+'\n')
subprocess.run(['tmux','new-session','-d','-s','alex2023-1-source-retirement-20261101','sh',str(w/'source1-retirement.sh')],check=True)
print(json.dumps({'restarted':True,'prelaunch_encoding_corrected':True,'batch_id':gate['batch_id']}))
'''
    print(json.dumps(remote_json(SSH, 'detecdiv-server', code, new)))


def finalize():
    gate = json.loads((HERE/'source1-retirement-execution-gates.json').read_text())
    receipt = remote('linux', r'''
from pathlib import Path
import json,hashlib
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101/2023_1');s=json.loads((w/'retirement-state.json').read_text())
assert s['phase']=='source_manifest_retired_catalog_finalization_pending'
expected={r['path']:r['bytes'] for line in (w/'source-current.jsonl').read_text().splitlines() if (r:=json.loads(line))['kind']=='file'}
seen=set();size=0;complete=None;h=hashlib.sha256()
with (w/'unlink-receipt.jsonl').open('rb') as f:
 for line in f:
  h.update(line);r=json.loads(line)
  if r['event']=='unlinked':
   assert r['path'] not in seen and expected[r['path']]==r['bytes'];seen.add(r['path']);size+=r['bytes']
  elif r['event']=='complete':complete=r
assert seen==set(expected) and len(seen)==87364 and size==737182660223
assert h.hexdigest()==s['receipt_sha256'] and complete==s['result'] and complete['remaining_source_payload_files']==0
print(json.dumps({'success':True,'state':s,'receipt_verified':True,'all_manifest_paths_verified':True}))
''', timeout=60)
    save('source1-retirement-receipt-verification.json', receipt)
    code = COMMON+r'''
from pathlib import Path
from api.services.project_locks import release_project_lock
from api.routes_raw_datasets import raw_dataset_detail_view,get_raw_preview_artifact_content,get_raw_dataset_display_settings
from api.models import Artifact
v=json.load(sys.stdin);gate=v['gate'];receipt=v['receipt'];result=receipt['state']['result']
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"));b=db.get(StorageMigrationBatch,uuid.UUID(gate['batch_id']),with_for_update=True)
 assert b.status=='source_retirement_prepared'
 r=db.get(RawDataset,uuid.UUID('094444cd-1aa8-49d6-b6ab-d7c7df730b96'),with_for_update=True)
 assert digest(snapshot(r))==digest(b.metadata_json['raw_before'])
 assert other_locations_digest(db,[str(r.id)])==b.metadata_json['other_locations']
 for l in list(r.locations):
  if l.id in {3703,4288}:db.delete(l)
 db.flush();db.expire(r,['locations']);after=json.loads(json.dumps(snapshot(r),default=str));before=b.metadata_json['raw_before']
 assert after['columns']==before['columns'] and after['project_links']==before['project_links'] and after['experiment_links']==before['experiment_links'] and after['positions']==before['positions']
 assert after['locations']==[x for x in before['locations'] if x['id'] not in {3703,4288}]
 assert [x['id'] for x in after['locations'] if x['is_preferred']]==[5512]
 assert other_locations_digest(db,[str(r.id)])==b.metadata_json['other_locations']
 operator=db.scalar(select(User).where(User.user_key=='gilles',User.role=='admin'));verified=0
 cache='/data/.detecdiv-hub-api-cache/alexander-2023_1/'
 for a in db.scalars(select(Artifact).where(Artifact.uri.startswith(cache))):
  response=get_raw_preview_artifact_content(a.id,db,operator);assert Path(response.path).is_file();verified+=1
 for raw in db.scalars(select(RawDataset).where(RawDataset.display_settings_uri.startswith(cache))):
  response=get_raw_dataset_display_settings(raw.id,db,operator);assert Path(response.path).is_file();verified+=1
 assert verified==774
 for lease_id in gate['project_leases']:
  lease=db.get(ProjectLock,uuid.UUID(lease_id),with_for_update=True)
  assert lease.status=='active' and lease.holder_key in {'alex-remaining-20261101-gilles-json','alexander-2023-1-residual-source-retirement-20261101'}
  release_project_lock(db,lock=lease)
  p=db.get(Project,lease.project_id,with_for_update=True);m=dict(p.metadata_json)
  key='path_verification_20261101' if p.id==uuid.UUID(b.metadata_json['alex_project']['project_id']) else 'raw_path_verification_20261101'
  info=dict(m[key]);info['primary_source_retired_at']=datetime.now(timezone.utc).isoformat();info['source_retirement_batch_id']=str(b.id)
  if 'raw_primary_retirement_deferred' in info:info['raw_primary_retirement_deferred']=None
  m[key]=info;p.metadata_json=m
 for key in ['alex_project','gilles_project']:
  previous=db.get(StorageMigrationBatch,uuid.UUID(b.metadata_json[key]['batch_id']))
  previous.status='verified_complete_primary_source_retired';s=dict(previous.summary_json or {});s['primary_source_deleted']=True
  if 'raw_source_deleted' in s:s['raw_source_deleted']=True
  s['source_retirement_batch_id']=str(b.id);previous.summary_json=s
 b.status='source_retired_verified';meta=dict(b.metadata_json);meta['retirement_receipt']=receipt;meta['raw_after']=after;b.metadata_json=meta
 summary=dict(b.summary_json);summary.update(source_deleted=True,files_unlinked=87364,logical_bytes_unlinked=737182660223,measured_free_bytes_increase=result['measured_free_bytes_increase'],free_bytes_after=result['free_bytes_after'],source_location_rows_retired=2,api_content_handlers_verified=verified,original_projects_preserved=True);b.summary_json=summary
 for item in b.items:item.status='source_retired_verified'
 print(json.dumps({'success':True,'committed_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(b.id),'files_unlinked':87364,'logical_bytes_unlinked':737182660223,'measured_free_bytes_increase':result['measured_free_bytes_increase'],'free_bytes_after':result['free_bytes_after'],'preferred_location_id':5512,'source_location_rows_retired':[3703,4288],'api_content_handlers_verified':verified,'both_project_leases_released':True,'original_projects_preserved':True,'other_locations':b.metadata_json['other_locations']}))
'''
    result = remote_json(SSH, 'webserver-labo', code, {'gate': gate, 'receipt': receipt}, container=True)
    save('source1-retirement-catalog-finalization.json', result); print(json.dumps(result))
    finish_state()


def finish_state():
    result = json.loads((HERE/'source1-retirement-catalog-finalization.json').read_text(encoding='utf-8'))
    assert result['success'] and result['both_project_leases_released'] and result['files_unlinked'] == 87364
    code = r'''
from pathlib import Path
from datetime import datetime,timezone
import sys,json
r=json.load(sys.stdin);w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101/2023_1')
s=json.loads((w/'retirement-state.json').read_text());assert s['batch_id']==r['batch_id'] and s['phase'] in {'source_manifest_retired_catalog_finalization_pending','source_retired_verified'}
p=w/'catalog-finalization.json'
if p.exists():assert json.loads(p.read_text())==r
else:p.write_text(json.dumps(r,indent=2)+'\n')
s['phase']='source_retired_verified';s['catalog_finalized_at']=r['committed_at'];s['heartbeat_at']=datetime.now(timezone.utc).isoformat()
p=w/'retirement-state.tmp';p.write_text(json.dumps(s,indent=2)+'\n');p.replace(w/'retirement-state.json')
print(json.dumps({'phase':s['phase'],'batch_id':s['batch_id']}))
'''
    print(json.dumps(remote_json(SSH, 'detecdiv-server', code, result)))


if __name__ == '__main__':
    {'prepare': prepare, 'status': status, 'finalize': finalize, 'refresh_encoding': refresh_encoding, 'finish_state': finish_state}[sys.argv[1]]()
