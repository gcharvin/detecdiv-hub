"""Admit, launch and audit the manifest-bounded 2023_3 source retirement."""
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

from project3_cutover_operations import COMMON
from project3_final_operations import HERE,RUNTIME,WORK,load,save,encoded
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote
from audit import SSH,remote_json


def prepare():
    verified=load('project3-catalog-cutover-verification.json');assert verified['success']
    admission=load('project3-cutover-admission.json',private=True)
    fixtures=load('project3-retirement-guard-fixtures.json');assert fixtures['success'] and len(fixtures['isolated_cases_passed'])==7
    target=remote('linux',r'''
import json,hashlib
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101/project3-raw-destination-check-after-timestamp-repair-20261105')
assert (w/'controller.exit').read_text().strip()=='0'
s=json.loads((w/'state.json').read_text());assert s['phase']=='native_destination_metadata_verified' and s['success']
assert hashlib.sha256((w/'receipt.json').read_bytes()).hexdigest()==s['receipt_sha256']
r=s['result'];assert r['success'] and r['files']==708590 and r['bytes']==5967100516555 and r['all_owned_by_maliavko']
print(json.dumps(s))
''',timeout=35)
    save('project3-raw-destination-completion-verification.json',target)
    controller=(HERE/'retire_project3_verified_source.py').read_bytes();native=(HERE/'retire_project3_manifest_native.py').read_bytes()
    payload={'admission':admission,'verified':verified,'target':target,'controller_sha256':hashlib.sha256(controller).hexdigest(),'native_script_sha256':hashlib.sha256(native).hexdigest()}
    code=COMMON+r'''
v=json.load(sys.stdin);a=v['admission'];t=v['target'];ids={uuid.UUID(x['project_id']) for x in a['leases']}
assert t['success'] and (datetime.now(timezone.utc)-datetime.fromisoformat(t['finished_at'])).total_seconds()<3600
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']),with_for_update=True);assert b.status=='catalog_and_api_verified_source_retirement_pending'
 own_leases(db,a)
 raws=[db.get(RawDataset,uuid.UUID(x['columns']['id']),with_for_update=True) for x in b.metadata_json['after_raws']]
 assert safety(db,ids,raws,a['holder'])['own_locks']==11
 for r,before in zip(raws,b.metadata_json['after_raws']):assert raw_snapshot(r)==before
 for before in b.metadata_json['after_projects']:
  p=db.get(Project,uuid.UUID(before['project_id']),with_for_update=True)
  assert columns(p)==before['columns'] and identity(p)==before['identity'] and [columns(l) for l in p.locations]==before['locations']
 pub=db.get(StorageMigrationBatch,uuid.UUID(b.metadata_json['publication_batch_id']))
 raw=pub.metadata_json['raw_verification']['raw_state'];assert raw['full_sha1_passed'] and raw['source_stability_verified']
 assert b.metadata_json['final_hosts']['success']
 content=b.metadata_json['api_content_verification'];assert content['state']['success'] and content['receipt_verified']
 assert content['state']['handlers_verified']==798 and content['state']['bytes_verified']==12055348763
 assert not list(db.scalars(select(Artifact.id).where(Artifact.uri.startswith(OLD+'/'))))
 assert not list(db.scalars(select(RawDataset.id).where(RawDataset.display_settings_uri.startswith(OLD+'/'))))
 gate={'success':True,'checked_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(b.id),'holder':a['holder'],'leases':a['leases'],'lease_deadline_unix':min(datetime.fromisoformat(x['expires_at']).timestamp() for x in a['leases']),'verified_cutover':True,'standing_source_retirement_authorization':True,'original_projects_preserved':True,'linux_windows_final_paths_verified':True,'actual_api_handlers_fully_verified':798,'raw_full_sha1_and_source_stability_passed':True,'source_manifest_sha256':'4abdfbfff41264b9bc069c3968ac742ac06fa9b7c52127bbf4369a7d95306fd3','destination_receipt_sha256':t['receipt_sha256'],'controller_sha256':v['controller_sha256'],'native_script_sha256':v['native_script_sha256']}
 meta=dict(b.metadata_json);meta['source_retirement_gate']=gate;b.metadata_json=meta;b.status='source_retirement_prepared'
 print(json.dumps(gate))
'''
    gate=remote_json(SSH,'webserver-labo',code,payload,container=True)
    save('project3-source-retirement-gates.json',gate)
    print(json.dumps(gate))


def launch():
    gate=load('project3-source-retirement-gates.json')
    assert (datetime.now(timezone.utc)-datetime.fromisoformat(gate['checked_at'])).total_seconds()<120
    files={n:base64.b64encode((HERE/n).read_bytes()).decode() for n in ['retire_project3_verified_source.py','retire_project3_manifest_native.py']}
    code='V='+repr(encoded({'gate':gate,'files':files}))+'\n'+r'''
import json,base64,hashlib,os,subprocess,shlex
from datetime import datetime,timezone
from pathlib import Path
os.umask(0o077);v=json.loads(base64.b64decode(V));gate=v['gate']
assert (datetime.now(timezone.utc)-datetime.fromisoformat(gate['checked_at'])).total_seconds()<120
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101');d=w/'project3-source-retirement-20261105'
assert not d.exists(),'Review existing retirement before any retry'
d.mkdir(mode=0o700)
for n,b in v['files'].items():(d/n).write_bytes(base64.b64decode(b))
assert hashlib.sha256((d/'retire_project3_verified_source.py').read_bytes()).hexdigest()==gate['controller_sha256']
assert hashlib.sha256((d/'retire_project3_manifest_native.py').read_bytes()).hexdigest()==gate['native_script_sha256']
(d/'gates.json').write_text(json.dumps(gate,indent=2)+'\n')
launch='#!/bin/bash\ncd '+shlex.quote(str(d))+'\npython3 -u retire_project3_verified_source.py > controller.log 2>&1\nrc=$?\nprintf "%s\\n" "$rc" > controller.exit\nexit "$rc"\n'
(d/'launch.sh').write_text(launch);task='alex2023-3-source-retirement-20261105'
assert subprocess.run(['tmux','has-session','-t',task],capture_output=True).returncode!=0
subprocess.run(['tmux','new-session','-d','-s',task,'bash '+shlex.quote(str(d/'launch.sh'))],check=True)
print(json.dumps({'started':True,'task':task,'private_directory':str(d),'batch_id':gate['batch_id'],'files':708590,'logical_bytes':5967100516555}))
'''
    result=remote('linux',code,timeout=40);save('project3-source-retirement-launch.json',result);print(json.dumps(result))


def status():
    result=remote('linux',r'''
import json
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101/project3-source-retirement-20261105');out={}
for n in ['state.json','controller.exit','controller.log','native-errors.log']:
 p=w/n
 if p.exists():
  if n.endswith('.json'):out[n]=json.loads(p.read_text())
  else:
   with p.open('rb') as f:f.seek(max(0,p.stat().st_size-2200));out[n]=f.read().decode(errors='replace')
print(json.dumps(out))
''',timeout=35)
    (RUNTIME/'project3-source-retirement-status.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result))


def receipt():
    result=remote('linux',r'''
import json,hashlib
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101');d=w/'project3-source-retirement-20261105'
assert (d/'controller.exit').read_text().strip()=='0'
s=json.loads((d/'state.json').read_text());assert s['phase']=='source_manifest_retired_catalog_finalization_pending'
expected={r['path']:r['bytes'] for line in (w/'2023_3/source-before.jsonl').read_text().splitlines() if (r:=json.loads(line))['kind']=='file'}
seen=set();size=0;h=hashlib.sha256();complete=None
with (d/'unlink-receipt.jsonl').open('rb') as f:
 for line in f:
  h.update(line);r=json.loads(line)
  if r['event']=='unlinked':
   assert r['path'] not in seen and expected[r['path']]==r['bytes'];seen.add(r['path']);size+=r['bytes']
  elif r['event']=='complete':assert complete is None;complete=r
  else:assert r['event']=='preflight_passed'
assert seen==expected.keys() and len(seen)==708590 and size==5967100516555
assert complete==s['result'] and h.hexdigest()==s['receipt_sha256']
assert (d/'native-errors.log').stat().st_size==0
print(json.dumps({'success':True,'state':s,'all_manifest_files_have_unique_unlink_receipts':True,'source_payload_empty_after_native_scan':True,'verified_at':__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()}))
''',timeout=55)
    save('project3-source-retirement-receipt-verification.json',result);print(json.dumps(result))


def finish():
    proof=load('project3-source-retirement-receipt-verification.json');assert proof['success']
    hosts=load('project3-post-retirement-hosts-completion-verification.json');assert hosts['success']
    content=load('project3-api-content-post-retirement-status.json',private=True)
    assert content['receipt_verified'] and content['state']['success']
    sha=proof['state']['receipt_sha256']
    assert content['state']['verification_phase']=='after_source_retirement'
    assert content['state']['source_retirement_receipt_sha256']==hosts['source_retirement_receipt_sha256']==sha
    admission=load('project3-cutover-admission.json',private=True)
    code=COMMON+r'''
from api.services.project_locks import release_project_lock
v=json.load(sys.stdin);a=v['admission'];proof=v['proof'];result=proof['state']['result'];ids={uuid.UUID(x['project_id']) for x in a['leases']}
assert proof['success'] and proof['all_manifest_files_have_unique_unlink_receipts'] and proof['source_payload_empty_after_native_scan']
assert result['files_unlinked']==708590 and result['logical_bytes_unlinked']==5967100516555
assert v['hosts']['success'] and v['content']['receipt_verified'] and v['content']['state']['handlers_verified']==798
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 b=db.get(StorageMigrationBatch,uuid.UUID(a['batch_id']),with_for_update=True);assert b.status=='source_retirement_prepared'
 own_leases(db,a)
 raws=[db.get(RawDataset,uuid.UUID(x['columns']['id']),with_for_update=True) for x in b.metadata_json['after_raws']]
 assert safety(db,ids,raws,a['holder'])['own_locks']==11
 changes={x['raw_id']:x for x in b.metadata_json['cutover_result']['raw_changes']};retired=[];after=[]
 for r,before in zip(raws,b.metadata_json['after_raws']):
  assert raw_snapshot(r)==before,'Raw catalog changed since retirement gate'
  change=changes[str(r.id)];remove=set(change['source_location_ids'])
  assert remove and source_location_ids(r)==change['source_location_ids']
  for l in list(r.locations):
   if l.id in remove:
    assert not l.is_preferred and compose_storage_path(l.storage_root.path_prefix,l.relative_path).startswith(OLD+'/')
    retired.append(l.id);db.delete(l)
  db.flush();db.expire(r,['locations']);now=raw_snapshot(r)
  for key in ['columns','positions','project_links','experiment_links']:assert now[key]==before[key]
  assert now['locations']==[x for x in before['locations'] if x['id'] not in remove]
  assert [l.id for l in r.locations if l.is_preferred]==[change['new_location_id']]
  detail=raw_dataset_detail_view(r);assert next(l for l in detail.locations if l.is_preferred).absolute_path==change['preferred_path']
  after.append(now)
 for before in b.metadata_json['after_projects']:
  p=db.get(Project,uuid.UUID(before['project_id']))
  assert columns(p)==before['columns'] and identity(p)==before['identity'] and [columns(l) for l in p.locations]==before['locations']
  expected=next(x for x in b.metadata_json['cutover_result']['project_changes'] if x['project_id']==str(p.id))
  detail=project_detail_view(p);assert next(l for l in detail.locations if l.is_preferred).project_mat_path==expected['preferred_mat_path']
 for row in v['content']['receipt']:
  obj=db.get(Artifact if row['kind']=='artifact' else RawDataset,uuid.UUID(row['id']))
  assert (obj.uri if row['kind']=='artifact' else obj.display_settings_uri)==CACHE+row['relative_path']
 assert len(retired)==47 and sum(len(r.positions) for r in raws)==775
 released=[]
 for row in a['leases']:
  l=db.get(ProjectLock,uuid.UUID(row['id']),with_for_update=True);assert l.holder_key==a['holder'];release_project_lock(db,lock=l);released.append(str(l.id))
 b.status='source_retired_verified'
 metadata=dict(b.metadata_json);metadata.update(source_retirement_receipt=proof,post_retirement_hosts=v['hosts'],post_retirement_api={k:value for k,value in v['content'].items() if k!='receipt'},source_location_ids_retired=retired,final_raws=after,own_leases_released=released);b.metadata_json=metadata
 summary=dict(b.summary_json);summary.update(source_deleted=True,files_unlinked=708590,logical_bytes_unlinked=5967100516555,measured_free_bytes_increase=result['measured_free_bytes_increase'],free_bytes_after=result['free_bytes_after'],source_locations_retired=len(retired),api_content_handlers_verified_after_retirement=798,original_projects_preserved=True);b.summary_json=summary
 for item in b.items:item.status='source_retired_verified' if item.item_type=='raw_dataset' else 'final_project_verified_original_preserved'
 publication=db.get(StorageMigrationBatch,uuid.UUID(b.metadata_json['publication_batch_id']));publication.status='verified_complete_originals_preserved_primary_raw_source_retired'
 copied=db.get(StorageMigrationBatch,uuid.UUID('a8c8e0da-72b6-42c5-9644-2028709c9707'))
 copied.status='source_retired_verified';m=dict(copied.metadata_json or {});m['source_retirement_batch_id']=str(b.id);copied.metadata_json=m
 total=4315222548480+result['measured_free_bytes_increase']
 print(json.dumps({'success':True,'finished_at':datetime.now(timezone.utc).isoformat(),'batch_id':str(b.id),'projects_on_homes2':11,'raws_on_homes2':24,'positions_preserved':775,'files_unlinked':708590,'logical_bytes_unlinked':5967100516555,'measured_free_bytes_increase':result['measured_free_bytes_increase'],'free_bytes_after':result['free_bytes_after'],'cumulative_2023_measured_free_bytes_increase':total,'source_location_ids_retired':retired,'post_retirement_linux_windows_verified':True,'post_retirement_actual_api_handlers_verified':798,'original_projects_preserved':True,'own_leases_released':released,'temporary_api_cache_bytes_retained':12055348763}))
'''
    result=remote_json(SSH,'webserver-labo',code,{'admission':admission,'proof':proof,'hosts':hosts,'content':content},container=True)
    save('project3-source-retirement-catalog-finalization.json',result)
    code='V='+repr(encoded(result))+'\n'+r'''
import base64,json
from pathlib import Path
r=json.loads(base64.b64decode(V));w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101/project3-source-retirement-20261105')
s=json.loads((w/'state.json').read_text());assert s['phase']=='source_manifest_retired_catalog_finalization_pending' and s['batch_id']==r['batch_id']
assert not (w/'catalog-finalization.json').exists();(w/'catalog-finalization.json').write_text(json.dumps(r,indent=2)+'\n')
s['phase']='source_retired_verified';s['catalog_finalized_at']=r['finished_at'];p=w/'state.tmp';p.write_text(json.dumps(s,indent=2)+'\n');p.replace(w/'state.json')
print(json.dumps({'source_retirement_state_finalized':True,'batch_id':r['batch_id']}))
'''
    print(json.dumps(remote('linux',code,timeout=35)));print(json.dumps(result))


if __name__=='__main__':
    {'prepare':prepare,'launch':launch,'status':status,'receipt':receipt,'finish':finish}[sys.argv[1]]()
