"""Read-only verification of the completed catalog and worker state."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from catalog_operations import COMMON, WORK, save, remote, remote_json, SSH

gate=json.loads((HERE/'retirement-execution-gates.json').read_text())
project=json.loads((HERE/'project-catalog-cutover.json').read_text())
before_workers=json.loads((HERE/'worker-status-during-retirement.json').read_text())['workers']
code=COMMON+r'''
from api.models import WorkerInstance
from api.routes_projects import project_detail_view
from api.routes_raw_datasets import raw_dataset_detail_view
payload=json.load(sys.stdin);project=payload['project'];gate=payload['gate']
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 p=db.get(Project,uuid.UUID(project['project_id']));detail=project_detail_view(p)
 assert p.project_key==project['project_key'] and p.fov_count==12 and p.roi_count==5724
 protected={'owner_user_id':str(p.owner_user_id),'project_key':p.project_key,'visibility':p.visibility,'notes':p.notes,
  'acl':digest([scalar_columns(x) for x in p.acl_entries]),'groups':digest([scalar_columns(x) for x in p.group_memberships]),
  'notes_entries':digest([scalar_columns(x) for x in p.note_entries]),'raw_links':digest([scalar_columns(x) for x in p.raw_links])}
 assert protected==project['protected_identity_and_relationships']
 assert next(l.project_mat_path for l in detail.locations if l.is_preferred)==project['preferred_mat_path']
 assert any(l.id==2765 and not l.is_preferred and l.access_mode=='readonly' for l in p.locations)
 raws=[db.get(RawDataset,uuid.UUID(row['raw_id'])) for row in gate['source_locations']]
 assert len(raws)==21
 for raw,row in zip(raws,gate['source_locations']):
  assert len(raw.locations)==1 and raw.locations[0].id==row['preferred_location']['id'] and raw.locations[0].is_preferred
  assert next(l.absolute_path for l in raw_dataset_detail_view(raw).locations if l.is_preferred)==row['destination_path']
  assert str(raw.owner_user_id)==row['owner_user_id']
 assert sum(len(r.positions) for r in raws)==337
 guards=safety(db,raws)
 lease=db.get(ProjectLock,uuid.UUID(gate['lease_id']));assert lease.status=='released'
 batch=db.get(StorageMigrationBatch,uuid.UUID(gate['batch_id']));assert batch.status=='verified_source_retirement_complete'
 assert other_locations_digest(db,[str(r.id) for r in raws])==batch.metadata_json['other_locations']
 workers=[];expected={r['instance']:r for r in payload['workers']}
 for w in db.scalars(select(WorkerInstance)):
  previous=expected.pop(w.worker_instance)
  assert w.process_id==previous['pid'] and w.code_fingerprint==previous['fingerprint']
  age=(datetime.now(timezone.utc)-w.last_seen_at).total_seconds();assert age<30
  workers.append({'instance':w.worker_instance,'pid':w.process_id,'fingerprint':w.code_fingerprint,'heartbeat_age_seconds':age})
 assert not expected and len(workers)==7
 print(json.dumps({'success':True,'checked_at':datetime.now(timezone.utc).isoformat(),'project_id':str(p.id),'project_key':p.project_key,
  'project_identity_notes_acl_groups_and_links_preserved':True,'original_project_location_preserved':2765,'preferred_project_location':project['new_location_id'],
  'raws':21,'positions':337,'unrelated_locations_preserved':batch.metadata_json['other_locations'],
  'project_lease_released':True,'guards':guards,'workers_unchanged':workers,'retirement_batch_status':batch.status}))
'''
catalog=remote_json(SSH,'webserver-labo',code,{'gate':gate,'project':project,'workers':before_workers},container=True)
save('final-catalog-worker-verification.json',catalog)
filesystem=remote('linux',f'''
import json,subprocess,shlex
from pathlib import Path
w=Path({WORK!r});retired=json.loads((w/'raw-retirement/retirement-state.json').read_text())
assert retired['phase']=='verified_source_retirement_complete'
home=json.loads((w/'homes2-final-inventory-verification.json').read_text());assert home['success']
script="import shutil,json;v=shutil.disk_usage('/volume1/DATA');print(json.dumps({{'total_bytes':v.total,'used_bytes':v.used,'free_bytes':v.free}}))"
args=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','-o','IdentitiesOnly=yes','-i','/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519','Gilles@10.20.11.250',shlex.join(['python3','-c',script])]
p=subprocess.run(args,capture_output=True,text=True,timeout=30);assert p.returncode==0,p.stderr
print(json.dumps({{'retirement_phase':retired['phase'],'homes2_final_inventory':home,'primary_space_at_final_check':json.loads(p.stdout)}}))
''',timeout=60)
save('final-filesystem-verification.json',filesystem)
save('homes2-final-inventory-verification.json',filesystem['homes2_final_inventory'])
summary={'success':True,'project':project,'raw_source_retirement':json.loads((HERE/'raw-retirement-catalog-finalization.json').read_text()),
         'final_catalog':catalog,'final_filesystem':filesystem,'original_project_preserved':True,
         'automatic_hub_backups_enabled':False,'untouched_years':['2023_1','2023_3'],'services_restarted':False,
         'windows_diagnostic_task_removed':json.loads((HERE/'windows-cleanup.json').read_text())['taskRemoved']}
save('completion-summary.json',summary)
print(json.dumps({'success':True,'raws':21,'fovs':12,'rois':5724,'workers_unchanged':7,
 'primary_free_bytes':filesystem['primary_space_at_final_check']['free_bytes'],
 'measured_reclaimed_bytes':summary['raw_source_retirement']['source_retirement']['measured_free_bytes_increase']}))
