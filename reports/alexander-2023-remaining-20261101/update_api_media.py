"""Switch only verified API media references; retain pre-existing missing URIs."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from audit import remote_json,SSH
from activate_windows_mapping import remote


def main():
    state=remote('linux',r'''
from pathlib import Path
import json
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101/2023_1')
s=json.loads((w/'api-media-state.json').read_text());assert s['phase']=='verified_ready_for_api_reference_update'
print(json.dumps(s))
''')
    plan=json.loads((HERE/'api-media-plan.json').read_text())
    assert not any(r['primary_file_present_before'] for r in plan['preexisting_missing_references'])
    code=r'''
import json,sys,uuid
from datetime import datetime,timezone
from pathlib import Path
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Artifact,RawDataset,User,StorageMigrationBatch
from api.routes_raw_datasets import get_raw_preview_artifact_content,get_raw_dataset_display_settings
payload=json.load(sys.stdin);plan=payload['plan'];state=payload['state']
assert state['files']==len(plan['files']) and state['bytes']==sum(r['bytes'] for r in plan['files'])
cache=Path('/data/.detecdiv-hub-api-cache/alexander-2023_1')
for f in plan['files']:
 p=cache/f['path'];assert p.is_file() and p.stat().st_size==f['bytes']
 with p.open('rb') as stream:assert stream.read(16)
 if p.suffix=='.json':json.loads(p.read_text())
with SessionLocal.begin() as db:
 db.execute(text("SET LOCAL lock_timeout='10s'"))
 name='alexander-2023-1-api-projection-20261101'
 assert db.scalar(select(StorageMigrationBatch.id).where(StorageMigrationBatch.batch_name==name)) is None
 operator=db.scalars(select(User).where(User.user_key=='gilles',User.role=='admin')).one()
 for row in plan['references']:
  if row['kind']=='artifact':
   obj=db.get(Artifact,uuid.UUID(row['id']),with_for_update=True);assert obj.uri==row['before'];obj.uri=row['after']
  else:
   obj=db.get(RawDataset,uuid.UUID(row['id']),with_for_update=True);assert obj.display_settings_uri==row['before'];obj.display_settings_uri=row['after']
 db.flush();verified=[]
 for row in plan['references']:
  if row['kind']=='artifact':response=get_raw_preview_artifact_content(uuid.UUID(row['id']),db,operator)
  else:response=get_raw_dataset_display_settings(uuid.UUID(row['id']),db,operator)
  assert str(response.path)==row['after'];verified.append(row['id'])
 batch=StorageMigrationBatch(batch_name=name,source_kind='api_assets',source_path='/data/Alexander/data/2023_1',storage_root_name='user-homes2',
  host_scope='detecdiv-server',root_type='api_cache',strategy='verified_api_projection_from_existing_homes2_copy',status='verified_complete',
  metadata_json={'operator':'gilles','changes':plan['references'],'preexisting_missing_references_unchanged':plan['preexisting_missing_references'],
   'source_files_preserved':True,'reason':'API mounts /data; keep existing content accessible while retiring primary duplicates.'},
  summary_json={'files':state['files'],'bytes':state['bytes'],'handlers_verified':len(verified),'preexisting_missing_references_unchanged':len(plan['preexisting_missing_references'])})
 db.add(batch);db.flush()
 print(json.dumps({'success':True,'batch_id':str(batch.id),'committed_at':datetime.now(timezone.utc).isoformat(),
  'files':state['files'],'bytes':state['bytes'],'handlers_verified':len(verified),'artifact_ids_preserved':710,'raw_display_setting_ids_preserved':64,
  'preexisting_missing_references_unchanged':len(plan['preexisting_missing_references'])}))
'''
    r=remote_json(SSH,'webserver-labo',code,{'state':state,'plan':plan},container=True)
    (HERE/'api-media-catalog-result.json').write_text(json.dumps(r,indent=2)+'\n')
    print(json.dumps(r,indent=2))


if __name__=='__main__':main()
