"""Inventory real year1 media references read-only; no URI or catalog update."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
PRIVATE = HERE / 'runtime'
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from audit import remote_json, SSH
from activate_windows_mapping import remote

SNAPSHOT = r'''
import json,sys,hashlib,stat
from datetime import datetime,timezone
from pathlib import Path
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import Artifact,RawDataset
from api.services.path_resolution import compose_storage_path
prefix='/data/Alexander/data/2024_1/'
target='/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/2024_1/'
references=[];raws=[]
def record(kind,obj,path,raw_id):
 relative=path[len(prefix):];assert relative and all(x not in ('','..','.') for x in relative.split('/'))
 source=Path(path);dest=Path(target+relative)
 def availability(p):
  if not p.exists():return {'exists':False}
  s=p.lstat();return {'exists':True,'regular':stat.S_ISREG(s.st_mode),'symlink':p.is_symlink(),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns}
 references.append({'kind':kind,'id':str(obj.id),'raw_dataset_id':str(raw_id),'original_uri':path,
  'relative_path':relative,'proposed_homes2_uri':target+relative,'api_source':availability(source),'api_homes2':availability(dest)})
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 for raw in db.scalars(select(RawDataset).order_by(RawDataset.id)):
  locations=[{'id':x.id,'path':compose_storage_path(x.storage_root.path_prefix,x.relative_path),'preferred':x.is_preferred} for x in raw.locations]
  if any(x['path'].startswith(prefix) for x in locations):raws.append({'id':str(raw.id),'owner_user_id':str(raw.owner_user_id),'locations':locations})
  uri=str(raw.display_settings_uri or '')
  if uri.startswith(prefix):record('display_settings',raw,uri,raw.id)
 for obj in db.scalars(select(Artifact).order_by(Artifact.id)):
  uri=str(obj.uri or '')
  if uri.startswith(prefix):record('artifact',obj,uri,obj.job.raw_dataset_id if obj.job else None)
print(json.dumps({'read_only':True,'catalog_changed':False,'checked_at':datetime.now(timezone.utc).isoformat(),'raws':raws,'references':references}))
'''

MEMBERSHIP = r'''
import json,hashlib
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
work=root/'year1-api-media-inventory-20261109';assert not work.exists();work.mkdir(mode=0o700)
snapshot=INPUT
(work/'catalog-snapshot.json').write_text(json.dumps(snapshot,indent=2)+'\n')
manifest=root/'2024_1/source-before.jsonl'
assert hashlib.sha256(manifest.read_bytes()).hexdigest()=='8fd64d094fe29707e0ddd0b1480e9b861786d3adde677087ca7a6e95e252f61e'
state=json.loads((root/'state.json').read_text())['years']['2024_1']
assert state['full_sha1_passed'] and state['source_stability_verified']
paths={x['relative_path'] for x in snapshot['references']};found={}
for line in manifest.open():
 row=json.loads(line)
 if row['kind']=='file' and row['path'] in paths:
  assert row['path'] not in found;found[row['path']]=row
assert set(found)==paths,'Media reference outside verified native manifest'
raw_ids={x['id'] for x in snapshot['raws']}
for row in snapshot['references']:
 native=found[row['relative_path']];row['native_source']=native
 assert row['raw_dataset_id'] in raw_ids,'Media ownership needs review'
 for key in ['api_source','api_homes2']:
  observed=row[key];assert observed['exists'] and observed['regular'] and not observed['symlink'] and observed['bytes']==native['bytes'],'API media availability needs review'
snapshot.update(native_source_manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
 raw_year1_checksum_and_stability_passed=True,handlers_streamed=False,media_content_hashed=False,source_deleted=False)
payload=(json.dumps(snapshot,indent=2)+'\n').encode();(work/'plan.json').write_bytes(payload)
result={'success':True,'read_only':True,'catalog_changed':False,'uri_changed':False,'source_deleted':False,
 'raw_datasets':len(snapshot['raws']),'references':len(snapshot['references']),
 'artifact_references':sum(x['kind']=='artifact' for x in snapshot['references']),
 'display_settings_references':sum(x['kind']=='display_settings' for x in snapshot['references']),
 'unique_files':len(paths),'unique_bytes':sum(x['bytes'] for x in found.values()),
 'all_references_in_verified_native_manifest':True,'api_source_and_homes2_presence_and_sizes_match':True,
 'handlers_streamed':False,'media_content_hashed':False,'fresh_reference_review_required_before_cutover':True,
 'plan_sha256':hashlib.sha256(payload).hexdigest(),'checked_at':snapshot['checked_at']}
(work/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
'''

def main():
    started=PRIVATE/'year1-api-media-inventory-started.json'
    with started.open('x',encoding='utf-8') as stream:
        json.dump({'started_at':datetime.now(timezone.utc).isoformat(),'read_only':True},stream)
    snapshot=remote_json(SSH,'webserver-labo',SNAPSHOT,{},container=True)
    (PRIVATE/'year1-api-media-catalog-snapshot.json').write_text(json.dumps(snapshot,indent=2)+'\n',encoding='utf-8')
    code='INPUT=json.loads('+repr(json.dumps(snapshot))+')\n'+MEMBERSHIP
    # json must be imported before decoding the transported snapshot.
    result=remote('linux','import json\n'+code,timeout=90)
    result['script_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (HERE/'year1-api-media-inventory-verification.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result))

if __name__=='__main__':main()
