"""Readonly catalog path inventory for a future bounded raw namespace cutover.

Only path-bearing application fields are inspected. Full references stay in
ignored runtime; the public proof contains counts, hashes and scope limits.
"""
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from audit import remote_json, SSH

CODE = r'''
import hashlib,json,re
from collections import Counter
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import RawDataset,RawDatasetLocation,RawDatasetPosition,Artifact,Project,ProjectLocation,StorageRoot,Job,IndexingJob
from api.services.path_resolution import compose_storage_path
namespaces={
 '.incoming-alexander-2023-20261008':['2023_1','2023_3'],
 '.incoming-alexander-2024-20261105':['2024_1','2024_2']}
records=[];unclassified=[];scanned=Counter();active_jobs=[]
def inspect_value(table,row_id,field,value,context=None):
 if isinstance(value,dict):
  for key,child in value.items():inspect_value(table,row_id,field+[str(key)],child,context)
 elif isinstance(value,list):
  for index,child in enumerate(value):inspect_value(table,row_id,field+[index],child,context)
 elif isinstance(value,str):
  normal=value.replace(chr(92),'/')
  for namespace,years in namespaces.items():
   if namespace not in normal:continue
   matches=[]
   for year in years:
    token=namespace+'/'+year
    for match in re.finditer(re.escape(token)+r'(?=/|$)',normal):matches.append((year,match.start(),match.end()))
   record={'table':table,'id':str(row_id),'field':field,'value':value,'context':context}
   if len(matches)!=1:
    unclassified.append(record);continue
   year,start,end=matches[0];record.update(year=year,old_namespace=namespace,
    proposed_value=normal[:start]+year+normal[end:],policy='requires_consumer_classification_not_automatic_rewrite')
   records.append(record)
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
 db.execute(text("SET LOCAL statement_timeout='45s'"))
 roots={r.id:r.path_prefix for r in db.scalars(select(StorageRoot))}
 for row in db.execute(select(RawDatasetLocation.id,RawDatasetLocation.raw_dataset_id,RawDatasetLocation.storage_root_id,RawDatasetLocation.relative_path,RawDatasetLocation.is_preferred)):
  scanned['raw_dataset_locations']+=1
  inspect_value('raw_dataset_locations',row.id,['composed_path'],compose_storage_path(roots[row.storage_root_id],row.relative_path),{'raw_dataset_id':str(row.raw_dataset_id),'preferred':row.is_preferred,'storage_root_id':row.storage_root_id,'relative_path':row.relative_path})
 for row in db.execute(select(ProjectLocation.id,ProjectLocation.project_id,ProjectLocation.storage_root_id,ProjectLocation.relative_path,ProjectLocation.project_file_name,ProjectLocation.is_preferred)):
  scanned['project_locations']+=1
  inspect_value('project_locations',row.id,['composed_path'],compose_storage_path(roots[row.storage_root_id],row.relative_path,row.project_file_name),{'project_id':str(row.project_id),'preferred':row.is_preferred})
 for model,fields in [(RawDataset,['archive_uri','display_settings_uri','metadata_json']),(RawDatasetPosition,['metadata_json']),(Artifact,['uri','metadata_json']),(Project,['archive_uri','metadata_json'])]:
  for row in db.execute(select(model.id,*[getattr(model,key) for key in fields])):
   scanned[model.__tablename__]+=1
   for key,value in zip(fields,row[1:]):inspect_value(model.__tablename__,row.id,[key],value)
 for row in db.execute(select(Job.id,Job.status,Job.project_id,Job.params_json).where(Job.status.in_(['queued','running','cancelling']))):
  scanned['active_jobs']+=1;prior=len(records)+len(unclassified)
  inspect_value('active_jobs',row.id,['params_json'],row.params_json)
  if len(records)+len(unclassified)>prior:active_jobs.append({'id':str(row.id),'status':row.status,'project_id':str(row.project_id) if row.project_id else None})
 indexing=[str(i) for i in db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running'])))]
snapshot={'read_only':True,'checked_at':datetime.now(timezone.utc).isoformat(),'records':records,'unclassified':unclassified,
 'scanned_rows':dict(scanned),'active_referencing_jobs':active_jobs,'active_indexing_jobs':indexing,
 'live_renames_allowed':False,'database_changes':False,'scientific_files_read_or_saved':False,
 'scope':'Current catalog locations, raw/project metadata, raw display settings, artifact URIs/metadata and active job params only. Historical jobs, on-disk MAT/JSON/processor/HDF5 and external consumers require separate proven coverage; no absence-of-reference inference.'}
encoded=(json.dumps(snapshot,sort_keys=True,separators=(',',':'))+'\n').encode()
print(json.dumps({'snapshot':snapshot,'snapshot_sha256':hashlib.sha256(encoded).hexdigest(),
 'summary':{'success':True,'read_only':True,'checked_at':snapshot['checked_at'],'matched_path_values':len(records),
 'counts_by_table':dict(Counter(r['table'] for r in records)),'counts_by_year':dict(Counter(r['year'] for r in records)),
 'preferred_raw_locations':dict(Counter(r['year'] for r in records if r['table']=='raw_dataset_locations' and r['context']['preferred'])),
 'unclassified_path_values':len(unclassified),'active_referencing_jobs':len(active_jobs),'active_indexing_jobs':len(indexing),
 'scanned_rows':dict(scanned),'snapshot_sha256':hashlib.sha256(encoded).hexdigest(),'live_renames_allowed':False,
 'database_changed':False,'scientific_files_read_or_saved':False,'on_disk_and_historical_consumers_still_require_coverage':True}}))
'''

if __name__ == '__main__':
    public = HERE / 'readable-raw-names-catalog-inventory-verification.json'
    private = HERE / 'runtime/readable-raw-names-catalog-inventory-20261110.json'
    assert not public.exists() and not private.exists(), 'Inspect existing inventory first'
    result = remote_json(SSH, 'webserver-labo', CODE, {}, container=True)
    raw = (json.dumps(result['snapshot'], sort_keys=True, separators=(',', ':')) + '\n').encode()
    assert hashlib.sha256(raw).hexdigest() == result['snapshot_sha256']
    with private.open('xb') as stream:
        stream.write(raw)
    with public.open('x', encoding='utf-8') as stream:
        json.dump(result['summary'], stream, indent=2)
        stream.write('\n')
    print(json.dumps(result['summary']))
