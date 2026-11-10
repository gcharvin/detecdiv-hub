"""Readonly catalog continuity review of the exact 17 unpublished project IDs.

No write admission or lease is acquired. Full snapshots stay in ignored runtime.
"""
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from audit import remote_json, SSH

CODE = r'''
import hashlib,json,sys,uuid
from collections import Counter
from datetime import datetime,timezone
from sqlalchemy import inspect,select,text
from api.db import SessionLocal
from api.models import Project,User,ProjectLock,Job,IndexingJob,StorageMigrationBatch,RawDatasetLocation,StorageRoot
from api.services.path_resolution import compose_storage_path
v=json.load(sys.stdin);ids={uuid.UUID(p['project_id']) for p in v};assert len(v)==len(ids)==17
def columns(obj):return {p.key:getattr(obj,p.key) for p in inspect(type(obj)).column_attrs}
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,default=str).encode()).hexdigest()
def identity(p):return {'key':p.project_key,'owner':str(p.owner_user_id),'visibility':p.visibility,'notes':p.notes,
 'acl':[columns(x) for x in p.acl_entries],'groups':[columns(x) for x in p.group_memberships],'note_entries':[columns(x) for x in p.note_entries],'raw_links':[columns(x) for x in p.raw_links]}
batch_ids=['bf1a270b-1e18-419a-b11b-b2241871f13b','90c03028-f0b9-4f86-86b6-a90912e3ef50','e72ca211-c526-4855-99ad-d2839da288a6','98f46050-bfe3-409f-a807-a2fb30c104ef','dd188133-e2b3-4279-b7ac-773a673e5d3b','5aa13bf8-5456-4276-a27c-0a52d32b6999']
keys=['identity_sha256','metadata_sha256','locations_sha256'];prior={};before=[];comparisons=[];active_jobs=[];owners=Counter();now=datetime.now(timezone.utc)
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
 for batch_id in batch_ids:
  batch=db.get(StorageMigrationBatch,uuid.UUID(batch_id));assert batch is not None
  for snapshot in batch.metadata_json['before_projects']:
   if uuid.UUID(snapshot['project_id']) not in ids:continue
   if snapshot['project_id'] in prior:assert all(snapshot[k]==prior[snapshot['project_id']][k] for k in keys)
   prior[snapshot['project_id']]=snapshot
 assert set(prior)=={str(i) for i in ids},'A project has no verified historical continuity snapshot'
 raw_ids=set()
 for row in db.execute(select(RawDatasetLocation.raw_dataset_id,StorageRoot.path_prefix,RawDatasetLocation.relative_path).join(StorageRoot,RawDatasetLocation.storage_root_id==StorageRoot.id)):
  path=compose_storage_path(row.path_prefix,row.relative_path)
  if path.startswith('/data/Alexander/data/2024_2/') or '/raw/.incoming-alexander-2024-20261105/2024_2/' in path:raw_ids.add(str(row.raw_dataset_id))
 for row in db.execute(select(Job.id,Job.project_id,Job.raw_dataset_id,Job.params_json,Job.status).where(Job.status.in_(['queued','running','cancelling']))):
  params=json.dumps(row.params_json or {},default=str)
  affected=row.project_id in ids or str(row.raw_dataset_id) in raw_ids or any(str(i) in params for i in ids) or any(p['name'] in params for p in v) or '/data/Alexander/data/2024_2' in params or '/raw/.incoming-alexander-2024-20261105/2024_2' in params
  if affected:active_jobs.append({'id':str(row.id),'status':row.status})
 active_leases=[str(x.id) for x in db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_(ids),ProjectLock.status=='active')) if x.expires_at is None or x.expires_at>now]
 indexing=[str(i) for i in db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running'])))]
 for selected in v:
  project=db.get(Project,uuid.UUID(selected['project_id']));assert project is not None
  owner=db.scalar(select(User.user_key).where(User.id==project.owner_user_id));owners[owner]+=1
  current={'project_id':str(project.id),'identity_sha256':digest(identity(project)),'metadata_sha256':digest(project.metadata_json),'locations_sha256':digest([columns(x) for x in project.locations])}
  matched={key:current[key]==prior[str(project.id)][key] for key in keys}
  comparisons.append({'project_id':str(project.id),'name_matches':project.project_name==selected['name'],'owner_matches':owner==('gilles' if selected['name']=='281124_Alex' else 'alexander'),'digest_matches':matched})
  before.append(dict(current,name=project.project_name,owner_key=owner,columns=columns(project),locations=[columns(x) for x in project.locations]))
unchanged=all(p['name_matches'] and p['owner_matches'] and all(p['digest_matches'].values()) for p in comparisons)
private={'checked_at':now.isoformat(),'read_only':True,'before_projects':before,'comparisons':comparisons,'active_affected_jobs':active_jobs,'active_project_leases':active_leases,'active_indexing_jobs':indexing,'referenced_raw_dataset_ids':sorted(raw_ids)}
public={'review_complete':True,'checked_at':now.isoformat(),'projects':17,'owners':dict(owners),'all_catalog_identity_metadata_locations_unchanged':unchanged,
 'projects_with_differences':sum(not(p['name_matches'] and p['owner_matches'] and all(p['digest_matches'].values())) for p in comparisons),
 'affected_active_jobs':len(active_jobs),'affected_active_project_leases':len(active_leases),'active_indexing_jobs':len(indexing),
 'raw_datasets_in_job_safety_scope':len(raw_ids),'read_only':True,'leases_acquired':0,'database_changed':False,'source_deleted':False,
 'fresh_write_admission_still_required':True,'raw_year2_integrity_and_final_host_catalog_api_retirement_gates_not_waived':True}
print(json.dumps({'private':private,'summary':public},default=str))
'''

if __name__ == '__main__':
    public = HERE / 'year2-catalog-prepublication-review-verification.json'
    private = HERE / 'runtime/year2-catalog-prepublication-review.json'
    assert not public.exists() and not private.exists(), 'Inspect the completed review instead of replay'
    plan = json.loads((HERE / 'year1-next-stage-candidate-plan-20261109.json').read_text())
    selected = [{'project_id': p['project_id'], 'name': p['name']} for p in plan['projects'] if p['metadata_relink_completed'] and '2024_2' in p['candidate_raw_years']]
    assert len(selected) == 17
    result = remote_json(SSH, 'webserver-labo', CODE, selected, container=True)
    private.write_text(json.dumps(result['private'], indent=2) + '\n', encoding='utf-8')
    result['summary']['private_snapshot_sha256'] = hashlib.sha256(private.read_bytes()).hexdigest()
    public.write_text(json.dumps(result['summary'], indent=2) + '\n', encoding='utf-8')
    print(json.dumps(result['summary']))
