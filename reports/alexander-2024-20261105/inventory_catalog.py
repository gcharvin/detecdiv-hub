"""Read-only 2024 admission snapshot. Full catalog details remain private."""
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import api

CODE = r'''
import json
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import RawDataset,Project,Artifact,Job,ProjectLock,IndexingJob
from api.services.path_resolution import compose_storage_path
prefixes=['/data/Alexander/data/2024_1','/data/Alexander/data/2024_2']
def affected(path):return any(path==p or path.startswith(p+'/') for p in prefixes)
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 raws=[]
 for r in db.scalars(select(RawDataset)):
  paths=[compose_storage_path(l.storage_root.path_prefix,l.relative_path) for l in r.locations]
  if not any(affected(p) for p in paths):continue
  raws.append({'id':str(r.id),'label':r.acquisition_label,'owner':r.owner.user_key if r.owner else None,
   'locations':[{'id':l.id,'prefix':l.storage_root.path_prefix,'relative':l.relative_path,'preferred':l.is_preferred,'access':l.access_mode} for l in r.locations],
   'paths':paths,'project_ids':[str(l.project_id) for l in r.project_links],'bytes':r.total_bytes,
   'positions':len(r.positions),'display_settings_uri':r.display_settings_uri})
 projects=[]
 for p in db.scalars(select(Project)):
  projects.append({'id':str(p.id),'name':p.project_name,'key':p.project_key,'owner':p.owner.user_key if p.owner else None,
   'metadata':p.metadata_json,'locations':[{'id':l.id,'prefix':l.storage_root.path_prefix,'relative':l.relative_path,'file':l.project_file_name,'preferred':l.is_preferred,'access':l.access_mode} for l in p.locations],
   'raw_ids':[str(l.raw_dataset_id) for l in p.raw_links],'fovs':p.fov_count,'rois':p.roi_count,
   'project_bytes':p.project_mat_bytes+p.project_dir_bytes})
 jobs=[]
 ids={r['id'] for r in raws}
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  params=json.dumps(j.params_json or {},default=str)
  jobs.append({'id':str(j.id),'status':j.status,'project_id':str(j.project_id) if j.project_id else None,
   'raw_id':str(j.raw_dataset_id) if j.raw_dataset_id else None,
   'affected_raw':str(j.raw_dataset_id) in ids or any(p in params for p in prefixes) or any(i in params for i in ids)})
 now=datetime.now(timezone.utc)
 locks=[{'id':str(l.id),'project_id':str(l.project_id),'holder':l.holder_key,'expires_at':str(l.expires_at)} for l in db.scalars(select(ProjectLock).where(ProjectLock.status=='active')) if l.expires_at is None or l.expires_at>now]
 indexing=[{'id':str(i.id),'status':i.status} for i in db.scalars(select(IndexingJob).where(IndexingJob.status.in_(['queued','running'])))]
 artifacts=[{'id':str(a.id),'uri':a.uri,'kind':a.artifact_kind} for a in db.scalars(select(Artifact)) if affected(a.uri or '')]
 print(json.dumps({'checked_at':now.isoformat(),'raws':raws,'projects':projects,'artifacts':artifacts,'jobs':jobs,'locks':locks,'indexing':indexing}))
'''

if __name__ == '__main__':
    result = api(CODE)
    private = HERE / 'runtime'
    private.mkdir(exist_ok=True)
    with (private / 'catalog-before.json').open('x', encoding='utf-8') as f:
        json.dump(result, f, indent=2)
    assert not any(j['affected_raw'] for j in result['jobs']), 'Affected raw job requires review before launch'
    summary = {'checked_at': result['checked_at'], 'raws': len(result['raws']),
               'raw_owners': sorted({r['owner'] or '(none)' for r in result['raws']}),
               'linked_projects': sorted({p for r in result['raws'] for p in r['project_ids']}),
               'catalog_projects_to_inspect': len(result['projects']),
               'api_preview_references': len(result['artifacts']),
               'api_settings_references': sum(bool(r['display_settings_uri']) for r in result['raws']),
               'active_jobs': result['jobs'], 'active_locks': result['locks'], 'indexing': result['indexing'],
               'read_only': True}
    (HERE / 'catalog-admission-summary.json').write_text(json.dumps(summary, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(summary, indent=2))
