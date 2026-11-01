"""Read-only live catalog snapshot for 2023_1 and 2023_3."""
import json
from pathlib import Path
import sys
from collections import Counter

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import api,remote

CODE=r'''
import json
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import RawDataset,Project,Artifact,Job,ProjectLock
from api.services.path_resolution import compose_storage_path
prefixes=['/data/Alexander/data/2023_1','/data/Alexander/data/2023_3']
with SessionLocal() as db:
 db.execute(text('SET TRANSACTION READ ONLY'))
 raws=[];pids=set()
 for r in db.scalars(select(RawDataset)):
  paths=[compose_storage_path(l.storage_root.path_prefix,l.relative_path) for l in r.locations]
  if not any(any(p==pre or p.startswith(pre+'/') for pre in prefixes) for p in paths):continue
  links=[str(l.project_id) for l in r.project_links];pids.update(links)
  raws.append({'id':str(r.id),'owner':r.owner.user_key if r.owner else None,'label':r.acquisition_label,
   'source_paths':[p for p in paths if any(p.startswith(pre+'/') for pre in prefixes)],
   'locations':[{'id':l.id,'prefix':l.storage_root.path_prefix,'root':l.storage_root.name,'relative':l.relative_path,'preferred':l.is_preferred,'access':l.access_mode} for l in r.locations],
   'project_ids':links,'positions':len(r.positions),'display_settings_uri':r.display_settings_uri,'archive_uri':r.archive_uri})
 projects=[]
 for p in db.scalars(select(Project)):
  textdata=json.dumps(p.metadata_json or {})
  if str(p.id) in pids or any(pre in textdata for pre in prefixes) or (p.owner and p.owner.user_key=='alexander'):
   projects.append({'id':str(p.id),'key':p.project_key,'name':p.project_name,'owner':p.owner.user_key if p.owner else None,
    'metadata':p.metadata_json,'locations':[{'id':l.id,'prefix':l.storage_root.path_prefix,'relative':l.relative_path,'file':l.project_file_name,'preferred':l.is_preferred,'access':l.access_mode} for l in p.locations],
    'raw_ids':[str(l.raw_dataset_id) for l in p.raw_links],'fovs':p.fov_count,'rois':p.roi_count,'bytes':p.total_bytes})
 artifacts=[{'id':str(a.id),'uri':a.uri,'kind':a.artifact_kind} for a in db.scalars(select(Artifact)) if any((a.uri or '').startswith(pre+'/') for pre in prefixes)]
 jobs=[{'id':str(j.id),'status':j.status,'project_id':str(j.project_id),'raw_id':str(j.raw_dataset_id)} for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling'])))]
 locks=[{'id':str(l.id),'project_id':str(l.project_id),'expires_at':str(l.expires_at)} for l in db.scalars(select(ProjectLock).where(ProjectLock.status=='active'))]
 print(json.dumps({'raws':raws,'projects':projects,'artifacts':artifacts,'jobs':jobs,'locks':locks}))
'''

if __name__=='__main__':
 r=api(CODE)
 (HERE/'catalog-before.json').write_text(json.dumps(r,indent=2)+'\n')
 print(json.dumps({'raw_count':len(r['raws']),'owners':dict(Counter(x['owner'] for x in r['raws'])),
  'years':dict(Counter('2023_1' if '/2023_1/' in x['source_paths'][0] else '2023_3' for x in r['raws'])),
  'projects':[{'id':x['id'],'name':x['name'],'owner':x['owner'],'fovs':x['fovs'],'rois':x['rois'],'locations':x['locations'],'linked_raws':len(x['raw_ids'])} for x in r['projects']],
  'artifacts':len(r['artifacts']),'jobs':r['jobs'],'locks':r['locks']},indent=2))
