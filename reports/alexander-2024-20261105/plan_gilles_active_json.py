"""Plan only reusable run refs and two active loader paths; do not save projects."""
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import copy,hashlib,json,re
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
base=root/'exception-path-audit-20261106';assert (base/'controller.exit').read_text().strip()=='0'
p=next(p for p in json.loads((base/'request.json').read_text())['projects'] if p['name']=='281124_Alex')
container='/homes2/maliavko/DetecdivHub/projects/281124_Alex'
rawroot='/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/2024_2/281124_X5152bud4s'
def canonical(x):return json.dumps(x,sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(',',':'))
def digest(x):return hashlib.sha256(canonical(x).encode()).hexdigest()
def get(doc,path):
 for key in path:doc=doc[key]
 return doc
def put(doc,path,value):
 for key in path[:-1]:doc=doc[key]
 doc[path[-1]]=value
def mapped(value):
 assert isinstance(value,str)
 if not value:return value
 v=re.sub('/+','/',value.replace(chr(92),'/')).rstrip('/')
 for prefix in ['X:/Gilles/analysis/projects/281124_Alex','/data/Gilles/analysis/projects/281124_Alex','/Volumes/DATA/Gilles/analysis/projects/281124_Alex']:
  if v==prefix or v.startswith(prefix+'/') or v==prefix+'.mat':return container+'/281124_Alex'+v[len(prefix):]
 for prefix in ['X:/Alexander/data/2024_2/281124_X5152bud4s','/data/Alexander/data/2024_2/281124_X5152bud4s']:
  if v==prefix or v.startswith(prefix+'/'):
   old=Path('/data/Alexander/data/2024_2/281124_X5152bud4s'+v[len(prefix):]);assert old.exists()
   return rawroot+v[len(prefix):]
 for suffix in ['analysis/pipelines/daughter_lineage/daughter_lineage','analysis/pipelines/sam31_inference/sam31_inference','sam31/sam31_1']:
  for prefix in ['X:/Gilles/'+suffix,'/data/Gilles/'+suffix]:
   if v==prefix or v.startswith(prefix+'/'):
    target='/data/Gilles/'+suffix+v[len(prefix):];assert Path(target).exists()
    return target if prefix.startswith('X:') else value
 raise AssertionError('Unclassified active reference')
records=[];archival=[]
for record in p['verified_files']:
 relative=record['path']
 if not relative.endswith('.json'):continue
 file=Path(p['stage'])/relative;raw=file.read_bytes();assert hashlib.sha256(raw).hexdigest()==record['sha256']
 if relative.endswith('/run_params.json') or relative.endswith('/dependency_audit.json'):
  archival.append({'path':relative,'sha256':record['sha256'],'policy':'preserve_bytes'});continue
 before=json.loads(raw);after=copy.deepcopy(before)
 if relative.endswith('/run.json'):
  fields=[['templatePath'],['projectPath'],['pipelineRef','path'],['targetRef','projectPath'],['targetRef','classiPath']]
 elif relative.endswith('/sam31_inference/pipeline.json'):fields=[['nodes','params','path']]
 elif relative.endswith('/sam31_inference/modules/dataloader_1/module.json'):fields=[['params','path']]
 else:raise AssertionError('Unclassified project JSON')
 changes=[]
 for path in fields:
  prior=get(before,path);target=mapped(prior)
  if prior!=target:changes.append({'field':path,'before':prior,'after':target});put(after,path,target)
 restored=copy.deepcopy(after)
 for change in changes:put(restored,change['field'],change['before'])
 assert canonical(restored)==canonical(before)
 frozen={key:digest(before[key]) for key in ['ctx','outputs','progress'] if key in before}
 assert all(digest(after[key])==h for key,h in frozen.items())
 assert hashlib.sha256(file.read_bytes()).hexdigest()==record['sha256']
 records.append({'project_id':p['project_id'],'relative_path':relative,'before_sha256':record['sha256'],'before_canonical_sha256':digest(before),
  'after_canonical_sha256':digest(after),'changes':changes,'frozen_subtree_sha256':frozen,'unapproved_values_preserved':True})
assert len(records)==9 and sum(len(r['changes']) for r in records)==20 and len(archival)==8
plan={'project_id':p['project_id'],'stage':p['stage'],'files':records,'archival_json':archival,'read_only':True,'scientific_files_saved':False,
 'historical_run_resumption_validated':False,'external_models_and_templates_not_moved':True}
output=root/'gilles-active-json-plan-20261107.json';assert not output.exists(),'Inspect prior plan instead of replay'
encoded=(json.dumps(plan,indent=2)+'\n').encode();output.write_bytes(encoded)
print(json.dumps({'success':True,'planned_json_files':9,'planned_leaf_changes':20,'archival_json_files_preserved_byte_exact':8,'read_only':True,
 'scientific_files_saved':False,'plan_sha256':hashlib.sha256(encoded).hexdigest(),'unapproved_values_and_frozen_contexts_preserved':True,'source_deleted':False,
 'note':'Typed readonly plan only. Backups, admission, exact scientific/path save-reopen checks and both real hosts remain required.'}))
'''

if __name__=='__main__':
    output=HERE/'gilles-active-json-plan-verification.json';assert not output.exists()
    result=remote('linux',CODE,timeout=90)
    with output.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))
