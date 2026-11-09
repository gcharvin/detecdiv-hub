"""Build host inputs only from frozen pre-relink baselines and verified manifests."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import json,hashlib,gzip
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');work=root/'year1-host-inputs-20261109'
assert not work.exists(),'Inspect existing host inputs instead of replay'
audit=root/'year1-prepublication-audit-20261109';q=json.loads((audit/'request.json').read_text());assert len(q['projects'])==16
baselines={};provenance=[]
def read(folder,file='result.json',pin=None):
 path=root/folder/file;raw=path.read_bytes();sha=hashlib.sha256(raw).hexdigest()
 if pin:assert sha==pin
 provenance.append({'file':folder+'/'+file,'sha256':sha});return json.loads(raw)
for name in ['identity-audit-20261106-batch1','identity-audit-20261106-batch2','identity-audit-20261106-batch3','identity-audit-20261106-batch4','identity-audit-20261106-batch5','availability-variants-identity-v2-20261107','movie-cache-identity-20261107']:
 folder=root/name
 file='result.json' if (folder/'result.json').exists() else 'result.partial.json'
 d=read(name,file);rows=d['projects'] if isinstance(d,dict) else d
 for p in rows:
  if p.get('success',True) and 'fov_details' in p:baselines[p['project_id']]=p
hist=read('historical-reference-baseline-20261106',pin='4b4832b751f5ec63938522fd60f6fdacdb568dcad4c0fa3992194a14a7b86bc5')
diagnosis=read('saved-size-diagnosis-v2-20261106',pin='6a8a9a48ddaa77a615507940a2443dbb17994ea6e0696d373a8e61d155ceb1d1')
assert diagnosis['success'] and diagnosis['name']=='anof_060224_cres'
for p in hist['projects']:
 fovs=p['fovs']
 if p['name']=='anof_060224_cres':
  for f,diag in zip(fovs,diagnosis['fovs']):
   assert f['fov']==diag['fov'] and any(x.startswith('exact_xywh_frame_1') for x in diag['crop_matches'])
   f['samples']=[{'channel':s['channel'],'frame':s['frame'],'year':'2024_1','relative_path':s['relative_path'],'class':s['image_class'],'size':s['image_size'],'pixel_sha256':s['pixel_sha256']} for s in diag['samples']]
   f['crop_match']='exact_xywh_first_frame'
 baselines[p['project_id']]={'name':p['name'],'fov_details':fovs,'mat_sha256_unchanged':p['main_hash_unchanged']}
missing={};expected_missing_counts={'anof_060224_cres':9,'anof_060224_cres_ARS1':6}
histrows=root/'historical-reference-baseline-20261106/references.jsonl'
assert hashlib.sha256(histrows.read_bytes()).hexdigest()=='7c590a9c5950d711c7a517aeee28568577f150409cbccffd63f6ad07a5aff255'
for line in histrows.open():
 r=json.loads(line)
 if not r['present']:missing.setdefault(r['project_id'],set()).add(r['path'])
projects=[];raw_dirs=set();total_fovs=total_rois=total_samples=0
for p in q['projects']:
 pid=p['project_id'];stem=p['name'];b=baselines[pid];assert b['name']==stem
 copied=[json.loads(line) for line in (root/'project-copies'/stem/'verify-receipt.jsonl').open()]
 original_main=next(r for r in copied if r['path']==stem+'.mat');assert b['mat_sha256_unchanged']==original_main['sha256']
 main=next(f for f in p['files'] if f['path']==stem+'.mat');assert main['metadata_relinked']
 fovs=[]
 for f in b['fov_details']:
  assert f['fov']==len(fovs)+1
  channels=[]
  for c in f['channels']:
   assert c['year']=='2024_1';relative=c['relative_path'];assert '..' not in Path(relative).parts
   raw_dirs.add(relative);channels.append({'relative_path':relative,'saved_entries':c.get('saved_entries',c.get('entries'))})
  samples=f['samples'];assert samples and all(s['year']=='2024_1' for s in samples)
  assert f['crop_match'] in ['exact_xywh_first_frame','exact_inclusive_xywh_first_frame','not_applicable_no_saved_roi_images']
  blanks=[i+1 for i,a in enumerate(f.get('availability',[])) if a.get('original_path_empty')]
  assert not blanks or stem=='movie_250124_diploids'
  fovs.append({'fov':f['fov'],'rois':f['rois'],'channels':channels,'samples':samples,'crop_match':f['crop_match'],'blank_roi_indices':blanks})
 all_files=[{'path':f['path'],'bytes':f['source_bytes'],'sha256':f['expected_sha256'],'metadata_relinked':f['metadata_relinked']} for f in p['files']]
 processors=[f for f in all_files if '/processor/' in f['path'] and f['path'].endswith('.mat') and f['metadata_relinked']]
 allowed_missing=sorted(missing.get(pid,set()));assert len(allowed_missing)==expected_missing_counts.get(stem,0)
 projects.append({'project_id':pid,'name':stem,'main_sha256':main['expected_sha256'],'fovs':fovs,'files':all_files,'directories':p['directories'],
  'processors':processors,'expected_missing_raw_paths':allowed_missing,'expected_saved_size_mismatches':{'anof_060224_cres':27,'anof_060224_cres_ARS1':18,'movie_250124_diploids':6396}.get(stem,0),
  'preexisting_missing_derived_layout':stem=='anof_230124_RMng'})
 total_fovs+=len(fovs);total_rois+=sum(f['rois'] for f in fovs);total_samples+=sum(len(f['samples']) for f in fovs)
manifest=root/'2024_1/source-before.jsonl';h=hashlib.sha256();directories={d:[] for d in raw_dirs}
for line in manifest.open('rb'):
 h.update(line);r=json.loads(line)
 if r['kind']=='file' and str(Path(r['path']).parent) in directories:directories[str(Path(r['path']).parent)].append({'name':Path(r['path']).name,'bytes':r['bytes']})
assert h.hexdigest()=='8fd64d094fe29707e0ddd0b1480e9b861786d3adde677087ca7a6e95e252f61e'
assert all(directories.values())
work.mkdir(mode=0o700)
request={'projects':projects,'project_namespace':'final','read_only':True,'source_deleted':False,'engine_sha':'d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7',
 'raw_directories':[{'path':d,'files':files} for d,files in sorted(directories.items())],'expected_projects':16,'expected_fovs':total_fovs,'expected_rois':total_rois,'expected_raw_samples':total_samples,'provenance':provenance,
 'note':'Host validation must preserve the immutable availability baselines; absent data must never be reported as readable. Request preparation does not publish folders or waive any gate.'}
raw=(json.dumps(request,separators=(',',':'))+'\n').encode();(work/'request.json').write_bytes(raw);(work/'request.json.gz').write_bytes(gzip.compress(raw,mtime=0))
summary={'success':True,'request_sha256':hashlib.sha256(raw).hexdigest(),'compressed_request_sha256':hashlib.sha256((work/'request.json.gz').read_bytes()).hexdigest(),
 'projects':16,'fovs':total_fovs,'rois':total_rois,'raw_samples':total_samples,'raw_directories':len(directories),'raw_expected_files':sum(map(len,directories.values())),
 'request_bytes':len(raw),'compressed_request_bytes':(work/'request.json.gz').stat().st_size,'publication_performed':False,'host_tests_started':False,'source_deleted':False}
(work/'preparation-summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary))
'''

if __name__=='__main__':
    result=remote('linux',CODE,timeout=90)
    (HERE/'year1-host-input-preparation-verification.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result))
