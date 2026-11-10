"""Build future final-host inputs; this does not publish or launch any reader."""
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE = r'''
import gzip,hashlib,json
from pathlib import Path
r=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');out=r/'year2-final-host-inputs-20261110'
assert not out.exists(),'Inspect prepared request instead of replay'
base=(r/'year2-host-baselines-20261110/baseline-request.json').read_bytes()
assert hashlib.sha256(base).hexdigest()=='d75d3505121fc321d9e4727e18ce129e576f1cb9fb7f6e0967e613c643c61a43'
q=json.loads(base);audit=r/'year2-incoming-integrity-20261110'
assert (audit/'controller.exit').read_text().strip()=='0'
raw=(audit/'result.json').read_bytes();assert hashlib.sha256(raw).hexdigest()=='3d619d2a8c6bad75ec3118e333cbafd87550b0e8f5fa16326cf21c09c9a2ee29'
integrity=json.loads(raw);assert integrity['success'] and not integrity['deferred_projects']
assert len(q['projects'])==len(integrity['projects'])==17
planraw=(r/'gilles-active-json-plan-20261107.json').read_bytes()
assert hashlib.sha256(planraw).hexdigest()=='bd7d86902d14a844046a2140f91c926dbcd9e6984db0ca7054f74a76607466c3'
plan=json.loads(planraw)
for p,proof in zip(q['projects'],integrity['projects']):
 assert p['project_id']==proof['project_id'] and p['name']==proof['name']
 receipt=(audit/p['name']/'target-full-hashes.jsonl').read_bytes();assert hashlib.sha256(receipt).hexdigest()==proof['target_receipt_sha256']
 actual={f['path']:f for f in map(json.loads,receipt.splitlines()) if f['kind']=='verified_file'}
 assert set(actual)=={f['path'] for f in p['files']}
 for f in p['files']:
  assert actual[f['path']]['sha256']==f['expected_sha256']
  f['bytes']=actual[f['path']]['bytes'];f['sha256']=f['expected_sha256']
 p['processors']=[f for f in p['files'] if '/processor/' in f['path'] and f['path'].endswith('.mat') and f['metadata_relinked']]
 p['typed_json']=[];p['external_dependencies']=[]
 if p['name']=='281124_Alex':
  assert p['project_id']==plan['project_id']
  for f in plan['files']:
   assert f['unapproved_values_preserved']
   record=actual[f['relative_path']]
   p['typed_json'].append({'path':f['relative_path'],'sha256':record['sha256'],'changes':f['changes']})
  for relative in ['Gilles/analysis/pipelines/daughter_lineage/daughter_lineage/pipeline.json','Gilles/analysis/pipelines/sam31_inference/sam31_inference/pipeline.json','Gilles/sam31/sam31_1/sam31_1_classification.mat']:
   path=Path('/data')/relative;before=path.stat();h=hashlib.sha256()
   with path.open('rb') as stream:
    for block in iter(lambda:stream.read(4*1024*1024),b''):h.update(block)
   after=path.stat();assert (before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns)
   p['external_dependencies'].append({'canonical_path':str(path),'bytes':before.st_size,'sha256':h.hexdigest()})
  assert len(p['typed_json'])==9 and len(p['external_dependencies'])==3
q.update(purpose='future_final_host_validation_inputs',read_only=True,project_namespace='final',host_execution_allowed=False,
 expected_projects=17,expected_fovs=239,expected_rois=43102,expected_raw_samples=3525,expected_blank_roi_paths=412,
 terminal_integrity_result_sha256=hashlib.sha256(raw).hexdigest(),raw_full_sha1_stability_and_publication_required_before_execution=True)
assert sum(len(p['fovs']) for p in q['projects'])==239 and sum(f['rois'] for p in q['projects'] for f in p['fovs'])==43102
raw=(json.dumps(q,separators=(',',':'))+'\n').encode();out.mkdir(mode=0o700)
(out/'request.json').write_bytes(raw);(out/'request.json.gz').write_bytes(gzip.compress(raw,mtime=0))
summary={'success':True,'request_sha256':hashlib.sha256(raw).hexdigest(),'request_bytes':len(raw),'projects':17,'fovs':239,'rois':43102,'raw_samples':3525,
 'blank_roi_paths':412,'gilles_typed_json_files':9,'external_dependencies_fingerprinted':3,'host_execution_allowed':False,
 'host_tests_started':False,'publication_performed':False,'scientific_files_saved':False,'source_deleted':False,
 'raw_year2_sha1_stability_publication_and_fresh_own_leases_still_required':True}
(out/'preparation-summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary))
'''

if __name__ == '__main__':
    output = HERE / 'year2-final-host-input-preparation-verification.json'
    assert not output.exists()
    result = remote('linux', CODE, timeout=90)
    with output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))
