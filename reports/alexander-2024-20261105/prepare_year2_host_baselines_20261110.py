"""Assemble frozen year2 reader baselines without reading or saving scientific files.

This creates private evidence inputs only. It neither launches readers nor grants
publication, catalog cutover or retirement permission.
"""
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE = r'''
import gzip,hashlib,json
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
work=root/'year2-host-baselines-20261110'
assert not work.exists(),'No replay: inspect existing baseline preparation'
provenance=[]
def read(relative,pin):
 p=root/relative;raw=p.read_bytes();sha=hashlib.sha256(raw).hexdigest();assert sha==pin,relative
 provenance.append({'file':relative,'sha256':sha});return json.loads(raw)
pins=[
 ('identity-audit-20261106-batch1/result.partial.json','0b185958de375c30dab634a9981ea830362996f126cab5b935e5743901a5ad20'),
 ('identity-audit-20261106-batch2/result.json','a78f2f75a3b59db332bea3a6347ad7cbb6c03e68a571629ba887b4b5f1d144ff'),
 ('identity-audit-20261106-batch3/result.json','b15ee80f7dd9b86af722979e0ea6b27b789256e74f91af2d53b7d8304d7feb38'),
 ('identity-audit-20261106-batch4/result.json','bf0230b5523a20c9963e9e37bf59715944f4d6082e03b11685253d1bbe4aaf7a'),
 ('identity-audit-20261106-batch5/result.json','3c4b5944cc235b29e88aa3ccc23837f9142679fbf574cd0ccd20edca35f7beac'),
 ('availability-variants-identity-v2-20261107/result.json','96116cbf6eb3460ee66bcd123d3c163976dfc13be3d4d4ed138493583558652d'),
 ('blank-roi-identity-continuation-20261106/result.json','2ff4bcfe55a26ef04f879aeb066869781e91cdf248627e930904a177c987df01'),
 ('exception-identity-20261106/result.json','6970f3d083855fca06a8a1af455e99188c4bd6ef97e350b6fc5f18ea79579298'),
 ('gilles-hdf5-identity-20261106/result.json','38fad3529aa509ea3598926c8737e7ac126743b9769c916e89ac9bf723ff2edf')]
baselines={}
for relative,pin in pins:
 d=read(relative,pin);rows=d['projects'] if isinstance(d,dict) else d
 for row in rows:
  if row.get('success',True) and 'fov_details' in row:
   assert row['project_id'] not in baselines,'Duplicate successful scientific baseline'
   assert not row['saved'];baselines[row['project_id']]=row
blank=read('blank-roi-layout-20261106/result.json','9ec2c8956ce2be6ea3bb010d0be218476ef10a4c7c5b227c5c3a2b5b9db7d836')
assert blank['success'] and not blank['saved'] and len(blank['fovs'])==14
audit=root/'year2-incoming-integrity-20261110'
script=audit/'audit_year2_incoming_integrity_20261110.py'
assert hashlib.sha256(script.read_bytes()).hexdigest()=='05639082a0b5400f5ad25d88b62303c60624de757fc918908b2098622ebc9045'
qraw=(audit/'request.json').read_bytes();q=json.loads(qraw)
assert q['read_only'] and not q['publication_allowed'] and not q['source_deletion_allowed'] and len(q['projects'])==17
projects=[];raw_dirs=set();blank_count=0
for p in q['projects']:
 stem=p['name'];pid=p['project_id'];b=baselines[pid];assert b['name']==stem
 original=root/p['source_receipt_relative'];native=original.read_bytes()
 assert hashlib.sha256(native).hexdigest()==p['source_inventory_sha256']
 original_files={r['path']:r for r in map(json.loads,native.splitlines()) if r['kind']=='file'}
 oldcopy=original.parent
 if p['stage_suffix']=='20261106':
  original_sha=original_files[stem+'.mat']['sha256']
 else:
  original_sha=next(r['sha256'] for r in map(json.loads,(oldcopy/'verify-receipt.jsonl').read_text().splitlines()) if r['path']==stem+'.mat')
 assert original_sha==b['mat_sha256_unchanged']
 main=next(f for f in p['files'] if f['path']==stem+'.mat');assert main['metadata_relinked']
 fovs=[]
 for f in b['fov_details']:
  assert f['fov']==len(fovs)+1
  channels=[]
  for c in f['channels']:
   year=c['year'];relative=c['relative_path'];assert year in ['2024_1','2024_2'] and not Path(relative).is_absolute() and '..' not in Path(relative).parts
   raw_dirs.add((year,relative));channels.append({'year':year,'relative_path':relative,'saved_entries':c.get('saved_entries',c.get('entries'))})
  samples=f['samples'];assert samples
  for s in samples:
   c=channels[s['channel']-1];assert s['year']==c['year'] and str(Path(s['relative_path']).parent)==c['relative_path']
   assert 1<=s['frame']<=c['saved_entries'] and len(s['pixel_sha256'])==64
  blanks=[]
  if stem==blank['name']:
   layout=blank['fovs'][f['fov']-1];assert layout['fov']==f['fov'] and layout['rois']==f['rois']
   blanks=[i+1 for i,r in enumerate(layout['roi_details']) if not r['path']]
   assert len(blanks)==layout['blank_roi_paths']
   assert all(r['embedded_image_empty'] and not r['matching_image_paths'] and not r['matching_result_paths'] for r in layout['roi_details'] if not r['path'])
  blank_count+=len(blanks)
  crop=f['crop_match'];assert crop in ['exact_xywh_first_frame','exact_inclusive_xywh_first_frame','not_applicable_no_saved_roi_images','not_applicable_preexisting_placeholder','not_applicable_preexisting_unextracted']
  if crop=='not_applicable_preexisting_unextracted':assert stem==blank['name'] and len(blanks)==f['rois']
  assert (crop=='not_applicable_preexisting_placeholder')==(stem=='qp_250724_dbud4stest')
  fovs.append({'fov':f['fov'],'rois':f['rois'],'channels':channels,'samples':samples,'crop_match':crop,'blank_roi_indices':blanks})
 assert len(fovs)==b['fovs'] and sum(f['rois'] for f in fovs)==b['rois']
 assert set(original_files)=={f['path'] for f in p['files']}
 projects.append({'project_id':pid,'name':stem,'main_sha256':main['expected_sha256'],'original_main_sha256':original_sha,
  'fovs':fovs,'files':p['files'],'directories':p['directories'],'stage_suffix':p['stage_suffix'],
  'expected_missing_raw_paths':[],'expected_saved_size_mismatches':0,
  'image_policy':'empty_mat_placeholder' if stem=='qp_250724_dbud4stest' else 'hdf5' if stem=='281124_Alex' else 'mat',
  'additional_final_reader_requirements':['parsedData.folder','typed_reusable_run_references','external_dependencies_linux_and_limited_windows','frozen_execution_history_preserved'] if stem=='281124_Alex' else []})
assert blank_count==412
directories={key:[] for key in raw_dirs};manifest_pins={
 '2024_1':'8fd64d094fe29707e0ddd0b1480e9b861786d3adde677087ca7a6e95e252f61e',
 '2024_2':'004fb3404b4af8fe09b6063a09e3a162488f689499ddb51087513daf3d9f4fad'}
for year in sorted({key[0] for key in raw_dirs}):
 h=hashlib.sha256()
 for line in (root/year/'source-before.jsonl').open('rb'):
  h.update(line);row=json.loads(line);key=(year,str(Path(row['path']).parent))
  if row['kind']=='file' and key in directories:directories[key].append({'name':Path(row['path']).name,'bytes':row['bytes']})
 assert h.hexdigest()==manifest_pins[year]
assert all(directories.values())
request={'purpose':'frozen_scientific_baseline_preparation_only','host_execution_allowed':False,'publication_allowed':False,'source_deletion_allowed':False,
 'engine_sha':'d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7','projects':projects,
 'raw_directories':[{'year':key[0],'path':key[1],'files':value} for key,value in sorted(directories.items())],
 'provenance':provenance,'incoming_integrity_request_sha256':hashlib.sha256(qraw).hexdigest(),
 'raw_full_sha1_stability_and_terminal_integrity_final_host_catalog_api_gates_still_required':True}
raw=(json.dumps(request,separators=(',',':'))+'\n').encode()
summary={'success':True,'baseline_preparation_only':True,'projects':len(projects),'fovs':sum(len(p['fovs']) for p in projects),
 'rois':sum(f['rois'] for p in projects for f in p['fovs']),'raw_samples':sum(len(f['samples']) for p in projects for f in p['fovs']),
 'raw_directories':len(directories),'raw_expected_files':sum(map(len,directories.values())),'blank_roi_paths_preserved':blank_count,
 'placeholder_projects':1,'hdf5_projects':1,'request_sha256':hashlib.sha256(raw).hexdigest(),'request_bytes':len(raw),
 'scientific_files_read_or_saved':False,'host_tests_started':False,'leases_acquired':0,'publication_performed':False,'catalog_changed':False,'source_deleted':False,
 'raw_year2_sha1_and_stability_not_replaced':True,'gilles_additional_final_reader_checks_required':True}
work.mkdir(mode=0o700);(work/'baseline-request.json').write_bytes(raw);(work/'baseline-request.json.gz').write_bytes(gzip.compress(raw,mtime=0))
(work/'preparation-summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary))
'''

if __name__ == '__main__':
    output = HERE / 'year2-host-baseline-preparation-verification.json'
    assert not output.exists(), 'Inspect existing inputs instead of replay'
    result = remote('linux', CODE, timeout=120)
    with output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))
