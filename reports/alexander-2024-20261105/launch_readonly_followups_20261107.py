"""Bounded readonly follow-ups; never write scientific files or replay a task."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE = r'''
import base64,hashlib,json,os,shlex,subprocess
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
def completed(name,sha):
 d=root/name;assert (d/'controller.exit').read_text().strip()=='0'
 raw=(d/'result.json').read_bytes();assert hashlib.sha256(raw).hexdigest()==sha
 result=json.loads(raw);assert result['success']
 return d,result
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
if MODE=='gpu':
 d,result=completed('relink-batch1-20261106','e2f0b82b394bb7f7355a7e0ec503bbe0ee45b5c1cae49d60db1f4892993c4cea')
 before,typed=completed('relink-value-types-20261106','c521b807b7d8b3f3e8329ca4242eec1062e7f91a25e8439f8631f19399172229')
 assert len(result['files'])==len(list(d.glob('modified-*.json')))==23
 assert sum(f['path_changes'] for f in result['files'])==636618
 plans={(f['project_id'],f['relative_path']):f for f in json.loads((d/'request.json').read_text())['files']}
 files=[]
 for f in result['files']:
  assert all(f[k] for k in ['scientific_values_types_and_annotations_preserved','gpu_class_and_content_preserved','save_reopen_verified','original_preserved'])
  item=plans[f['project_id'],f['relative_path']]
  assert item['sha256']==f['before_sha256']
  files.append({'project_id':f['project_id'],'relative_path':f['relative_path'],'target':item['target'],'sha256':f['after_sha256']})
 name='relink-batch1-gpu-postflight-20261107';function='audit_relink_value_types'
 request={'files':files,'read_only':True,'script_sha256':SHA,'baseline_result_sha256':hashlib.sha256((before/'result.json').read_bytes()).hexdigest(),
  'relink_result_sha256':hashlib.sha256((d/'result.json').read_bytes()).hexdigest(),'comparison_pending':True}
elif MODE=='availability':
 old=root/'availability-variants-identity-20261106'
 assert (old/'controller.exit').read_text().strip()=='1'
 log=(old/'controller.log').read_text();assert 'Dot indexing is not supported' in log and 'line 6' in log
 assert not (old/'result.json').exists() and not list(old.glob('project-*.partial.json')) and not (old/'progress.json').exists()
 assert 'alex2024-availability-variants-20261106' not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
 request=json.loads((old/'request.json').read_text());assert len(request['projects'])==6 and request['read_only']
 request.update(script_sha256=SHA,prior_failure='Heterogeneous JSON project objects decode to a MATLAB cell array; failed before any FOV',
  prior_request_sha256=hashlib.sha256((old/'request.json').read_bytes()).hexdigest())
 name='availability-variants-identity-v2-20261107';function='inspect_availability_variants_v2'
elif MODE=='hdf5':
 base=root/'exception-copies-20261106';assert (base/'controller.exit').read_text().strip()=='0'
 state=json.loads((base/'state.json').read_text());p=next(p for p in state['projects'] if p['name']=='281124_Alex')
 assert p['hashes_equal'] and p['original_unchanged'] and p['catalog_owner']=='gilles'
 raw=(base/p['name']/'destination.jsonl').read_bytes();assert hashlib.sha256(raw).hexdigest()==p['destination_receipt_sha256']
 source=(base/p['name']/'source-before.jsonl').read_bytes();assert source==(base/p['name']/'source-after.jsonl').read_bytes()
 records=[r for r in map(json.loads,raw.splitlines()) if r['kind']=='file' and r['path'].endswith('.h5')];assert len(records)==58
 files=[{'relative_path':r['path'],'target':str(Path(p['stage'])/r['path']),'sha256':r['sha256']} for r in records]
 name='gilles-hdf5-metadata-20261107';function='audit_gilles_hdf5_metadata'
 request={'files':files,'read_only':True,'script_sha256':SHA,'verified_copy_receipt_sha256':p['destination_receipt_sha256']}
elif MODE in ['figure','types']:
 projects=[];files=[];audit_hashes=[]
 for folder,sha in [('persistent-path-audit-20261106-batch3','91a4ef8b030afdad786a3d45b1667c31b1d6ea553bc2f4377e568316c0194f8d'),
                    ('persistent-path-audit-20261106-batch4','16204a8dca332677a6ac46dd9ae22ad814924acee3d120f94b325256f0909169')]:
  d,result=completed(folder,sha);assert all(p['files_with_unresolved_paths']==0 for p in result['projects'])
  records={p['project_id']:p for p in json.loads((d/'request.json').read_text())['projects']}
  raw=(d/'receipt.jsonl').read_bytes();audit_hashes.append(hashlib.sha256(raw).hexdigest())
  for row in map(json.loads,raw.splitlines()):
   assert row['unresolved_path_values']==0 and not row['file_saved']
   if row['recognized_rebase_values']:
    assert row['relative_path'].endswith('.mat') and not Path(row['relative_path']).name.startswith('im_')
    p=records[row['project_id']]
    files.append({'project_id':row['project_id'],'relative_path':row['relative_path'],'target':str(Path(p['stage'])/row['relative_path']),'sha256':row['sha256_unchanged']})
  projects.extend(records.values())
 assert len(projects)==len({p['project_id'] for p in projects})==9 and len(files)==18
 if MODE=='figure':
  for p in projects:p['verified_files']=[f for f in p['verified_files'] if f['path'].lower().endswith('.fig')]
  request={'projects':projects,'read_only':True,'script_sha256':SHA,'audit_receipt_hashes':audit_hashes}
  name='figure-path-audit-20261107-batch3';function='audit_figure_paths_v2'
 else:
  request={'files':files,'read_only':True,'script_sha256':SHA,'audit_receipt_hashes':audit_hashes}
  name='relink-value-types-20261107-batch2';function='audit_relink_value_types'
else:raise AssertionError('Unsupported bounded followup')
w=root/name;assert not w.exists(),'Review existing receipts; do not replay'
task='alex2024-'+name
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
w.mkdir(mode=0o700);(w/'request.json').write_text(json.dumps(request)+'\n');(w/(function+'.m')).write_bytes(script)
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');"+function+"('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'script_sha256':SHA,'read_only':True,'source_deleted':False,
 'projects':len(request.get('projects',[])),'files':len(request.get('files',[])),'comparison_pending':MODE=='gpu'}))
'''

def main():
    mode=sys.argv[1]
    filename={'gpu':'audit_relink_value_types.m','availability':'inspect_availability_variants_v2.m',
              'figure':'audit_figure_paths_v2.m','types':'audit_relink_value_types.m','hdf5':'audit_gilles_hdf5_metadata.m'}[mode]
    output=HERE/('readonly-'+mode+'-launch-20261107.json')
    assert not output.exists(), 'Inspect prior launch instead of replay'
    script=(HERE/filename).read_bytes()
    prefix='MODE='+repr(mode)+'\nSHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'
    result=remote('linux',prefix+CODE,timeout=90)
    with output.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))

if __name__=='__main__':main()
