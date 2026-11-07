"""Readonly types and FIG review for only four availability-qualified metadata-batch6 projects."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import base64,hashlib,json,os,shlex,subprocess
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');base=root/'persistent-path-audit-20261107-batch6'
assert (base/'controller.exit').read_text().strip()=='0'
raw=(base/'result.json').read_bytes();assert hashlib.sha256(raw).hexdigest()=='af9ec9e5e742e897160cbc73e6d7cbb61612476adcd51ad916a1b2b066766c6a'
result=json.loads(raw);assert result['success'] and all(p['files_with_unresolved_paths']==0 for p in result['projects'])
projects=json.loads((base/'request.json').read_text())['projects'];assert len(projects)==4
assert not {p['project_id'] for p in projects}&{p['project_id'] for p in json.loads((root/'relink-batch2-20261107/controller-request.json').read_text())['projects']}
receipt=(base/'receipt.jsonl').read_bytes();changes=[r for r in map(json.loads,receipt.splitlines()) if r['recognized_rebase_values']]
assert len(changes)==6 and sum(r['recognized_rebase_values'] for r in changes)==110444 and all(not r['unresolved_path_values'] and not r['file_saved'] for r in changes)
if MODE=='types':
 mapping={p['project_id']:p for p in projects};files=[]
 for r in changes:
  assert r['relative_path'].endswith('.mat') and not Path(r['relative_path']).name.startswith('im_')
  files.append({'project_id':r['project_id'],'relative_path':r['relative_path'],'target':str(Path(mapping[r['project_id']]['stage'])/r['relative_path']),'sha256':r['sha256_unchanged']})
 name='relink-value-types-20261107-batch4';function='audit_relink_value_types'
 request={'files':files,'read_only':True,'script_sha256':SHA,'metadata_receipt_sha256':hashlib.sha256(receipt).hexdigest()}
elif MODE=='figure':
 for p in projects:p['verified_files']=[r for r in p['verified_files'] if r['path'].lower().endswith('.fig')]
 name='figure-path-audit-20261107-batch5';function='audit_figure_paths_v2'
 request={'projects':projects,'read_only':True,'script_sha256':SHA,'metadata_receipt_sha256':hashlib.sha256(receipt).hexdigest()}
else:raise AssertionError('Unsupported operation')
w=root/name;assert not w.exists();task='alex2024-'+name
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
w.mkdir(mode=0o700);(w/'request.json').write_text(json.dumps(request)+'\n');(w/(function+'.m')).write_bytes(script)
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');"+function+"('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'script_sha256':SHA,'read_only':True,'scientific_files_saved':False,
 'projects':len(request.get('projects',[])),'files':len(request.get('files',[])),'figures':sum(len(p['verified_files']) for p in request.get('projects',[]))}))
'''

if __name__=='__main__':
    mode=sys.argv[1];filename={'types':'audit_relink_value_types.m','figure':'audit_figure_paths_v2.m'}[mode]
    output=HERE/('metadata6-'+mode+'-launch.json');assert not output.exists(),'Inspect prior launch instead of replay'
    script=(HERE/filename).read_bytes()
    result=remote('linux','MODE='+repr(mode)+'\nSHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+CODE,timeout=90)
    with output.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))
