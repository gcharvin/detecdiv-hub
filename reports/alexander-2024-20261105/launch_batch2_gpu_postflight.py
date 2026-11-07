"""Readonly type/content corroboration of only 18 completed batch2 MATs."""
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
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');base=root/'relink-batch2-20261107'
assert (base/'controller.exit').read_text().strip()==(base/'matlab.exit').read_text().strip()=='0'
raw=(base/'result.json').read_bytes();assert hashlib.sha256(raw).hexdigest()=='42637ffb72b75ed3af7cd96b97c5d16b1caeeace1137f26d4f0db19e0fc09f93'
result=json.loads(raw);assert result['success'] and len(result['files'])==len(list(base.glob('modified-*.json')))==18
assert sum(f['path_changes'] for f in result['files'])==394123
before=root/'relink-value-types-20261107-batch2';assert (before/'controller.exit').read_text().strip()=='0'
typed=(before/'result.json').read_bytes();assert hashlib.sha256(typed).hexdigest()=='e360db59bdfe41b458d6fefe21a72731fe897a5be588552ef4048162241b410d'
assert sum(len(f['gpu_values']) for f in json.loads(typed)['files'])==2585
plans={(f['project_id'],f['relative_path']):f for f in json.loads((base/'request.json').read_text())['files']};files=[]
for f in result['files']:
 assert all(f[k] for k in ['scientific_values_types_and_annotations_preserved','gpu_class_and_content_preserved','save_reopen_verified','original_preserved'])
 p=plans[f['project_id'],f['relative_path']];assert p['sha256']==f['before_sha256']
 files.append({'project_id':f['project_id'],'relative_path':f['relative_path'],'target':p['target'],'sha256':f['after_sha256']})
w=root/'relink-batch2-gpu-postflight-20261107';assert not w.exists(),'Inspect existing postflight instead of replay'
task='alex2024-relink-batch2-gpu-postflight-20261107'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
w.mkdir(mode=0o700);(w/'audit_relink_value_types.m').write_bytes(script)
(w/'request.json').write_text(json.dumps({'files':files,'read_only':True,'script_sha256':SHA,'baseline_result_sha256':hashlib.sha256(typed).hexdigest(),
 'relink_result_sha256':hashlib.sha256(raw).hexdigest(),'comparison_pending':True})+'\n')
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');audit_relink_value_types('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'script_sha256':SHA,'files':18,'expected_gpu_values':2585,'read_only':True,'comparison_pending':True,'source_deleted':False}))
'''

if __name__=='__main__':
    script=(HERE/'audit_relink_value_types.m').read_bytes()
    result=remote('linux','SHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+CODE,timeout=90)
    with (HERE/'relink-batch2-gpu-postflight-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))
