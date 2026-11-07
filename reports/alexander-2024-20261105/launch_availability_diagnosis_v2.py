"""Diagnose only two failed availability variants; do not repeat pixel reads."""
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
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');base=root/'availability-variants-identity-v2-20261107'
assert (base/'controller.exit').read_text().strip()=='0'
raw=(base/'result.json').read_bytes();assert hashlib.sha256(raw).hexdigest()=='96116cbf6eb3460ee66bcd123d3c163976dfc13be3d4d4ed138493583558652d'
result=json.loads(raw);failed={p['name'] for p in result['projects'] if not p['success']}
assert failed=={'movie_250124_diploids','anof_290424_loxP15'}
original=json.loads((base/'request.json').read_text());projects=[]
for p in original['projects']:
 if p['name'] not in failed:continue
 p['acquisition']='250124_diploids' if p['name']=='movie_250124_diploids' else '290424_loxP15';projects.append(p)
assert len(projects)==2
prior=root/'pending-availability-diagnosis-20261107';assert (prior/'controller.exit').read_text().strip()=='1'
assert 'uniqueCellstrHelper' in (prior/'controller.log').read_text() and not (prior/'result.json').exists()
assert not (root/'pending-availability-diagnosis-v2-20261107').exists()
w=root/'pending-availability-diagnosis-v2-20261107';assert not w.exists();task='alex2024-pending-availability-diagnosis-v2-20261107'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
w.mkdir(mode=0o700);(w/'diagnose_pending_availability_v2.m').write_bytes(script)
(w/'request.json').write_text(json.dumps({'projects':projects,'read_only':True,'script_sha256':SHA,'failed_result_sha256':hashlib.sha256(raw).hexdigest(),'prior_references':str(prior/'references.jsonl'),'prior_references_sha256':hashlib.sha256((prior/'references.jsonl').read_bytes()).hexdigest()})+'\n')
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');diagnose_pending_availability_v2('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'projects':2,'script_sha256':SHA,'read_only':True,'raw_pixels_not_replayed':True,'scientific_files_saved':False}))
'''

if __name__=='__main__':
    script=(HERE/'diagnose_pending_availability_v2.m').read_bytes()
    result=remote('linux','SHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+CODE,timeout=90)
    with (HERE/'pending-availability-diagnosis-v2-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))
