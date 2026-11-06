"""Prepare read-only scientific path/identity checks for nine verified copies."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote


def main():
    script=(HERE/'inspect_verified_projects.m').read_bytes()
    code=r'''
import json,hashlib,base64,os,subprocess,shlex
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');copies=root/'project-copies'
state=json.loads((copies/'state.json').read_text());assert state['phase']!='failed_safe'
assert len(state['projects'])>=9
projects=[]
for r in state['projects'][:9]:
 w=copies/r['name'];before=(w/'source-before.jsonl').read_bytes();after=(w/'source-after.jsonl').read_bytes()
 assert before==after and hashlib.sha256(before).hexdigest()==r['source_inventory']['sha256']
 raw=(w/'verify-receipt.jsonl').read_bytes();assert hashlib.sha256(raw).hexdigest()==r['verification_receipt']['sha256']
 rows=[json.loads(line) for line in raw.splitlines()];assert len(rows)==r['files'] and sum(f['bytes'] for f in rows)==r['bytes']
 assert all(f['source_destination_hash_equal'] and f['owner']=='maliavko' for f in rows)
 mat=next(f for f in rows if f['path']==r['name']+'.mat')
 projects.append({'project_id':r['project_id'],'name':r['name'],'stage':r['stage'],'source_mat':r['source_mat'],
  'mat_sha256':mat['sha256'],'verified_paths':[f['path'] for f in rows]})
original=json.loads((root/'project-discovery-request.json').read_text())
request={'projects':projects,'acquisitions':original['acquisitions'],'read_only':True,'script_sha256':SHA}
w=root/'identity-audit-20261106-batch1';assert not w.exists()
task='alex2024-identity-audit-batch1-20261106'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
w.mkdir(mode=0o700);(w/'request.json').write_text(json.dumps(request)+'\n');(w/'inspect_verified_projects.m').write_bytes(script)
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');inspect_verified_projects('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'projects':len(projects),'project_ids':[p['project_id'] for p in projects],
 'script_sha256':SHA,'source_copy_receipts_verified':True,'read_only':True,'source_deleted':False,'catalog_changed':False}))
'''
    code='SHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+code
    result=remote('linux',code,timeout=90)
    with (HERE/'identity-batch1-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
