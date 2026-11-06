"""Audit only newly verified projects 17..23 after completed project 16; preserve all completed/error receipts."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote


def main():
    script=(HERE/'inspect_verified_projects_v2.m').read_bytes()
    code=r'''
import json,hashlib,base64,os,subprocess,shlex
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');copies=root/'project-copies'
assert (root/'identity-audit-20261106-batch3/controller.exit').read_text().strip()=='0'
assert json.loads((root/'identity-audit-20261106-batch3/result.json').read_text())['success']
state=json.loads((copies/'state.json').read_text());assert state['phase']!='failed_safe' and len(state['projects'])>=23
projects=[]
for r in state['projects'][16:23]:
 assert r['phase']=='copy_verified_identity_relink_and_hosts_pending'
 w=copies/r['name'];before=(w/'source-before.jsonl').read_bytes();after=(w/'source-after.jsonl').read_bytes()
 assert before==after and hashlib.sha256(before).hexdigest()==r['source_inventory']['sha256']
 raw=(w/'verify-receipt.jsonl').read_bytes();assert hashlib.sha256(raw).hexdigest()==r['verification_receipt']['sha256']
 rows=[json.loads(line) for line in raw.splitlines()];assert len(rows)==r['files'] and sum(f['bytes'] for f in rows)==r['bytes']
 assert all(f['source_destination_hash_equal'] and f['owner']=='maliavko' for f in rows)
 mat=next(f for f in rows if f['path']==r['name']+'.mat')
 projects.append({'project_id':r['project_id'],'name':r['name'],'stage':r['stage'],'source_mat':r['source_mat'],'mat_sha256':mat['sha256'],'verified_paths':[f['path'] for f in rows]})
assert len(projects)==7 and all(p['name']!='anof_060224_cres' for p in projects)
original=json.loads((root/'project-discovery-request.json').read_text())
w=root/'identity-audit-20261106-batch4';assert not w.exists()
task='alex2024-identity-audit-batch4-20261106'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
request={'projects':projects,'acquisitions':original['acquisitions'],'read_only':True,'script_sha256':SHA,'completed_or_reviewed_projects_1_to_16_not_replayed':True}
w.mkdir(mode=0o700);(w/'request.json').write_text(json.dumps(request)+'\n');(w/'inspect_verified_projects_v2.m').write_bytes(script)
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');inspect_verified_projects_v2('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'projects':len(projects),'project_ids':[p['project_id'] for p in projects],'script_sha256':SHA,'read_only':True,
 'per_project_failures_are_explicit_and_do_not_starve_other_projects':True,'completed_fovs_not_replayed':True,'source_deleted':False,'catalog_changed':False}))
'''
    result=remote('linux','SHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+code,timeout=90)
    with (HERE/'identity-batch4-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
