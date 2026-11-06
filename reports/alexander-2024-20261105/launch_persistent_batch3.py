"""Readonly metadata plans for four newly reviewed standard-layout copies."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import json,hashlib,base64,os,subprocess,shlex
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');sources=[]
baseline=root/'historical-reference-baseline-20261106'
assert (baseline/'controller.exit').read_text().strip()=='0'
parity=root/'historical-reference-parity-20261106';assert (parity/'controller.exit').read_text().strip()=='0'
assert json.loads((parity/'result.json').read_text())['success']
sources.extend((p['project_id'],p['name'],p['fovs']) for p in json.loads((baseline/'result.json').read_text())['projects'])
for folder in ['blank-roi-identity-continuation-20261106','identity-audit-20261106-batch3']:
 b=root/folder;assert (b/'controller.exit').read_text().strip()=='0'
 result=json.loads((b/'result.json').read_text());assert result['success']
 sources.extend((p['project_id'],p['name'],p['fov_details']) for p in result['projects'])
assert len(sources)==4 and len({p[0] for p in sources})==4
state=json.loads((root/'project-copies/state.json').read_text());verified={p['project_id']:p for p in state['projects']}
projects=[]
for project_id,name,fovs in sources:
 p=verified[project_id];assert p['name']==name and p['phase']=='copy_verified_identity_relink_and_hosts_pending'
 w=root/'project-copies'/name;before=(w/'source-before.jsonl').read_bytes();assert before==(w/'source-after.jsonl').read_bytes()
 assert hashlib.sha256(before).hexdigest()==p['source_inventory']['sha256']
 raw=(w/'verify-receipt.jsonl').read_bytes();assert hashlib.sha256(raw).hexdigest()==p['verification_receipt']['sha256']
 files=[json.loads(line) for line in raw.splitlines()];assert len(files)==p['files']
 assert all(f['source_destination_hash_equal'] and f['owner']=='maliavko' for f in files)
 container='/homes2/maliavko/DetecdivHub/projects/'+name
 parents=['Z:/Alexander/analysis','/DataVault/Alexander/analysis','/Volumes/DATA/Alexander/analysis','/data/Alexander/analysis','/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis']
 mappings={}
 for parent in parents:
  mappings[parent+'/'+name]=container+'/'+name;mappings[parent+'/'+name+'.mat']=container+'/'+name+'.mat'
 for fov in fovs:
  for channel in fov['channels']:
   acq=channel['relative_path'].split('/')[0];old=channel['old_path'].replace(chr(92),'/').rstrip('/')
   marker='/'+acq;at=(old+'/').find(marker+'/');assert at>=0 and old.split('/').count(acq)==1
   oldroot=old[:at+len(marker)];target='/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/'+channel['year']+'/'+acq
   assert oldroot not in mappings or mappings[oldroot]==target;mappings[oldroot]=target
 projects.append({'project_id':project_id,'name':name,'stage':p['stage'],'final_container':container,'io_parent_aliases':parents,
  'mappings':[{'old':a,'target':b} for a,b in sorted(mappings.items(),key=lambda v:-len(v[0]))],
  'verified_files':[{'path':r['path'],'sha256':r['sha256']} for r in files]})
w=root/'persistent-path-audit-20261106-batch3';assert not w.exists();task='alex2024-persistent-paths-batch3-20261106'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
w.mkdir(mode=0o700);(w/'audit_persistent_paths.m').write_bytes(script)
(w/'request.json').write_text(json.dumps({'projects':projects,'read_only':True,'script_sha256':SHA,'historical_baseline_is_not_strict_cache_success':True})+'\n')
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');audit_persistent_paths('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'projects':[p['name'] for p in projects],'script_sha256':SHA,'read_only':True,'scientific_saved':False,'source_deleted':False}))
'''


def main():
    script=(HERE/'audit_persistent_paths.m').read_bytes()
    prefix='SHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'
    result=remote('linux',prefix+CODE,timeout=90)
    with (HERE/'persistent-batch3-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
