"""Plan MAT/JSON paths of the two independently verified special layouts."""
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
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
identities={}
for folder in ['exception-identity-20261106','gilles-hdf5-identity-20261106']:
 b=root/folder;assert (b/'controller.exit').read_text().strip()=='0'
 for p in json.loads((b/'result.json').read_text())['projects']:
  if p['success']:identities[p['name']]=p
assert set(identities)=={'qp_250724_dbud4stest','281124_Alex'}
requests={p['name']:p for p in json.loads((root/'exception-identity-20261106/request.json').read_text())['projects']}
projects=[]
for name,row in identities.items():
 item=requests[name];b=root/'exception-copies-20261106'/name
 raw=(b/'source-before.jsonl').read_bytes();assert raw==(b/'source-after.jsonl').read_bytes()
 files=[r for r in (json.loads(line) for line in raw.splitlines()) if r['kind']=='file']
 actual=[r for r in (json.loads(line) for line in (b/'destination.jsonl').read_bytes().splitlines()) if r['kind']=='file']
 assert {r['path']:r['sha256'] for r in files}=={r['path']:r['sha256'] for r in actual}
 parents=(['/data/Alexander/qp','/Volumes/DATA/Alexander/qp','/mnt/detecdiv-secondary-sauvegarde/Alexander/nobackup/qp','/Volumes/Sauvegarde/Alexander/nobackup/qp']
          if name=='qp_250724_dbud4stest' else ['X:/Gilles/analysis/projects','/data/Gilles/analysis/projects','/Volumes/DATA/Gilles/analysis/projects'])
 container='/homes2/maliavko/DetecdivHub/projects/'+name;mappings={}
 for parent in parents:
  mappings[parent+'/'+name]=container+'/'+name;mappings[parent+'/'+name+'.mat']=container+'/'+name+'.mat'
 for fov in row['fov_details']:
  for channel in fov['channels']:
   acq=channel['relative_path'].split('/')[0];old=channel['old_path'].replace(chr(92),'/').rstrip('/');marker='/'+acq
   at=(old+'/').find(marker+'/');assert at>=0 and old.split('/').count(acq)==1
   oldroot=old[:at+len(marker)];target='/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/'+channel['year']+'/'+acq
   assert oldroot not in mappings or mappings[oldroot]==target;mappings[oldroot]=target
 projects.append({'project_id':item['project_id'],'name':name,'stage':item['stage'],'final_container':container,'io_parent_aliases':parents,
   'mappings':[{'old':a,'target':b} for a,b in sorted(mappings.items(),key=lambda p:-len(p[0]))],
   'verified_files':[{'path':r['path'],'sha256':r['sha256']} for r in files]})
w=root/'exception-path-audit-20261106';assert not w.exists();task='alex2024-exception-paths-20261106'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
w.mkdir(mode=0o700);(w/'audit_persistent_paths.m').write_bytes(script);(w/'request.json').write_text(json.dumps({'projects':projects,'read_only':True,'script_sha256':SHA})+'\n')
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');audit_persistent_paths('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'projects':[p['name'] for p in projects],'script_sha256':SHA,'read_only':True,'owners_and_scientific_values_unchanged':True,'hdf5_image_metadata_review_separate':True}))
'''


def main():
    script=(HERE/'audit_persistent_paths.m').read_bytes();prefix='SHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'
    result=remote('linux',prefix+CODE,timeout=90)
    with (HERE/'exception-path-audit-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
