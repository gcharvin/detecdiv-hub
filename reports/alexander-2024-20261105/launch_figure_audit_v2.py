"""Plan every persistent path in only the three identity-corroborated projects."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote


def main():
    script=(HERE/'audit_figure_paths_v2.m').read_bytes()
    code=r'''
import json,hashlib,base64,os,subprocess,shlex
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');identity=root/'identity-audit-20261106-batch1'
assert (root/'figure-path-audit-20261106-batch1/controller.exit').read_text().strip()=='0'
assert (identity/'controller.exit').read_text().strip()=='1'
raw=(identity/'result.partial.json').read_bytes();assert hashlib.sha256(raw).hexdigest()=='0b185958de375c30dab634a9981ea830362996f126cab5b935e5743901a5ad20'
rows=json.loads(raw);assert len(rows)==3
assert all(f['crop_match']!='not_established' for p in rows for f in p['fov_details'])
request_identity=json.loads((identity/'request.json').read_text());originals={p['project_id']:p for p in request_identity['projects']}
projects=[]
for row in rows:
 item=originals[row['project_id']];name=item['name'];container='/homes2/maliavko/DetecdivHub/projects/'+name
 mappings={}
 parents=['Z:/Alexander/analysis','/DataVault/Alexander/analysis','/Volumes/DATA/Alexander/analysis','/data/Alexander/analysis','/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis']
 for parent in parents:
  mappings[parent+'/'+name]=container+'/'+name
  mappings[parent+'/'+name+'.mat']=container+'/'+name+'.mat'
 for fov in row['fov_details']:
  for channel in fov['channels']:
   relative=channel['relative_path'];acq=relative.split('/')[0];old=channel['old_path'].replace(chr(92),'/').rstrip('/')
   marker='/'+acq;at=(old+'/').find(marker+'/');assert at>=0 and old.split('/').count(acq)==1
   oldroot=old[:at+len(marker)];newroot='/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/'+channel['year']+'/'+acq
   assert oldroot not in mappings or mappings[oldroot]==newroot;mappings[oldroot]=newroot
 receipt=root/'project-copies'/name/'verify-receipt.jsonl';files=[json.loads(line) for line in receipt.open()]
 assert all(r['source_destination_hash_equal'] and r['owner']=='maliavko' for r in files)
 projects.append({'project_id':item['project_id'],'name':name,'stage':item['stage'],'final_container':container,'io_parent_aliases':parents,
  'mappings':[{'old':a,'target':b} for a,b in sorted(mappings.items(),key=lambda pair:-len(pair[0]))],
  'verified_files':[{'path':r['path'],'sha256':r['sha256']} for r in files if r['path'].lower().endswith('.fig')]})
w=root/'figure-path-audit-v2-20261106-batch1';assert not w.exists()
task='alex2024-figure-paths-v2-batch1-20261106'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
request={'projects':projects,'identity_receipt_sha256':hashlib.sha256(raw).hexdigest(),'read_only':True,'script_sha256':SHA}
w.mkdir(mode=0o700);(w/'request.json').write_text(json.dumps(request)+'\n');(w/'audit_figure_paths_v2.m').write_bytes(script)
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');audit_figure_paths_v2('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'projects':len(projects),'script_sha256':SHA,'read_only':True,'images_not_loaded_or_written':True,'identity_receipt_sha256':request['identity_receipt_sha256']}))
'''
    result=remote('linux','SHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+code,timeout=90)
    with (HERE/'figure-batch1-v2-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
