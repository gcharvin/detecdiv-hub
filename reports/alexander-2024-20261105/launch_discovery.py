"""Discover cross-project 2024 references with the protected MATLAB engine."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote


def main():
    snapshot=json.loads((HERE/'runtime/catalog-before.json').read_text())
    busy={j['project_id'] for j in snapshot['jobs'] if j['project_id']}|{l['project_id'] for l in snapshot['locks']}
    projects=[]
    for p in snapshot['projects']:
        locations=sorted(p['locations'],key=lambda l:not l['preferred'])
        files=[]
        for l in locations:
            if l['prefix'].startswith('/') and l['file']:
                files.append('/'.join(x.strip('/') for x in [l['prefix'],l['relative'],l['file']] if x))
                files[-1]='/'+files[-1]
        if files:projects.append({k:p[k] for k in ['id','name','owner']}|{'files':files})
    projects.sort(key=lambda p:(p['owner']!='alexander',p['name']))
    script=(HERE/'discover_projects.m').read_bytes()
    code=r'''
import base64,hashlib,json,os,shlex,subprocess
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
os.umask(0o077)
assert w.is_dir() and not (w/'project-discovery-request.json').exists()
task='alex2024-project-discovery-20261105'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
acquisitions=[]
for year in ['2024_1','2024_2']:
 for p in sorted((Path('/data/Alexander/data')/year).iterdir()):
  if p.name not in {'@eaDir','#recycle','#snapshot','.snapshot','@Recycle'} and p.is_dir():
   assert not p.is_symlink();acquisitions.append({'year':year,'name':p.name})
request={'projects':PROJECTS,'busy_project_ids':BUSY,'acquisitions':acquisitions,'read_only':True,'script_sha256':SHA}
(w/'project-discovery-request.json').write_text(json.dumps(request)+'\n')
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
(w/'discover_projects.m').write_bytes(script)
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
assert Path(release).is_dir()
expression="maxNumCompThreads(1);addpath('"+str(w)+"');discover_projects('"+str(w)+"','"+release+"');"
args=['nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'project-discovery.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'project-discovery.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'projects':len(request['projects']),'alexander_projects_first':sum(p['owner']=='alexander' for p in request['projects']),
 'acquisition_directories':len(acquisitions),'busy_projects_deferred':len(request['busy_project_ids']),'script_sha256':SHA,'engine_release':release,'read_only':True}))
'''
    code=('PROJECTS='+repr(projects)+'\nBUSY='+repr(sorted(busy))+'\nSHA='+repr(hashlib.sha256(script).hexdigest())+
          '\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+code)
    result=remote('linux',code,timeout=60)
    (HERE/'project-discovery-launch.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
