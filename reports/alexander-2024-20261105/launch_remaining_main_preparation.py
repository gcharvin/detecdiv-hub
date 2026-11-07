"""Readonly readiness of three remaining main MATs and typed Gilles folder audit."""
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
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');files=[];projects=[]
for folder in ['persistent-path-audit-movie-20261107','exception-path-audit-20261106']:
 d=root/folder;assert (d/'controller.exit').read_text().strip()=='0'
 req=json.loads((d/'request.json').read_text());projects.extend(req['projects'])
 for p in req['projects']:
  main=next(r for r in p['verified_files'] if r['path']==p['name']+'.mat')
  files.append({'project_id':p['project_id'],'relative_path':main['path'],'target':str(Path(p['stage'])/main['path']),'sha256':main['sha256']})
assert {p['name'] for p in projects}=={'movie_250124_diploids','qp_250724_dbud4stest','281124_Alex'} and len(files)==3
if MODE=='types':
 name='relink-value-types-remaining-mains-20261107';function='audit_relink_value_types'
 request={'files':files,'read_only':True,'script_sha256':SHA}
elif MODE=='gilles':
 p=next(p for p in projects if p['name']=='281124_Alex');p['verified_files']=[r for r in p['verified_files'] if r['path']=='281124_Alex.mat']
 assert len(p['verified_files'])==1
 name='gilles-main-typed-paths-20261107';function='audit_gilles_main_typed'
 request={'projects':[p],'read_only':True,'script_sha256':SHA,'policy':'Exact parsedData.folder maps to its io.path project container; all other scanner/path guards unchanged.'}
else:raise AssertionError('Unsupported operation')
w=root/name;assert not w.exists(),'Inspect existing task instead of replay';task='alex2024-'+name
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
w.mkdir(mode=0o700);(w/'request.json').write_text(json.dumps(request)+'\n');(w/(function+'.m')).write_bytes(script)
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');"+function+"('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'script_sha256':SHA,'read_only':True,'scientific_files_saved':False,'files':len(request.get('files',[])),'projects':len(request.get('projects',[]))}))
'''

if __name__=='__main__':
    mode=sys.argv[1];filename={'types':'audit_relink_value_types.m','gilles':'audit_gilles_main_typed.m'}[mode]
    output=HERE/('remaining-main-'+mode+'-launch.json');assert not output.exists()
    script=(HERE/filename).read_bytes()
    result=remote('linux','MODE='+repr(mode)+'\nSHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+CODE,timeout=90)
    with output.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))
