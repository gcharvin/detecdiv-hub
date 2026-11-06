"""Launch new readonly reviews; preserve all previous partial/success receipts."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE = r'''
import json,hashlib,base64,os,subprocess,shlex
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
identities=[]
for directory in ['identity-audit-20261106-batch1','identity-audit-20261106-batch2']:
 w=root/directory
 assert (w/'controller.exit').read_text().strip() in ['0','1']
 identities.extend(json.loads((w/'request.json').read_text())['projects'])
unique={}
for p in identities:
 if p['project_id'] in unique:assert p==unique[p['project_id']],'Identity requests disagree'
 unique[p['project_id']]=p
projects=[p for p in unique.values() if p['name'] in NAMES]
assert len(projects)==len(NAMES) and len({p['name'] for p in projects})==len(NAMES)
w=root/WORK;assert not w.exists()
sessions=subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True,check=True).stdout.splitlines()
assert SESSION not in sessions
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
request={'read_only':True,'script_sha256':SHA,'previous_audits_not_replayed':True}
if len(projects)==1:request['project']=projects[0]
else:request['projects']=projects
w.mkdir(mode=0o700);(w/(FUNCTION+'.m')).write_bytes(script);(w/'request.json').write_text(json.dumps(request)+'\n')
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');"+FUNCTION+"('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',SESSION,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':SESSION,'work':str(w),'projects':[p['name'] for p in projects],'script_sha256':SHA,'read_only':True,'source_deleted':False,'catalog_changed':False}))
'''


def main():
    parser=argparse.ArgumentParser();parser.add_argument('audit',choices=['historical','blank-roi']);args=parser.parse_args()
    if args.audit=='historical':
        function='audit_historical_raw_references';work='historical-reference-baseline-20261106'
        names=['anof_060224_cres','anof_060224_cres_ARS1'];session='alex2024-historical-refs-20261106'
    else:
        function='inspect_blank_roi_layout';work='blank-roi-layout-20261106'
        names=['anof_061224_X5152moth'];session='alex2024-blank-roi-layout-20261106'
    script=(HERE/(function+'.m')).read_bytes()
    prefix='\n'.join(f'{key}={value!r}' for key,value in {'NAMES':names,'WORK':work,'SESSION':session,'FUNCTION':function,'SCRIPT':base64.b64encode(script).decode(),'SHA':hashlib.sha256(script).hexdigest()}.items())
    result=remote('linux',prefix+'\n'+CODE,timeout=90)
    with (HERE/(work+'-launch.json')).open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
