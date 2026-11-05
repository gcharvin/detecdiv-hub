"""Inspect only the three previously uncataloged main files, read-only."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import json,hashlib,os,shlex,subprocess
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
assert (root/'project-discovery.exit').read_text().strip()=='0'
original=json.loads((root/'project-discovery-request.json').read_text())
script=(root/'discover_projects.m').read_bytes()
assert hashlib.sha256(script).hexdigest()==original['script_sha256']
w=root/'project-discovery-supplemental';assert not w.exists()
task='alex2024-project-discovery-extra-20261105'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
names=['anof_130224_dfob_rep_dslog','anof_10112023_nh_dhapxs_1530','anof_14092023_leftRDN_nhadhhap4']
projects=[{'id':'uncataloged:'+name,'name':name,'owner':'alexander','files':['/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis/'+name+'.mat']} for name in names]
request={'projects':projects,'busy_project_ids':[],'acquisitions':original['acquisitions'],'read_only':True,
 'script_sha256':original['script_sha256'],'catalog_registration_allowed':False}
w.mkdir(mode=0o700);(w/'discover_projects.m').write_bytes(script);(w/'project-discovery-request.json').write_text(json.dumps(request)+'\n')
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');discover_projects('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'project-discovery.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'project-discovery.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'projects':3,'read_only':True,'script_sha256':original['script_sha256'],'catalog_registration_allowed':False}))
'''

if __name__=='__main__':
    result=remote('linux',CODE,timeout=60)
    with (HERE/'supplemental-discovery-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))
