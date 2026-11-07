"""Readonly path/type plans for the protected Lox main; no access/deletion waiver."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote,api
from launch_lox_available_fields import ADMISSION

CODE=r'''
import base64,hashlib,json,os,shlex,subprocess
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
w=root/'lox-metadata-preparation-20261107';task='alex2024-lox-metadata-preparation-20261107'
assert not w.exists() and task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
identity=root/'lox-available-fields-20261107'
assert (identity/'controller.exit').read_text().strip()=='0'
raw=(identity/'result.json').read_bytes();assert hashlib.sha256(raw).hexdigest()=='a745d623392c399830379ffc3665c960dca665762758dbb0cb1897eab2c07f41'
result=json.loads(raw);assert result['available_fields_verification_success'] and not result['strict_project_access_gate_passed'] and not result['cleanup_gate_passed']
original=json.loads((identity/'request.json').read_text());p=original['project'];name=p['name']
assert name=='anof_290424_loxP15' and p['project_id']=='de636b9e-684a-4ab1-bb4a-0bc64d435020'
assert p['mat_sha256']=='c1a32a8cca1847fe72937423501a1ae8ddbb0c9d1b03460d33f081b5b6558898'
assert [f['path'] for f in p['verified_files']]==[name+'.mat'],'Scope only the verified main-only copy'
container='/homes2/maliavko/DetecdivHub/projects/'+name
parents=['Z:/Alexander/analysis','/DataVault/Alexander/analysis','/Volumes/DATA/Alexander/analysis','/data/Alexander/analysis','/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis']
mappings={}
for parent in parents:
 mappings[parent+'/'+name]=container+'/'+name
 mappings[parent+'/'+name+'.mat']=container+'/'+name+'.mat'
mappings['Z:/Alexander/data/290424_loxP15']='/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/2024_1/290424_loxP15'
item={'project_id':p['project_id'],'name':name,'stage':p['stage'],'final_container':container,'io_parent_aliases':parents,
 'mappings':[{'old':a,'target':b} for a,b in sorted(mappings.items(),key=lambda pair:-len(pair[0]))],
 'verified_files':[{'path':name+'.mat','sha256':p['mat_sha256']}]}
scripts={key:base64.b64decode(value) for key,value in PAYLOAD.items()}
assert all(hashlib.sha256(value).hexdigest()==SHAS[key] for key,value in scripts.items())
w.mkdir(mode=0o700)
for key,value in scripts.items():(w/key).write_bytes(value)
for sub in ['paths','types']:(w/sub).mkdir(mode=0o700)
common={'read_only':True,'identity_result_sha256':hashlib.sha256(raw).hexdigest(),'strict_project_access_gate_passed':False,'cleanup_gate_passed':False,'admission':ADMITTED}
(w/'paths/request.json').write_text(json.dumps(dict(common,projects=[item],script_sha256=SHAS['audit_persistent_paths.m']))+'\n')
(w/'types/request.json').write_text(json.dumps(dict(common,files=[{'project_id':p['project_id'],'relative_path':name+'.mat','target':str(Path(p['stage'])/(name+'.mat')),'sha256':p['mat_sha256']}],script_sha256=SHAS['audit_relink_value_types.m']))+'\n')
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');audit_persistent_paths('"+str(w/'paths')+"','"+release+"');audit_relink_value_types('"+str(w/'types')+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'script_sha256':SHAS,'read_only':True,'metadata_mats':1,'strict_project_access_gate_passed':False,'cleanup_gate_passed':False,'writer_launched':False,'scientific_files_saved':False}))
'''

if __name__=='__main__':
    output=HERE/'lox-metadata-preparation-launch.json';assert not output.exists(),'Inspect current task instead of replay'
    admitted=api(ADMISSION)
    (HERE/'runtime/lox-metadata-preparation-admission.json').write_text(json.dumps(admitted,indent=2)+'\n')
    scripts={name:(HERE/name).read_bytes() for name in ['audit_persistent_paths.m','audit_relink_value_types.m']}
    payload={name:base64.b64encode(data).decode() for name,data in scripts.items()}
    hashes={name:hashlib.sha256(data).hexdigest() for name,data in scripts.items()}
    result=remote('linux','PAYLOAD='+repr(payload)+'\nSHAS='+repr(hashes)+'\nADMITTED='+repr(admitted)+'\n'+CODE,timeout=90)
    with output.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result))
