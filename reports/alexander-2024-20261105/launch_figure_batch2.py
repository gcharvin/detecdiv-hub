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
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');base=root/'persistent-path-audit-20261106-batch2'
assert (base/'controller.exit').read_text().strip()=='0'
raw=(base/'result.json').read_bytes();assert hashlib.sha256(raw).hexdigest()=='b50266cc462af82dc4e2fb09c35fe9c78955b9d12db2507c8e8dd02455a21471'
result=json.loads(raw);assert result['success'] and all(p['files_with_unresolved_paths']==0 for p in result['projects'])
projects=json.loads((base/'request.json').read_text())['projects']
for project in projects:project['verified_files']=[r for r in project['verified_files'] if r['path'].lower().endswith('.fig')]
assert sum(len(p['verified_files']) for p in projects)==12
w=root/'figure-path-audit-20261106-batch2';assert not w.exists()
task='alex2024-figure-paths-batch2-20261106'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
request={'projects':projects,'metadata_audit_receipt_sha256':hashlib.sha256(raw).hexdigest(),'read_only':True,'script_sha256':SHA}
w.mkdir(mode=0o700);(w/'request.json').write_text(json.dumps(request)+'\n');(w/'audit_figure_paths_v2.m').write_bytes(script)
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');audit_figure_paths_v2('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'projects':len(projects),'script_sha256':SHA,'read_only':True,'roi_image_sidecars_not_loaded_or_written':True,'figures_loaded_read_only_as_mat_including_possible_cdata':True,'metadata_audit_receipt_sha256':request['metadata_audit_receipt_sha256']}))
'''
    result=remote('linux','SHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+code,timeout=90)
    with (HERE/'figure-batch2-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
