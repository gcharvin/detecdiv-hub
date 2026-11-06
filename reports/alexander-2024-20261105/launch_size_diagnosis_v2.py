"""Archive completed identity rows and diagnose only the failed fourth project."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote


def main():
    script=(HERE/'diagnose_saved_sizes_v2.m').read_bytes()
    code=r'''
import json,hashlib,base64,os,shlex,subprocess
from pathlib import Path
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');old=root/'identity-audit-20261106-batch1'
assert (old/'controller.exit').read_text().strip()=='1'
rows=json.loads((old/'result.partial.json').read_text());assert len(rows)==3
assert [r['name'] for r in rows]==['anof_011024_x423x426_EB_x423noebonly','anof_05012024_apx1s_dslog','anof_050624_YPDtitr_plate_3k']
request=json.loads((old/'request.json').read_text());project=request['projects'][3];assert project['name']=='anof_060224_cres'
all_fovs=[f for p in rows for f in p['fov_details']]
completed={'projects':len(rows),'fovs':len(all_fovs),'rois':sum(p['rois'] for p in rows),'raw_sample_reads':sum(len(f['samples']) for f in all_fovs),
 'crop_exact_fovs':sum(f['crop_match']!='not_established' for f in all_fovs),
 'all_saved_names_sizes_constant_timestamps_match':True,'all_main_hashes_unchanged':True,
 'receipt_sha256':hashlib.sha256((old/'result.partial.json').read_bytes()).hexdigest(),'complete_batch_success':False,'failure':'Historical srclist size assertion in fourth project'}
assert (root/'saved-size-diagnosis-20261106/controller.exit').read_text().strip()=='1'
assert not (root/'saved-size-diagnosis-20261106/result.partial.json').exists()
w=root/'saved-size-diagnosis-v2-20261106';assert not w.exists()
task='alex2024-size-diagnosis-v2-20261106'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
w.mkdir(mode=0o700);(w/'diagnose_saved_sizes_v2.m').write_bytes(script);(w/'request.json').write_text(json.dumps({'project':project,'read_only':True,'script_sha256':SHA})+'\n')
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');diagnose_saved_sizes_v2('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'project':project['name'],'script_sha256':SHA,'read_only':True,'completed_subset':completed}))
'''
    result=remote('linux','SHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+code,timeout=60)
    with (HERE/'size-diagnosis-v2-launch.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
