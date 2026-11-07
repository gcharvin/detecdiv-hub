"""Read movie pixels only after exact native initial/current parity succeeds."""
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
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');parity=root/'pending-reference-parity-20261107'
assert (parity/'controller.exit').read_text().strip()=='0'
raw=(parity/'result.json').read_bytes();p=json.loads(raw)
assert p['success'] and p['initial_current_source_and_target_availability_equal'] and p['native_current_source_metadata_unchanged']
assert p['references']==112956 and not p['strict_identity_and_access_gate_passed']
base=root/'availability-variants-identity-v2-20261107';assert (base/'controller.exit').read_text().strip()=='0'
result=json.loads((base/'result.json').read_text());row=next(r for r in result['projects'] if r['name']=='movie_250124_diploids')
assert not row['success'] and row['completed_fovs_in_partial']==0 and row['main_original_and_copy_hashes_unchanged']
assert row['error']=='Raw size differs from saved srclist'
request=json.loads((base/'request.json').read_text());request['projects']=[p for p in request['projects'] if p['name']=='movie_250124_diploids'];assert len(request['projects'])==1
request.update(baseline_reference_file=str(root/'pending-availability-diagnosis-v2-20261107/references.jsonl'),
 baseline_reference_sha256='3924c8a996832bc48b539ec80662234eb2bfb50446d64a38a85eeb07aedb217d',native_parity_result_sha256=hashlib.sha256(raw).hexdigest(),script_sha256=SHA,read_only=True)
w=root/'movie-cache-identity-20261107';assert not w.exists();task='alex2024-movie-cache-identity-20261107'
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
w.mkdir(mode=0o700);(w/'request.json').write_text(json.dumps(request)+'\n');(w/'inspect_movie_cache_variant.m').write_bytes(script)
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
expression="maxNumCompThreads(1);addpath('"+str(w)+"');inspect_movie_cache_variant('"+str(w)+"','"+release+"');"
args=['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'session':task,'work':str(w),'script_sha256':SHA,'read_only':True,'scientific_files_saved':False,
 'project':'movie_250124_diploids','fovs':4,'cached_reference_exceptions':6396,'native_parity_result_sha256':request['native_parity_result_sha256'],'source_deleted':False}))
'''

if __name__=='__main__':
    output=HERE/'movie-cache-identity-launch.json';assert not output.exists(),'Inspect prior launch instead of replay'
    script=(HERE/'inspect_movie_cache_variant.m').read_bytes()
    result=remote('linux','SHA='+repr(hashlib.sha256(script).hexdigest())+'\nSCRIPT='+repr(base64.b64encode(script).decode())+'\n'+CODE,timeout=90)
    with output.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))
