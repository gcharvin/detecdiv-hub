"""Launch a one-shot read-only scan of persistent metadata in verified copies."""
import base64
import argparse
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote


def main(retry_scoping_fix=False):
    identity = json.loads((HERE/'project3-identity-readonly-verification.json').read_text(encoding='utf-8'))
    assert identity['success'] and identity['project_count'] == 11 and identity['fovs'] == 123
    assert all(not p['raw_crop_matches'].get('not_established') for p in identity['projects'])
    script = (HERE/'audit_project3_persistent_paths.m').read_bytes()
    target = HERE/'project3-persistent-paths-launch.json'
    if retry_scoping_fix:
        assert target.exists() and not (HERE/'attempt1-project3-persistent-paths-launch.json').exists()
    else:
        assert not target.exists()
    code = ('SCRIPT='+repr(base64.b64encode(script).decode())+'\nRUNNER_SHA='+repr(hashlib.sha256(script).hexdigest())+
            '\nRETRY_SCOPING_FIX='+repr(retry_scoping_fix)+
            '\nIDENTITY_SHA='+repr(identity['private_receipt_sha256'])+'\n')+r'''
from pathlib import Path
import base64,hashlib,json,os,subprocess,shlex
os.umask(0o077)
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101')
task='alex2023-3-persistent-paths-20261102'
sessions=subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
assert task not in sessions and 'alex2023-3-project-inspection-20261102' not in sessions
if RETRY_SCOPING_FIX:
 assert (w/'project3-persistent-paths.exit').read_text().strip()=='1'
 assert (w/'project3-persistent-paths-receipt.jsonl').stat().st_size==0
 assert not (w/'project3-persistent-paths.json').exists()
 previous=json.loads((w/'project3-persistent-paths-request.json').read_text())
 assert previous['read_only'] and previous['runner_sha256']=='858d38653a2f052bd87d5819cfa4b4d6b6f03adb54bd3ec3dd7fb82d7977347b'
 assert hashlib.sha256((w/'audit_project3_persistent_paths.m').read_bytes()).hexdigest()==previous['runner_sha256']
 log=(w/'project3-persistent-paths.log').read_text();assert 'Unrecognized field name' in log and 'divDuration' in log
 archive=w/'project3-persistent-paths-attempt1';assert not archive.exists();archive.mkdir(mode=0o700)
 for name in ['project3-persistent-paths-request.json','project3-persistent-paths-receipt.jsonl','project3-persistent-paths.exit','project3-persistent-paths.log','audit_project3_persistent_paths.m','project3-persistent-paths-progress.json']:
  p=w/name
  if p.exists():p.rename(archive/name)
 (archive/'correction.json').write_text(json.dumps({'reason':'MATLAB nested scanner shared the outer verified file-name variable; use distinct outer verifiedNames/basename variables.','read_only_attempt':True,'science_files_written':False,'previous_runner_sha256':previous['runner_sha256'],'corrected_runner_sha256':RUNNER_SHA},indent=2)+'\n')
for name in ['project3-persistent-paths-request.json','project3-persistent-paths-receipt.jsonl','project3-persistent-paths.exit','project3-persistent-paths.log','audit_project3_persistent_paths.m']:
 assert not (w/name).exists(),'Inspect existing evidence; do not replay this one-shot audit'
assert (w/'project3-readonly-inspection.exit').read_text().strip()=='0'
identity=w/'project3-readonly-inspection.json';assert hashlib.sha256(identity.read_bytes()).hexdigest()==IDENTITY_SHA
request=w/'project3-readonly-request.json';r=json.loads(request.read_text());assert r['read_only'] and len(r['projects'])==11
state=json.loads((w/'projects3/state.json').read_text());assert state['phase']=='all_project_copies_verified_raw_copy_and_relink_pending'
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==RUNNER_SHA
(w/'audit_project3_persistent_paths.m').write_bytes(script)
control={'read_only':True,'runner_sha256':RUNNER_SHA,'input_request_sha256':hashlib.sha256(request.read_bytes()).hexdigest(),'identity_receipt_sha256':IDENTITY_SHA,'source_deleted':False,'catalog_changed':False,'project_paths_rewritten':False}
(w/'project3-persistent-paths-request.json').write_text(json.dumps(control,indent=2)+'\n')
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7';assert Path(release).is_dir()
expression="maxNumCompThreads(1);addpath('"+str(w)+"');audit_project3_persistent_paths('"+str(w)+"','"+release+"');"
args=['nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'project3-persistent-paths.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'project3-persistent-paths.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'started':True,'retry_after_readonly_scoping_fix':RETRY_SCOPING_FIX,'task':task,'read_only':True,'project_count':11,'runner_sha256':RUNNER_SHA,'input_request_sha256':control['input_request_sha256'],'identity_receipt_sha256':IDENTITY_SHA,'source_deleted':False,'catalog_changed':False,'project_paths_rewritten':False}))
'''
    result = remote('linux', code, timeout=45)
    if retry_scoping_fix:
        target.rename(HERE/'attempt1-project3-persistent-paths-launch.json')
    assert not target.exists()
    target.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(result))


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--retry-readonly-scoping-fix',action='store_true')
    main(parser.parse_args().retry_readonly_scoping_fix)
