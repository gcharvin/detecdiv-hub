"""One-shot launch of read-only copy/path/pixel preparation in detached tmux."""
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote


def main():
    admission = json.loads((HERE/'project3-readonly-admission.json').read_text(encoding='utf-8'))
    assert not admission['affected_jobs'] and not admission['affected_locks']
    assert (datetime.now(timezone.utc)-datetime.fromisoformat(admission['checked_at'])).total_seconds() < 900
    script = (HERE/'inspect_project3_copies.m').read_bytes()
    code = r'''
from pathlib import Path
import base64,hashlib,json,subprocess,shlex,os
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101')
os.umask(0o077)
task='alex2023-3-project-inspection-20261102'
sessions=subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
assert task not in sessions
for name in ['project3-readonly-request.json','project3-readonly-inspection.json','project3-readonly-inspection.exit','inspect_project3_copies.m']:
 assert not (w/name).exists(),'Do not replay a one-shot inspection'
plan={r['project_id']:r for r in json.loads((w/'2023_3-project-candidate-plan.json').read_text())['candidate_projects']}
state=json.loads((w/'projects3/state.json').read_text())
assert state['phase']=='all_project_copies_verified_raw_copy_and_relink_pending' and len(state['projects'])==11
projects=[]
for r in state['projects']:
 d=w/'projects3'/r['name'];before=(d/'source-before.jsonl').read_bytes();after=(d/'source-after.jsonl').read_bytes()
 assert before==after and hashlib.sha256(before).hexdigest()==r['source_inventory']['receipt_sha256']
 raw=(d/'verify-receipt.jsonl').read_bytes();assert hashlib.sha256(raw).hexdigest()==r['verification_receipt']['receipt_sha256']
 files=[json.loads(x) for x in raw.splitlines()];assert len(files)==r['files']
 assert all(f['source_and_destination_match'] and f['owner']=='maliavko' for f in files)
 mat=next(x for x in files if x['path']==r['name']+'.mat');p=plan[r['project_id']]
 projects.append({**{k:p[k] for k in ['project_id','name','source_mat','fovs','rois','candidate_raw_acquisitions']},
  'stage':r['stage'],'mat_sha256':mat['sha256'],'verified_file_paths':[f['path'] for f in files]})
request={'read_only':True,'source_deleted':False,'project_paths_rewritten':False,'catalog_changed':False,'projects':projects,'runner_sha256':RUNNER_SHA}
(w/'project3-readonly-request.json').write_text(json.dumps(request)+'\n')
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==RUNNER_SHA
(w/'inspect_project3_copies.m').write_bytes(script)
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
assert Path(release).is_dir()
expression="maxNumCompThreads(1);addpath('"+str(w)+"');inspect_project3_copies('"+str(w)+"','"+release+"');"
args=['nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression]
shell=shlex.join(args)+' > '+shlex.quote(str(w/'project3-readonly-inspection.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'project3-readonly-inspection.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'started':True,'task':task,'read_only':True,'project_count':len(projects),'fovs':sum(p['fovs'] for p in projects),'rois':sum(p['rois'] for p in projects),'runner_sha256':RUNNER_SHA,'source_deleted':False,'project_paths_rewritten':False,'catalog_changed':False}))
'''
    code = ('SCRIPT='+repr(base64.b64encode(script).decode())+'\nRUNNER_SHA='+repr(hashlib.sha256(script).hexdigest())+'\n'+code)
    result = remote('linux', code, timeout=60)
    target = HERE/'project3-readonly-inspection-launch.json'
    assert not target.exists()
    target.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
