"""Start the new bounded archived-main inspection after fresh admission."""

import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import api
from audit import SSH, remote_json
from launch_archived_legacy_readonly import ADMISSION

LAUNCH = r'''
import base64,hashlib,json,os,shlex,subprocess,sys
from pathlib import Path
v=json.load(sys.stdin)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
work=root/'legacy-linked-raw-main-path-review-continuation-20261112'
task='alex2024-legacy-linked-raw-main-path-review-continuation-20261112'
assert not work.exists() and v['admission']['indexing_count']==0
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
os.umask(0o077);work.mkdir(mode=0o700)
for name,item in v['scripts'].items():
    data=base64.b64decode(item['data'],validate=True);assert hashlib.sha256(data).hexdigest()==item['sha256']
    (work/name).write_bytes(data)
control={'read_only':True,'source_saved':False,'busy_project_ids':v['admission']['busy_project_ids'],
 'runner_sha256':v['scripts']['inspect_linked_raw_archive_mains_continuation.py']['sha256'],
 'matlab_sha256':v['scripts']['review_legacy_paths_v3.m']['sha256']}
(work/'control.json').write_text(json.dumps(control)+'\n')
args=['nice','-n','19','ionice','-c','3',sys.executable,'-u',str(work/'inspect_linked_raw_archive_mains_continuation.py')]
shell=shlex.join(args)+' > '+shlex.quote(str(work/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(work/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'launched':True,'work':work.name,'script_sha256':control['runner_sha256'],
 'matlab_sha256':control['matlab_sha256'],'read_only':True,'source_saved':False}))
'''


def main():
    output = HERE / 'legacy-linked-raw-main-path-review-continuation-launch-verification.json'
    assert not output.exists()
    admission = api(ADMISSION)
    scripts = {}
    for name in ('inspect_linked_raw_archive_mains_continuation.py', 'review_legacy_paths_v3.m'):
        data = (HERE / name).read_bytes()
        scripts[name] = {'data': base64.b64encode(data).decode(), 'sha256': hashlib.sha256(data).hexdigest()}
    result = remote_json(SSH, 'detecdiv-server', LAUNCH, {'admission': admission, 'scripts': scripts}, container=False)
    result['started_at'] = datetime.now(timezone.utc).isoformat()
    with output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
