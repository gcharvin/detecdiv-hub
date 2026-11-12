"""Inspect raw archive indexes linked to projects with no project archive URI."""

import base64
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from audit import SSH, remote_json

LAUNCH = r'''
import base64,hashlib,json,os,shlex,subprocess,sys
from pathlib import Path
incoming=json.load(sys.stdin)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
work=root/'legacy-linked-raw-zip-indexes-20261112';task='alex2024-legacy-linked-raw-zip-indexes-20261112'
assert not work.exists()
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
data=(root/'project-discovery-format-review.json').read_bytes()
assert hashlib.sha256(data).hexdigest()=='93636f5c65e545e24f2e7e52b5d92d700033212ecc4fabc50c009fa29c47de6f'
old={r['id']:r for r in json.loads(data)['still_missing']}
assert len(old)==1030
archives=[]
for path,ids in sorted(incoming['archives'].items()):
    projects=[{'id':pid,'declared_names':[Path(name).name for name in old[pid]['files']]} for pid in sorted(ids)]
    archives.append({'path':path,'projects':projects})
assert len(archives)==66 and sum(len(x['projects']) for x in archives)==197
original=json.loads((root/'project-discovery-request.json').read_bytes())
script=base64.b64decode(incoming['script'],validate=True);assert hashlib.sha256(script).hexdigest()==incoming['sha256']
request={'archives':archives,'acquisitions':[x['name'] for x in original['acquisitions']],
         'read_only':True,'extract_members':False,'script_sha256':incoming['sha256']}
os.umask(0o077);work.mkdir(mode=0o700)
(work/'request.json').write_text(json.dumps(request)+'\n')
(work/'scan_linked_raw_archive_indexes.py').write_bytes(script)
args=['nice','-n','19','ionice','-c','3',sys.executable,'-u',str(work/'scan_linked_raw_archive_indexes.py')]
shell=shlex.join(args)+' > '+shlex.quote(str(work/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(work/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'started':True,'work':work.name,'archives':66,'projects':197,
                  'script_sha256':incoming['sha256'],'read_only':True,'member_payload_reads':0}))
'''


def main():
    output = HERE / 'legacy-linked-raw-zip-indexes-launch-verification.json'
    assert not output.exists()
    source = (HERE / 'runtime/legacy-no-project-archive-linked-raws-20261112.json').read_bytes()
    assert hashlib.sha256(source).hexdigest() == 'd5c0e0ab9d3c6dac381553cd10c83fa35438d4b2af489ffa31639ea5a04d4e1c'
    links = json.loads(source)['links']
    archives = defaultdict(list)
    for row in links:
        if row['archive_uri'].lower().endswith('.zip'):
            archives[row['archive_uri']].append(row['project_id'])
    assert len(archives) == 66 and sum(map(len, archives.values())) == 197
    script = (HERE / 'scan_linked_raw_archive_indexes.py').read_bytes()
    result = remote_json(SSH, 'detecdiv-server', LAUNCH, {
        'archives': dict(archives), 'sha256': hashlib.sha256(script).hexdigest(),
        'script': base64.b64encode(script).decode(),
    }, container=False)
    result['started_at'] = datetime.now(timezone.utc).isoformat()
    with output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
