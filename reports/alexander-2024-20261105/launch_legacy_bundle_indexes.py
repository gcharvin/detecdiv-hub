"""Launch the bounded read-only ZIP index inventory from Hub provenance."""

import base64
from collections import defaultdict
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
ARCHIVES=incoming['archives'];SHA=incoming['sha256'];SCRIPT=incoming['script']
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
work=root/'legacy-bundle-zip-indexes-20261112'
task='alex2024-legacy-bundle-zip-indexes-20261112'
assert not work.exists()
assert task not in subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()
plan=(root/'project-discovery-format-review.json').read_bytes()
assert hashlib.sha256(plan).hexdigest()=='93636f5c65e545e24f2e7e52b5d92d700033212ecc4fabc50c009fa29c47de6f'
old={item['id']:item for item in json.loads(plan)['still_missing']}
assert len(old)==1030
archives=[]
for path,ids in sorted(ARCHIVES.items()):
    projects=[]
    for project_id in sorted(ids):
        row=old[project_id]
        projects.append({'id':project_id,'declared_names':[Path(name).name for name in row['files']]})
    archives.append({'path':path,'projects':projects})
assert len(archives)==667 and sum(len(x['projects']) for x in archives)==679
original=json.loads((root/'project-discovery-request.json').read_bytes())
script=base64.b64decode(SCRIPT);assert hashlib.sha256(script).hexdigest()==SHA
request={'archives':archives,'acquisitions':[x['name'] for x in original['acquisitions']],
         'read_only':True,'extract_members':False,'script_sha256':SHA}
os.umask(0o077);work.mkdir(mode=0o700)
(work/'request.json').write_text(json.dumps(request)+'\n')
(work/'scan_legacy_bundle_indexes.py').write_bytes(script)
args=['nice','-n','19','ionice','-c','3',sys.executable,'-u',str(work/'scan_legacy_bundle_indexes.py')]
shell=shlex.join(args)+' > '+shlex.quote(str(work/'controller.log'))+' 2>&1; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(work/'controller.exit'))
subprocess.run(['tmux','new-session','-d','-s',task,'bash','-c',shell],check=True)
print(json.dumps({'started':True,'work':work.name,'archives':len(archives),'projects':679,
                  'script_sha256':SHA,'read_only':True,'member_payload_reads':0}))
'''


def read_private(name, digest):
    data = (HERE / 'runtime' / name).read_bytes()
    assert hashlib.sha256(data).hexdigest() == digest
    return json.loads(data)


def main():
    output = HERE / 'legacy-bundle-zip-indexes-launch-verification.json'
    assert not output.exists()
    bundles = read_private('legacy-archive-bundle-roots-20261112.json',
                           'b9e351f82f949c2b6c85c00e95f99967eecf9644b9680885020e360edba8eae5')
    history = read_private('legacy-archive-bundle-history-20261112.json',
                           '7f579b42914c33545816f598eae04165eca6dc9d8359e28889bdb41f63cd141c')['zip_hints']
    raw_hints = defaultdict(lambda: defaultdict(set))
    for hint in history:
        raw_hints[hint['raw_id']][hint['zip']].add('job' if 'job_id' in hint else 'event')
    archives = defaultdict(list)
    for row in bundles:
        choices = raw_hints[row['raw_id']]
        assert len(choices) == 1
        path, origins = next(iter(choices.items()))
        assert origins == {'job', 'event'}
        archives[path].append(row['id'])
    assert len(archives) == 667 and sum(map(len, archives.values())) == 679
    script = (HERE / 'scan_legacy_bundle_indexes.py').read_bytes()
    result = remote_json(SSH, 'detecdiv-server', LAUNCH, {
        'archives': dict(archives), 'sha256': hashlib.sha256(script).hexdigest(),
        'script': base64.b64encode(script).decode(),
    }, container=False)
    with output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
