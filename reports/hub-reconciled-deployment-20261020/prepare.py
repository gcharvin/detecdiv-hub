"""Reconcile only the published cleanup delta with each host's own adapters."""
import base64
import hashlib
import json
import subprocess
from pathlib import Path

import operations as ops

BASE = '222dea1ba6842c63df28d4c5ca96f8225b4e59ba'
ROLLOUT = 'hub-reconciled-20261020'


def changed_paths():
    return [p for p in subprocess.check_output(
        ['git', 'diff', '--name-only', BASE, ops.COMMIT], cwd=ops.ROOT, text=True,
    ).splitlines() if p.startswith(('api/', 'worker/', 'db/')) or
        p == 'scripts/register_windows_worker_task.ps1']


def get_blob(commit, name):
    p = subprocess.run(['git', 'show', f'{commit}:{name}'], cwd=ops.ROOT,
                       capture_output=True)
    return p.stdout if p.returncode == 0 else b''


def prepare(label):
    paths = changed_paths()
    sources = ops.remote(label, f'''
import json,base64
from pathlib import Path
print(json.dumps({{name:base64.b64encode((Path.cwd()/name).read_bytes()).decode()
 if (Path.cwd()/name).is_file() else None for name in {paths!r}}}))
''')
    folder = ops.OUT / (label + '-reconciliation')
    if folder.exists():
        raise FileExistsError(folder)
    folder.mkdir()
    manifest = {'commit': ops.COMMIT, 'base': BASE, 'files': {}, 'conflicts': []}
    for name in paths:
        encoded = sources[name]
        before = base64.b64decode(encoded) if encoded is not None else b''
        desired = get_blob(ops.COMMIT, name)
        base = get_blob(BASE, name)
        normalize = lambda b: b.replace(b'\r\n', b'\n')
        if normalize(before) == normalize(desired):
            merged = before
        elif not before or normalize(before) == normalize(base):
            merged = desired
        else:
            work = folder / 'merge' / name
            work.parent.mkdir(parents=True, exist_ok=True)
            work.write_bytes(normalize(before))
            base_path = work.with_name(work.name + '.base')
            base_path.write_bytes(normalize(base))
            desired_path = work.with_name(work.name + '.desired')
            desired_path.write_bytes(normalize(desired))
            result = subprocess.run(['git', 'merge-file', '-p', str(work),
                                     str(base_path), str(desired_path)], capture_output=True)
            merged = result.stdout
            if result.returncode:
                manifest['conflicts'].append(name)
        for prefix, data in [('before', before), ('files', merged)]:
            p = folder / prefix / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
        manifest['files'][name] = {
            'before': hashlib.sha256(before).hexdigest() if encoded is not None else None,
            'after': hashlib.sha256(merged).hexdigest(),
            'changed': before != merged, 'published': hashlib.sha256(desired).hexdigest(),
            'host_adapted': normalize(merged) != normalize(desired),
        }
    (folder / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({'host': label, 'conflicts': manifest['conflicts'],
                      'changes': [p for p, v in manifest['files'].items() if v['changed']],
                      'adapted': [p for p, v in manifest['files'].items() if v['host_adapted']]}),
          flush=True)


if __name__ == '__main__':
    for host in ['api', 'linux', 'windows']:
        prepare(host)
