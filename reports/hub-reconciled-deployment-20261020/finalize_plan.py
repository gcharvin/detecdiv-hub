"""Resolve reviewed whitespace-only conflicts and define runtime host roles."""
import ast
import hashlib
import json
import re

import operations as ops


def applicable(label, name):
    if label == 'api':
        return name.startswith(('api/', 'db/'))
    return (name.startswith('worker/') or name in {
        'api/models.py', 'api/services/pipeline_runs.py', 'api/services/project_deletion.py',
    } or label == 'windows' and name == 'scripts/register_windows_worker_task.ps1')


for label in ['api', 'linux', 'windows']:
    folder = ops.OUT / (label + '-reconciliation')
    manifest = json.loads((folder / 'manifest.json').read_text())
    for name, item in manifest['files'].items():
        item['applicable'] = applicable(label, name)
        before = (folder / 'before' / name).read_bytes()
        if not item['applicable']:
            (folder / 'files' / name).write_bytes(before)
            item['changed'] = False
            item['after'] = item['before']
            continue
        p = folder / 'files' / name
        value = p.read_text(encoding='utf-8')
        if name in manifest['conflicts']:
            def resolve(match):
                ours, incoming = match.group(1), match.group(2)
                if name == 'api/services/pipeline_runs.py':
                    assert not ours.strip() and not incoming.strip()
                elif name == 'scripts/register_windows_worker_task.ps1':
                    assert not ours.strip() and incoming.startswith('# A polling worker')
                else:
                    raise RuntimeError('Unreviewed conflict: ' + name)
                return incoming
            value = re.sub(r'^<<<<<<<[^\n]*\n(.*?)^=======\n(.*?)^>>>>>>>[^\n]*\n',
                           resolve, value, flags=re.M | re.S)
            p.write_bytes(value.encode('utf-8'))
        assert not re.search(r'^(<<<<<<<|=======|>>>>>>>)', value, re.M), name
        if name.endswith('.py'):
            ast.parse(value)
        item['after'] = hashlib.sha256(p.read_bytes()).hexdigest()
        item['changed'] = before != p.read_bytes()
    manifest['conflicts'] = []
    manifest['role'] = 'API container' if label == 'api' else 'Compute worker and shared services'
    (folder / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(label, [p for p, v in manifest['files'].items() if v['changed']])
