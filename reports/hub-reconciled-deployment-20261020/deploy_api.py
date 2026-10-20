"""Apply the tested API-only delta and rebuild only the API container."""
import json

import operations as ops

before = ops.save('before-api-activation.json', ops.snapshot())
assert not before['indexing'], 'API-side indexing is active; defer rebuild'
result = ops.remote('api', '''
import hashlib,json,os,shutil,subprocess
from pathlib import Path
root=Path.cwd();stage=root/'.deploy-staging/hub-reconciled-20261020'
manifest=json.loads((stage/'manifest.json').read_text());backup=root/'.deploy-backups/hub-reconciled-20261020'
for name,item in manifest['files'].items():
 p=root/name;actual=hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
 assert actual in [item['before'],item['after']],'Concurrent source change: '+name
 assert hashlib.sha256((stage/'files'/name).read_bytes()).hexdigest()==item['after']
for name,item in manifest['files'].items():
 p=root/name
 if hashlib.sha256(p.read_bytes()).hexdigest()==item['after']:continue
 saved=backup/name;saved.parent.mkdir(parents=True,exist_ok=True)
 assert not saved.exists() or hashlib.sha256(saved.read_bytes()).hexdigest()==item['before']
 if not saved.exists():shutil.copy2(p,saved)
 temporary=p.with_suffix(p.suffix+'.reconciled-tmp');shutil.copy2(stage/'files'/name,temporary);os.replace(temporary,p)
from api.services.runtime_version import _compute_code_fingerprint
assert _compute_code_fingerprint(root)==manifest['expected_fingerprint']
command=['docker-compose','up','-d','--no-deps','--build','api']
p=subprocess.run(command,cwd=root/'ops/compose/webserver-labo',capture_output=True,text=True,timeout=240)
(stage/'api-rebuild.log').write_text(p.stdout+p.stderr)
assert p.returncode==0,(p.stdout+p.stderr)[-2000:]
print(json.dumps({'files_updated':len(manifest['files']),'expected_fingerprint':manifest['expected_fingerprint'],
 'api_rebuild_exit':p.returncode,'backup':str(backup),'container_only':'api'}))
''', timeout=270)
ops.save('api-deployment.json', result)
print(json.dumps(result), flush=True)
