import base64
import json
import ops

result=ops.remote('api', r'''
import base64,hashlib,json,re,shutil,os
from pathlib import Path
root=Path.cwd();name='api/static/project.html';p=root/name
old=p.read_bytes()
text=old.decode()
text,count=re.subn(r'<script src="[^"]+" defer></script>',lambda match:'<script src="/web/app.js?v=20261020-force-stop" defer></script>',text)
assert count==1
new=text.encode()
backup=root/'.deploy-backups/job-force-stop-20261020'/name
backup.parent.mkdir(parents=True,exist_ok=True)
if not backup.exists():backup.write_bytes(old)
stage=root/'.deploy-staging/job-force-stop-20261020'
dest=stage/'files'/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(new)
p.write_bytes(new)
manifest=json.loads((stage/'manifest.json').read_text())
manifest['files'][name]={'before':hashlib.sha256(backup.read_bytes()).hexdigest(),'after':hashlib.sha256(new).hexdigest()}
from api.services.runtime_version import _compute_code_fingerprint
manifest['expected_fingerprint']=_compute_code_fingerprint(root)
(stage/'manifest.json').write_text(json.dumps(manifest,indent=2))
print(json.dumps({'expected_fingerprint':manifest['expected_fingerprint'],'html_sha256':hashlib.sha256(new).hexdigest(),'data':base64.b64encode(new).decode()}))
''')
data=result.pop('data')
(ops.OUT/'api-intended/api/static/project.html').write_bytes(base64.b64decode(data))
manifest=json.loads((ops.OUT/'api-staging.json').read_text())
manifest['expected_fingerprint']=result['expected_fingerprint']
(ops.OUT/'api-staging.json').write_text(json.dumps(manifest,indent=2))
ops.save('asset-version-fix.json',result)
print(result)
