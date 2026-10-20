import base64
import json
import ops
from english_messages import FILES, TRANSLATIONS

payload=base64.b64encode(json.dumps({'files':FILES,'translations':TRANSLATIONS}).encode()).decode()
result=ops.remote('api',f'''
import base64,hashlib,json,os
from pathlib import Path
root=Path.cwd();stage=root/'.deploy-staging/{ops.ROLLOUT}'
manifest=json.loads((stage/'manifest.json').read_text());spec=json.loads(base64.b64decode({payload!r}))
changes={{}}
for name in spec['files']:
 p=root/name;old=p.read_bytes();text=old.decode()
 for source,target in spec['translations'].items():text=text.replace(source,target)
 new=text.encode()
 assert new!=old,'No translation applied: '+name
 if name.endswith('.py'):compile(new,name,'exec')
 backup=root/'.deploy-backups/{ops.ROLLOUT}'/name;backup.parent.mkdir(parents=True,exist_ok=True)
 if not backup.exists():backup.write_bytes(old)
 dest=stage/'files'/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(new)
 temp=p.with_suffix(p.suffix+'.english-tmp');temp.write_bytes(new);os.replace(temp,p)
 manifest['files'][name]={{'before':hashlib.sha256(backup.read_bytes()).hexdigest(),'after':hashlib.sha256(new).hexdigest()}}
 changes[name]=manifest['files'][name]['after']
from api.services.runtime_version import _compute_code_fingerprint
manifest['expected_fingerprint']=_compute_code_fingerprint(root)
(stage/'manifest.json').write_text(json.dumps(manifest,indent=2))
print(json.dumps({{'expected_fingerprint':manifest['expected_fingerprint'],'files':changes,'manifest':manifest}}))
''')
ops.save('api-staging.json',result.pop('manifest'))
ops.save('english-deployment.json',result)
print(result)
