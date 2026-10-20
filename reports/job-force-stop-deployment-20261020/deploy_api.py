import base64
import json
import ops

result = ops.remote("api", f'''
import hashlib,json,os,shutil,subprocess
from pathlib import Path
root=Path.cwd();stage=root/'.deploy-staging/{ops.ROLLOUT}'
manifest=json.loads((stage/'manifest.json').read_text())
backup=root/'.deploy-backups/{ops.ROLLOUT}'
for name,item in manifest['files'].items():
 p=root/name;actual=hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
 assert actual in (item['before'],item['after']),'Source changed: '+name
 assert hashlib.sha256((stage/'files'/name).read_bytes()).hexdigest()==item['after']
for name,item in manifest['files'].items():
 p=root/name
 if p.exists() and hashlib.sha256(p.read_bytes()).hexdigest()==item['after']:continue
 if p.exists():
  saved=backup/name;saved.parent.mkdir(parents=True,exist_ok=True)
  if not saved.exists():shutil.copy2(p,saved)
 p.parent.mkdir(parents=True,exist_ok=True)
 temp=p.with_suffix(p.suffix+'.force-stop-deploy-tmp')
 shutil.copy2(stage/'files'/name,temp);os.replace(temp,p)
print(json.dumps({{'files_verified':len(manifest['files']),'fingerprint':manifest['expected_fingerprint'],'backup':str(backup)}}))
''')
ops.save("api-files.json", result)
print(result, flush=True)
migration = base64.b64encode((ops.ROOT / "db/migrations/20261020_job_force_stop.sql").read_bytes()).decode()
result = ops.api(f'''
import base64,json,time
from sqlalchemy import text
from api.db import engine
sql=base64.b64decode({migration!r}).decode()
for attempt in range(3):
 try:
  with engine.begin() as c:
   c.execute(text("SET LOCAL lock_timeout='3s'"))
   c.exec_driver_sql(sql)
  break
 except Exception:
  if attempt==2:raise
  time.sleep(1)
with engine.connect() as c:
 names=list(c.execute(text("select column_name from information_schema.columns where table_name='jobs' and column_name like 'force_%' order by column_name")).scalars())
assert set(names)=={{'force_stop_requested_at','force_stop_requested_by','force_stop_requested_by_user_id','force_stopped_at'}}
print(json.dumps({{'migration_applied':True,'columns':names}}))
''')
ops.save("migration.json", result)
print(result, flush=True)
