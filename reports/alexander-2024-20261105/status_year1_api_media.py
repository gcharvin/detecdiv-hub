"""Inspect the existing readonly API verifier; never start or replay it."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from audit import remote_json,SSH

CODE=r'''
import json,subprocess,os
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/year1-api-media-readonly-20261109')
result={'exists':w.exists()}
if w.exists():
 if (w/'controller.exit').exists():result['controller_exit']=(w/'controller.exit').read_text().strip()
 launch=json.loads((w/'launch.json').read_text());p=Path('/proc')/str(launch['pid'])
 result['launcher_pid']=launch['pid'];result['launcher_process_exists']=p.exists()
 if p.exists():result['launcher_command_matches']=str(w/'run.sh').encode() in (p/'cmdline').read_bytes()
 copied=w/'container-reports'
 if (copied/'state.json').exists():
  result['state']=json.loads((copied/'state.json').read_text())
  if (copied/'result.json').exists():result['result']=json.loads((copied/'result.json').read_text())
 else:
  code="import json;from pathlib import Path;p=Path('/tmp/alexander-year1-api-readonly-20261109/state.json');print(p.read_text() if p.exists() else '{}')"
  response=subprocess.run(['docker','exec','detecdiv-hub-api','python','-c',code],capture_output=True,text=True,timeout=30)
  result['state_read_exit']=response.returncode
  if response.returncode==0:result['state']=json.loads(response.stdout)
 log=w/'controller.log'
 if log.exists():
  result['durable_log_bytes']=log.stat().st_size
  with log.open('rb') as stream:stream.seek(max(0,log.stat().st_size-1600));result['log_tail']=stream.read().decode(errors='replace')
print(json.dumps(result))
'''

def status():return remote_json(SSH,'webserver-labo',CODE,{},container=False)

if __name__=='__main__':
    value=status();(HERE/'runtime/year1-api-media-readonly-status.json').write_text(json.dumps(value,indent=2)+'\n',encoding='utf-8');print(json.dumps(value))
