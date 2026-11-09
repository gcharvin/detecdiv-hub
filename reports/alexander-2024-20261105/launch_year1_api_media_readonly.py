"""Launch one detached readonly media verifier, with durable host-side receipts/logs."""
import ast
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote
from audit import remote_json,SSH

def main():
    script=(HERE/'verify_year1_api_media_readonly.py').read_bytes();ast.parse(script)
    packet=remote('linux',r'''
import json,base64,hashlib
from pathlib import Path
p=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/year1-api-media-inventory-v2-20261109/plan.json');b=p.read_bytes()
assert hashlib.sha256(b).hexdigest()=='5eca1863ef4b0b6909bdca1409fd360a1aea3f8032eaa69ac4344a99a706bf9a'
print(json.dumps({'plan':base64.b64encode(b).decode()}))
''',timeout=45)
    control={'runner_sha256':hashlib.sha256(script).hexdigest(),'plan_sha256':'5eca1863ef4b0b6909bdca1409fd360a1aea3f8032eaa69ac4344a99a706bf9a','deadline_unix':time.time()+7200,'read_only':True,'source_deletion_allowed':False,'uri_changes_allowed':False}
    files={'verify_year1_api_media_readonly.py':base64.b64encode(script).decode(),'plan.json':packet['plan'],'controller-request.json':base64.b64encode(json.dumps(control).encode()).decode()}
    code=r'''
import json,sys,base64,subprocess,os,shlex
from pathlib import Path
os.umask(0o077);v=json.load(sys.stdin)
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/year1-api-media-readonly-20261109');assert not w.exists(),'No duplicate media verifier'
w.mkdir(mode=0o700,parents=True);container='/tmp/alexander-year1-api-readonly-20261109'
for name,value in v['files'].items():(w/name).write_bytes(base64.b64decode(value))
subprocess.run(['docker','exec','detecdiv-hub-api','python','-c','import os;os.mkdir('+repr(container)+',0o700)'],check=True)
for name in v['files']:subprocess.run(['docker','cp',str(w/name),'detecdiv-hub-api:'+container+'/'+name],check=True,capture_output=True)
command=shlex.join(['docker','exec','detecdiv-hub-api','python','-u',container+'/verify_year1_api_media_readonly.py'])
shell=command+'; result=$?; printf "%s\\n" "$result" > '+shlex.quote(str(w/'controller.exit'))+'; '
shell+='mkdir -p '+shlex.quote(str(w/'container-reports'))+'; docker cp '+shlex.quote('detecdiv-hub-api:'+container+'/.')+' '+shlex.quote(str(w/'container-reports'))+'; exit "$result"'
(w/'run.sh').write_text(shell+'\n')
with (w/'controller.log').open('xb') as log:
 p=subprocess.Popen(['bash',str(w/'run.sh')],stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
receipt={'launched':True,'pid':p.pid,'work':str(w),'container_work':container,'read_only':True,'source_deleted':False,'uri_changed':False,'durable_per_file_receipts_in_host_log':True}
(w/'launch.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
'''
    result=remote_json(SSH,'webserver-labo',code,{'files':files},container=False)
    result.update(control,launched_at=datetime.now(timezone.utc).isoformat(),expected_handlers=1204,expected_bytes=10858665796)
    with (HERE/'year1-api-media-readonly-launch-verification.json').open('x',encoding='utf-8') as stream:json.dump(result,stream,indent=2);stream.write('\n')
    print(json.dumps(result))

if __name__=='__main__':main()
