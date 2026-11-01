
import importlib.util,subprocess,shlex,base64,json,hashlib
from pathlib import Path
from datetime import datetime,timezone
w=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('inventory_helpers',w/'raw-retirement/refresh_raw_inventory.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
root='/volume1/homes/maliavko/DetecdivHub/raw/2023_2'
launcher='import base64;exec(base64.b64decode('+repr(base64.b64encode(m.NATIVE.encode()).decode())+'))'
args=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','-o','IdentitiesOnly=yes','-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',shlex.join(['nice','-n','19','python3','-u','-c',launcher,root])]
count=size=0;h=hashlib.sha256();started=datetime.now(timezone.utc).isoformat()
with (w/'homes2-final-inventory.jsonl').open('xb') as output,(w/'homes2-final-inventory.errors.log').open('xb') as errors:
 p=subprocess.Popen(args,stdout=subprocess.PIPE,stderr=errors)
 for line in p.stdout:
  output.write(line);h.update(line);row=json.loads(line)
  if row['kind']=='file':count+=1;size+=row['bytes']
 assert p.wait()==0,'Final homes2 native inventory failed'
prior=json.loads((w/'raw-retirement/state.json').read_text())['destination_inventory']
assert count==prior['files']==425112 and size==prior['bytes']==3583194799325 and h.hexdigest()==prior['sha256']
r={'success':True,'started_at':started,'finished_at':datetime.now(timezone.utc).isoformat(),'files':count,'bytes':size,'sha256':h.hexdigest(),'exact_metadata_matches_pre_retirement':True}
(w/'homes2-final-inventory-verification.json').write_text(json.dumps(r,indent=2)+'\n')
