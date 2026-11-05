"""Preserve completed content receipts outside the replaceable API container."""
import json
from media_operations import HERE,RUNTIME,load
from activate_windows_mapping import remote


def main():
    expected={}
    for mode in ['pairs','handlers','post-retirement']:
        if not (RUNTIME/(mode+'-status.json')).exists():continue
        status=load(mode+'-status.json')
        if status.get('receipt_verified'):expected[mode]=status['state']['receipt_sha256']
    assert expected
    code='EXPECTED='+repr(expected)+'\n'+r'''
import json,hashlib,subprocess,os
from pathlib import Path
os.umask(0o077);w=Path('/home/charvin-admin/storage-transfers/alexander-api-cache-retirement-20261105');assert w.is_dir()
archive=w/'media-receipts';archive.mkdir(mode=0o700,exist_ok=True);rows=[]
for mode,digest in EXPECTED.items():
 assert mode in {'pairs','handlers','post-retirement'}
 target=archive/mode
 if not target.exists():
  p=subprocess.run(['docker','cp','detecdiv-hub-api:/tmp/detecdiv-api-cache-retirement-20261105/'+mode,str(target)],capture_output=True,text=True,timeout=30)
  assert p.returncode==0,'Could not archive completed API receipts'
 assert hashlib.sha256((target/'receipt.jsonl').read_bytes()).hexdigest()==digest
 state=json.loads((target/'state.json').read_text());assert state['success']
 rows.append({'mode':mode,'receipt_sha256':digest,'private_directory':str(target)})
print(json.dumps({'success':True,'archived':rows}))
'''
    result=remote('api',code,timeout=80)
    (HERE/'api-receipts-archive.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');print(json.dumps(result))


if __name__=='__main__':main()
