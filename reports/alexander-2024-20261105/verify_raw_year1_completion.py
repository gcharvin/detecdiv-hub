"""Corroborate terminal raw2024_1 checks from the pinned sequential runner, readonly."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import json,hashlib,subprocess
from pathlib import Path
from datetime import datetime,timezone
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');d=w/'2024_1'
request=json.loads((w/'request.json').read_text());state=json.loads((w/'state.json').read_text())
expected='a0576fffa3c1d43d232f54567c6c9f7153aed40b436ace98f26079af72432b99'
assert request['runner_sha256']==expected and hashlib.sha256((w/'copy_raws.py').read_bytes()).hexdigest()==expected
row=state['years']['2024_1'];assert row['phase']=='verified_in_staging_project_host_catalog_gates_pending'
assert row['full_sha1_passed'] is True and row['source_stability_verified'] is True
assert state['current_year']=='2024_2' and not state['source_deleted'] and not state['catalog_changed']
log=d/'verify-full-sha1.log';assert log.is_file() and log.stat().st_size==0
for line in subprocess.run(['ps','-eo','comm=,args='],capture_output=True,text=True,check=True).stdout.splitlines():
 if line.split(None,1)[0]=='rsync' and '--checksum' in line and '/data/Alexander/data/2024_1/' in line:raise AssertionError('Verification still active')
digests=[];counts=[]
for filename in ['source-before.jsonl','source-after.jsonl']:
 h=hashlib.sha256();files=size=0
 with (d/filename).open('rb') as stream:
  for line in stream:
   h.update(line);item=json.loads(line)
   if item['kind']=='file':files+=1;size+=item['bytes']
 digests.append(h.hexdigest());counts.append((files,size))
assert digests==['8fd64d094fe29707e0ddd0b1480e9b861786d3adde677087ca7a6e95e252f61e']*2
assert counts==[(732579,5177814875236)]*2
print(json.dumps({'success':True,'year':'2024_1','files':732579,'logical_bytes':5177814875236,
 'full_sha1_passed':True,'sha1_finished_at':row['sha1_finished_at'],'verification_log_bytes':0,
 'source_stability_verified':True,'source_stability_finished_at':row['finished_at'],
 'native_before_after_manifest_sha256':digests[0],'pinned_runner_sha256':expected,
 'rsync_zero_exit_established_by_pinned_runner_control_flow':True,'separate_rsync_exit_receipt_present':False,
 'verified_in_staging_only':True,'project_final_host_catalog_api_retirement_gates_pending':True,
 'source_deleted':False,'catalog_changed':False,'checked_at':datetime.now(timezone.utc).isoformat()}))
'''

if __name__=='__main__':
    result=remote('linux',CODE,timeout=90)
    (HERE/'raw-year1-completion-verification.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result))
