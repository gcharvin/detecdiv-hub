"""Corroborate terminal raw2 receipts only; never run another content checksum."""
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE = r'''
import hashlib,json,subprocess
from datetime import datetime,timezone
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');d=w/'2024_2'
request=json.loads((w/'request.json').read_text());state=json.loads((w/'state.json').read_text())
pin='a0576fffa3c1d43d232f54567c6c9f7153aed40b436ace98f26079af72432b99'
assert request['runner_sha256']==pin and hashlib.sha256((w/'copy_raws.py').read_bytes()).hexdigest()==pin
row=state['years']['2024_2'];assert row['phase']=='verified_in_staging_project_host_catalog_gates_pending'
assert row['full_sha1_passed'] is True and row['source_stability_verified'] is True
assert state['phase']=='both_years_verified_project_host_catalog_gates_pending' and state['current_year']=='2024_2'
assert (w/'controller.exit').read_text().strip()=='0'
assert state['source_deleted'] is False and state['catalog_changed'] is False
log=d/'verify-full-sha1.log';assert log.is_file() and log.stat().st_size==0
for line in subprocess.run(['ps','-eo','comm=,args='],capture_output=True,text=True,check=True).stdout.splitlines():
 if line.split(None,1)[0]=='rsync' and '--checksum' in line and '/data/Alexander/data/2024_2/' in line:raise AssertionError('Checksum still active')
for filename in ['source-before.jsonl','source-after.jsonl']:
 h=hashlib.sha256();files=size=0
 with (d/filename).open('rb') as stream:
  for line in stream:
   h.update(line);record=json.loads(line)
   if record['kind']=='file':files+=1;size+=record['bytes']
 assert h.hexdigest()=='004fb3404b4af8fe09b6063a09e3a162488f689499ddb51087513daf3d9f4fad' and files==974112 and size==5856516249162
print(json.dumps({'success':True,'year':'2024_2','files':974112,'logical_bytes':5856516249162,'full_sha1_passed':True,
 'sha1_finished_at':row['sha1_finished_at'],'verification_log_bytes':0,'source_stability_verified':True,'source_stability_finished_at':row['finished_at'],
 'native_before_after_manifest_sha256':h.hexdigest(),'pinned_runner_sha256':pin,'controller_exit':0,
 'rsync_zero_exit_established_by_pinned_runner_control_flow':True,'separate_rsync_exit_receipt_present':False,
 'verified_in_staging_only':True,'project_final_host_catalog_api_retirement_gates_pending':True,
 'source_deleted':False,'catalog_changed':False,'checked_at':datetime.now(timezone.utc).isoformat()}))
'''

if __name__ == '__main__':
    output = HERE / 'raw-year2-completion-verification.json'
    assert not output.exists(), 'Inspect completed verification instead of replay'
    result = remote('linux', CODE, timeout=90)
    output.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(result))
