"""Check all terminal receipts of the existing readonly audit, without replay."""
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE = r'''
import hashlib,json
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
w=root/'year2-incoming-integrity-20261110'
assert (w/'controller.exit').read_text().strip()=='0'
assert hashlib.sha256((w/'audit_year2_incoming_integrity_20261110.py').read_bytes()).hexdigest()=='05639082a0b5400f5ad25d88b62303c60624de757fc918908b2098622ebc9045'
qraw=(w/'request.json').read_bytes();q=json.loads(qraw);raw=(w/'result.json').read_bytes();d=json.loads(raw)
assert d['success'] and d['read_only'] and not d['deferred_projects']
assert all(d[k] is False for k in ['source_deleted','published','catalog_changed'])
assert d['raw_year2_full_sha1_stability_and_final_host_catalog_api_gates_still_required']
assert len(q['projects'])==len(d['projects'])==17 and sum(len(p['files']) for p in q['projects'])==103243
rows=[]
for p,done in zip(q['projects'],d['projects']):
 folder=w/p['name'];assert json.loads((folder/'result.json').read_text())==done
 assert done['project_id']==p['project_id'] and done['name']==p['name'] and done['stage_suffix']==p['stage_suffix']
 assert done['source_before_after_unchanged'] and done['target_full_after_hashes_verified'] and done['target_native_owner']=='maliavko' and not done['published']
 original=(root/p['source_receipt_relative']).read_bytes()
 assert (folder/'source-fresh-before.jsonl').read_bytes()==original==(folder/'source-fresh-after.jsonl').read_bytes()
 assert hashlib.sha256(original).hexdigest()==p['source_inventory_sha256']==done['source_manifest_sha256']
 receipt=(folder/'target-full-hashes.jsonl').read_bytes()
 assert hashlib.sha256(receipt).hexdigest()==done['target_receipt_sha256']
 records=[json.loads(line) for line in receipt.splitlines()];tail=records.pop()
 assert tail['kind']=='complete' and tail['owner']=='maliavko' and not tail['published']
 assert len(records)==tail['files']==done['target_files']==len(p['files'])
 assert len(p['directories'])==tail['directories']
 actual={r['path']:r for r in records};assert len(actual)==len(records) and set(actual)=={f['path'] for f in p['files']}
 for f in p['files']:
  a=actual[f['path']];assert a['kind']=='verified_file' and a['sha256']==f['expected_sha256'] and a['uid']==1031
  if not f['metadata_relinked']:assert a['bytes']==f['source_bytes']
 assert sum(r['bytes'] for r in records)==done['target_bytes']
 rows.append(done)
print(json.dumps({'success':True,'controller_exit':0,'projects':len(rows),'files':sum(p['target_files'] for p in rows),
 'target_logical_bytes':sum(p['target_bytes'] for p in rows),'integrity_result_sha256':hashlib.sha256(raw).hexdigest(),
 'integrity_request_sha256':hashlib.sha256(qraw).hexdigest(),'finished_at':json.loads((w/'state.json').read_text())['finished_at'],
 'all_original_inventories_exact_before_after':True,'all_target_full_after_hashes_verified':True,'native_owner_maliavko':True,
 'source_deleted':False,'published':False,'catalog_changed':False,'raw_year2_sha1_stability_and_final_host_catalog_api_gates_still_required':True}))
'''

if __name__ == '__main__':
    output = HERE / 'year2-incoming-integrity-completion-verification.json'
    assert not output.exists(), 'Inspect prior verification instead of replay'
    result = remote('linux', CODE, timeout=90)
    with output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))
