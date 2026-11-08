"""Verify bounded header evidence; do not infer whole-file contents or raw relevance."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import json,hashlib
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/unreadable-header-classification-20261108')
raw=(w/'receipt.jsonl').read_bytes();assert hashlib.sha256(raw).hexdigest()=='18b995243f3189e62768c89f3f79c40d76af62a89d3bfaf1a33da9b0de8f3155'
rows=[json.loads(line) for line in raw.splitlines()];assert len(rows)==33 and len({r['project_id'] for r in rows})==33
mime=(w/'mime-followup.json').read_bytes();assert hashlib.sha256(mime).hexdigest()=='8ed1ee0a63da8a7b2b1eea8813904f2cad20482a197043546f1e16b13fd9ee24'
formats=json.loads(mime);assert {r['project_id'] for r in formats}=={r['project_id'] for r in rows}
assert all(r['header_unchanged'] and r['mime']=='application/octet-stream' and not any(r['known_markers'].values()) for r in formats)
zero=hashlib.sha256(bytes(4096)).hexdigest()
assert all(r['metadata_stable'] and r['header_bytes_read']==4096 and r['bytes']>4096 and r['header_sha256']==zero for r in rows)
print(json.dumps({'read_only_classification_verified':True,'files':33,'owner':'gilles','zero_first_4096_bytes':33,
 'headers_rechecked_unchanged':True,'mime':'application/octet-stream','whole_file_contents_verified':False,
 'timing_of_header_damage_established':False,'reference_relevance_resolved':False,
 'old_matlab_audits_replayed':False,'scientific_files_saved':False,'source_deleted':False,
 'header_receipt_sha256':hashlib.sha256(raw).hexdigest(),'mime_receipt_sha256':hashlib.sha256(mime).hexdigest()}))
'''

if __name__=='__main__':
    result=remote('linux',CODE,timeout=30)
    (HERE/'unreadable-header-final-classification-verification.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result))
