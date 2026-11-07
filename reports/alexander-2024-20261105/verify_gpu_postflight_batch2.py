"""Verify completed readonly postflight descriptors against immutable readiness."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import hashlib,json
from datetime import datetime,timezone
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
baseline=root/'relink-value-types-20261107-batch2';after=root/'relink-batch2-gpu-postflight-20261107'
assert (baseline/'controller.exit').read_text().strip()=='0'
assert (after/'controller.exit').read_text().strip()=='0','Postflight is not complete; never infer success from a counter'
oldraw=(baseline/'result.json').read_bytes();assert hashlib.sha256(oldraw).hexdigest()=='e360db59bdfe41b458d6fefe21a72731fe897a5be588552ef4048162241b410d'
newraw=(after/'result.json').read_bytes();old=json.loads(oldraw);new=json.loads(newraw)
assert old['success'] and new['success'] and len(old['files'])==len(new['files'])==18
prior={(f['project_id'],f['path']):f for f in old['files']};gpu=0
for f in new['files']:
 b=prior.pop((f['project_id'],f['path']))
 for key in ['classes','class_counts','gpu_values','function_handles','nonpublic_persistent_properties']:
  assert f[key]==b[key],'Scientific type/content descriptor changed: '+f['path']+' '+key
 assert not f['saved'] and not f['function_handles'] and not f['nonpublic_persistent_properties']
 gpu+=len(f['gpu_values'])
assert not prior and gpu==2585
print(json.dumps({'success':True,'metadata_files':18,'gpu_values_compared':gpu,'all_gpu_content_hashes_types_sizes_equal':True,
 'all_class_counts_equal':True,'read_only':True,'source_deleted':False,'baseline_result_sha256':hashlib.sha256(oldraw).hexdigest(),
 'postflight_result_sha256':hashlib.sha256(newraw).hexdigest(),'checked_at':datetime.now(timezone.utc).isoformat()}))
'''

if __name__=='__main__':
    result=remote('linux',CODE,timeout=90)
    (HERE/'relink-batch2-gpu-postflight-verification.json').write_text(json.dumps(result,indent=2)+'\n')
    p=HERE/'relink-batch2-completion-verification.json'
    summary=json.loads(p.read_text());summary['gpu_postflight_pending']=False
    summary['gpu_content_postflight_verified']=True
    p.write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(result,indent=2))
