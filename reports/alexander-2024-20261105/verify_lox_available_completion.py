"""Archive completed available-field evidence; never waive Lox's absent-field gates."""
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import json,hashlib
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/lox-available-fields-20261107')
assert (w/'controller.exit').read_text().strip()=='0','Reader not successfully terminal; inspect existing state'
assert hashlib.sha256((w/'verify_lox_available_fields.m').read_bytes()).hexdigest()=='b402c2629e1b1fc5abeaf0b65b22576f91b207cfe188d52b10fa7d678ae9a727'
raw=(w/'result.json').read_bytes()
print(json.dumps({'result':json.loads(raw),'result_sha256':hashlib.sha256(raw).hexdigest()}))
'''

if __name__=='__main__':
    r=remote('linux',CODE,timeout=30)
    (HERE/'runtime/lox-available-fields-terminal.json').write_text(json.dumps(r,indent=2)+'\n')
    d=r['result'];assert d['available_fields_verification_success'] and d['main_hashes_unchanged']
    assert d['available_fovs']==15 and d['missing_fovs_unresolved']==list(range(2,31,2))
    assert d['new_source_samples']==168 and d['target_samples']==180 and d['prior_fov1_source_samples_reused']==12
    assert all(d[k] is False for k in ['strict_project_access_gate_passed','cleanup_gate_passed','source_deleted','scientific_files_saved'])
    fields=d['fov_details'];assert [x['fov'] for x in fields]==list(range(1,30,2))
    for f in fields:
        assert len(f['channels'])==4 and len(f['samples'])==12
        assert all(sum(s['channel']==c for s in f['samples'])==3 for c in range(1,5))
        assert f['prior_source_samples_reused']==(f['fov']==1)
    out={k:v for k,v in d.items() if k!='fov_details'}
    out.update(controller_exit=0,result_sha256=r['result_sha256'],new_fields_checked=14)
    (HERE/'lox-available-fields-completion-verification.json').write_text(json.dumps(out,indent=2)+'\n')
    print(json.dumps({'available_field_verification_complete':True,'available_fovs':15,'strict_project_access_gate_passed':False}))
