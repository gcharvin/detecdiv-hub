"""Verify readonly Lox metadata plans without granting missing-field access gates."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import json,hashlib
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105/lox-metadata-preparation-20261107')
assert (w/'controller.exit').read_text().strip()=='0'
r={}
for name in ['paths/result.json','paths/receipt.jsonl','types/result.json']:
 raw=(w/name).read_bytes();r[name]={'sha256':hashlib.sha256(raw).hexdigest(),'value':[json.loads(line) for line in raw.splitlines()] if name.endswith('.jsonl') else json.loads(raw)}
print(json.dumps(r))
'''

if __name__=='__main__':
    r=remote('linux',CODE,timeout=30)
    (HERE/'runtime/lox-metadata-terminal-20261108.json').write_text(json.dumps(r,indent=2)+'\n')
    paths=r['paths/result.json']['value'];types=r['types/result.json']['value'];rows=r['paths/receipt.jsonl']['value']
    assert paths['success'] and types['success'] and len(paths['projects'])==len(types['files'])==len(rows)==1
    p=paths['projects'][0];t=types['files'][0];row=rows[0]
    assert p['project_id']==t['project_id']==row['project_id']=='de636b9e-684a-4ab1-bb4a-0bc64d435020'
    assert p['metadata_files_audited']==p['files_needing_rebase']==1 and p['files_with_unresolved_paths']==0
    assert row['recognized_rebase_values']==row['absolute_path_values']==118384 and row['unresolved_path_values']==0
    assert row['sha256_unchanged']==t['sha256_unchanged']=='c1a32a8cca1847fe72937423501a1ae8ddbb0c9d1b03460d33f081b5b6558898'
    assert not t['gpu_values'] and not t['function_handles'] and not t['nonpublic_persistent_properties']
    assert not paths['saved'] and not types['saved'] and not row['file_saved']
    result={'read_only_preparation_success':True,'controller_exit':0,'metadata_mats':1,'planned_path_changes':118384,
      'unknown_path_values':0,'gpu_values':0,'function_handles':0,'nonpublic_persistent_properties':0,'copy_main_hash_unchanged':True,
      'result_sha256':{name:v['sha256'] for name,v in r.items()},'scientific_save_reopen_verified':False,
      'strict_project_access_gate_passed':False,'cleanup_gate_passed':False,'source_deleted':False,'scientific_files_saved':False}
    (HERE/'lox-metadata-preparation-completion-verification.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result))
