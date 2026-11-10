"""Remove only the fully verified 50-byte fixture; keep all reports and requests."""
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE = r'''
import base64,hashlib,json,os,shlex,subprocess
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');work=root/'raw-namespace-alias-probe-20261110'
assert not (work/'cleanup.json').exists()
fixture=Path('/homes2/maliavko/DetecdivHub/.raw-namespace-alias-fixture-20261110')
assert fixture.resolve()==fixture and fixture.parent==Path('/homes2/maliavko/DetecdivHub')
expected=hashlib.sha256(b'DetecDiv namespace compatibility fixture 20261110\n').hexdigest()
assert {p.name for p in fixture.iterdir()}=={'.incoming-fixture','2024_2'}
assert {p.name for p in (fixture/'2024_2').iterdir()}=={'payload.bin'}
assert {p.name for p in (fixture/'.incoming-fixture').iterdir()}=={'2024_2'}
assert hashlib.sha256((fixture/'2024_2/payload.bin').read_bytes()).hexdigest()==expected
native=r"""
import hashlib,json,os,stat
from pathlib import Path
r=Path('/volume1/homes/maliavko/DetecdivHub/.raw-namespace-alias-fixture-20261110');old=r/'.incoming-fixture/2024_2';real=r/'2024_2'
assert r.resolve()==r and {p.name for p in r.iterdir()}=={'.incoming-fixture','2024_2'}
assert old.parent.resolve()==old.parent and old.is_symlink() and os.readlink(old)=='../2024_2' and old.resolve()==real
assert {p.name for p in old.parent.iterdir()}=={'2024_2'} and {p.name for p in real.iterdir()}=={'payload.bin'}
assert stat.S_ISREG((real/'payload.bin').lstat().st_mode) and hashlib.sha256((real/'payload.bin').read_bytes()).hexdigest()==hashlib.sha256(b'DetecDiv namespace compatibility fixture 20261110\n').hexdigest()
old.unlink();assert not os.path.lexists(old);print(json.dumps({'own_fixture_symlink_removed':True,'dataset_paths_changed':False}))
"""
loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(native.encode()).decode())+'))'
p=subprocess.run(['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ConnectTimeout=10','-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',shlex.join(['python3','-u','-c',loader])],capture_output=True,text=True,timeout=30)
(work/'native-cleanup.log').write_text(p.stdout+p.stderr);assert p.returncode==0
proof=json.loads(p.stdout.strip().splitlines()[-1]);assert proof['own_fixture_symlink_removed']
assert hashlib.sha256((fixture/'2024_2/payload.bin').read_bytes()).hexdigest()==expected
(fixture/'2024_2/payload.bin').unlink();(fixture/'2024_2').rmdir();(fixture/'.incoming-fixture').rmdir();fixture.rmdir()
assert not os.path.lexists(fixture)
result={'success':True,'fixture_removed':True,'fixture_bytes_removed':50,'actual_scientific_space_reclaimed_2024':0,'dataset_paths_changed':False,'scientific_files_modified':False,'requests_and_reports_preserved':True}
(work/'cleanup.json').write_text(json.dumps(result,indent=2)+'\n');(work/'state.json').write_text(json.dumps(dict(result,phase='fixture_verified_three_hosts_and_cleaned'))+'\n');print(json.dumps(result))
'''

if __name__ == '__main__':
    output = HERE / 'raw-namespace-alias-fixture-completion-verification.json'
    assert not output.exists()
    linux = json.loads((HERE / 'raw-namespace-alias-linux-fixture-verification.json').read_text())
    api = json.loads((HERE / 'raw-namespace-alias-api-fixture-verification.json').read_text())
    windows = json.loads((HERE / 'runtime/raw-namespace-windows-status.json').read_text())
    cleanup = json.loads((HERE / 'raw-namespace-windows-cleanup.json').read_text())
    assert linux['success'] and api['success'] and windows['result.json']['success'] and cleanup['success']
    assert windows['task']['state'] == 'Ready' and windows['task']['result'] == 0
    assert all(r['sha256'] == linux['expected_sha256'] for r in windows['result.json']['fixture_paths_verified'])
    assert api['expected_sha256'] == linux['expected_sha256']
    result = remote('linux', CODE, timeout=60)
    result.update(compatibility_fixture_verified_linux_windows_api=True,
                  actual_windows_identity=windows['result.json']['windows_identity'], limited_windows_session=1,
                  expected_sha256=linux['expected_sha256'], actual_dataset_namespace_cutover_performed=False,
                  actual_catalog_handlers_checked=False)
    output.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(result))
