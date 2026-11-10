"""Test namespace compatibility on tiny owned fixtures only; never rename data."""
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import remote
from audit import remote_json, SSH

PREPARE = r'''
import base64,ctypes,hashlib,json,os,shlex,subprocess
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');work=root/'raw-namespace-alias-probe-20261110'
assert not work.exists(),'Inspect existing probe instead of replay'
parent=Path('/homes2/maliavko/DetecdivHub');assert parent.resolve()==parent
fixture=parent/'.raw-namespace-alias-fixture-20261110';assert not os.path.lexists(fixture)
mount=json.loads(subprocess.check_output(['findmnt','-J','-T',str(parent),'-o','SOURCE,TARGET,OPTIONS'],text=True))['filesystems'][0]
assert mount['source']=='//10.20.8.250/home' and {'username=maliavko','rw'}.issubset(set(mount['options'].split(',')))
payload=b'DetecDiv namespace compatibility fixture 20261110\n';expected=hashlib.sha256(payload).hexdigest()
os.umask(0o077);work.mkdir(mode=0o700);fixture.mkdir();incoming=fixture/'.incoming-fixture';incoming.mkdir();old=incoming/'2024_2';old.mkdir();(old/'payload.bin').write_bytes(payload)
(work/'state.json').write_text(json.dumps({'phase':'fixture_created','fixture':str(fixture),'expected_sha256':expected,'dataset_paths_changed':False})+'\n')
parent_fd=os.open(fixture,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW);incoming_fd=os.open(incoming,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
try:
 before=old.stat();libc=ctypes.CDLL(None,use_errno=True);call=libc.renameat2
 call.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint];call.restype=ctypes.c_int
 assert call(incoming_fd,b'2024_2',parent_fd,b'2024_2',1)==0
 after=(fixture/'2024_2').stat();assert before.st_ino==after.st_ino
finally:os.close(parent_fd);os.close(incoming_fd)
native=r"""
import hashlib,json,os,pwd,stat
from pathlib import Path
root=Path('/volume1/homes/maliavko/DetecdivHub/.raw-namespace-alias-fixture-20261110')
assert root.resolve()==root and root.is_dir() and {p.name for p in root.iterdir()}=={'.incoming-fixture','2024_2'}
old=root/'.incoming-fixture/2024_2';new=root/'2024_2';payload=new/'payload.bin'
assert old.parent.resolve()==old.parent and not os.path.lexists(old) and new.resolve()==new
uid=pwd.getpwnam('maliavko').pw_uid;assert stat.S_ISREG(payload.lstat().st_mode) and payload.stat().st_uid==uid
expected=hashlib.sha256(b'DetecDiv namespace compatibility fixture 20261110\n').hexdigest();assert hashlib.sha256(payload.read_bytes()).hexdigest()==expected
os.symlink('../2024_2',old,target_is_directory=True)
assert old.is_symlink() and os.readlink(old)=='../2024_2' and old.resolve()==new and os.path.samefile(old/'payload.bin',payload)
print(json.dumps({'native_relative_symlink_created':True,'payload_owner_maliavko':True,'link_uid':old.lstat().st_uid,'payload_uid':payload.stat().st_uid,'expected_sha256':expected,'real_dataset_paths_changed':False}))
"""
loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(native.encode()).decode())+'))'
command=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ConnectTimeout=10','-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',shlex.join(['python3','-u','-c',loader])]
p=subprocess.run(command,capture_output=True,text=True,timeout=30)
(work/'native.log').write_text(p.stdout+p.stderr);assert p.returncode==0,'Native fixture alias denied/failed; preserve for review, no privilege workaround'
native_result=json.loads(p.stdout.strip().splitlines()[-1]);assert native_result['expected_sha256']==expected
paths=[old/'payload.bin',fixture/'2024_2/payload.bin']
assert all(hashlib.sha256(path.read_bytes()).hexdigest()==expected for path in paths)
result={'success':True,'fixture_path':str(fixture),'expected_sha256':expected,'bytes':len(payload),'linux_old_and_readable_paths_verified':True,
 'native_relative_symlink_verified':True,'payload_owner_maliavko':True,'fixture_link_uid':native_result['link_uid'],'dataset_paths_changed':False,
 'scientific_files_read_or_saved':False,'windows_and_api_checks_pending':True}
(work/'linux-result.json').write_text(json.dumps(result,indent=2)+'\n');(work/'state.json').write_text(json.dumps(dict(result,phase='linux_fixture_verified_windows_api_pending'))+'\n');print(json.dumps(result))
'''

API = r'''
import asyncio,hashlib,json
from pathlib import Path
from starlette.responses import FileResponse
root=Path('/homes2/maliavko/DetecdivHub/.raw-namespace-alias-fixture-20261110');expected=hashlib.sha256(b'DetecDiv namespace compatibility fixture 20261110\n').hexdigest()
async def stream(path):
 h=hashlib.sha256();count=0
 async def receive():return {'type':'http.request','body':b'','more_body':False}
 async def send(message):
  nonlocal count
  if message['type']=='http.response.start':assert message['status']==200
  elif message['type']=='http.response.body':h.update(message.get('body',b''));count+=len(message.get('body',b''))
  else:raise AssertionError('Unexpected ASGI message')
 await FileResponse(path)({'type':'http','method':'GET','headers':[],'path':'/namespace-fixture','asgi':{'version':'3.0'},'extensions':{}},receive,send)
 return h.hexdigest(),count
for path in [root/'.incoming-fixture/2024_2/payload.bin',root/'2024_2/payload.bin']:
 assert path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest()==expected
 digest,count=asyncio.run(stream(path));assert digest==expected and count==len(b'DetecDiv namespace compatibility fixture 20261110\n')
print(json.dumps({'success':True,'api_container_old_and_readable_paths_verified':True,'file_response_full_body_verified':True,'expected_sha256':expected,
 'actual_catalog_handlers_checked':False,'database_changed':False,'dataset_paths_changed':False,'fixture_only':True}))
'''

if __name__ == '__main__':
    output = HERE / 'raw-namespace-alias-linux-fixture-verification.json'
    assert not output.exists(), 'Inspect existing probe instead of replay'
    linux = remote('linux', PREPARE, timeout=90)
    output.write_text(json.dumps(linux, indent=2) + '\n', encoding='utf-8')
    api = remote_json(SSH, 'webserver-labo', API, {}, container=True)
    (HERE / 'raw-namespace-alias-api-fixture-verification.json').write_text(json.dumps(api, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'linux': linux, 'api': api}))
