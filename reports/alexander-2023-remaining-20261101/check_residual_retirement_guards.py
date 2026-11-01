"""Exercise the exact native residual unlink routine on isolated tiny fixtures."""
import base64
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

body = base64.b64encode((HERE/'retire_remaining_2023_1.py').read_bytes()).decode()
code = r'''
import base64,json,tempfile,subprocess,sys,importlib.util
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101')
p=w/'retire_remaining_2023_1.py';data=base64.b64decode(SCRIPT)
assert not (w/'source1-retirement-execution-gates.json').exists()
assert not (w/'2023_1/retirement-state.json').exists()
p.write_bytes(data)
spec=importlib.util.spec_from_file_location('residual_unlink',p);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
native=module.native_script(module.LEGACY.read_text());checks=[]
with tempfile.TemporaryDirectory(prefix='alex-residual-unlink-guards-') as temporary:
 base=Path(temporary)
 for case in ['unexpected_file','changed_file','symlink_directory','hardlinked_file','valid_manifest']:
  root=base/case;root.mkdir();(root/'acquisition').mkdir()
  a=root/'acquisition/a.bin';b=root/'acquisition/b.bin';a.write_bytes(b'abc');b.write_bytes(b'defg')
  rows=[{'path':str(p.relative_to(root)),'bytes':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns} for p in [a,b]]
  preserve=root/'#snapshot';preserve.mkdir();(preserve/'keep.bin').write_bytes(b'preserve')
  if case=='unexpected_file':(root/'new.bin').write_bytes(b'new')
  if case=='changed_file':b.write_bytes(b'changed')
  if case=='symlink_directory':(root/'unsafe').symlink_to(preserve,target_is_directory=True)
  if case=='hardlinked_file':(base/'external-hardlink.bin').hardlink_to(a)
  request={'approved_source_root':str(root),'approval':module.APPROVAL,'project_original_preserved':True,'linux_windows_project_validated':True,'files':rows}
  script=native.replace("Path('/volume1/DATA/Alexander/data/2023_1')",'Path('+repr(str(root))+')').replace('==87364','==2').replace('==737182660223','==7').replace('os.sync()','pass # fixture only')
  run=subprocess.run([sys.executable,'-c',script],input=json.dumps(request),capture_output=True,text=True)
  assert (preserve/'keep.bin').read_bytes()==b'preserve'
  if case=='valid_manifest':assert run.returncode==0 and not a.exists() and not b.exists() and root.is_dir(),run.stderr
  else:assert run.returncode!=0 and a.exists() and b.exists() and not run.stdout,case
  checks.append({'case':case,'passed':True})
print(json.dumps({'success':True,'fixtures_only':True,'production_data_touched':False,'checks':checks}))
'''.replace('SCRIPT', repr(body))
result = remote('linux', code, timeout=60)
(HERE/'source1-retirement-guard-tests.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result))
