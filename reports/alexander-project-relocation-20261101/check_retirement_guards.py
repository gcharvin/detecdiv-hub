"""Exercise the actual native unlink routine on isolated tiny fixtures only."""
import ast
import base64
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

tree=ast.parse((HERE/'retire_verified_raws.py').read_text())
assignment=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='NATIVE' for t in n.targets))
native=ast.literal_eval(assignment.value.func.value).replace('APPROVAL',repr('test fixture approval'))
compile(native,'native_retirement','exec')
code=r'''
import base64,json,tempfile,subprocess,sys
from pathlib import Path
native=base64.b64decode(SCRIPT).decode()
checks=[]
with tempfile.TemporaryDirectory(prefix='alex-retirement-guard-test-') as temporary:
 base=Path(temporary)
 for case in ['unexpected_file','changed_file','symlink_directory','valid_manifest']:
  root=base/case;root.mkdir();(root/'acquisition').mkdir()
  a=root/'acquisition/a.bin';b=root/'acquisition/b.bin';a.write_bytes(b'abc');b.write_bytes(b'defg')
  rows=[{'path':str(p.relative_to(root)),'bytes':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns} for p in [a,b]]
  preserved=root/'#snapshot';preserved.mkdir();(preserved/'keep.bin').write_bytes(b'preserved')
  if case=='unexpected_file':(root/'unexpected.bin').write_bytes(b'new')
  if case=='changed_file':b.write_bytes(b'changed')
  if case=='symlink_directory':(root/'unsafe-link').symlink_to(preserved,target_is_directory=True)
  request={'approved_source_root':str(root),'approval':'test fixture approval','project_original_preserved':True,'linux_windows_project_validated':True,'files':rows}
  script=native.replace('/volume1/DATA/Alexander/data/2023_2',str(root)).replace('425112','2').replace('3583194799325','7').replace('os.sync()','pass # fixture: no global sync')
  p=subprocess.run([sys.executable,'-c',script],input=json.dumps(request),capture_output=True,text=True)
  assert (preserved/'keep.bin').read_bytes()==b'preserved'
  if case=='valid_manifest':
   assert p.returncode==0,p.stderr
   assert not a.exists() and not b.exists() and root.exists()
   events=[json.loads(x) for x in p.stdout.splitlines()];assert events[-1]['event']=='complete' and events[-1]['files_unlinked']==2
  else:
   assert p.returncode!=0 and a.exists() and b.exists(),case+' must fail before unlink'
   assert not p.stdout,case+' must fail before preflight passed'
  checks.append({'case':case,'passed':True})
print(json.dumps({'success':True,'fixtures_only':True,'production_data_touched':False,'checks':checks}))
'''.replace('SCRIPT',repr(base64.b64encode(native.encode()).decode()))
result=remote('linux',code,timeout=60)
(HERE/'retirement-guard-tests.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
