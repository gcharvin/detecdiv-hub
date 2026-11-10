"""Run the exact rename primitive on tiny owned CIFS fixtures, not project data."""
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE = r'''
import hashlib,importlib.util,json,os,stat,subprocess
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');work=root/'year2-publication-cifs-fixtures-20261110'
assert not work.exists(),'Inspect previous CIFS fixture state instead of replay'
source=root/'year2-publication-fixtures-20261110/publish_year2_folders.py'
pin='b44f97ccba7736a85e61d1f9d745d8c27d8a2e1abb41c2a857c28497f2de8e44'
assert hashlib.sha256(source.read_bytes()).hexdigest()==pin
prior=json.loads((source.parent/'fixture-result.json').read_text());assert prior['success'] and prior['publisher_sha256']==pin and len(prior['tests'])==21
spec=importlib.util.spec_from_file_location('year2_cifs_primitive',source);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
parent=Path('/homes2/maliavko/DetecdivHub/projects');assert parent.resolve()==parent
mount=json.loads(subprocess.check_output(['findmnt','-J','-T',str(parent),'-o','SOURCE,TARGET,OPTIONS'],text=True))['filesystems'][0]
assert mount['source']=='//10.20.8.250/home' and mount['target']=='/homes2/maliavko' and {'username=maliavko','rw'}.issubset(set(mount['options'].split(',')))
fixture=parent/'.year2-publication-rename-fixture-20261110';assert not os.path.lexists(fixture)
os.umask(0o077);work.mkdir(mode=0o700);fixture.mkdir(mode=0o700)
passed=[];expected_files={};expected_directories=set()
def save(phase):
 (work/'state.json').write_text(json.dumps({'phase':phase,'tests':passed,'fixture_path':str(fixture),'publisher_sha256':pin,'project_or_raw_data_changed':False},indent=2)+'\n')
save('running_tiny_owned_fixture_cases')
try:
 for stem in ['anof_050624_YPDtitr_plate_3k','qp_250724_dbud4stest','281124_Alex']:
  suffix='20261106' if stem in module.SPECIAL else '20261105'
  for scenario in ['success','directory_collision','file_collision']:
   case=fixture/(stem+'-'+scenario);case.mkdir();expected_directories.add(case)
   incoming=case/('.incoming-'+stem+'-'+suffix);incoming.mkdir();final=case/stem
   (incoming/'payload').write_bytes(b'isolated-CIFS-fixture')
   if scenario=='directory_collision':final.mkdir();(final/'sentinel').write_bytes(b'preserve-destination')
   if scenario=='file_collision':final.write_bytes(b'preserve-destination')
   fd=os.open(case,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
   try:
    before=(incoming/'payload').stat().st_ino
    if scenario=='success':
     evidence=module.rename_directory_noreplace(fd,incoming.name,stem)
     assert evidence['no_overwrite'] and not incoming.exists() and (final/'payload').read_bytes()==b'isolated-CIFS-fixture' and (final/'payload').stat().st_ino==before
     expected_directories.add(final);expected_files[final/'payload']=b'isolated-CIFS-fixture'
    else:
     try:module.rename_directory_noreplace(fd,incoming.name,stem)
     except OSError:pass
     else:raise AssertionError('CIFS overwrote existing destination')
     assert (incoming/'payload').read_bytes()==b'isolated-CIFS-fixture'
     expected_directories.add(incoming);expected_files[incoming/'payload']=b'isolated-CIFS-fixture'
     if scenario=='directory_collision':
      assert (final/'sentinel').read_bytes()==b'preserve-destination';expected_directories.add(final);expected_files[final/'sentinel']=b'preserve-destination'
     else:assert final.read_bytes()==b'preserve-destination';expected_files[final]=b'preserve-destination'
   finally:os.close(fd)
   passed.append(stem+':'+scenario);save('running_tiny_owned_fixture_cases')
 assert len(passed)==9 and fixture.parent==parent and fixture.resolve()==fixture
 actual_files=set();actual_directories=set()
 for directory,dirs,files in os.walk(fixture,followlinks=False):
  for name in dirs:
   p=Path(directory)/name;assert not p.is_symlink();actual_directories.add(p)
  for name in files:actual_files.add(Path(directory)/name)
 assert actual_files==set(expected_files) and actual_directories==expected_directories,'Unexpected fixture entry: preserve for review'
 save('guarded_cleanup_own_fixture_only')
 for path,expected in expected_files.items():
  assert fixture in path.parents and path.parent.resolve()==path.parent and stat.S_ISREG(path.lstat().st_mode) and path.read_bytes()==expected
  path.unlink()
 for path in sorted(expected_directories,key=lambda x:len(x.parts),reverse=True):
  assert fixture in path.parents and path.resolve()==path;path.rmdir()
 fixture.rmdir();assert not os.path.lexists(fixture)
 result={'success':True,'publisher_sha256':pin,'tests':passed,'real_cifs_mount_verified':True,'authenticated_mount_user_maliavko':True,
  'isolated_symlink_and_traversal_cases_previously_passed':True,'cifs_symlink_cases_not_claimed':True,
  'fixture_cleaned':True,'project_or_raw_data_changed':False,'publication_started':False,'source_deleted':False}
 (work/'result.json').write_text(json.dumps(result,indent=2)+'\n');(work/'controller.exit').write_text('0\n');save('fixture_verified_and_cleaned');print(json.dumps(result))
except Exception as error:
 save('failed_safe_fixture_preserved_for_review');(work/'error.json').write_text(json.dumps({'error_type':type(error).__name__,'error':str(error)})+'\n');(work/'controller.exit').write_text('1\n');raise
'''

if __name__ == '__main__':
    output = HERE / 'year2-publication-cifs-fixture-verification.json'
    assert not output.exists()
    result = remote('linux', CODE, timeout=90)
    with output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))
