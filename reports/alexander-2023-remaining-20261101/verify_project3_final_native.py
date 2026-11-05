"""Read-only native inventories and MAT hashes before catalog publication."""
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess

WORK = Path(__file__).resolve().parent
NATIVE = r'''
import json,sys,os,stat,hashlib
from pathlib import Path
v=json.load(sys.stdin);stem=v['project']['name']
assert __import__('re').fullmatch(r'[A-Za-z0-9_]+',stem)
source=Path('/volume1/Sauvegarde/Alexander/analysis')
dest=Path('/volume1/homes/maliavko/DetecdivHub/projects')/stem
expected={r['path']:r for r in v['rows']};changes={r['relative_path']:r for r in v['changes']}
assert len(expected)==len(v['rows']) and dest.is_dir() and not dest.is_symlink()
assert dest.lstat().st_uid==1031
def fail(e):raise e
def paths(root,whole):
 seen=set()
 entries=[root/(stem+'.mat')] if not whole else []
 tree=root/stem if not whole else root
 assert tree.is_dir() and not tree.is_symlink()
 for parent,dirs,files in os.walk(tree,followlinks=False,onerror=fail):
  dirs[:]=[n for n in dirs if n not in {'@eaDir','#recycle','#snapshot'}]
  for n in dirs:assert not (Path(parent)/n).is_symlink()
  rel=str(Path(parent).relative_to(root))
  if rel!='.':seen.add(rel)
  entries.extend(Path(parent)/n for n in files)
 for p in entries:seen.add(str(p.relative_to(root)))
 return seen
assert paths(source,False)==set(expected),'Original project inventory changed'
assert paths(dest,True)==set(expected),'Published project inventory differs'
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for block in iter(lambda:f.read(4*1024*1024),b''):h.update(block)
 return h.hexdigest()
count=total=directory_bytes=mat_bytes=0;modified=[]
for rel,row in expected.items():
 assert not Path(rel).is_absolute() and '..' not in Path(rel).parts
 a=source/rel;b=dest/rel;s=a.lstat();d=b.lstat()
 if row['kind']=='directory':
  assert stat.S_ISDIR(s.st_mode) and stat.S_ISDIR(d.st_mode)
  continue
 assert row['kind']=='file' and stat.S_ISREG(s.st_mode) and stat.S_ISREG(d.st_mode)
 assert s.st_size==row['bytes'] and s.st_mtime_ns==row['mtime_ns'],'Original file changed: '+rel
 assert d.st_uid==1031 and d.st_nlink==1 and (s.st_dev,s.st_ino)!=(d.st_dev,d.st_ino)
 if rel in changes:
  change=changes[rel];a_sha=digest(a);b_sha=digest(b)
  assert a_sha==change['before_sha256'] and b_sha==change['after_sha256']
  assert (a.stat().st_size,a.stat().st_mtime_ns)==(s.st_size,s.st_mtime_ns)
  assert (b.stat().st_size,b.stat().st_mtime_ns)==(d.st_size,d.st_mtime_ns)
  modified.append({'relative_path':rel,'original_sha256':a_sha,'published_sha256':b_sha,'bytes':d.st_size})
 else:
  assert d.st_size==row['bytes'] and abs(d.st_mtime_ns-row['mtime_ns'])<=100,'Unreviewed copied file change: '+rel
 count+=1;total+=d.st_size
 if rel==stem+'.mat':mat_bytes=d.st_size
 else:directory_bytes+=d.st_size
assert len(modified)==len(changes) and mat_bytes>0
print(json.dumps({'success':True,'project_id':v['project']['project_id'],'name':stem,'files':count,'total_bytes':total,'project_mat_bytes':mat_bytes,'project_dir_bytes':directory_bytes,'original_inventory_unchanged':True,'published_inventory_verified':True,'all_copy_files_owned_by_maliavko':True,'modified_mat_files':modified,'project_saved':False}))
'''


def main():
    os.umask(0o077)
    assert not (WORK/'result.json').exists() and not (WORK/'receipt.jsonl').exists()
    request = json.loads((WORK/'request.json').read_text())
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == request['runner_sha256']
    assert len(request['projects']) == 11 and len(request['changes']) == 32
    results = []
    state = {'phase':'verifying_native_originals_and_final_copies','source_deleted':False,'project_saved':False}
    def save():
        state['heartbeat_at'] = datetime.now(timezone.utc).isoformat()
        p = WORK/'state.tmp';p.write_text(json.dumps(state,indent=2)+'\n');p.replace(WORK/'state.json')
    save()
    try:
        loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(NATIVE.encode()).decode())+'))'
        cmd=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','charvin-admin@10.20.8.250',shlex.join(['nice','-n','19','python3','-c',loader])]
        with (WORK/'receipt.jsonl').open('x') as receipt:
            for project in request['projects']:
                manifest=WORK.parent/'projects3'/project['name']/'source-before.jsonl'
                content=manifest.read_bytes()
                assert hashlib.sha256(content).hexdigest()==project['source_inventory_sha256']
                rows=[json.loads(x) for x in content.splitlines()]
                changes=[r for r in request['changes'] if r['project_id']==project['project_id']]
                payload={'project':project,'rows':rows,'changes':changes}
                p=subprocess.run(cmd,input=json.dumps(payload),text=True,capture_output=True,timeout=1200)
                assert p.returncode==0,p.stderr
                result=json.loads(p.stdout);assert result['success']
                results.append(result);receipt.write(json.dumps(result)+'\n');receipt.flush();os.fsync(receipt.fileno())
                state['projects_verified']=len(results);save()
        assert sum(r['files'] for r in results)==152277
        result={'success':True,'projects':results,'finished_at':datetime.now(timezone.utc).isoformat(),'source_deleted':False,'project_saved':False}
        (WORK/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        state.update(phase='native_originals_and_final_copies_verified',success=True,result_sha256=hashlib.sha256((WORK/'result.json').read_bytes()).hexdigest());save()
    except Exception as error:
        state.update(phase='failed_safe_readonly',error=type(error).__name__+': '+str(error));save();raise


if __name__=='__main__':main()
