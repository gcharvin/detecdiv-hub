"""Three fixed unpublished project mains and nine typed Gilles JSON files only."""
import ast
import base64
from datetime import datetime,timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess

WORK=Path(__file__).resolve().parent
ROOT=WORK.parent
SCOPES={
 'movie_250124_diploids':('/volume1/Sauvegarde/Alexander/analysis','20261105','secondary'),
 'qp_250724_dbud4stest':('/volume1/Sauvegarde/Alexander/nobackup/qp','20261106','secondary'),
 '281124_Alex':('/volume1/DATA/Gilles/analysis/projects','20261106','primary')}
STATE={'phase':'preflight','copies_may_have_been_relinked':False,'source_deleted':False,'catalog_changed':False}
STARTED=False

NATIVE=r'''
import hashlib,json,os,stat,sys,pwd as posix_accounts
from pathlib import Path
v=json.load(sys.stdin);home=Path('/volume1/homes/maliavko/DetecdivHub');uid=posix_accounts.getpwnam('maliavko').pw_uid if v['role']=='secondary' else None
scopes={'movie_250124_diploids':('/volume1/Sauvegarde/Alexander/analysis','20261105','secondary'),
 'qp_250724_dbud4stest':('/volume1/Sauvegarde/Alexander/nobackup/qp','20261106','secondary'),
 '281124_Alex':('/volume1/DATA/Gilles/analysis/projects','20261106','primary')}
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
 return h.hexdigest()
def checked(p,sha,owned=False):
 assert p.parent.resolve()==p.parent
 s=p.lstat();assert stat.S_ISREG(s.st_mode)
 if owned:assert s.st_uid==uid and s.st_nlink==1
 assert digest(p)==sha
 a=p.lstat();assert (s.st_ino,s.st_size,s.st_mtime_ns)==(a.st_ino,a.st_size,a.st_mtime_ns)
 return (s.st_dev,s.st_ino)
rows=[]
for r in v['files']:
 stem=r['stem'];source,suffix,role=scopes[stem];rel=Path(r['relative_path']);assert not rel.is_absolute() and '..' not in rel.parts
 assert rel.parts[0] in [stem,stem+'.mat']
 if role==v['role']:checked(Path(source)/rel,r['sha256'])
 if v['role']=='secondary':
  target=home/'projects'/('.incoming-'+stem+'-'+suffix)/rel
  target_id=checked(target,r.get('after_sha256',r['sha256']),True)
  if v['with_backups']:
   backup_id=checked(home/'.migration-backups/alexander-2024-20261105/relink-remaining-mains-20261107'/rel,r['sha256'],True)
   assert backup_id!=target_id
 rows.append({'path':r['relative_path'],'source_role':role,'target_checked':v['role']=='secondary'})
print(json.dumps({'success':True,'files':rows,'role':v['role'],'backups_checked':v['with_backups']}))
'''

TARGET_TREE=r'''
import hashlib,json,os,stat,sys,pwd as posix_accounts
from pathlib import Path
v=json.load(sys.stdin);uid=posix_accounts.getpwnam('maliavko').pw_uid
scopes={'movie_250124_diploids':'20261105','qp_250724_dbud4stest':'20261106','281124_Alex':'20261106'}
ignored={'@eaDir','#recycle','#snapshot','.snapshot','@Recycle','.rsync-partial'};out=[]
for p in v['projects']:
 stem=p['name'];root=Path('/volume1/homes/maliavko/DetecdivHub/projects')/('.incoming-'+stem+'-'+scopes[stem]);assert root.resolve()==root
 expected={r['path']:r['sha256'] for r in p['verified_files']};found={};found_dirs=set()
 def walk_error(e):raise e
 for parent,dirs,files in os.walk(root,followlinks=False,onerror=walk_error):
  if Path(parent)!=root:found_dirs.add(str(Path(parent).relative_to(root)))
  dirs[:]=sorted(d for d in dirs if d not in ignored)
  for d in dirs:assert not (Path(parent)/d).is_symlink()
  for name in sorted(files):
   if name in ignored:continue
   file=Path(parent)/name;rel=str(file.relative_to(root));assert rel in expected and file.parent.resolve()==file.parent
   s=file.lstat();assert stat.S_ISREG(s.st_mode) and s.st_uid==uid and s.st_nlink==1
   h=hashlib.sha256()
   with file.open('rb') as f:
    for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
   assert h.hexdigest()==expected[rel];a=file.lstat();assert (s.st_ino,s.st_size,s.st_mtime_ns)==(a.st_ino,a.st_size,a.st_mtime_ns)
   found[rel]=h.hexdigest()
 assert found==expected and found_dirs==set(p['verified_dirs']) and {x.name for x in root.iterdir() if x.name not in ignored}=={stem,stem+'.mat'}
 out.append({'project_id':p['project_id'],'files':len(found),'all_expected_hashes_equal':True,'native_owner':'maliavko'})
print(json.dumps({'success':True,'projects':out,'unselected_files_unchanged':True}))
'''

def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    p=WORK/'state.tmp';p.write_text(json.dumps(STATE,indent=2)+'\n');p.replace(WORK/'state.json')

def ssh_native(role,code,payload,output):
    assert role in ['primary','secondary'] and not output.exists()
    user,host,key=('Gilles','10.20.11.250','detecdiv_main_nas_ed25519') if role=='primary' else ('charvin-admin','10.20.8.250','detecdiv_backup_ed25519')
    loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(code.encode()).decode())+'))'
    args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-i','/home/charvin-admin/.ssh/'+key,user+'@'+host,
          shlex.join(['nice','-n','19','python3','-u','-c',loader])]
    with output.open('xb') as stream,Path(str(output)+'.errors.log').open('xb') as errors:
        result=subprocess.run(args,input=json.dumps(payload).encode(),stdout=stream,stderr=errors,timeout=3600)
    assert result.returncode==0,'Native gate failed; inspect preserved private receipt'

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
    return h.hexdigest()

def canonical(doc):return json.dumps(doc,sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(',',':'))
def strict_doc(raw):
    def pairs(values):
        assert len(values)==len({key for key,value in values}),'Duplicate JSON keys require review'
        return dict(values)
    return json.loads(raw,object_pairs_hook=pairs)
def get(doc,path):
    for key in path:doc=doc[key]
    return doc
def put(doc,path,value):
    for key in path[:-1]:doc=doc[key]
    doc[path[-1]]=value

def main():
    global STARTED
    import copy
    os.umask(0o077)
    with (WORK/'relink.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);assert not (WORK/'state.json').exists(),'No replay'
        STARTED=True
        r=json.loads((WORK/'controller-request.json').read_text());deadline=r['lease_deadline_unix']
        assert digest(Path(__file__))==r['runner_sha256'] and digest(WORK/'relink_remaining_mains.m')==r['matlab_sha256']
        assert {p['name'] for p in r['projects']}==set(SCOPES) and len(r['files'])==3 and len(r['lease_ids'])==3 and len(r['json_plan']['files'])==9
        assert not r['source_deletion_allowed'] and not r['catalog_cutover_allowed']
        assert {f['relative_path'] for f in r['files']}=={s+'.mat' for s in SCOPES}
        backup=Path('/homes2/maliavko/DetecdivHub/.migration-backups/alexander-2024-20261105/relink-remaining-mains-20261107')
        for row in list(r['files'])+r['json_files']:
            stem=row['stem'];rel=Path(row['relative_path']);assert not rel.is_absolute() and '..' not in rel.parts
            stage=Path('/homes2/maliavko/DetecdivHub/projects')/('.incoming-'+stem+'-'+SCOPES[stem][1])
            assert Path(row['target'])==stage/rel
            if row in r['files']:assert Path(row['backup'])==backup/rel
            else:assert stem=='281124_Alex' and str(rel) in {f['relative_path'] for f in r['json_plan']['files']}
        def lease():assert datetime.now(timezone.utc).timestamp()<deadline,'Own lease deadline passed'
        lease();save()
        mount=json.loads(subprocess.check_output(['findmnt','-J','-T','/homes2/maliavko/DetecdivHub/projects','-o','SOURCE,TARGET,OPTIONS'],text=True))['filesystems'][0]
        assert mount['source']=='//10.20.8.250/home' and mount['target']=='/homes2/maliavko' and 'username=maliavko' in mount['options'].split(',')
        free=os.statvfs('/homes2/maliavko');assert free.f_bavail*free.f_frsize>8_000_000_000_000
        for p in r['projects']:
            lease();stem=p['name'];STATE.update(phase='recheck_original_inventory',project=stem);save()
            if stem=='movie_250124_diploids':
                module=ROOT/'project-copies/copy_projects.py';expected=json.loads((ROOT/'project-copies/request.json').read_text())['runner_sha256'];args={'name':stem,'mode':'inventory'}
                before=ROOT/'project-copies'/stem/'source-before.jsonl'
            else:
                module=ROOT/'exception-copies-20261106/exception_copies.py';expected=json.loads((ROOT/'exception-copies-20261106/request.json').read_text())['runner_sha256'];args={'name':stem,'role':'source'}
                before=ROOT/'exception-copies-20261106'/stem/'source-before.jsonl'
            assert digest(module)==expected
            tree=ast.parse(module.read_text());native=ast.literal_eval(next(n.value for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='NATIVE' for t in n.targets)))
            output=WORK/('source-fresh-'+stem+'.jsonl');ssh_native(SCOPES[stem][2],native,args,output);assert output.read_bytes()==before.read_bytes(),'Original changed'
        assert not backup.exists()
        all_files=list(r['files'])+r['json_files'];assert len(all_files)==12
        for role in ['primary','secondary']:ssh_native(role,NATIVE,{'role':role,'files':all_files,'with_backups':False},WORK/('input-'+role+'.json'))
        backup.mkdir(parents=True,mode=0o700)
        for i,row in enumerate(all_files):
            lease();src=Path(row['target']);dst=backup/row['relative_path'];dst.parent.mkdir(parents=True,exist_ok=True,mode=0o700);assert not dst.exists()
            with src.open('rb') as a,dst.open('xb') as b:shutil.copyfileobj(a,b,4*1024*1024)
            STATE.update(phase='backup_metadata',backups=i+1);save()
        for role in ['primary','secondary']:ssh_native(role,NATIVE,{'role':role,'files':all_files,'with_backups':True},WORK/('backup-gate-'+role+'.json'))
        for row in r['json_files']:strict_doc(Path(row['target']).read_bytes())
        lease();(WORK/'request.json').write_text(json.dumps(r,indent=2)+'\n');STATE.update(phase='relinking_mains',copies_may_have_been_relinked=True);save()
        release='/home/charvin-admin/repos/DetecDiv-jobs/releases/d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7'
        expression="maxNumCompThreads(1);addpath('"+str(WORK)+"');relink_remaining_mains('"+str(WORK)+"','"+release+"');"
        with (WORK/'matlab.log').open('xb') as out:
            child=subprocess.Popen(['env','CUDA_VISIBLE_DEVICES=','nice','-n','19','ionice','-c','3','/usr/local/bin/matlab','-batch',expression],stdout=out,stderr=subprocess.STDOUT)
            STATE['matlab_pid']=child.pid;save();code=child.wait()
        (WORK/'matlab.exit').write_text(str(code)+'\n');assert code==0
        mat=json.loads((WORK/'result-mats.json').read_text());assert mat['success'] and len(mat['files'])==3
        reports=list(mat['files']);by_path={f['relative_path']:f for f in r['json_files']}
        for index,plan in enumerate(r['json_plan']['files']):
            lease();row=by_path[plan['relative_path']];target=Path(row['target']);assert digest(target)==row['sha256'] and digest(backup/row['relative_path'])==row['sha256']
            before=strict_doc(target.read_bytes());assert hashlib.sha256(canonical(before).encode()).hexdigest()==plan['before_canonical_sha256'];after=copy.deepcopy(before)
            for change in plan['changes']:assert get(after,change['field'])==change['before'];put(after,change['field'],change['after'])
            assert hashlib.sha256(canonical(after).encode()).hexdigest()==plan['after_canonical_sha256']
            temp=Path(str(target)+'.homes2-relink-tmp.json');assert not temp.exists();temp.write_text(json.dumps(after,ensure_ascii=False,allow_nan=False,indent=2)+'\n',encoding='utf-8')
            reopened=strict_doc(temp.read_bytes());assert canonical(reopened)==canonical(after)
            for key,value in plan['frozen_subtree_sha256'].items():assert hashlib.sha256(canonical(reopened[key]).encode()).hexdigest()==value
            restored=copy.deepcopy(reopened)
            for change in plan['changes']:put(restored,change['field'],change['before'])
            assert canonical(restored)==canonical(before);lease();assert digest(target)==row['sha256'];temp.replace(target)
            report={'project_id':row['project_id'],'relative_path':row['relative_path'],'before_sha256':row['sha256'],'after_sha256':digest(target),'path_changes':len(plan['changes']),
                    'scientific_values_types_and_annotations_preserved':True,'save_reopen_verified':True,'original_preserved':True,'frozen_contexts_unchanged':True}
            reports.append(report);(WORK/('modified-json-%02d.json'%index)).write_text(json.dumps(report,indent=2)+'\n')
            STATE.update(phase='json_reusable_refs_relinked',json_files_complete=index+1);save()
        lease();changed={f['relative_path']:f['after_sha256'] for f in reports};post_files=[dict(f,after_sha256=changed[f['relative_path']]) for f in all_files]
        for role in ['primary','secondary']:ssh_native(role,NATIVE,{'role':role,'files':post_files,'with_backups':True},WORK/('postflight-'+role+'.json'))
        expected=copy.deepcopy(r['projects'])
        for p in expected:
            for f in p['verified_files']:
                if f['path'] in changed:f['sha256']=changed[f['path']]
        ssh_native('secondary',TARGET_TREE,{'projects':expected},WORK/'postflight-all-target-files.json')
        assert len(reports)==12 and sum(f['path_changes'] for f in reports)==16889
        result={'success':True,'projects':3,'files':reports,'main_mats':3,'json_files':9,'path_changes':16889,'original_projects_preserved':True,'image_files_rewritten':False,'source_deleted':False,'catalog_changed':False}
        (WORK/'result.json').write_text(json.dumps(result,indent=2)+'\n');STATE.update(phase='copied_paths_relinked_science_verified_raw_and_host_gates_pending',finished_at=datetime.now(timezone.utc).isoformat());save()

if __name__=='__main__':
    try:main()
    except Exception as error:
        if STARTED:STATE.update(phase='failed_safe_unpublished_copies',error=type(error).__name__+': '+str(error));save()
        raise
