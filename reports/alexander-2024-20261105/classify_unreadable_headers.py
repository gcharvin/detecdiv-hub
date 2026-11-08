"""Bounded readonly container classification of exactly 33 unresolved legacy MATs."""
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote
from audit import remote_json,SSH

ADMIT=r'''
import json,sys,uuid
from datetime import datetime,timezone
from sqlalchemy import select
from api.db import SessionLocal
from api.models import Project,Job,ProjectLock,IndexingJob
v=json.load(sys.stdin);ids={uuid.UUID(x['id']) for x in v['projects']};assert len(ids)==33
now=datetime.now(timezone.utc);busy=set()
with SessionLocal() as db:
 assert not list(db.scalars(select(IndexingJob.id).where(IndexingJob.status.in_(['queued','running'])))),'Indexing active: defer classification'
 for i in ids:assert db.get(Project,i).owner.user_key=='gilles'
 for j in db.scalars(select(Job).where(Job.status.in_(['queued','running','cancelling']))):
  text=json.dumps(j.params_json or {},default=str)
  busy.update(i for i in ids if i==j.project_id or str(i) in text)
 for lock in db.scalars(select(ProjectLock).where(ProjectLock.project_id.in_(ids),ProjectLock.status=='active')):
  if lock.expires_at is None or lock.expires_at>now:busy.add(lock.project_id)
 print(json.dumps({'read_only':True,'checked_at':now.isoformat(),'leases_acquired':0,'busy_project_ids':sorted(map(str,busy))}))
'''

CODE=r'''
import json,hashlib,os,stat,struct
from pathlib import Path
from datetime import datetime,timezone
os.umask(0o077)
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105');work=root/'unreadable-header-classification-20261108'
assert not work.exists(),'No replay: inspect existing receipt'
receipt=root/'legacy-path-review/receipt.jsonl';raw=receipt.read_bytes()
assert hashlib.sha256(raw).hexdigest()=='13bb535dc5de28255ef4c99ec35675cdba73e1b411110b703550cf05761adacb'
items=[x for x in (json.loads(line) for line in raw.splitlines()) if x.get('status')=='requires_review' and x.get('error')=='Unreadable MAT variable directory']
assert len(items)==33 and all(x['owner']=='gilles' for x in items)
assert {x['id'] for x in items}=={x['id'] for x in PROJECTS}
work.mkdir(mode=0o700);(work/'request.json').write_text(json.dumps({'admission':ADMITTED,'receipt_sha256':hashlib.sha256(raw).hexdigest(),'scope':items,'read_only':True})+'\n')
flags=os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW
def open_payload(path):
 parts=Path(path).parts
 assert parts[0]=='/' and '..' not in parts and path.endswith('.mat')
 assert path.startswith(('/data/','/mnt/detecdiv-secondary-sauvegarde/'))
 fd=os.open('/',flags)
 try:
  for part in parts[1:-1]:
   nxt=os.open(part,flags,dir_fd=fd);os.close(fd);fd=nxt
  file=os.open(parts[-1],os.O_RDONLY|os.O_NOFOLLOW,dir_fd=fd)
  return file
 finally:os.close(fd)
rows=[];busy=set(ADMITTED['busy_project_ids'])
with (work/'receipt.jsonl').open('x') as stream:
 for item in items:
  row={'project_id':item['id'],'name':item['name'],'source':item['file'],'reference_relevance_resolved':False,'scientific_content_saved':False}
  if item['id'] in busy:row['classification']='deferred_project_occupied'
  else:
   fd=None
   try:
    fd=open_payload(item['file']);before=os.fstat(fd);assert stat.S_ISREG(before.st_mode)
    data=os.read(fd,4096);after=os.fstat(fd)
    assert (before.st_ino,before.st_dev,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_dev,after.st_size,after.st_mtime_ns)
    row.update(bytes=before.st_size,mtime_ns=before.st_mtime_ns,uid=before.st_uid,gid=before.st_gid,header_bytes_read=len(data),header_sha256=hashlib.sha256(data).hexdigest(),metadata_stable=True)
    if before.st_size<=4096:row['full_file_sha256']=hashlib.sha256(data).hexdigest()
    if data.startswith(b'MATLAB 5.0 MAT-file'):
     row['classification']='mat_v5_header';row['endian_marker']=data[126:128].decode('ascii',errors='replace')
     if len(data)>=136 and data[126:128] in [b'IM',b'MI']:
      endian='<' if data[126:128]==b'IM' else '>';kind,length=struct.unpack(endian+'II',data[128:136]);row['first_element_type']=kind;row['first_element_declared_bytes']=length
      row['first_element_exceeds_file']=length>before.st_size-136 if kind in [14,15] else None
    elif data.startswith(b'MATLAB 7.3 MAT-file'):row['classification']='mat_v73_header'
    elif data.startswith(b'\x89HDF\r\n\x1a\n'):row['classification']='hdf5_header'
    elif not data:row['classification']='empty_file'
    else:row['classification']='unrecognized_container_header'
   except (OSError,AssertionError) as error:row.update(classification='not_classified',error_type=type(error).__name__)
   finally:
    if fd is not None:os.close(fd)
  rows.append(row);stream.write(json.dumps(row)+'\n');stream.flush()
result={'read_only':True,'loop_completed':True,'files':len(rows),'classifications':{key:sum(r['classification']==key for r in rows) for key in sorted({r['classification'] for r in rows})},
 'all_reference_relevance_resolved':False,'source_deleted':False,'scientific_files_saved':False,'checked_at':datetime.now(timezone.utc).isoformat()}
(work/'result.json').write_text(json.dumps(result,indent=2)+'\n');(work/'controller.exit').write_text('0\n')
result['receipt_sha256']=hashlib.sha256((work/'receipt.jsonl').read_bytes()).hexdigest()
result['sizes']=[r.get('bytes') for r in rows]
result['first_elements_exceeding_file']=sum(r.get('first_element_exceeds_file') is True for r in rows)
print(json.dumps(result))
'''

if __name__=='__main__':
    source=json.loads((HERE/'runtime/unreadable-header-classification-inputs-20261108.json').read_text())
    admitted=remote_json(SSH,'webserver-labo',ADMIT,source,container=True)
    (HERE/'runtime/unreadable-header-admission-20261108.json').write_text(json.dumps(admitted,indent=2)+'\n')
    result=remote('linux','PROJECTS='+repr(source['projects'])+'\nADMITTED='+repr(admitted)+'\n'+CODE,timeout=90)
    with (HERE/'unreadable-header-classification-verification.json').open('x') as stream:json.dump(result,stream,indent=2);stream.write('\n')
    print(json.dumps(result))
