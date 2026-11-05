"""Restore audited destination mtimes only, after equal full per-file SHA1."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess

WORK=Path(__file__).resolve().parent
SOURCE=Path('/data/Alexander/data/2023_3')
TARGET=Path('/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_3')
FLAGS=os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW


def open_file(root,relative):
    parts=relative.split('/');assert all(x not in {'','.','..'} for x in parts)
    fd=os.open('/',FLAGS)
    try:
        for part in root.parts[1:]+tuple(parts[:-1]):
            child=os.open(part,FLAGS,dir_fd=fd);os.close(fd);fd=child
        return os.open(parts[-1],os.O_RDONLY|os.O_NOFOLLOW,dir_fd=fd)
    finally:os.close(fd)


def digest(fd):
    os.lseek(fd,0,os.SEEK_SET);h=hashlib.sha1()
    while True:
        data=os.read(fd,4*1024*1024)
        if not data:break
        h.update(data)
    return h.hexdigest()


def main():
    os.umask(0o077);request=json.loads((WORK/'request.json').read_text())
    assert request['only_destination_mtimes_may_change'] and request['source_deletion_allowed'] is False
    assert not (WORK/'receipt.jsonl').exists(),'Review partial metadata repair; do not replay'
    audit=WORK.parent/'project3-raw-timestamp-audit-20261105'
    content=(audit/'timestamp-audit.jsonl').read_bytes()
    assert hashlib.sha256(content).hexdigest()==request['audit_receipt_sha256']
    assert (audit/'controller.exit').read_text().strip()=='0'
    events=[json.loads(x) for x in content.splitlines()];summary=events[-1];rows=events[:-1]
    assert summary['audit_completed'] and len(rows)==summary['timestamp_outliers']>0
    assert len(rows)==request['files_to_repair'] and all(x['event']=='timestamp_outlier' for x in rows)
    assert summary['files']==708590 and summary['bytes']==5967100516555
    mount=json.loads(subprocess.check_output(['findmnt','-J','-T',str(TARGET),'-o','SOURCE,TARGET,OPTIONS'],text=True))['filesystems'][0]
    assert mount['source']=='//10.20.8.250/home' and mount['target']=='/homes2/maliavko' and 'username=maliavko' in mount['options'].split(',')
    source_manifest=(WORK.parent/'2023_3/source-before.jsonl').read_bytes()
    assert hashlib.sha256(source_manifest).hexdigest()=='4abdfbfff41264b9bc069c3968ac742ac06fa9b7c52127bbf4369a7d95306fd3'
    wanted={r['path'] for r in rows}
    expected={r['path']:r for line in source_manifest.splitlines() if (r:=json.loads(line))['path'] in wanted}
    assert len(expected)==len(rows)
    with (WORK/'receipt.jsonl').open('x') as receipt:
        for row in rows:
            baseline=expected[row['path']]
            assert row['expected_mtime_ns']==baseline['mtime_ns'] and row['bytes']==baseline['bytes']
            a=open_file(SOURCE,row['path']);b=open_file(TARGET,row['path'])
            try:
                sa=os.fstat(a);sb=os.fstat(b)
                assert stat.S_ISREG(sa.st_mode) and stat.S_ISREG(sb.st_mode) and sa.st_nlink==sb.st_nlink==1
                assert sa.st_size==sb.st_size==baseline['bytes'] and sa.st_mtime_ns==baseline['mtime_ns']
                assert abs(sb.st_mtime_ns-row['observed_mtime_ns'])<=100,'Destination changed since audited timestamp'
                source_sha=digest(a);target_sha=digest(b);assert source_sha==target_sha
                assert (os.fstat(a).st_size,os.fstat(a).st_mtime_ns)==(sa.st_size,sa.st_mtime_ns)
                assert (os.fstat(b).st_size,os.fstat(b).st_mtime_ns)==(sb.st_size,sb.st_mtime_ns)
                os.utime(b,ns=(sb.st_atime_ns,baseline['mtime_ns']))
                after=os.fstat(b);assert abs(after.st_mtime_ns-baseline['mtime_ns'])<=100 and after.st_size==sb.st_size
                assert digest(b)==target_sha,'Metadata repair changed file content'
                result={'path':row['path'],'bytes':sb.st_size,'before_mtime_ns':sb.st_mtime_ns,'after_mtime_ns':after.st_mtime_ns,'sha1_source_and_destination':source_sha,'source_unchanged':True,'destination_content_unchanged':True,'changed_only_destination_mtime':True}
                receipt.write(json.dumps(result)+'\n');receipt.flush();os.fsync(receipt.fileno())
            finally:os.close(a);os.close(b)
    result={'success':True,'files_repaired':len(rows),'only_destination_mtimes_changed':True,'source_deleted':False,'finished_at':datetime.now(timezone.utc).isoformat(),'receipt_sha256':hashlib.sha256((WORK/'receipt.jsonl').read_bytes()).hexdigest()}
    (WORK/'result.json').write_text(json.dumps(result,indent=2)+'\n')


if __name__=='__main__':main()
