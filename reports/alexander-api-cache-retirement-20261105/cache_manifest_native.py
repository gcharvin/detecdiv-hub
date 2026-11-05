"""Native inventory or exact unlink of only the three approved API caches."""
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time

BASE=Path('/volume1/DATA/.detecdiv-hub-api-cache')
YEARS={'2023_1':774,'2023_2':354,'2023_3':798}
TOTAL_FILES=sum(YEARS.values())
TOTAL_BYTES=28981723684
DIR_FLAGS=os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW
SYSTEM_DIRS={'@eaDir','#recycle','#snapshot'}


def directory(parent,parts):
    fd=os.dup(parent)
    try:
        for part in parts:
            child=os.open(part,DIR_FLAGS,dir_fd=fd);os.close(fd);fd=child
        return fd
    except BaseException:os.close(fd);raise


def root_fd(year):
    assert year in YEARS
    slash=os.open('/',DIR_FLAGS)
    try:return directory(slash,(BASE/('alexander-'+year)).parts[1:])
    finally:os.close(slash)


def inventory(year,fd):
    def fail(error):raise error
    rows=[]
    for parent,dirs,files,current in os.fwalk('.',dir_fd=fd,onerror=fail):
        for name in dirs:
            if not stat.S_ISDIR(os.stat(name,dir_fd=current,follow_symlinks=False).st_mode):raise ValueError('Unexpected cache directory entry')
        dirs[:]=[n for n in dirs if n not in SYSTEM_DIRS]
        for name in files:
            rel=str(Path(parent)/name);s=os.stat(name,dir_fd=current,follow_symlinks=False)
            if not (stat.S_ISREG(s.st_mode) and s.st_nlink==1):raise ValueError('Unexpected cache file kind: '+rel)
            rows.append({'year':year,'path':rel,'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'inode':s.st_ino,'uid':s.st_uid,'mode':stat.S_IMODE(s.st_mode)})
    return sorted(rows,key=lambda x:x['path'])


def main():
    request=json.loads(sys.stdin.buffer.readline());mode=request['mode'];assert mode in {'inventory','unlink'}
    rows=[];fds={}
    try:
        for year in YEARS:
            fd=root_fd(year);fds[year]=fd;part=inventory(year,fd)
            assert len(part)==YEARS[year],'Cache file count changed'
            rows.extend(part)
        assert len(rows)==TOTAL_FILES and sum(r['bytes'] for r in rows)==TOTAL_BYTES
        if mode=='inventory':
            for row in rows:print(json.dumps(row,sort_keys=True))
            return
        assert request['verified_home_serving'] is True
        manifest=sys.stdin.buffer.read();assert hashlib.sha256(manifest).hexdigest()==request['manifest_sha256']
        expected=[json.loads(line) for line in manifest.splitlines()]
        assert rows==expected,'Cache changed since verified inventory'
        deadline=float(request['lease_deadline_unix']);assert time.time()<deadline
        for row in expected:
            assert row['year'] in YEARS and all(x not in {'','.','..'} for x in row['path'].split('/'))
        disk=os.fstatvfs(fds['2023_1']);before=disk.f_bavail*disk.f_frsize
        print(json.dumps({'event':'preflight_passed','files':TOTAL_FILES,'free_bytes_before':before}),flush=True)
        count=size=0
        for row in expected:
            assert time.time()<deadline,'Own cache retirement lease deadline reached'
            rel=Path(row['path']);fd=directory(fds[row['year']],rel.parent.parts)
            try:
                s=os.stat(rel.name,dir_fd=fd,follow_symlinks=False)
                assert stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_size==row['bytes'] and s.st_mtime_ns==row['mtime_ns']
                assert s.st_ino==row['inode'] and s.st_uid==row['uid'] and stat.S_IMODE(s.st_mode)==row['mode']
                os.unlink(rel.name,dir_fd=fd)
            finally:os.close(fd)
            count+=1;size+=row['bytes'];print(json.dumps({'event':'unlinked','year':row['year'],'path':row['path'],'bytes':row['bytes']}),flush=True)
        for year,fd in fds.items():assert not inventory(year,fd),'Unexpected remaining cache payload'
        os.sync();disk=os.fstatvfs(fds['2023_1']);after=disk.f_bavail*disk.f_frsize
        print(json.dumps({'event':'complete','files_unlinked':count,'logical_bytes_unlinked':size,'free_bytes_before':before,'free_bytes_after':after,'measured_free_bytes_increase':after-before}),flush=True)
    finally:
        for fd in fds.values():os.close(fd)


if __name__=='__main__':main()
