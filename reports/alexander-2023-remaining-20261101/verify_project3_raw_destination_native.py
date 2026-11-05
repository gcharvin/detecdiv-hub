"""Read-only native ownership/metadata check of the fully SHA1-verified copy."""
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

ROOT=Path('/volume1/homes/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_3')
MANIFEST_SHA='4abdfbfff41264b9bc069c3968ac742ac06fa9b7c52127bbf4369a7d95306fd3'


def main():
    audit_timestamps=sys.argv[1:]==['--audit-timestamps']
    assert not sys.argv[1:] or audit_timestamps
    manifest=sys.stdin.buffer.read()
    assert hashlib.sha256(manifest).hexdigest()==MANIFEST_SHA
    rows=[r for line in manifest.splitlines() if (r:=json.loads(line))['kind']=='file']
    assert len(rows)==708590 and sum(r['bytes'] for r in rows)==5967100516555
    expected={r['path']:r for r in rows};assert len(expected)==len(rows)
    for name in expected:
        assert not Path(name).is_absolute() and all(x not in {'','.','..'} for x in name.split('/'))
    flags=os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW;fd=os.open('/',flags)
    try:
        for part in ROOT.parts[1:]:
            child=os.open(part,flags,dir_fd=fd);os.close(fd);fd=child
        seen=set();maximum=0;total=0;outliers=0
        def fail(error):raise error
        for parent,dirs,files,current in os.fwalk('.',dir_fd=fd,onerror=fail):
            for name in dirs:assert stat.S_ISDIR(os.stat(name,dir_fd=current,follow_symlinks=False).st_mode)
            dirs[:]=[n for n in dirs if n not in {'@eaDir','#recycle','#snapshot'}]
            for name in files:
                rel=str(Path(parent)/name);assert rel in expected,'Unexpected destination file: '+rel
                row=expected[rel];s=os.stat(name,dir_fd=current,follow_symlinks=False)
                assert stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_uid==1031
                assert s.st_size==row['bytes']
                difference=abs(s.st_mtime_ns-row['mtime_ns'])
                if difference>100:
                    if not audit_timestamps:raise AssertionError('Destination timestamp changed: '+rel)
                    outliers+=1
                    print(json.dumps({'event':'timestamp_outlier','path':rel,'bytes':s.st_size,'expected_mtime_ns':row['mtime_ns'],'observed_mtime_ns':s.st_mtime_ns}),flush=True)
                maximum=max(maximum,difference);seen.add(rel);total+=s.st_size
        assert seen==expected.keys()
        print(json.dumps({'success':outliers==0,'audit_completed':True,'timestamp_outliers':outliers,'source_manifest_sha256':MANIFEST_SHA,'files':len(seen),'bytes':total,'all_owned_by_maliavko':True,'all_regular_single_link_files':True,'source_paths_and_sizes_match':True,'max_mtime_difference_ns':maximum,'source_deleted':False}))
    finally:
        os.close(fd)


if __name__=='__main__':main()
