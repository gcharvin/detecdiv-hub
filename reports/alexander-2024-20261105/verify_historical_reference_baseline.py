"""Verify exact historical-reference availability against the original manifest.

No mutation, recovery, publication, checksum acceptance or retirement operation.
"""
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

WORK=Path(__file__).resolve().parent
ROOT=WORK.parent
MANIFEST_SHA='8fd64d094fe29707e0ddd0b1480e9b861786d3adde677087ca7a6e95e252f61e'
NATIVE=r'''
import json,os,stat,sys
from pathlib import Path
role=sys.argv[1];assert role in ['source','target']
root=Path('/volume1/DATA/Alexander/data/2024_1' if role=='source' else '/volume1/homes/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/2024_1')
assert root.is_dir() and root.resolve()==root
paths=json.load(sys.stdin);assert len(paths)==len(set(paths))
checked=set()
for relative in paths:
 assert relative.startswith('060224_cres/') and not any(p in ['', '.', '..'] for p in relative.split('/'))
 path=root/relative
 for parent in reversed(path.parents):
  if parent==root or str(parent).startswith(str(root)+'/'):
   if parent not in checked:
    mode=parent.lstat().st_mode;assert stat.S_ISDIR(mode),'Unexpected or missing parent';checked.add(parent)
 try:s=path.lstat()
 except FileNotFoundError:print(json.dumps({'path':relative,'present':False}));continue
 assert stat.S_ISREG(s.st_mode),'Unexpected nonregular raw reference'
 print(json.dumps({'path':relative,'present':True,'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'inode':s.st_ino,'uid':s.st_uid,'gid':s.st_gid}))
'''


def main():
    request=json.loads((WORK/'request.json').read_text())
    reference=ROOT/'historical-reference-baseline-20261106/references.jsonl'
    assert hashlib.sha256(reference.read_bytes()).hexdigest()==request['references_sha256']
    rows=[json.loads(line) for line in reference.open()]
    paths=sorted({r['path'] for r in rows});assert all(r['year']=='2024_1' for r in rows)
    required=set(paths);before={};digest=hashlib.sha256()
    with (ROOT/'2024_1/source-before.jsonl').open('rb') as stream:
        for line in stream:
            digest.update(line);r=json.loads(line)
            if r['kind']=='file' and r['path'] in required:before[r['path']]=r
    assert digest.hexdigest()==MANIFEST_SHA
    missing=sorted(required-before.keys());assert len(missing)==15
    for row in rows:
        original=before.get(row['path']);assert row['present']==bool(original),'Availability changed from initial native manifest'
        if original:assert row['actual_bytes']==original['bytes'],'Size changed from initial native manifest'
    targets=[('source','Gilles@10.20.11.250','/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519'),
             ('target','charvin-admin@10.20.8.250','/home/charvin-admin/.ssh/detecdiv_backup_ed25519')]
    receipts={}
    for role,host,key in targets:
        output=WORK/(role+'-stat-receipt.jsonl');assert not output.exists()
        args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-i',key,host,shlex.join(['nice','-n','19','python3','-u','-c',NATIVE,role])]
        with output.open('xb') as stream,(WORK/(role+'-errors.log')).open('xb') as errors:
            result=subprocess.run(args,input=json.dumps(paths).encode(),stdout=stream,stderr=errors)
        assert result.returncode==0,'Inspect native stat receipt before bounded continuation'
        actual=[json.loads(line) for line in output.open()];assert [r['path'] for r in actual]==paths
        for row in actual:
            original=before.get(row['path']);assert row['present']==bool(original),'Current availability differs from initial source'
            if not original:continue
            assert row['bytes']==original['bytes']
            if role=='source':
                for field in ['mtime_ns','inode','uid','gid']:assert row[field]==original[field],'Native source metadata changed'
            else:assert row['uid']==1031,'Target raw owner changed'
        receipts[role]={'files':len(actual),'receipt_sha256':hashlib.sha256(output.read_bytes()).hexdigest()}
        (WORK/'progress.json').write_text(json.dumps({'role':role,'stat_parity_verified':True,'references':len(rows),'unique_paths':len(paths),'source_deleted':False})+'\n')
    projects=[]
    for project_id in sorted({r['project_id'] for r in rows}):
        subset=[r for r in rows if r['project_id']==project_id]
        projects.append({'project_id':project_id,'cached_references':len(subset),'missing_paths':sorted({r['path'] for r in subset if not r['present']}),'saved_size_differences':sum(r['size_mismatch'] for r in subset)})
    output={'success':True,'availability_matches_initial_native_manifest':True,'source_current_native_metadata_matches_initial_manifest':True,
            'target_current_presence_and_sizes_match_initial_manifest':True,'manifest_sha256':MANIFEST_SHA,'references_sha256':request['references_sha256'],
            'reference_count':len(rows),'unique_paths':len(paths),'missing_paths':missing,'projects':projects,'native_receipts':receipts,
            'read_only':True,'source_deleted':False,'strict_saved_cache_gate_passed':False,'full_raw_checksum_gate_passed':False,
            'note':'Explicit immutable historical absence/size baseline, not image recovery or full-content transfer validation. Final hosts must preserve availability parity and scientific metadata; any new absence/change fails.'}
    (WORK/'result.json').write_text(json.dumps(output,indent=2)+'\n')


if __name__=='__main__':main()
