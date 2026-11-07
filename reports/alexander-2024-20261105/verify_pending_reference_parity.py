"""Readonly exact initial/current source/target parity for two diagnosed projects.

No identity waiver, candidate substitution, relink, publication or deletion.
"""
import base64
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

WORK=Path(__file__).resolve().parent
ROOT=WORK.parent
MANIFEST_SHA='8fd64d094fe29707e0ddd0b1480e9b861786d3adde677087ca7a6e95e252f61e'
REFERENCES_SHA='3924c8a996832bc48b539ec80662234eb2bfb50446d64a38a85eeb07aedb217d'
NATIVE=r'''
import json,stat,sys
from pathlib import Path
role=sys.argv[1];assert role in ['source','target']
root=Path('/volume1/DATA/Alexander/data/2024_1' if role=='source' else '/volume1/homes/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/2024_1')
assert root.is_dir() and root.resolve()==root
paths=json.load(sys.stdin);assert paths==sorted(set(paths));checked={root:True}
for relative in paths:
 parts=relative.split('/');assert parts[0] in ['250124_diploids','290424_loxP15'] and all(p not in ['', '.', '..'] for p in parts)
 path=root/relative;parent_present=True
 for parent in reversed(path.parents):
  if parent==root or str(parent).startswith(str(root)+'/'):
   if parent not in checked:
    try:s=parent.lstat()
    except FileNotFoundError:checked[parent]=False
    else:assert stat.S_ISDIR(s.st_mode),'Unexpected symlink/non-directory ancestor';checked[parent]=True
   if not checked[parent]:parent_present=False;break
 if not parent_present:print(json.dumps({'path':relative,'present':False}));continue
 try:s=path.lstat()
 except FileNotFoundError:print(json.dumps({'path':relative,'present':False}));continue
 assert stat.S_ISREG(s.st_mode),'Unexpected symlink/nonregular raw reference'
 print(json.dumps({'path':relative,'present':True,'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'inode':s.st_ino,'uid':s.st_uid,'gid':s.st_gid}))
'''

def main():
    request=json.loads((WORK/'request.json').read_text());assert request['read_only']
    raw=(ROOT/'pending-availability-diagnosis-v2-20261107/references.jsonl').read_bytes()
    assert hashlib.sha256(raw).hexdigest()==REFERENCES_SHA
    rows=[json.loads(line) for line in raw.splitlines()]
    paths=sorted({r['path'] for r in rows});assert len(rows)==len(paths)==112956 and all(r['year']=='2024_1' for r in rows)
    required=set(paths);before={};digest=hashlib.sha256()
    with (ROOT/'2024_1/source-before.jsonl').open('rb') as stream:
        for line in stream:
            digest.update(line);r=json.loads(line)
            if r['kind']=='file' and r['path'] in required:before[r['path']]=r
    assert digest.hexdigest()==MANIFEST_SHA
    missing=set(paths)-before.keys();assert len(missing)==53280
    for row in rows:
        original=before.get(row['path']);assert row['present']==bool(original),'Diagnostic availability differs from initial source'
        if original:assert row['actual_bytes']==original['bytes']
    receipts={}
    for role,host,key in [('source','Gilles@10.20.11.250','/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519'),
                          ('target','charvin-admin@10.20.8.250','/home/charvin-admin/.ssh/detecdiv_backup_ed25519')]:
        output=WORK/(role+'-receipt.jsonl');assert not output.exists(),'Review partial receipts; never replay'
        loader='import base64;exec(base64.b64decode('+repr(base64.b64encode(NATIVE.encode()).decode())+'))'
        args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-i',key,host,
              shlex.join(['nice','-n','19','python3','-u','-c',loader,role])]
        with output.open('xb') as stream,(WORK/(role+'-errors.log')).open('xb') as errors:
            result=subprocess.run(args,input=json.dumps(paths).encode(),stdout=stream,stderr=errors)
        assert result.returncode==0,'Native parity failed; preserve diagnostics'
        actual=[json.loads(line) for line in output.open()];assert [r['path'] for r in actual]==paths
        for row in actual:
            original=before.get(row['path']);assert row['present']==bool(original),'Current availability changed from initial source'
            if not original:continue
            assert row['bytes']==original['bytes']
            if role=='source':
                for field in ['mtime_ns','inode','uid','gid']:assert row[field]==original[field],'Native source metadata changed'
            else:assert row['uid']==1031,'Unexpected target raw owner'
        receipts[role]={'references':len(actual),'sha256':hashlib.sha256(output.read_bytes()).hexdigest()}
        (WORK/'progress.json').write_text(json.dumps({'role_verified':role,'references':len(rows),'source_deleted':False})+'\n')
    summary=[]
    for project_id in sorted({r['project_id'] for r in rows}):
        selected=[r for r in rows if r['project_id']==project_id]
        summary.append({'project_id':project_id,'references':len(selected),'missing_initial_references':sum(not r['present'] for r in selected),
                        'saved_cache_size_mismatches':sum(r['size_mismatch'] for r in selected),'fovs_with_missing_references':sorted({r['fov'] for r in selected if not r['present']})})
    result={'success':True,'references':len(rows),'missing_initial_references':len(missing),'manifest_sha256':MANIFEST_SHA,'references_sha256':REFERENCES_SHA,
            'initial_current_source_and_target_availability_equal':True,'native_current_source_metadata_unchanged':True,'native_receipts':receipts,'projects':summary,
            'strict_identity_and_access_gate_passed':False,'raw_full_checksum_gate_passed':False,'candidate_substitutions':0,'read_only':True,'source_deleted':False,
            'note':'Availability diagnosis only. Missing historical raw fields remain inaccessible; size-cache differences require pixel/crop corroboration. No cleanup gate is waived.'}
    (WORK/'result.json').write_text(json.dumps(result,indent=2)+'\n')

if __name__=='__main__':main()
