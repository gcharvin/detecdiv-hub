"""Readonly filename search in Alexander payloads on the two existing NASes.

Matches are candidates only; names alone cannot establish image identity.
No snapshot, recycle, hidden directory, symlink or credential file is read.
"""
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

WORK=Path(__file__).resolve().parent
NATIVE=r'''
import json,os,stat,sys,time
from pathlib import Path
root=Path(sys.argv[1]);assert str(root) in ['/volume1/DATA/Alexander','/volume1/Sauvegarde/Alexander']
assert root.is_dir() and root.resolve()==root
names=set(json.loads(sys.argv[2]));assert len(names)==9 and all('/' not in n for n in names)
ignored={'@eaDir','#recycle','#snapshot','.snapshot','@Recycle','@tmp'}
files=directories=skipped=0;last=time.monotonic()
def fail(e):raise e
for parent,dirs,leafs in os.walk(root,followlinks=False,onerror=fail):
 clean=[]
 for name in sorted(dirs):
  p=Path(parent)/name
  if name in ignored or name.startswith('.') or p.is_symlink():skipped+=1
  else:clean.append(name)
 dirs[:]=clean;directories+=1
 for name in leafs:
  files+=1
  if name not in names:continue
  p=Path(parent)/name;s=p.lstat()
  if stat.S_ISREG(s.st_mode):print(json.dumps({'kind':'candidate','path':str(p),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns}),flush=True)
 if time.monotonic()-last>30:
  print(json.dumps({'kind':'progress','files':files,'directories':directories,'skipped':skipped}),flush=True);last=time.monotonic()
print(json.dumps({'kind':'complete','files':files,'directories':directories,'skipped':skipped}),flush=True)
'''


def main():
    request=json.loads((WORK/'request.json').read_text());names=request['basenames']
    assert len(names)==9 and len(set(names))==9
    targets=[('primary','Gilles@10.20.11.250','/home/charvin-admin/.ssh/detecdiv_main_nas_ed25519','/volume1/DATA/Alexander'),
             ('secondary','charvin-admin@10.20.8.250','/home/charvin-admin/.ssh/detecdiv_backup_ed25519','/volume1/Sauvegarde/Alexander')]
    results=[]
    for label,host,key,root in targets:
        output=WORK/(label+'-receipt.jsonl');assert not output.exists()
        args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-i',key,host,
              shlex.join(['nice','-n','19','python3','-u','-c',NATIVE,root,json.dumps(names)])]
        with output.open('xb') as stream,(WORK/(label+'-errors.log')).open('xb') as errors:
            result=subprocess.run(args,stdout=stream,stderr=errors)
        rows=[json.loads(line) for line in output.read_bytes().splitlines()]
        assert result.returncode==0 and rows[-1]['kind']=='complete','Inspect durable partial/error receipt before any continuation'
        summary={'target':label,'root':root,'candidates':[r for r in rows if r['kind']=='candidate'],'inventory_counts':rows[-1],'receipt_sha256':hashlib.sha256(output.read_bytes()).hexdigest()}
        results.append(summary);(WORK/'result.partial.json').write_text(json.dumps(results)+'\n')
    (WORK/'result.json').write_text(json.dumps({'success':True,'read_only':True,'results':results,'note':'Exact filenames only, not pixel/content identity; no hidden metadata, snapshots or recycle traversal, no recovery or deletion performed.'})+'\n')


if __name__=='__main__':main()
