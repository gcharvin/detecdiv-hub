"""Readonly directory-name search for an absent 2024 project data folder."""
import json
import hashlib
from pathlib import Path
import shlex
import subprocess

WORK=Path(__file__).resolve().parent
NATIVE=r'''
import os,json,time,sys
from pathlib import Path
root=Path(sys.argv[1]);assert str(root) in ['/volume1/DATA/Alexander','/volume1/Sauvegarde/Alexander']
assert root.is_dir() and root.resolve()==root
ignored={'@eaDir','#recycle','#snapshot','.snapshot','@Recycle','@tmp'};count=0;last=time.monotonic()
def fail(e):raise e
for parent,dirs,files in os.walk(root,followlinks=False,onerror=fail):
 dirs[:]=sorted(d for d in dirs if d not in ignored and not d.startswith('.') and not (Path(parent)/d).is_symlink())
 count+=1
 for name in dirs:
  if '290424_loxp15' in name.lower():print(json.dumps({'kind':'candidate','path':str(Path(parent)/name)}),flush=True)
 if time.monotonic()-last>30:print(json.dumps({'kind':'progress','directories':count}),flush=True);last=time.monotonic()
print(json.dumps({'kind':'complete','directories':count}),flush=True)
'''


def main():
    result=[]
    for label,host,key,root in [('primary','Gilles@10.20.11.250','detecdiv_main_nas_ed25519','/volume1/DATA/Alexander'),
                               ('secondary','charvin-admin@10.20.8.250','detecdiv_backup_ed25519','/volume1/Sauvegarde/Alexander')]:
        output=WORK/(label+'-receipt.jsonl');assert not output.exists()
        args=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-i','/home/charvin-admin/.ssh/'+key,host,shlex.join(['nice','-n','19','python3','-u','-c',NATIVE,root])]
        with output.open('xb') as out,(WORK/(label+'-errors.log')).open('xb') as errors:p=subprocess.run(args,stdout=out,stderr=errors)
        rows=[json.loads(line) for line in output.open()];assert p.returncode==0 and rows[-1]['kind']=='complete','Inspect partial receipt before continuation'
        result.append({'target':label,'root':root,'candidates':[r for r in rows if r['kind']=='candidate'],'summary':rows[-1],'receipt_sha256':hashlib.sha256(output.read_bytes()).hexdigest()})
        (WORK/'result.partial.json').write_text(json.dumps(result)+'\n')
    (WORK/'result.json').write_text(json.dumps({'success':True,'read_only':True,'results':result,'note':'Names only, no resource identity inferred. Raw acquisition folders are not automatically the missing derived project folder. No snapshots, hidden directories or recycle traversal.'})+'\n')


if __name__=='__main__':main()
