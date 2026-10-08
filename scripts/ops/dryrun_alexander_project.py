"""Bounded copy simulation of a MAT and its paired project directory; no writes."""
from datetime import datetime, timezone
import json
from pathlib import Path
from register_alexander_2023_manifest import remote_json

CODE = r'''
import json,subprocess
from pathlib import Path
name='anof_03112023_dhy_fobs_ah4s'
source=Path('/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis')
destination=Path('/homes2/maliavko/DetecdivHub/projects')/name
mounts={line.split(' - ')[0].split()[4]:line for line in Path('/proc/self/mountinfo').read_text().splitlines()}
home=mounts['/homes2/maliavko']; archive=mounts['/mnt/detecdiv-secondary-sauvegarde']
assert home.split(' - ')[1].split()[:2]==['cifs','//10.20.8.250/home']
assert 'username=maliavko' in home.split(' - ')[1].split()[2].split(',')
assert 'rw' in home.split(' - ')[0].split()[5].split(',')
assert archive.split(' - ')[1].split()[:2]==['cifs','//10.20.8.250/Sauvegarde']
assert 'ro' in archive.split(' - ')[0].split()[5].split(',')
assert (source/(name+'.mat')).is_file() and (source/name).is_dir()
assert not destination.exists(), 'Pilot destination already exists; inspect before any replacement'
command=['timeout','45s','rsync','--dry-run','--archive','--stats','--itemize-changes',
         '--no-owner','--no-group','--no-perms','--exclude=@eaDir','--exclude=#recycle',
         str(source/(name+'.mat')),str(source/name),str(destination)+'/']
completed=subprocess.run(command,text=True,capture_output=True)
print(json.dumps({'source':str(source),'destination':str(destination),
 'dry_run':True,'exit_code':completed.returncode,'complete':completed.returncode==0,
 'stdout':completed.stdout,'stderr':completed.stderr,'destination_created':destination.exists()}))
'''

def main() -> None:
    repo = Path(__file__).resolve().parents[2]
    result = remote_json('C:/Windows/System32/OpenSSH/ssh.exe','detecdiv-server',CODE,{})
    result['captured_at'] = datetime.now(timezone.utc).isoformat()
    output = repo / 'reports/alexander-project-copy-dryrun-20261001.json'
    output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'report':str(output),'complete':result['complete'],
                      'exit_code':result['exit_code'],'destination_created':result['destination_created']}))

if __name__ == '__main__':
    main()
