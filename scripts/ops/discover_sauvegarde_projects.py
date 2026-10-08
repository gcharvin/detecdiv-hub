"""Resumable, read-only discovery of loose project MATs, not ROI/image payloads."""
import argparse
import base64
import json
from pathlib import Path
from register_alexander_2023_manifest import remote_json

SCAN = r'''
import json,os,re,sys,time
from pathlib import Path
state=json.load(sys.stdin)
root=Path('/mnt/detecdiv-secondary-sauvegarde')
mount=next(line for line in Path('/proc/self/mountinfo').read_text().splitlines()
           if line.split(' - ')[0].split()[4]==str(root))
assert 'ro' in mount.split(' - ')[0].split()[5].split(',')
assert mount.split(' - ')[1].split()[:2]==['cifs','//10.20.8.250/Sauvegarde']
started=time.monotonic()
while state['queue'] and time.monotonic()-started<35:
 relative=state['queue'].pop(0); folder=root/relative
 try:
  entries=list(os.scandir(folder))
  directories={e.name:e for e in entries if e.is_dir(follow_symlinks=False)}
  project_children=set()
  for entry in entries:
   name=entry.name
   if not entry.is_file(follow_symlinks=False): continue
   if name.lower().endswith(('.mat.bz2','.mat.gz','.mat.zip')):
    state['compressed_mat_files'].append(str(Path(relative)/name))
   if not name.lower().endswith('.mat') or name.lower() in {'bk-project.mat','temp-project.mat'}: continue
   stem=Path(name).stem; paired=stem in directories
   legacy=name.lower().endswith('-project.mat')
   is_analysis=folder.name.lower() in {'analysis','analyses','qp'}
   if not (paired or legacy or is_analysis): continue
   st=entry.stat(follow_symlinks=False)
   state['candidates'].append({'relative_path':str(Path(relative)/name),
       'mat_bytes':st.st_size,'mtime_ns':st.st_mtime_ns,
       'paired_directory_present':paired,'legacy_filename':legacy,
       'project_dir_relative':str(Path(relative)/stem) if paired else relative})
   if paired: project_children.add(stem)
  for name in sorted(directories):
   reason=None
   if name in project_children: reason='paired_project_payload'
   elif name.startswith(('@','#','.')) or name.lower() in {'__pycache__','.git'}: reason='metadata'
   elif re.match(r'(?i)^(?:pos|im_)[0-9]',name) or re.search(r'(?i)-pos[0-9]+',name): reason='image_or_roi_payload'
   elif name.lower().endswith(('.zarr','.hbk')): reason='non_project_store'
   elif relative=='Fred' and name=='Duplicity': reason='backup_archive_not_loose_projects'
   if reason:
    state['pruned'].append({'path':str(Path(relative)/name),'reason':reason})
   else:
    state['queue'].append(str(Path(relative)/name))
  state['directories_seen']+=1
 except OSError as exc:
  state['errors'].append({'path':relative,'error':str(exc)})
state['complete']=not state['queue']
print(json.dumps(state))
'''

NATIVE_SCAN = """
import json,os,re,sys,time
from pathlib import Path
state=json.load(sys.stdin)
root=Path('/volume1/Sauvegarde')
assert root.is_dir() and root.resolve()==root
started=time.monotonic()
""" + SCAN.split('started=time.monotonic()\n', 1)[1]


def native_scan(state: dict) -> dict:
    encoded = base64.b64encode(NATIVE_SCAN.encode()).decode()
    bridge = f"""
import json,subprocess,sys,shlex
state=json.load(sys.stdin)
launcher="import base64;exec(base64.b64decode('{encoded}'))"
command=['ssh','-o','BatchMode=yes','-o','IdentitiesOnly=yes',
         '-i','/home/charvin-admin/.ssh/detecdiv_backup_ed25519',
         'charvin-admin@10.20.8.250',shlex.join(['python3','-c',launcher])]
result=subprocess.run(command,input=json.dumps(state),text=True,capture_output=True,check=True,timeout=50)
print(result.stdout)
"""
    return remote_json('C:/Windows/System32/OpenSSH/ssh.exe','detecdiv-server',bridge,state)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--until-complete', action='store_true')
    parser.add_argument('--expand-unconfirmed', action='store_true',
                        help='Revisit paired folders whose already-read headers were not projects')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    output = repo / 'reports/sauvegarde-project-discovery-20261001.json'
    state = json.loads(output.read_text(encoding='utf-8')) if output.exists() else {
        'source_root':'/mnt/detecdiv-secondary-sauvegarde', 'read_only':True,
        'queue':['Alexander','Alumni/Sandrine','Alumni/basile','Fred'],
        'directories_seen':0,'candidates':[],'compressed_mat_files':[],
        'pruned':[],'errors':[],'complete':False}
    if args.expand_unconfirmed:
        header_path=repo/'reports/sauvegarde-project-headers-native-progress-20261001.json'
        headers=json.loads(header_path.read_text(encoding='utf-8'))
        inspected={row['relativePath']:row for row in headers['candidates']}
        expanded=set(state.get('expanded_unconfirmed',[]))
        for item in list(state['candidates']):
            row=inspected.get(item['relative_path'])
            if not item['paired_directory_present'] or row is None or row['status']=='valid_project_header':
                continue
            if row['status']=='inspection_failed' and row['review'].startswith('Operands to the logical AND'):
                continue  # Legacy header classification is repaired separately.
            folder=item['project_dir_relative']
            if folder not in expanded:
                state['queue'].append(folder)
                expanded.add(folder)
        state['expanded_unconfirmed']=sorted(expanded)
        state['complete']=False
    while True:
        state = native_scan(state)
        output.write_text(json.dumps(state,indent=2)+'\n',encoding='utf-8')
        print(json.dumps({'complete':state['complete'],'directories':state['directories_seen'],
                         'queued':len(state['queue']),'candidates':len(state['candidates']),
                         'compressed_mat_files':len(state['compressed_mat_files']), 'errors':state['errors']}), flush=True)
        if state['complete'] or not args.until_complete:
            break


if __name__ == '__main__':
    main()
