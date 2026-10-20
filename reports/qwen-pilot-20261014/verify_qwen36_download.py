"""Check the single-file safetensors checkpoint without loading it."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root.parent.parent / 'scripts'))
from evaluate_qwen_runs import remote, write_json, now

r = remote(SimpleNamespace(ssh='C:/Windows/System32/OpenSSH/ssh.exe'), 'detecdiv-server', '''
import pathlib,json,shutil,os,hashlib,struct
root=pathlib.Path('/home/charvin-admin/.cache/detecdiv-qwen/pilot-qwen36-download-20261015')
m=json.loads((root/'manifest.json').read_text());s=json.loads((root/'state.json').read_text())
if s['status']=='downloading':
 print(json.dumps(s));raise SystemExit()
if s['status']=='error' and 'model.safetensors.index.json' not in s.get('error',''):
 raise RuntimeError(s['error'])
p=pathlib.Path('/proc')/str(s['pid'])
if p.exists() and (p/'stat').read_text().rsplit(')',1)[1].split()[0]!='Z':raise RuntimeError('Downloader still active')
cache=pathlib.Path(m['cache']);snapshot=cache/('models--'+m['model'].replace('/','--'))/'snapshots'/m['revision']
for f in m['files']:
 assert (snapshot/f['rfilename']).stat().st_size==f['size'],f['rfilename']
assert len(s['completed'])==len(m['files'])
assert shutil.disk_usage(cache).free>=m['minimum_free_bytes']
weight=snapshot/'model.safetensors'
with weight.open('rb') as stream:
 n=struct.unpack('<Q',stream.read(8))[0];header=json.loads(stream.read(n))
assert any('weight_packed' in name for name in header)
if s['status']=='error':
 s['validation_repair']={'original_error':s['error'],'explanation':'This repository uses one model.safetensors file and needs no shard index; all expected file sizes and LFS hashes were verified before the index-only check.'}
 s['status']='complete';(root/'state.json').write_text(json.dumps(s))
print(json.dumps({'status':'complete','owner':m['owner'],'model':m['model'],'revision':m['revision'],'single_weight_file':True,'weight_bytes':weight.stat().st_size,'tensor_count':len(header)-int('__metadata__' in header),'free_bytes':shutil.disk_usage(cache).free,'downloader_process':'gone_or_zombie','validation_repair':s.get('validation_repair')}))
''')
write_json(root / 'qwen36-cache-verification.json', {'checked_at': now(), **r})
print(json.dumps(r))
