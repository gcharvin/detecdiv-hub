"""Pin public metadata and download one data-only quant without GPU admission."""
import hashlib
import json
import sys
import urllib.request
import uuid
from pathlib import Path
from types import SimpleNamespace

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root.parent.parent / 'scripts'))
from evaluate_qwen_runs import remote, write_json, now

model = 'Alennnndy/Qwen3.6-27B-AWQ-4bit'
revision = 'e13b92a483e9114f49541c09478dcff9db8ce3d6'
info = json.load(urllib.request.urlopen('https://huggingface.co/api/models/' + model + '/revision/' + revision + '?blobs=true'))
assert info['sha'] == revision
config = json.load(urllib.request.urlopen('https://huggingface.co/' + model + '/raw/' + revision + '/config.json'))
assert not config.get('auto_map')
assert config['quantization_config']['config_groups']['group_0']['weights']['num_bits'] == 4
assert hashlib.sha256((root / 'corpus.json').read_bytes()).hexdigest() == 'e3fc6d927b24cbb18bf2b936d7405e4bd7b42eadee0be76c2dbc6ea216e276ce'
files = [f for f in info['siblings'] if f['rfilename'].endswith(('.json', '.txt', '.safetensors', '.jinja')) or f['rfilename'] in ('LICENSE', 'README.md')]
files.sort(key=lambda f: (f['rfilename'].endswith('.safetensors'), f['rfilename']))
manifest = {'owner': 'qwen36-prefetch-' + str(uuid.uuid4()), 'model': model, 'revision': revision,
            'base_model': 'Qwen/Qwen3.6-27B', 'source': 'community AWQ W4A16, not an official Qwen quant',
            'cache': '/home/charvin-admin/.cache/detecdiv-qwen/huggingface/hub',
            'root': '/home/charvin-admin/.cache/detecdiv-qwen/pilot-qwen36-download-20261015',
            'files': files, 'minimum_free_bytes': 10 * 1024**3, 'max_workers': 1}
write_json(root / 'qwen36-download-manifest.json', manifest)
plan = {'created_at': now(), 'purpose': 'Separate supplementary Qwen3.6 comparison; frozen original main plan is unchanged',
        'corpus_sha256': 'e3fc6d927b24cbb18bf2b936d7405e4bd7b42eadee0be76c2dbc6ea216e276ce',
        'candidates': [{'model': model, 'revision': revision, 'provisional_gpu_memory_utilization': 0.8,
                        'tool_call_parser': 'qwen3_coder', 'language_model_only': True,
                        'quantization': config['quantization_config'], 'community_provenance': manifest['source']}]}
write_json(root / 'qwen36-model-plan.json', plan)
source = '''
import json,pathlib,time,shutil,hashlib,os
os.environ['HF_HUB_DISABLE_XET']='1'
os.environ['HF_HUB_DOWNLOAD_TIMEOUT']='60'
from huggingface_hub import hf_hub_download
root=pathlib.Path(__file__).parent
m=json.loads((root/'manifest.json').read_text())
state={'owner':m['owner'],'status':'starting','pid':os.getpid(),'started_at':time.time(),'completed':[]}
def save():
 state['checked_at']=time.time();p=root/'state.tmp';p.write_text(json.dumps(state));p.replace(root/'state.json')
try:
 cache=pathlib.Path(m['cache']);snapshot=cache/('models--'+m['model'].replace('/','--'))/'snapshots'/m['revision']
 missing=[f for f in m['files'] if not (snapshot/f['rfilename']).exists()]
 needed=sum(f['size'] for f in missing)
 if shutil.disk_usage(cache).free < needed+m['minimum_free_bytes']:raise RuntimeError('Not enough disk to retain 10 GiB free')
 for f in m['files']:
  state.update(status='downloading',current=f['rfilename']);save()
  if shutil.disk_usage(cache).free < m['minimum_free_bytes']:raise RuntimeError('Disk safety margin reached')
  p=pathlib.Path(hf_hub_download(m['model'],f['rfilename'],revision=m['revision'],cache_dir=m['cache'],token=False))
  assert p.stat().st_size==f['size'],f['rfilename']
  if f.get('lfs',{}).get('sha256'):
   h=hashlib.sha256()
   with p.open('rb') as stream:
    for block in iter(lambda:stream.read(8*1024**2),b''):h.update(block)
   assert h.hexdigest()==f['lfs']['sha256'],f['rfilename']
  state['completed'].append(f['rfilename']);save()
 index_path=snapshot/'model.safetensors.index.json'
 if index_path.exists():
  index=json.loads(index_path.read_text());assert all((snapshot/name).exists() for name in set(index['weight_map'].values()))
 else:assert (snapshot/'model.safetensors').exists()
 assert shutil.disk_usage(cache).free >= m['minimum_free_bytes']
 state.update(status='complete',finished_at=time.time(),free_bytes=shutil.disk_usage(cache).free,snapshot=str(snapshot));save()
except Exception as e:
 state.update(status='error',error=str(e),finished_at=time.time());save();raise
'''
args = SimpleNamespace(ssh='C:/Windows/System32/OpenSSH/ssh.exe')
result = remote(args, 'detecdiv-server', f'''
import pathlib,json,subprocess
root=pathlib.Path({manifest['root']!r});root.mkdir(exist_ok=True)
old=root/'manifest.json'
if old.exists():raise RuntimeError('Existing downloader needs inspection before reuse')
old.write_text({json.dumps(manifest)!r})
(root/'prefetch.py').write_text({source!r})
log=(root/'download.log').open('a')
command=['nice','-n','19','ionice','-c','3','timeout','-k','30s','1800s','/home/charvin-admin/venvs/detecdiv-qwen/bin/python',str(root/'prefetch.py')]
p=subprocess.Popen(command,stdout=log,stderr=log,start_new_session=True)
print(json.dumps({{'pid':p.pid,'command':command,'owner':{manifest['owner']!r}}}))
''')
write_json(root / 'qwen36-download-controller.json', result)
print(json.dumps(result))
