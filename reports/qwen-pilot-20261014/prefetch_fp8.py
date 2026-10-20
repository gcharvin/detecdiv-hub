"""Fetch only pinned official checkpoints; no GPU access or admission changes."""
import json
import os
import shutil
import signal
import sys
import time
from pathlib import Path

from huggingface_hub import snapshot_download

root = Path(sys.argv[1])
plan = json.loads((root / 'manifest.json').read_text())
cache = Path(plan['cache'])
state_path = root / 'state.json'
state = {'owner': plan['owner'], 'pid': os.getpid(), 'command': sys.argv,
         'status': 'starting', 'started_at': time.time(), 'models': []}


def save():
    state['checked_at'] = time.time()
    temporary = state_path.with_suffix('.tmp')
    temporary.write_text(json.dumps(state, indent=2))
    temporary.replace(state_path)


def terminated(_number, _frame):
    raise TimeoutError('Bounded checkpoint prefetch expired or was stopped')


signal.signal(signal.SIGTERM, terminated)
signal.signal(signal.SIGINT, terminated)
try:
    pending = list(plan['models'])
    for item in plan['models']:
        if time.time() >= plan['expires_at']:
            raise TimeoutError('Checkpoint prefetch lease expired')
        free = shutil.disk_usage(cache).free / 1024**3
        if free < sum(m['weights_gib'] for m in pending) + 10:
            raise RuntimeError('Insufficient cache disk headroom; no files removed')
        current = {'model': item['model'], 'revision': item['revision'],
                   'status': 'downloading', 'started_at': time.time(), 'disk_free_gib': free}
        state['models'].append(current)
        state['status'] = 'downloading'
        save()
        target = snapshot_download(repo_id=item['model'], revision=item['revision'],
            cache_dir=str(cache), max_workers=1,
            allow_patterns=['*.json', '*.safetensors', 'tokenizer*', '*.txt', '*.model', '*.jinja'])
        target = Path(target)
        index = target / 'model.safetensors.index.json'
        if index.exists():
            shards = set(json.loads(index.read_text())['weight_map'].values())
            if not all((target / s).is_file() for s in shards):
                raise RuntimeError('Downloaded checkpoint has missing shards')
        elif not list(target.glob('*.safetensors')):
            raise RuntimeError('Downloaded checkpoint has no weights')
        current.update(status='cached', snapshot=str(target), finished_at=time.time())
        pending.pop(0)
        save()
    state['status'] = 'complete'
except Exception as exc:
    state['status'] = 'interrupted' if isinstance(exc, TimeoutError) else 'error'
    state['error_type'] = type(exc).__name__
    # Do not persist HTTP headers, credentials or a traceback from a downloader.
finally:
    state['finished_at'] = time.time()
    save()
