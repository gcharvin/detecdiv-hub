"""CPU-only tokenizer check for frozen evidence and optional history."""
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root.parent.parent / 'scripts'))
from evaluate_qwen_runs import remote, write_json, SYSTEM_PROMPT, TOOLS, evidence

assert hashlib.sha256((root / 'corpus.json').read_bytes()).hexdigest() == 'e3fc6d927b24cbb18bf2b936d7405e4bd7b42eadee0be76c2dbc6ea216e276ce'
corpus = json.loads((root / 'corpus.json').read_text(encoding='utf-8'))
cases = []
for case in corpus['cases'].values():
    job = case['job_id']
    messages = [{'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user', 'content': case['question'] + ' ID du job: ' + job}]
    for i, name in enumerate(('get_job_evidence', 'get_progress_history')):
        call = {'id': 'call_' + str(i), 'type': 'function',
                'function': {'name': name, 'arguments': {'job_id': job}}}
        messages.append({'role': 'assistant', 'content': None, 'tool_calls': [call]})
        messages.append({'role': 'tool', 'tool_call_id': call['id'],
                         'content': json.dumps(evidence(corpus, name, job), ensure_ascii=False)})
    cases.append({'job_id': job, 'messages': messages})
payload = {'cases': cases, 'tools': TOOLS}
source = '''
import sys,json,os
os.environ['CUDA_VISIBLE_DEVICES']='-1'
from transformers import AutoTokenizer
payload=json.load(sys.stdin)
path='/home/charvin-admin/.cache/detecdiv-qwen/huggingface/hub/models--Alennnndy--Qwen3.6-27B-AWQ-4bit/snapshots/e13b92a483e9114f49541c09478dcff9db8ce3d6'
t=AutoTokenizer.from_pretrained(path,local_files_only=True,trust_remote_code=False)
counts={c['job_id']:len(t.apply_chat_template(c['messages'],tools=payload['tools'],enable_thinking=False,tokenize=True,return_dict=False,add_generation_prompt=True)) for c in payload['cases']}
assert all(v>200 for v in counts.values()), 'Count token IDs, not fields of a BatchEncoding'
print(json.dumps({'tokenizer_only_no_weights_loaded':True,'context_limit':8192,'output_reserve':1100,'input_tokens_with_both_tools':counts,'all_fit':all(v+1100<=8192 for v in counts.values())}))
'''
args = SimpleNamespace(ssh='C:/Windows/System32/OpenSSH/ssh.exe')
result = remote(args, 'detecdiv-server', f'''
import subprocess
p=subprocess.run(['nice','-n','19','/home/charvin-admin/venvs/detecdiv-qwen/bin/python','-c',{source!r}],input={json.dumps(payload)!r},text=True,capture_output=True,timeout=60)
if p.returncode:raise RuntimeError(p.stderr[-1500:])
print(p.stdout)
''', timeout=80)
write_json(root / 'qwen36-context-validation.json', result)
print(json.dumps(result))
