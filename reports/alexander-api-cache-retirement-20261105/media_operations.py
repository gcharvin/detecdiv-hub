"""Stage and archive bounded long-running media checks in the live API."""
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
RUNTIME=HERE/'runtime';RUNTIME.mkdir(exist_ok=True)
PREVIOUS=HERE.parent/'alexander-2023-remaining-20261101'
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from audit import SSH,remote_json


def load(name):return json.loads((RUNTIME/name).read_text(encoding='utf-8'))


def launch(mode):
    assert mode in {'pairs','handlers','post-retirement'}
    plan=json.loads((PREVIOUS/'runtime/api-homes2-cache-serving-plan-20261105.json').read_text(encoding='utf-8'))
    script=(HERE/'verify_media.py').read_bytes()
    request={'mode':mode,'runner_sha256':hashlib.sha256(script).hexdigest(),'plan':plan}
    if mode!='pairs':
        state=load('pairs-status.json');assert state['state']['success'] and state['receipt_verified']
        request.update(pairs=state['receipt'],batch_id=load('admission.json')['batch_id'])
    if mode=='post-retirement':assert json.loads((HERE/'cache-retirement-receipt-verification.json').read_text())['success']
    code=r'''
import json,base64,hashlib,os,subprocess,sys
from pathlib import Path
v=json.load(sys.stdin);os.umask(0o077);mode=v['request']['mode'];assert mode in {'pairs','handlers','post-retirement'}
base=Path('/tmp/detecdiv-api-cache-retirement-20261105');base.mkdir(mode=0o700,exist_ok=True)
w=base/mode;assert not w.exists(),'Inspect the existing media task before any retry';w.mkdir(mode=0o700)
script=base64.b64decode(v['script']);assert hashlib.sha256(script).hexdigest()==v['request']['runner_sha256']
(w/'verify_media.py').write_bytes(script);(w/'request.json').write_text(json.dumps(v['request'])+'\n')
assert Path('api/db.py').is_file()
env=dict(os.environ);env['PYTHONPATH']=os.getcwd()+os.pathsep+env.get('PYTHONPATH','')
with (w/'controller.log').open('xb') as log:p=subprocess.Popen([sys.executable,'-u',str(w/'verify_media.py')],stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
print(json.dumps({'started':True,'mode':mode,'pid':p.pid,'private_directory':str(w),'script_sha256':v['request']['runner_sha256'],'uri_changes':0,'cache_files_deleted':0}))
'''
    result=remote_json(SSH,'webserver-labo',code,{'script':base64.b64encode(script).decode(),'request':request},container=True)
    with (HERE/(mode+'-launch.json')).open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result))


def status(mode):
    assert mode in {'pairs','handlers','post-retirement'}
    code=r'''
import json,sys,hashlib
from pathlib import Path
mode=json.load(sys.stdin)['mode'];assert mode in {'pairs','handlers','post-retirement'}
w=Path('/tmp/detecdiv-api-cache-retirement-20261105')/mode;out={}
p=w/'state.json'
if p.exists():out['state']=json.loads(p.read_text())
p=w/'controller.log'
if p.exists():
 with p.open('rb') as f:f.seek(max(0,p.stat().st_size-2200));out['log_tail']=f.read().decode(errors='replace')
if out.get('state',{}).get('success'):
 data=(w/'receipt.jsonl').read_bytes();assert hashlib.sha256(data).hexdigest()==out['state']['receipt_sha256']
 rows=[json.loads(x) for x in data.splitlines()]
 assert len(rows)==1926 and sum(x['bytes'] for x in rows)==28981723684
 assert len({(x['year'],x['relative_path']) for x in rows})==1926
 out.update(receipt_verified=True,receipt=rows)
print(json.dumps(out))
'''
    result=remote_json(SSH,'webserver-labo',code,{'mode':mode},container=True)
    (RUNTIME/(mode+'-status.json')).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    if result.get('receipt_verified'):
        summary={k:v for k,v in result.items() if k!='receipt'}
        target=HERE/(mode+'-completion-verification.json')
        if not target.exists():target.write_text(json.dumps(summary,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k!='receipt'}))


if __name__=='__main__':
    assert len(sys.argv)==3
    {'launch':launch,'status':status}[sys.argv[1]](sys.argv[2])
