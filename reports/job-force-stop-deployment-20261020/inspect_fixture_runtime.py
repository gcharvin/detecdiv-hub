import json
import ops
context=json.loads((ops.OUT/'smoke-context.json').read_text())
result=ops.remote('linux',f'''
import json,os,subprocess
from pathlib import Path
pid=subprocess.check_output(['systemctl','show','detecdiv-worker-manager.service','-p','MainPID','--value'],text=True).strip()
inherited=dict(i.split('=',1) for i in Path('/proc',pid,'environ').read_text().split(chr(0)) if '=' in i)
os.environ.update({{k:v for k,v in inherited.items() if k.startswith('DETECDIV_HUB_')}})
from api.config import get_settings
from worker.matlab_code_checkout import cache_root
s=get_settings();repo,cache=cache_root(s.matlab_repo_root,s.matlab_job_checkout_root)
release=cache/'releases'/{context['release']!r}
print(json.dumps({{'release':str(release),'exists':release.is_dir(),'custom_module':list(map(str,release.glob('**/+customModule/setparam.m'))),'matlab_command':s.matlab_command}}))
''')
ops.save('fixture-runtime.json',result)
print(result)
