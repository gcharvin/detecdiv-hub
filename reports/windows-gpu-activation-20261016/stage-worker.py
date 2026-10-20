import base64
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'scripts/ops'))
import windows_gpu_activation as op
source=(op.ROOT/'worker/job_resources.py').read_text()
function=source[source.index('def resource_config_for_target('):source.index('\n\ndef resolve_job_resource_allocation(')]
code=r'''
import hashlib,json,shutil
from pathlib import Path
root=Path('C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub')
stage=root/'.deploy-staging/windows-gpu-20261016'
stage.mkdir(parents=True,exist_ok=True)
function=FUNCTION
manifest={}
for relative in ['worker/job_resources.py','worker/run_worker.py']:
 p=root/relative
 before=p.read_bytes()
 value=before.decode().replace('\r\n','\n')
 if relative.endswith('job_resources.py'):
  assert 'def resource_config_for_target(' not in value
  value=value.replace('import os\n','import os\nfrom dataclasses import replace\n',1)
  marker='\n\ndef resolve_job_resource_allocation('
  assert value.count(marker)==1
  value=value.replace(marker,'\n\n'+function+marker,1)
 else:
  assert value.count('    resolve_job_resource_allocation,')==1
  value=value.replace('    resolve_job_resource_allocation,','    resolve_job_resource_allocation,\n    resource_config_for_target,',1)
  old='        resource_config = resolve_job_resource_runtime_config(session)'
  assert value.count(old)==1
  value=value.replace(old,'        resource_config = resource_config_for_target(resolve_job_resource_runtime_config(session), target)',1)
 compile(value,relative,'exec')
 dest=stage/Path(relative).name
 dest.write_text(value,encoding='utf-8',newline='\n')
 manifest[relative]={'before':hashlib.sha256(before).hexdigest(),'after':hashlib.sha256(dest.read_bytes()).hexdigest()}
(stage/'manifest.json').write_text(json.dumps(manifest,indent=2))
print(json.dumps(manifest))
'''.replace('FUNCTION',repr(function))
encoded=base64.b64encode(code.encode()).decode()
result=op.win(f"Set-Location 'C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub'; & ./.venv/Scripts/python.exe -c \"import base64;exec(base64.b64decode('{encoded}'))\"; if ($LASTEXITCODE) {{throw 'Staging failed'}}")
(op.OUT/'worker-staging.json').write_text(result+'\n')
print(result)
