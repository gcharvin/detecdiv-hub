import base64
import json
from pathlib import Path
import subprocess
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'scripts/ops'))
import windows_gpu_activation as op
repo=op.ROOT.parent/'hoct_model'
commit=op.command(['git','-C',str(repo),'rev-parse','HEAD'])
source=base64.b64encode((repo/'plugins/classifier/+phaseCellposeSAM/train.m').read_bytes()).decode()
code=r'''
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,json,shutil
root=Path('/data/Gilles/analysis/classifiers/phase_cellposesam_v001_1')
assert not (root/'fit_v001/training_started.json').exists(),'Training already started'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
commit=COMMIT
old=root/'code/classifier/+phaseCellposeSAM'
new=root/'code/releases'/commit/'classifier/+phaseCellposeSAM'
assert not new.exists(),'Release already exists; inspect before retry'
shutil.copytree(old,new,ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
(new/'train.m').write_bytes(base64.b64decode(SOURCE))
protocol=json.loads((root/'protocol_frozen.json').read_text())
assert sha(new/'py/pilot.py')==protocol['pilot_sha256']
original=root/'pipeline_templates/train_phase_cellposesam_v001/pipeline.json'
pipeline=json.loads(original.read_text())
node=pipeline['nodes']
node['customPackageRoot']=str(new.parent)
node['customPackageDir']=str(new)
pipeline['version']='1.1-windows-gpu-runtime'
dest=original.parent/'windows_gpu_v1/pipeline.json'
dest.parent.mkdir(exist_ok=False)
dest.write_text(json.dumps(pipeline,indent=2)+'\n')
amendment={'schema_version':1,'at':datetime.now(timezone.utc).isoformat(),'authorization':'Gilles: make Windows GPU operational, validate in real worker session, move existing job without duplicate',
 'source_repository':'hoct_model','launcher_commit':commit,'original_protocol_sha256':sha(root/'protocol_frozen.json'),
 'original_launcher_sha256':sha(old/'train.m'),'launcher_sha256':sha(new/'train.m'),'pilot_sha256':sha(new/'py/pilot.py'),
 'package_root':str(new.parent),'pipeline_path':str(dest),'pipeline_sha256':sha(dest),
 'runtime':'C:/ProgramData/DetecDivHub/cellpose-gpu-v1/Scripts/python.exe',
 'scientific_parameters_changed':False,'frozen_protocol_preserved':True,
 'note':'Only the MATLAB process launcher changes. Original source and protocol remain intact; this amendment records the executed launcher.'}
(root/'runtime_amendment_windows_gpu_v1.json').write_text(json.dumps(amendment,indent=2)+'\n')
print(json.dumps(amendment))
'''.replace('COMMIT',repr(commit)).replace('SOURCE',repr(source))
result=op.linux(code)
(op.OUT/'launcher-publication.json').write_text(result+'\n')
print(result)
