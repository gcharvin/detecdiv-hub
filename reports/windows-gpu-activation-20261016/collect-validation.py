import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'scripts/ops'))
import windows_gpu_activation as op
for name in ['session.json','gpu-smoke-result.json','worker-preflight-result.json','requirements.lock.txt','pip-check.log','matlab-validation.log','validation-exit.txt']:
    value=op.win(f"Get-Content -LiteralPath 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/{name}' -Raw")
    (op.OUT/name).write_text(value+'\n',encoding='utf-8')
assert json.loads((op.OUT/'gpu-smoke-result.json').read_text())['status']=='passed'
assert json.loads((op.OUT/'worker-preflight-result.json').read_text())['status']=='passed'
assert (op.OUT/'validation-exit.txt').read_text().strip()=='0'
print(json.dumps({'validation':'passed','session':json.loads((op.OUT/'session.json').read_text())}))
