from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'scripts/ops'))
import windows_gpu_activation as op
op.upload(op.OUT/'patch-worker-paths.py','C:/ProgramData/DetecDivHub/gpu-validation-20261016/patch-worker-paths.py')
ps=(op.OUT/'activate-worker.ps1').read_text()
start=ps.index('    $envFile=')
end=ps.index('    $env:PYTHONPATH=')
ps=ps[:start]+ps[end:]
ps=ps.replace('/apply-worker.py','/patch-worker-paths.py')
result=op.win(ps)
(op.OUT/'worker-paths-activation.json').write_text(result+'\n')
print(result)
