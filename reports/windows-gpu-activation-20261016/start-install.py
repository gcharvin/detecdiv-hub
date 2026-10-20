import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'scripts/ops'))
import windows_gpu_activation as op
print(op.win("New-Item -ItemType Directory -Force 'C:/ProgramData/DetecDivHub/gpu-validation-20261016' | Out-Null"))
remote='C:/ProgramData/DetecDivHub/gpu-validation-20261016/install-runtime.ps1'
print(op.upload(Path(__file__).with_name('install-runtime.ps1'),remote))
print(op.win("$p=Start-Process powershell.exe -WindowStyle Hidden -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File','C:/ProgramData/DetecDivHub/gpu-validation-20261016/install-runtime.ps1' -PassThru; @{install_pid=$p.Id}|ConvertTo-Json -Compress"))
