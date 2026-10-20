Get-CimInstance Win32_Process | Where-Object {$_.Name -match '^MATLAB.exe$|^python.exe$'} | Select-Object ProcessId,ParentProcessId,SessionId,CommandLine | ConvertTo-Json -Compress
nvidia-smi --query-gpu=memory.used,utilization.gpu --format=csv,noheader,nounits
Get-ChildItem 'C:/ProgramData/DetecDivHub/gpu-validation-20261016' | Where-Object {$_.Name -match 'smoke|matlab|session'} | Select-Object Name,Length,LastWriteTime | ConvertTo-Json -Compress
