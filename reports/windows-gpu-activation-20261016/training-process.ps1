nvidia-smi --query-gpu=name,memory.used,utilization.gpu --format=csv,noheader,nounits
Get-CimInstance Win32_Process | Where-Object {$_.Name -eq 'python.exe' -and $_.CommandLine -match 'pilot.py|worker.run_worker'} | Select-Object ProcessId,ParentProcessId,SessionId,ExecutablePath,CommandLine | ConvertTo-Json -Compress
