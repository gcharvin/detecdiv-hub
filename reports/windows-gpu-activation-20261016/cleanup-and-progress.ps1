Get-Content 'C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub/logs/windows-worker.log' -Tail 15
nvidia-smi --query-gpu=memory.used,utilization.gpu --format=csv,noheader,nounits
Get-CimInstance Win32_Process | Where-Object {$_.Name -eq 'python.exe' -and $_.CommandLine -match 'pilot.py|worker.run_worker'} | Select-Object ProcessId,ParentProcessId,SessionId,CommandLine | ConvertTo-Json -Compress
foreach ($name in @('DetecDiv GPU Runtime Install 20261016','DetecDiv GPU Validation 20261016','DetecDiv GPU Final Preflight 20261016')) {
 $t=Get-ScheduledTask -TaskName $name
 if ($t.State -eq 'Running') {throw "Temporary task still running: $name"}
 Unregister-ScheduledTask -TaskName $name -Confirm:$false
}
