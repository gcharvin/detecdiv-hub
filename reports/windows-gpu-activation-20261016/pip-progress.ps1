Get-ScheduledTaskInfo -TaskName 'DetecDiv GPU Runtime Install 20261016' | Select-Object LastRunTime,LastTaskResult | ConvertTo-Json -Compress
Get-Content 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/pip-torch.log' -Tail 8 -ErrorAction SilentlyContinue
Get-Content 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/pip-cellpose.log' -Tail 5 -ErrorAction SilentlyContinue
if (Test-Path 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/install-exit.txt') {Get-Content 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/install-exit.txt'}
