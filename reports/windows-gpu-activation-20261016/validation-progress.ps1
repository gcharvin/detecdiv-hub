Get-ScheduledTaskInfo -TaskName 'DetecDiv GPU Validation 20261016' | Select-Object LastRunTime,LastTaskResult | ConvertTo-Json -Compress
foreach ($name in @('session.json','pip-check.log','matlab-validation.log','gpu-smoke.log','gpu-smoke-result.json','session-error.txt','validation-exit.txt')) {
 $p=Join-Path 'C:/ProgramData/DetecDivHub/gpu-validation-20261016' $name
 if (Test-Path $p) {Write-Output $name; Get-Content $p -Tail 14}
}
