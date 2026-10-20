$principal=(Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').Principal
$action=New-ScheduledTaskAction -Execute 'powershell.exe' -Argument '-NoProfile -ExecutionPolicy Bypass -File C:\ProgramData\DetecDivHub\gpu-validation-20261016\validate-session.ps1'
$settings=New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 15)
Register-ScheduledTask -TaskName 'DetecDiv GPU Validation 20261016' -Action $action -Principal $principal -Settings $settings -Force | Out-Null
Start-ScheduledTask -TaskName 'DetecDiv GPU Validation 20261016'
Get-ScheduledTask -TaskName 'DetecDiv GPU Validation 20261016' | Select-Object TaskName,State | ConvertTo-Json -Compress
