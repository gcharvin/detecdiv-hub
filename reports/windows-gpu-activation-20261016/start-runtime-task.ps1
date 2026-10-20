$stage='C:\ProgramData\DetecDivHub\gpu-validation-20261016'
$runtime='C:\ProgramData\DetecDivHub\cellpose-gpu-v1'
New-Item -ItemType Directory -Force $runtime | Out-Null
$principal=(Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').Principal
$identity='GMGM\Charvin-Admin'
& icacls.exe $stage /grant "${identity}:(OI)(CI)M" | Out-Null
if ($LASTEXITCODE) {throw 'stage ACL failed'}
& icacls.exe $runtime /grant "${identity}:(OI)(CI)M" | Out-Null
if ($LASTEXITCODE) {throw 'runtime ACL failed'}
$action=New-ScheduledTaskAction -Execute 'powershell.exe' -Argument '-NoProfile -ExecutionPolicy Bypass -File C:\ProgramData\DetecDivHub\gpu-validation-20261016\install-runtime.ps1'
$settings=New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 30)
Register-ScheduledTask -TaskName 'DetecDiv GPU Runtime Install 20261016' -Action $action -Principal $principal -Settings $settings -Force | Out-Null
Start-ScheduledTask -TaskName 'DetecDiv GPU Runtime Install 20261016'
Get-ScheduledTask -TaskName 'DetecDiv GPU Runtime Install 20261016' | Select-Object TaskName,State | ConvertTo-Json -Compress
