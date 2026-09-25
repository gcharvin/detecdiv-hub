param(
    [Parameter(Mandatory = $true)][string]$EnvFile,
    [string]$TaskName = "DetecDiv Hub Worker"
)

$ErrorActionPreference = "Stop"
$workerScript = (Resolve-Path (Join-Path $PSScriptRoot "run_worker.ps1")).Path
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$resolvedEnvFile = (Resolve-Path -LiteralPath $EnvFile).Path
$logFile = Join-Path $repoRoot "logs\windows-worker.log"
$userId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$powerShell = (Get-Process -Id $PID).Path

$argument = "-NoProfile -ExecutionPolicy Bypass -File `"$workerScript`" -EnvFile `"$resolvedEnvFile`" -LogFile `"$logFile`""
$action = New-ScheduledTaskAction -Execute $powerShell -Argument $argument
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $userId
$principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Write-Host "Registered '$TaskName' for $userId. It starts after this user signs in."
Write-Host "Start it now with: Start-ScheduledTask -TaskName '$TaskName'"
