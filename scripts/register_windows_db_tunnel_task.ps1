param(
    [Parameter(Mandatory = $true)][string]$SshHost,
    [Parameter(Mandatory = $true)][string]$SshUser,
    [Parameter(Mandatory = $true)][string]$DatabaseHost,
    [int]$DatabasePort = 5432,
    [int]$LocalPort = 15432,
    [Parameter(Mandatory = $true)][string]$IdentityFile,
    [Parameter(Mandatory = $true)][string]$KnownHostsFile,
    [string]$TunnelScript = (Join-Path $PSScriptRoot 'run_windows_db_tunnel.ps1'),
    [string]$TaskName = 'DetecDiv Hub Database Tunnel'
)

$ErrorActionPreference = 'Stop'
if (-not (Test-Path -LiteralPath $TunnelScript -PathType Leaf)) {
    throw "Missing tunnel script: $TunnelScript"
}

$arguments = @(
    '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
    '-File', "`"$TunnelScript`"",
    '-SshHost', "`"$SshHost`"",
    '-SshUser', "`"$SshUser`"",
    '-DatabaseHost', "`"$DatabaseHost`"",
    '-DatabasePort', $DatabasePort,
    '-LocalPort', $LocalPort,
    '-IdentityFile', "`"$IdentityFile`"",
    '-KnownHostsFile', "`"$KnownHostsFile`""
) -join ' '

$action = New-ScheduledTaskAction `
    -Execute (Join-Path $env:WINDIR 'System32\WindowsPowerShell\v1.0\powershell.exe') `
    -Argument $arguments
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0)
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -User 'SYSTEM' `
    -RunLevel Highest `
    -Force | Out-Null
Write-Output "Registered $TaskName. Start it with Start-ScheduledTask after any manual tunnel is stopped."
