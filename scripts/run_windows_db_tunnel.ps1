param(
    [Parameter(Mandatory = $true)][string]$SshHost,
    [Parameter(Mandatory = $true)][string]$SshUser,
    [Parameter(Mandatory = $true)][string]$DatabaseHost,
    [int]$DatabasePort = 5432,
    [int]$LocalPort = 15432,
    [Parameter(Mandatory = $true)][string]$IdentityFile,
    [Parameter(Mandatory = $true)][string]$KnownHostsFile,
    [int]$RetrySeconds = 15
)

$ErrorActionPreference = 'Stop'
$ssh = Join-Path $env:WINDIR 'System32\OpenSSH\ssh.exe'
$logFile = Join-Path $PSScriptRoot 'db-tunnel.log'
Start-Transcript -Path (Join-Path $PSScriptRoot 'db-tunnel-transcript.log') -Append | Out-Null
foreach ($file in @($ssh, $IdentityFile, $KnownHostsFile)) {
    if (-not (Test-Path -LiteralPath $file -PathType Leaf)) {
        throw "Missing SSH file: $file"
    }
}

while ($true) {
    Add-Content -LiteralPath $logFile -Value "$(Get-Date -Format o) Starting database tunnel."
    # Windows PowerShell 5.1 treats native stderr as an exception when this is Stop.
    $ErrorActionPreference = 'Continue'
    & $ssh -N -T `
        -i $IdentityFile `
        -o IdentitiesOnly=yes `
        -o BatchMode=yes `
        -o StrictHostKeyChecking=yes `
        -o "UserKnownHostsFile=$KnownHostsFile" `
        -o ExitOnForwardFailure=yes `
        -o ServerAliveInterval=30 `
        -o ServerAliveCountMax=3 `
        -L "127.0.0.1:${LocalPort}:${DatabaseHost}:${DatabasePort}" `
        "${SshUser}@${SshHost}" 2>> $logFile
    $ErrorActionPreference = 'Stop'
    Add-Content -LiteralPath $logFile -Value "$(Get-Date -Format o) SSH exited with code $LASTEXITCODE; retrying in $RetrySeconds seconds."
    Start-Sleep -Seconds $RetrySeconds
}
