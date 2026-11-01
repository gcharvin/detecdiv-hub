param(
    [string]$NasHost = 'srv-data-pb-lan1.u-strasbg.fr',
    [string]$ExpectedAddress = '10.20.8.250',
    [string]$DsmUser = 'maliavko'
)

$ErrorActionPreference = 'Stop'
$identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$session = (Get-Process -Id $PID).SessionId
if ($env:COMPUTERNAME -ne 'CG-PCDELL01-306' -or $identity -ne 'GMGM\Charvin-Admin') {
    throw 'Run this helper on the Windows worker, signed in as GMGM\Charvin-Admin.'
}
$admin = New-Object Security.Principal.WindowsPrincipal ([Security.Principal.WindowsIdentity]::GetCurrent())
if ($admin.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Open a normal PowerShell window, without Run as administrator, to use the worker logon token.'
}
$pollers = @(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -match '^python' -and $_.CommandLine -match 'worker\.run_worker' -and $_.SessionId -eq $session
})
if ($pollers.Count -eq 0) { throw 'No running worker was found in this Windows session.' }
$addresses = @(Resolve-DnsName -Name $NasHost -Type A -DnsOnly | Where-Object { $_.Type -eq 'A' } | Select-Object -ExpandProperty IPAddress -Unique)
if ($addresses.Count -ne 1 -or $addresses[0] -ne $ExpectedAddress) {
    throw 'NAS DNS does not resolve exclusively to the expected secondary NAS. Connection cancelled.'
}

$share = '\\' + $NasHost + '\home'
$root = Join-Path $share 'DetecdivHub'
Write-Host "Connecting $share as NAS user $DsmUser."
Write-Host 'Enter the NAS password at the masked prompt below.'
Write-Host 'Windows will save the credentials for this NAS DNS name in your Credential Manager.'
Write-Host 'Existing connections using the NAS IP address are preserved.'
& net.exe use $share '*' "/USER:$DsmUser" '/SAVECRED' '/PERSISTENT:NO'
if ($LASTEXITCODE -ne 0) {
    throw 'NAS authentication failed. No worker configuration was changed. If Windows reports 1219, keep the existing connections and report that code.'
}
if (-not (Test-Path -LiteralPath $root -PathType Container)) {
    throw 'The expected DetecdivHub folder was not found in this user home. No probe or worker configuration was written.'
}
$staging = Join-Path $root 'raw\.incoming-alexander-2023-20261008'
foreach ($year in @('2023_1', '2023_2')) {
    if (-not (Test-Path -LiteralPath (Join-Path $staging $year) -PathType Container)) {
        throw "Expected staging folder is unavailable: $year. No worker configuration was changed."
    }
}

$probeName = '.detecdiv-windows-home-probe-' + [guid]::NewGuid().ToString('N') + '.tmp'
$probe = Join-Path $root $probeName
$payload = [Text.Encoding]::UTF8.GetBytes('DetecDiv Windows home access probe')
$stream = [IO.File]::Open($probe, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
try { $stream.Write($payload, 0, $payload.Length) } finally { $stream.Dispose() }
$readback = [IO.File]::ReadAllBytes($probe)
if ([Convert]::ToBase64String($readback) -ne [Convert]::ToBase64String($payload)) {
    throw 'The temporary probe could not be read back correctly. Preserve it for inspection.'
}
$receipt = [ordered]@{
    utc = (Get-Date).ToUniversalTime().ToString('o')
    windows_identity = $identity
    session_id = $session
    nas_host = $NasHost
    nas_address = $ExpectedAddress
    requested_smb_user = $DsmUser
    share = $share
    root = $root
    staging_directories_readable = @('2023_1', '2023_2')
    write_read_probe_passed = $true
    probe_name = $probeName
    probe_bytes = $payload.Length
    nas_owner_verified = $false
    worker_configuration_changed = $false
}
$logRoot = 'C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub\logs'
$receiptPath = Join-Path $logRoot ('homes2-user-connection-' + [guid]::NewGuid().ToString('N') + '.json')
$receipt | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $receiptPath -Encoding UTF8
Write-Host 'Connection, staging access and temporary-file read/write succeeded.'
Write-Host "Receipt: $receiptPath"
Write-Host "Temporary probe retained for NAS-side ownership verification: $probeName"
Write-Host 'Worker configuration and catalog locations are unchanged.'
