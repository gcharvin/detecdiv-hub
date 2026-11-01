param(
    [ValidateSet('Submit', 'Apply', 'SelfTest')][string]$Mode = 'Submit',
    [string]$RequestId,
    [switch]$TestOnly
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$nas = 'srv-data-pb-lan1.u-strasbg.fr'
$expectedAddress = '10.20.8.250'
$dsmUser = 'maliavko'
$workerUser = 'GMGM\Charvin-Admin'
$privateDir = 'C:\ProgramData\DetecDivHub\homes2-connection-private'
$share = '\\' + $nas + '\home'
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
if ($env:COMPUTERNAME -ne 'CG-PCDELL01-306') { throw 'This helper is for the Windows worker only.' }
Add-Type -AssemblyName System.Security

function Assert-NasDns {
    $addresses = @(Resolve-DnsName -Name $nas -Type A -DnsOnly | Where-Object { $_.Type -eq 'A' } | Select-Object -ExpandProperty IPAddress -Unique)
    if ($addresses.Count -ne 1 -or $addresses[0] -ne $expectedAddress) {
        throw 'NAS DNS does not resolve exclusively to the expected secondary NAS.'
    }
}

if ($Mode -eq 'Apply') {
    if ($RequestId -notmatch '^[a-f0-9]{32}$') { throw 'Invalid request identifier.' }
    $encryptedPath = Join-Path $privateDir ($RequestId + '.dpapi')
    $receiptPath = Join-Path $privateDir ($RequestId + '.json')
    $secretBytes = $null
    $passwordPointer = [IntPtr]::Zero
    $probe = $null
    $result = [ordered]@{
        request_id = $RequestId
        utc = (Get-Date).ToUniversalTime().ToString('o')
        windows_identity = $identity.Name
        session_id = (Get-Process -Id $PID).SessionId
        self_test = [bool]$TestOnly
        success = $false
        worker_configuration_changed = $false
    }
    try {
        if ($identity.Name -ne $workerUser) { throw 'Apply must run under the worker Windows account.' }
        $principal = New-Object Security.Principal.WindowsPrincipal $identity
        if ($principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
            throw 'Apply must run with the worker limited token.'
        }
        $pollers = @(Get-CimInstance Win32_Process | Where-Object {
            $_.Name -match '^python' -and $_.CommandLine -match 'worker\.run_worker' -and $_.SessionId -eq $result.session_id
        })
        if ($pollers.Count -eq 0) { throw 'No running worker was found in this Windows session.' }
        $secretBytes = [Security.Cryptography.ProtectedData]::Unprotect(
            [IO.File]::ReadAllBytes($encryptedPath), $null, [Security.Cryptography.DataProtectionScope]::LocalMachine)
        Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class Homes2Credentials {
    [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)]
    public struct NETRESOURCE {
        public int Scope, Type, DisplayType, Usage;
        public string LocalName, RemoteName, Comment, Provider;
    }
    [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)]
    public struct CREDENTIAL {
        public uint Flags, Type;
        public string TargetName, Comment;
        public System.Runtime.InteropServices.ComTypes.FILETIME LastWritten;
        public uint CredentialBlobSize;
        public IntPtr CredentialBlob;
        public uint Persist, AttributeCount;
        public IntPtr Attributes;
        public string TargetAlias, UserName;
    }
    [DllImport("mpr.dll", EntryPoint="WNetAddConnection2W", CharSet=CharSet.Unicode)]
    public static extern int Connect(ref NETRESOURCE resource, IntPtr password, string user, int flags);
    [DllImport("advapi32.dll", EntryPoint="CredWriteW", CharSet=CharSet.Unicode, SetLastError=true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool Save(ref CREDENTIAL credential, uint flags);
}
'@
        if ($TestOnly) {
            $expected = [Text.Encoding]::Unicode.GetBytes('detecdiv-homes2-cross-session-selftest-v1')
            if ([Convert]::ToBase64String($secretBytes) -ne [Convert]::ToBase64String($expected)) {
                throw 'Cross-session protected-data self-test failed.'
            }
            $result.cross_session_decryption_passed = $true
            $result.native_credential_api_compiled = $true
            $result.nas_contacted = $false
        } else {
            Assert-NasDns
            $passwordPointer = [Runtime.InteropServices.Marshal]::AllocHGlobal($secretBytes.Length + 2)
            [Runtime.InteropServices.Marshal]::Copy($secretBytes, 0, $passwordPointer, $secretBytes.Length)
            [Runtime.InteropServices.Marshal]::WriteInt16($passwordPointer, $secretBytes.Length, 0)
            $resource = New-Object Homes2Credentials+NETRESOURCE
            $resource.Type = 1
            $resource.RemoteName = $share
            $code = [Homes2Credentials]::Connect([ref]$resource, $passwordPointer, $dsmUser, 0)
            if ($code -ne 0) { throw "NAS connection failed with Windows error $code. Existing sessions are preserved." }
            $root = Join-Path $share 'DetecdivHub'
            if (-not (Test-Path -LiteralPath $root -PathType Container)) { throw 'Expected DetecdivHub folder is unavailable.' }
            $staging = Join-Path $root 'raw\.incoming-alexander-2023-20261008'
            foreach ($year in @('2023_1', '2023_2')) {
                if (-not (Test-Path -LiteralPath (Join-Path $staging $year) -PathType Container)) {
                    throw "Expected staging folder is unavailable: $year."
                }
            }
            $probeName = '.detecdiv-windows-home-probe-' + $RequestId + '.tmp'
            $probe = Join-Path $root $probeName
            $payload = [Text.Encoding]::UTF8.GetBytes('DetecDiv Windows home access probe')
            $stream = [IO.File]::Open($probe, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
            try { $stream.Write($payload, 0, $payload.Length) } finally { $stream.Dispose() }
            if ([Convert]::ToBase64String([IO.File]::ReadAllBytes($probe)) -ne [Convert]::ToBase64String($payload)) {
                throw 'Temporary probe readback failed.'
            }
            $credential = New-Object Homes2Credentials+CREDENTIAL
            $credential.Type = 2
            $credential.TargetName = $nas
            $credential.UserName = $dsmUser
            $credential.Comment = 'DetecDiv Alexander home access'
            $credential.CredentialBlob = $passwordPointer
            $credential.CredentialBlobSize = $secretBytes.Length
            $credential.Persist = 2
            if (-not [Homes2Credentials]::Save([ref]$credential, 0)) {
                $code = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
                throw "Saving the credential in the worker account failed with Windows error $code."
            }
            $result.nas_host = $nas
            $result.nas_address = $expectedAddress
            $result.requested_smb_user = $dsmUser
            $result.root = $root
            $result.staging_directories_readable = @('2023_1', '2023_2')
            $result.write_read_probe_passed = $true
            $result.credentials_saved_for_worker = $true
            $result.probe_name = $probeName
            $result.probe_bytes = $payload.Length
            $result.nas_owner_verified = $false
        }
        $result.success = $true
    } catch {
        $result.error = $_.Exception.Message
    } finally {
        if ($passwordPointer -ne [IntPtr]::Zero) {
            for ($offset = 0; $offset -lt ($secretBytes.Length + 2); $offset++) {
                [Runtime.InteropServices.Marshal]::WriteByte($passwordPointer, $offset, 0)
            }
            [Runtime.InteropServices.Marshal]::FreeHGlobal($passwordPointer)
        }
        if ($null -ne $secretBytes) { [Array]::Clear($secretBytes, 0, $secretBytes.Length) }
        if (Test-Path -LiteralPath $encryptedPath) { Remove-Item -LiteralPath $encryptedPath -Force }
        if (-not $result.success -and $probe -and (Test-Path -LiteralPath $probe)) { Remove-Item -LiteralPath $probe -Force }
        $result | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $receiptPath -Encoding UTF8
    }
    if (-not $result.success) { exit 1 }
    exit 0
}

$principal = New-Object Security.Principal.WindowsPrincipal $identity
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Submit requires the elevated detecdiv-ops SSH account.'
}
$worker = Get-ScheduledTask -TaskName 'DetecDiv Hub Worker'
if ($worker.State -ne 'Running') { throw 'The worker task is not running in its existing user session.' }
Assert-NasDns
if (Test-Path -LiteralPath $privateDir) {
    $item = Get-Item -LiteralPath $privateDir -Force
    if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'Refusing a reparse point for the protected directory.' }
} else {
    New-Item -ItemType Directory -Path $privateDir | Out-Null
}
$acl = New-Object Security.AccessControl.DirectorySecurity
$acl.SetAccessRuleProtection($true, $false)
$adminSid = New-Object Security.Principal.SecurityIdentifier 'S-1-5-32-544'
$systemSid = New-Object Security.Principal.SecurityIdentifier 'S-1-5-18'
$workerSid = (New-Object Security.Principal.NTAccount $workerUser).Translate([Security.Principal.SecurityIdentifier])
$acl.SetOwner($adminSid)
foreach ($entry in @(@($adminSid, 'FullControl'), @($systemSid, 'FullControl'), @($workerSid, 'Modify'))) {
    $rule = New-Object Security.AccessControl.FileSystemAccessRule($entry[0], $entry[1], 'ContainerInherit,ObjectInherit', 'None', 'Allow')
    $acl.AddAccessRule($rule)
}
Set-Acl -LiteralPath $privateDir -AclObject $acl
$RequestId = [guid]::NewGuid().ToString('N')
$encryptedPath = Join-Path $privateDir ($RequestId + '.dpapi')
$receiptPath = Join-Path $privateDir ($RequestId + '.json')
$taskName = 'DetecDiv homes2 SSH connect ' + $RequestId
$secretBytes = $null
$inputPointer = [IntPtr]::Zero
$inputSecret = $null
$registered = $false
try {
    if ($Mode -eq 'SelfTest') {
        $secretBytes = [Text.Encoding]::Unicode.GetBytes('detecdiv-homes2-cross-session-selftest-v1')
    } else {
        Write-Host "NAS: $nas ($expectedAddress); NAS account: $dsmUser."
        Write-Host "The credential will be saved in the Windows Credential Manager of $workerUser."
        $inputSecret = Read-Host 'NAS password (masked)' -AsSecureString
        if ($inputSecret.Length -eq 0) { throw 'Empty password; cancelled.' }
        $inputPointer = [Runtime.InteropServices.Marshal]::SecureStringToGlobalAllocUnicode($inputSecret)
        $secretBytes = New-Object byte[] ($inputSecret.Length * 2)
        [Runtime.InteropServices.Marshal]::Copy($inputPointer, $secretBytes, 0, $secretBytes.Length)
    }
    $encrypted = [Security.Cryptography.ProtectedData]::Protect($secretBytes, $null, [Security.Cryptography.DataProtectionScope]::LocalMachine)
    [IO.File]::WriteAllBytes($encryptedPath, $encrypted)
    [Array]::Clear($secretBytes, 0, $secretBytes.Length)
    if ($inputPointer -ne [IntPtr]::Zero) { [Runtime.InteropServices.Marshal]::ZeroFreeGlobalAllocUnicode($inputPointer); $inputPointer = [IntPtr]::Zero }
    if ($inputSecret) { $inputSecret.Dispose(); $inputSecret = $null }
    $arguments = '-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "' + $PSCommandPath + '" -Mode Apply -RequestId ' + $RequestId
    if ($Mode -eq 'SelfTest') { $arguments += ' -TestOnly' }
    $action = New-ScheduledTaskAction -Execute 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -Argument $arguments
    $taskPrincipal = New-ScheduledTaskPrincipal -UserId $workerUser -LogonType Interactive -RunLevel Limited
    $settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 2) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    Register-ScheduledTask -TaskName $taskName -Action $action -Principal $taskPrincipal -Settings $settings | Out-Null
    $registered = $true
    Start-ScheduledTask -TaskName $taskName
    $deadline = (Get-Date).AddSeconds(45)
    do {
        Start-Sleep -Milliseconds 500
        $taskState = [string](Get-ScheduledTask -TaskName $taskName).State
    } while ((-not (Test-Path -LiteralPath $receiptPath) -or $taskState -eq 'Running') -and (Get-Date) -lt $deadline)
    if (-not (Test-Path -LiteralPath $receiptPath)) {
        throw "No worker-session receipt yet. Diagnostic request: $RequestId."
    }
    $receipt = Get-Content -Raw -LiteralPath $receiptPath | ConvertFrom-Json
    $receipt | ConvertTo-Json -Depth 5
    if (-not $receipt.success) { throw 'Connection check failed; worker configuration and catalog are unchanged.' }
    if ($Mode -ne 'SelfTest') { Write-Host 'Worker-session connection succeeded. The temporary probe is retained for NAS ownership verification.' }
} finally {
    if ($inputPointer -ne [IntPtr]::Zero) { [Runtime.InteropServices.Marshal]::ZeroFreeGlobalAllocUnicode($inputPointer) }
    if ($inputSecret) { $inputSecret.Dispose() }
    if ($null -ne $secretBytes) { [Array]::Clear($secretBytes, 0, $secretBytes.Length) }
    if (-not $registered -and (Test-Path -LiteralPath $encryptedPath)) { Remove-Item -LiteralPath $encryptedPath -Force }
    if ($registered -and (Get-ScheduledTask -TaskName $taskName).State -ne 'Running') {
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
        if (Test-Path -LiteralPath $encryptedPath) { Remove-Item -LiteralPath $encryptedPath -Force }
    }
}
