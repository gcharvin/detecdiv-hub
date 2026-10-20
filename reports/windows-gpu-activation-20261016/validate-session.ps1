$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$stage='C:\ProgramData\DetecDivHub\gpu-validation-20261016'
$repo='C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub'
try {
    Set-Location $repo
    foreach ($line in Get-Content -LiteralPath "$repo\.env") {
        if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') {
            $key=$Matches[1]; $value=$Matches[2].Trim().Trim('"').Trim("'")
            [Environment]::SetEnvironmentVariable($key,$value,'Process')
        }
    }
    $env:DETECDIV_PHASE_CELLPOSE_PYTHON='C:\ProgramData\DetecDivHub\cellpose-gpu-v1\Scripts\python.exe'
    $process=Get-Process -Id $PID
    $identity=[Security.Principal.WindowsIdentity]::GetCurrent().Name
    $workers=@(Get-CimInstance Win32_Process | Where-Object {$_.Name -eq 'python.exe' -and $_.CommandLine -match '-m worker.run_worker'})
    if (!$workers.Count) {throw 'No poller to compare session'}
    if (@($workers | Where-Object SessionId -ne $process.SessionId).Count) {throw 'Validation session differs from worker session'}
    @{account=$identity;session_id=$process.SessionId;worker_processes=@($workers | Select-Object ProcessId,SessionId);started_at=(Get-Date).ToString('o')} | ConvertTo-Json -Depth 4 | Set-Content "$stage\session.json"
    $ErrorActionPreference='Continue'
    & $env:DETECDIV_PHASE_CELLPOSE_PYTHON -m pip check *> "$stage\pip-check.log"
    $check=$LASTEXITCODE
    & $env:DETECDIV_PHASE_CELLPOSE_PYTHON -m pip freeze > "$stage\requirements.lock.txt"
    if ($check) {throw 'Python dependency consistency failed'}
    & 'C:\Program Files\MATLAB\R2025b\bin\matlab.exe' -batch "run('C:/ProgramData/DetecDivHub/gpu-validation-20261016/validate_worker.m')" *> "$stage\matlab-validation.log"
    $exit=$LASTEXITCODE
    $exit | Set-Content "$stage\validation-exit.txt"
    if ($exit) {throw 'MATLAB GPU validation failed'}
} catch {
    $_ | Out-String | Set-Content "$stage\session-error.txt"
    1 | Set-Content "$stage\validation-exit.txt"
    exit 1
}
