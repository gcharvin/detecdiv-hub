$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$repo='C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub'
Set-Location $repo
$processes=@(Get-CimInstance Win32_Process)
$managed=@($processes | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match '-m worker.run_worker' })
if ($managed.Count -ne 2) {throw 'Expected exactly the venv launcher and one worker interpreter'}
$ids=@($managed | ForEach-Object ProcessId)
$children=@($processes | Where-Object {$_.ParentProcessId -in $ids -and $_.ProcessId -notin $ids})
if ($children.Count) {throw 'Worker has active child processes'}
Stop-ScheduledTask -TaskName 'DetecDiv Hub Worker'
$stopped=$false
try {
    Start-Sleep -Seconds 2
    foreach ($expected in $managed) {
        $current=Get-CimInstance Win32_Process -Filter "ProcessId = $($expected.ProcessId)"
        if ($current) {
            if ($current.CreationDate -ne $expected.CreationDate -or $current.CommandLine -ne $expected.CommandLine) {throw 'Process identity changed'}
            Stop-Process -Id $current.ProcessId -Force
        }
    }
    if (@(Get-CimInstance Win32_Process | Where-Object {$_.Name -eq 'python.exe' -and $_.CommandLine -match '-m worker.run_worker'}).Count) {throw 'Poller survived stop'}
    $stopped=$true
    $envFile=Join-Path $repo '.env'
    $backup=Join-Path $repo '.deploy-backups\windows-gpu-20261016'
    New-Item -ItemType Directory -Force $backup | Out-Null
    $saved=Join-Path $backup 'env.saved'
    if (Test-Path $saved) {throw 'Environment backup exists; inspect before retry'}
    Copy-Item -LiteralPath $envFile -Destination $saved
    Set-Acl -LiteralPath $saved -AclObject (Get-Acl -LiteralPath $envFile)
    $value=Get-Content -LiteralPath $envFile -Raw
    if ($value -match '(?m)^DETECDIV_PHASE_CELLPOSE_PYTHON=') {throw 'Runtime already configured; inspect before retry'}
    [IO.File]::WriteAllText($envFile, $value.TrimEnd()+"`r`nDETECDIV_PHASE_CELLPOSE_PYTHON=C:\ProgramData\DetecDivHub\cellpose-gpu-v1\Scripts\python.exe`r`n",[Text.UTF8Encoding]::new($false))
    $env:PYTHONPATH=$repo
    & ./.venv/Scripts/python.exe 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/apply-worker.py'
    if ($LASTEXITCODE) {throw 'Worker source activation failed'}
} finally {
    if ($stopped) {Start-ScheduledTask -TaskName 'DetecDiv Hub Worker'}
}
