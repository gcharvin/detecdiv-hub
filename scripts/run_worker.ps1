param(
    [string]$EnvFile,
    [switch]$Check,
    [string]$LogFile
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if (-not $EnvFile) {
    $EnvFile = if ($env:DETECDIV_HUB_ENV_FILE) { $env:DETECDIV_HUB_ENV_FILE } else { Join-Path $repoRoot ".env" }
}
$pythonPath = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
    throw "Missing worker Python: $pythonPath. Install the virtual environment first."
}
if (-not (Test-Path -LiteralPath $EnvFile -PathType Leaf)) {
    throw "Missing worker environment file: $EnvFile"
}

foreach ($line in Get-Content -LiteralPath $EnvFile) {
    if ($line -match '^\s*(#|$)') { continue }
    if ($line -notmatch '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') {
        throw "Invalid environment file line in $EnvFile"
    }
    $key = $Matches[1]
    $value = $Matches[2].Trim()
    if ($value.Length -ge 2 -and (
        ($value.StartsWith('"') -and $value.EndsWith('"')) -or
        ($value.StartsWith("'") -and $value.EndsWith("'"))
    )) {
        $value = $value.Substring(1, $value.Length - 2)
    }
    [System.Environment]::SetEnvironmentVariable($key, $value, "Process")
}

# A Windows pilot must not consume unrelated server maintenance jobs.
if (-not $env:DETECDIV_HUB_WORKER_CLAIM_UNASSIGNED_JOBS) {
    $env:DETECDIV_HUB_WORKER_CLAIM_UNASSIGNED_JOBS = "false"
}
if (-not $env:DETECDIV_HUB_WORKER_JOB_KINDS) {
    $env:DETECDIV_HUB_WORKER_JOB_KINDS = "pipeline_run"
}
if (-not $env:DETECDIV_HUB_WORKER_ENABLE_SCHEDULERS) {
    $env:DETECDIV_HUB_WORKER_ENABLE_SCHEDULERS = "false"
}
if (-not $env:DETECDIV_HUB_WORKER_INSTANCE) {
    $env:DETECDIV_HUB_WORKER_INSTANCE = "$env:COMPUTERNAME-main"
}
if (-not $env:DETECDIV_HUB_WORKER_TARGET_KEY) {
    throw "DETECDIV_HUB_WORKER_TARGET_KEY is required for a Windows worker."
}
if (-not $env:DETECDIV_HUB_DATABASE_URL) {
    throw "DETECDIV_HUB_DATABASE_URL is required for a Windows worker."
}

Set-Location $repoRoot
if ($LogFile) {
    $logDirectory = Split-Path -Parent $LogFile
    New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
    & $pythonPath -m worker.check_readiness *>> $LogFile
} else {
    & $pythonPath -m worker.check_readiness
}
if ($LASTEXITCODE -ne 0) { throw "Worker readiness check failed." }
if ($Check) { return }

if ($LogFile) {
    & $pythonPath -u -m worker.run_worker *>> $LogFile
} else {
    & $pythonPath -u -m worker.run_worker
}
if ($LASTEXITCODE -ne 0) { throw "Worker exited with code $LASTEXITCODE." }
