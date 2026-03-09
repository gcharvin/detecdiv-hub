$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$envFile = if ($env:DETECDIV_HUB_ENV_FILE) { $env:DETECDIV_HUB_ENV_FILE } else { Join-Path $repoRoot ".env" }

Set-Location $repoRoot

$activateScript = Join-Path $repoRoot ".venv\\Scripts\\Activate.ps1"
if (-not (Test-Path $activateScript)) {
    throw "Missing virtual environment activate script: $activateScript"
}
. $activateScript

if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match "^\s*#" -or $_ -match "^\s*$") {
            return
        }
        $parts = $_ -split "=", 2
        if ($parts.Count -eq 2) {
            [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), "Process")
        }
    }
}

python worker/run_worker.py
