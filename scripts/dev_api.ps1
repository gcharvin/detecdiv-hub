$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not (Test-Path .venv)) {
    python -m venv .venv
}

. .\.venv\Scripts\Activate.ps1
pip install -e .[dev]
uvicorn api.app:app --reload --host 127.0.0.1 --port 8000

