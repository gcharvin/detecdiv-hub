$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$repo='C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub'
Set-Location $repo
foreach ($line in Get-Content -LiteralPath "$repo\.env") {
    if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') {
        $key=$Matches[1]; $value=$Matches[2].Trim().Trim('"').Trim("'")
        [Environment]::SetEnvironmentVariable($key,$value,'Process')
    }
}
& ./.venv/Scripts/python.exe 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/final-worker-preflight.py' *> 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/worker-preflight.log'
exit $LASTEXITCODE
