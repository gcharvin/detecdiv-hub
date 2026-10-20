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
$ErrorActionPreference='Continue'
& 'C:\Program Files\MATLAB\R2025b\bin\matlab.exe' -batch "run('C:/ProgramData/DetecDivHub/gpu-validation-20261016/validate_full_pipeline.m')" *> 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/full-validation.log'
$LASTEXITCODE | Set-Content 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/full-validation-exit.txt'
exit $LASTEXITCODE
