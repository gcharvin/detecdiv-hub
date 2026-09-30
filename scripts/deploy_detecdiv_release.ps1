param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidatePattern('^[0-9a-fA-F]{40}$')]
    [string]$Commit,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

$scriptPath = Join-Path $PSScriptRoot 'deploy_detecdiv_release.py'
$bundledPython = Join-Path $env:USERPROFILE '.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
$python = if ($env:DETECDIV_RELEASE_PYTHON) {
    $env:DETECDIV_RELEASE_PYTHON
} elseif (Test-Path -LiteralPath $bundledPython) {
    $bundledPython
} else {
    $candidate = Get-Command py.exe -ErrorAction SilentlyContinue
    if ($candidate) { $candidate.Source } else { $null }
}
if (-not $python) {
    throw 'Python 3 not found. Set DETECDIV_RELEASE_PYTHON to a Python 3 executable.'
}

if ([IO.Path]::GetFileName($python).ToLowerInvariant() -eq 'py.exe') {
    & $python -3 $scriptPath $Commit @ExtraArgs
} else {
    & $python $scriptPath $Commit @ExtraArgs
}
exit $LASTEXITCODE
