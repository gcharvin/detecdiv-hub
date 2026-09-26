param(
    [string]$EnvFile,
    [switch]$MatlabLicenseReady
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if (-not $EnvFile) {
    $EnvFile = Join-Path $repoRoot '.env'
} elseif (-not [IO.Path]::IsPathRooted($EnvFile)) {
    $EnvFile = Join-Path $repoRoot $EnvFile
}
$EnvFile = [IO.Path]::GetFullPath($EnvFile)
if (-not (Test-Path -LiteralPath $EnvFile -PathType Leaf)) {
    throw "Missing worker environment file: $EnvFile"
}

$excludedKinds = @('archive_raw_dataset', 'restore_raw_dataset')
if (-not $MatlabLicenseReady) {
    $excludedKinds += @('pipeline_run', 'legacy_matlab')
}
$updates = [ordered]@{
    'DETECDIV_HUB_WORKER_CLAIM_UNASSIGNED_JOBS' = 'true'
    'DETECDIV_HUB_WORKER_JOB_KINDS' = ''
    'DETECDIV_HUB_WORKER_EXCLUDED_JOB_KINDS' = ($excludedKinds -join ',')
}

$lines = [System.Collections.Generic.List[string]]::new()
foreach ($line in [IO.File]::ReadAllLines($EnvFile)) { $lines.Add($line) }
foreach ($entry in $updates.GetEnumerator()) {
    $pattern = '^' + [regex]::Escape($entry.Key) + '='
    # `$Matches` is a PowerShell automatic variable populated by `-match`.
    # Use another name so we can safely collect the matching .env line indexes.
    $matchingIndices = @()
    for ($index = 0; $index -lt $lines.Count; $index++) {
        if ($lines[$index] -match $pattern) { $matchingIndices += $index }
    }
    if ($matchingIndices.Count -gt 1) {
        throw "Duplicate $($entry.Key) entries found; .env was not changed."
    }
    if ($matchingIndices.Count -eq 1) {
        $lines[$matchingIndices[0]] = "$($entry.Key)=$($entry.Value)"
    } else {
        $lines.Add("$($entry.Key)=$($entry.Value)")
    }
}

[IO.File]::WriteAllLines($EnvFile, $lines, [Text.UTF8Encoding]::new($false))
& icacls.exe $EnvFile /inheritance:r /grant:r 'GMGM\Charvin-Admin:F' '*S-1-5-18:F' '*S-1-5-32-544:F' | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Failed to secure the worker .env permissions.' }

Write-Output 'Windows worker queue settings updated; the database URL was preserved.'
Write-Output "Claim unassigned jobs: true"
Write-Output "Excluded job kinds: $($excludedKinds -join ',')"
Write-Output 'Run .\scripts\run_worker.ps1 -EnvFile .env -Check, then restart the worker while it is idle.'
