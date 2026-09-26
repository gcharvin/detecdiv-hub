param(
    [string]$EnvFile,
    [switch]$MatlabLicenseReady,
    [string]$ArchiveSharePath,
    [switch]$EnableArchiveJobs
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

$lines = [System.Collections.Generic.List[string]]::new()
foreach ($line in [IO.File]::ReadAllLines($EnvFile)) { $lines.Add($line) }

$archiveConfigurationRequested = $PSBoundParameters.ContainsKey('ArchiveSharePath') -or $EnableArchiveJobs
$archiveTarget = $null
if ($archiveConfigurationRequested) {
    if ([string]::IsNullOrWhiteSpace($ArchiveSharePath) -or -not $EnableArchiveJobs) {
        throw 'To configure archive jobs, provide both -ArchiveSharePath (a UNC path) and -EnableArchiveJobs.'
    }
    if ($ArchiveSharePath -notmatch '^\\\\[^\\]+\\[^\\]+(?:\\.*)?$') {
        throw 'ArchiveSharePath must be a UNC path such as \\10.20.11.251\archive.'
    }
    if (-not (Test-Path -LiteralPath $ArchiveSharePath -PathType Container)) {
        throw "Archive share is not accessible as a directory: $ArchiveSharePath"
    }

    $trimChars = [char[]]@([char]92, [char]47)
    $archiveTarget = $ArchiveSharePath.TrimEnd($trimChars).Replace('\', '/')
    $probePath = Join-Path $ArchiveSharePath ('.detecdiv-hub-write-test-' + [guid]::NewGuid().ToString('N') + '.tmp')
    $probeCreated = $false
    try {
        New-Item -ItemType File -Path $probePath -ErrorAction Stop | Out-Null
        $probeCreated = $true
        Remove-Item -LiteralPath $probePath -Force -ErrorAction Stop
        $probeCreated = $false
    }
    finally {
        if ($probeCreated -and (Test-Path -LiteralPath $probePath)) {
            Remove-Item -LiteralPath $probePath -Force -ErrorAction Stop
        }
    }

    $mappingKey = 'DETECDIV_HUB_WORKER_PATH_MAPPINGS'
    $mappingPattern = '^' + [regex]::Escape($mappingKey) + '='
    $mappingIndices = @()
    for ($index = 0; $index -lt $lines.Count; $index++) {
        if ($lines[$index] -match $mappingPattern) { $mappingIndices += $index }
    }
    if ($mappingIndices.Count -gt 1) {
        throw "Duplicate $mappingKey entries found; .env was not changed."
    }
    $pathMappings = [System.Collections.Generic.List[object]]::new()
    if ($mappingIndices.Count -eq 1) {
        $mappingJson = $lines[$mappingIndices[0]].Substring($mappingKey.Length + 1)
        try {
            $existingMappings = @(ConvertFrom-Json -InputObject $mappingJson -ErrorAction Stop)
        }
        catch {
            throw "Could not parse $mappingKey; .env was not changed. $($_.Exception.Message)"
        }
        foreach ($mapping in $existingMappings) {
            if (-not $mapping.source -or -not $mapping.target) {
                throw "Invalid path mapping in $mappingKey; .env was not changed."
            }
            if ([string]$mapping.source -ne '/archive') { $pathMappings.Add($mapping) }
        }
    }
    $pathMappings.Add([pscustomobject]@{ source = '/archive'; target = $archiveTarget })
    $mappingArray = [object[]]$pathMappings.ToArray()
    $updatesPathMappings = ConvertTo-Json -InputObject $mappingArray -Depth 20 -Compress
} else {
    $updatesPathMappings = $null
}

$excludedKinds = @('restore_raw_dataset')
if (-not $EnableArchiveJobs) {
    $excludedKinds = @('archive_raw_dataset') + $excludedKinds
}
if (-not $MatlabLicenseReady) {
    $excludedKinds += @('pipeline_run', 'legacy_matlab')
}
$updates = [ordered]@{
    'DETECDIV_HUB_WORKER_CLAIM_UNASSIGNED_JOBS' = 'true'
    'DETECDIV_HUB_WORKER_JOB_KINDS' = ''
    'DETECDIV_HUB_WORKER_EXCLUDED_JOB_KINDS' = ($excludedKinds -join ',')
}
if ($archiveConfigurationRequested) {
    $updates['DETECDIV_HUB_WORKER_PATH_MAPPINGS'] = $updatesPathMappings
    $updates['DETECDIV_HUB_DEFAULT_ARCHIVE_ROOT'] = '/archive'
}

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
if ($archiveConfigurationRequested) {
    Write-Output "Archive share verified and mapped: /archive -> $archiveTarget"
    Write-Warning 'After restart, queued archive jobs may start. Jobs with mark_archived=true delete the source after a successful archive.'
}
Write-Output 'Run .\scripts\run_worker.ps1 -EnvFile .env -Check, then restart the worker while it is idle.'
