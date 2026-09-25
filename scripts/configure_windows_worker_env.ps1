param(
    [string]$RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
)

$ErrorActionPreference = 'Stop'
$envPath = Join-Path $RepositoryRoot '.env'
$dbUser = Read-Host 'Login PostgreSQL (partie avant les deux-points dans l URL)'
if ([string]::IsNullOrWhiteSpace($dbUser)) {
    throw 'Le login PostgreSQL ne peut pas etre vide.'
}

$securePassword = Read-Host 'Mot de passe PostgreSQL (saisie masquee)' -AsSecureString
$bstr = [IntPtr]::Zero
$dbPassword = $null
try {
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
    $dbPassword = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    $encodedUser = [Uri]::EscapeDataString($dbUser)
    $encodedPassword = [Uri]::EscapeDataString($dbPassword)
    $databaseUrl = "postgresql+psycopg://${encodedUser}:${encodedPassword}@127.0.0.1:15432/detecdiv_hub"

    if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) {
        New-Item -ItemType File -Path $envPath | Out-Null
    }
    & icacls.exe $envPath /inheritance:r /grant:r 'GMGM\Charvin-Admin:F' '*S-1-5-18:F' '*S-1-5-32-544:F' | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw 'Echec du reglage des permissions de .env.'
    }

    $lines = @(
        'DETECDIV_HUB_ENVIRONMENT=prod'
        "DETECDIV_HUB_DATABASE_URL=$databaseUrl"
        'DETECDIV_HUB_WORKER_TARGET_KEY=windows-10-20-11-56'
        'DETECDIV_HUB_WORKER_INSTANCE=windows-10-20-11-56-main'
        'DETECDIV_HUB_WORKER_CLAIM_UNASSIGNED_JOBS=false'
        'DETECDIV_HUB_WORKER_JOB_KINDS=pipeline_run'
        'DETECDIV_HUB_WORKER_ENABLE_SCHEDULERS=false'
        'DETECDIV_HUB_WORKER_POLL_INTERVAL_SEC=5'
        'DETECDIV_HUB_MATLAB_COMMAND=C:\Program Files\MATLAB\R2025b\bin\matlab.exe'
        'DETECDIV_HUB_MATLAB_REPO_ROOT=C:\Users\Charvin-Admin\Documents\MATLAB\DetecDiv'
        'DETECDIV_HUB_WORKER_PATH_MAPPINGS=[{"source":"/data","target":"//10.20.11.250/DATA"}]'
    )
    [IO.File]::WriteAllLines($envPath, [string[]]$lines, [Text.UTF8Encoding]::new($false))
    Write-Output "Worker environment written to $envPath (credentials omitted)."
}
finally {
    if ($bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
    if ($securePassword) {
        $securePassword.Dispose()
    }
    $dbPassword = $null
    $databaseUrl = $null
    $lines = $null
}
