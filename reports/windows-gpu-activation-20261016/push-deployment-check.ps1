$root='C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub'
$release='C:/Users/Charvin-Admin/Documents/GitHub/DetecDiv-jobs/releases/594217dd37bfa9b2b529f7f2204253d9901340bf'
@('worker/job_resources.py','worker/run_worker.py','worker/path_mappings.py') | ForEach-Object { Get-FileHash -Algorithm SHA256 (Join-Path $root $_) | Select-Object Path,Hash } | ConvertTo-Json -Compress
& git -c "safe.directory=$release" -C $release rev-parse HEAD
Get-Content (Join-Path $root '.env') | Where-Object {$_ -match '^DETECDIV_PHASE_CELLPOSE_PYTHON='}
Get-ScheduledTask -TaskName 'DetecDiv Hub Worker','DetecDiv Hub Database Tunnel' | Select-Object TaskName,State | ConvertTo-Json -Compress
