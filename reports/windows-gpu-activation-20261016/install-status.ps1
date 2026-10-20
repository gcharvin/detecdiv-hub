Get-ChildItem 'C:/ProgramData/DetecDivHub/gpu-validation-20261016' | Select-Object Name,Length | ConvertTo-Json -Compress
Get-CimInstance Win32_Process | Where-Object {$_.CommandLine -match 'pip install|install-runtime'} | Select-Object ProcessId,Name,CommandLine | ConvertTo-Json -Compress
Test-Path 'C:/ProgramData/DetecDivHub/cellpose-gpu-v1/Scripts/python.exe'
