Get-ScheduledTask -TaskName 'DetecDiv Hub Worker' | Select-Object -ExpandProperty Principal | ConvertTo-Json -Compress
Get-CimInstance Win32_Process | Where-Object {$_.Name -eq 'python.exe'} | Select-Object ProcessId,ParentProcessId,SessionId,CommandLine | ConvertTo-Json -Compress
Get-ChildItem 'C:\Users\Charvin-Admin\.conda\envs\detecdiv_python' -Recurse -Filter '*iomp*.dll' | Select-Object -ExpandProperty FullName
