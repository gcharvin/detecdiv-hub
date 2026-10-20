Get-Content 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/full-validation.log' -Tail 22 -ErrorAction SilentlyContinue
if (Test-Path 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/full-validation-exit.txt') {Get-Content 'C:/ProgramData/DetecDivHub/gpu-validation-20261016/full-validation-exit.txt'}
