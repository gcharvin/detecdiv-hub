$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$stage='C:\ProgramData\DetecDivHub\gpu-validation-20261016'
$runtime='C:\ProgramData\DetecDivHub\cellpose-gpu-v1'
New-Item -ItemType Directory -Force $stage | Out-Null
if (!(Test-Path "$runtime\Scripts\python.exe")) {
    & 'C:\ProgramData\DetecDivHub\python311\python.exe' -m venv $runtime
    if ($LASTEXITCODE) { throw 'venv failed' }
}
$p="$runtime\Scripts\python.exe"
& $p -m pip install --disable-pip-version-check torch==2.7.0+cu118 torchvision==0.22.0+cu118 --index-url https://download.pytorch.org/whl/cu118 *> "$stage\pip-torch.log"
if ($LASTEXITCODE) { $LASTEXITCODE | Set-Content "$stage\install-exit.txt"; exit $LASTEXITCODE }
& $p -m pip install --disable-pip-version-check cellpose==4.2.1.1 numpy==2.2.6 h5py==3.15.1 *> "$stage\pip-cellpose.log"
$LASTEXITCODE | Set-Content "$stage\install-exit.txt"
exit $LASTEXITCODE
