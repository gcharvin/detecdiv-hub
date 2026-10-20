% Run under the same interactive principal/session as the polling worker.
stage = 'C:/ProgramData/DetecDivHub/gpu-validation-20261016';
root = '//10.20.11.250/DATA/Gilles/analysis/classifiers/phase_cellposesam_v001_1';
release = fullfile(root,'code','releases','45f68ffc19e6faee2d727506a6bd4fa3b41f3a82','classifier');
addpath(release);
assert(contains(which('phaseCellposeSAM.train'),'45f68ffc19e6faee2d727506a6bd4fa3b41f3a82'));
issues = checkcode(fullfile(release,'+phaseCellposeSAM','train.m'),'-id');
assert(~any(strcmp({issues.id},'PARSE')),'Launcher syntax error');
device = gpuDevice();
disp(device);
python = getenv('DETECDIV_PHASE_CELLPOSE_PYTHON');
assert(isfile(python),'Configured Python missing');
args = {char(python),'-B',fullfile(stage,'gpu-smoke.py')};
builder = java.lang.ProcessBuilder(args);
builder.redirectErrorStream(true);
builder.redirectOutput(java.io.File(fullfile(stage,'gpu-smoke.log')));
proc = builder.start();
cleanup = onCleanup(@() stopValidationChild(proc));
assert(proc.waitFor()==0,'GPU validation failed; inspect gpu-smoke-result.json');
result=jsondecode(fileread(fullfile(stage,'gpu-smoke-result.json')));
assert(strcmp(result.status,'passed'));
disp('WINDOWS_GPU_MATLAB_AND_PYTHON_PASSED');

function stopValidationChild(proc)
if proc.isAlive(), proc.destroyForcibly(); end
end
