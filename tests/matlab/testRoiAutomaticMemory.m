function tests = testRoiAutomaticMemory
% Integration with the companion DetecDiv checkout already on MATLAB's path.
tests = functiontests(localfunctions);
end

function testSmallerJobBudgetUsesSmallerBlocksWithIdenticalPixels(testCase)
root = tempname;
mkdir(root);
addTeardown(testCase, @() removeFixture(root));
previous = getenv('DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB');
addTeardown(testCase, @() setenv('DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB', previous));
rawDir = fullfile(root, 'raw');
mkdir(rawDir);
expected = zeros(64, 64, 1, 120, 'uint16');
for frame = 1:120
    image = uint16(reshape(1:4096, 64, 64) + frame);
    expected(:,:,1,frame) = image;
    imwrite(image, fullfile(rawDir, sprintf('frame_%03d.tif', frame)));
end

% Scale the two job caps down for a small fixture. The production admission
% floor remains 24 GiB; only this disposable MATLAB process sees these caps.
budgets = [2 8];
blocks = zeros(1,2);
for index = 1:2
    folder = fullfile(root, sprintf('budget_%d', budgets(index)));
    mkdir(folder);
    project = shallow();
    project.io = struct('path', folder, 'file', 'adaptive_project.mat');
    project.projectId = 'auto-memory-test';
    f = fov();
    f.id = 'Pos1_1';
    f.number = 1;
    f.channel = {'raw'};
    f.frames = 120;
    f.interval = 1;
    f.srcpath = {rawDir};
    f.srclist = {dir(fullfile(rawDir, '*.tif'))};
    f.roi = roi('Pos1_1_roi_1', [1 1 64 64]);
    project.fov = f;
    setenv('DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB', num2str(budgets(index)));
    output = evalc("project.extractAllROICrops('CorrectDrift', false);");
    token = regexp(output, 'Tblock=(\d+)', 'tokens', 'once');
    verifyNotEmpty(testCase, token);
    blocks(index) = str2double(token{1});
    h5File = fullfile(folder, 'adaptive_project', 'Pos1_1', 'im_Pos1_1_roi_1.h5');
    actual = h5read(h5File, '/raw');
    verifyEqual(testCase, actual, expected);
end
verifyLessThan(testCase, blocks(1), blocks(2));
end

function removeFixture(folder)
resolved = char(java.io.File(folder).getCanonicalPath());
safeRoot = char(java.io.File(tempdir).getCanonicalPath());
assert(startsWith(resolved, [safeRoot filesep], 'IgnoreCase', ispc));
if isfolder(resolved), rmdir(resolved, 's'); end
end
