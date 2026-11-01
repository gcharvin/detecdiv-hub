function inspect_project_for_raw_cutover(work, release)
% Inspect the preserved project; do not save or process any scientific object.
addpath(genpath(fullfile(release, 'structure')));
addpath(fullfile(release, 'helpers'));
source = '/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis/anof_26102023_yam76s_100ms.mat';
oldRaw = '/data/Alexander/data/2023_2';
stageRaw = '/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_2';
finalRaw = '/homes2/maliavko/DetecdivHub/raw/2023_2';
header = whos('-file', source);
index = find(strcmp({header.class}, 'shallow'), 1);
assert(~isempty(index) && prod(header(index).size) == 1, 'Expected one shallow project');
loaded = load(source, header(index).name);
project = loaded.(header(index).name);
assert(isa(project, 'shallow') && numel(project.fov) == 12, 'Unexpected project structure');
ioBefore = project.io;
paths = {};
roiCount = 0;
for f = 1:numel(project.fov)
    values = project.fov(f).srcpath;
    if ischar(values) || isstring(values), values = cellstr(values); end
    paths = [paths, values(:)']; %#ok<AGROW>
    roiCount = roiCount + numel(project.fov(f).roi);
end
paths = unique(paths(~cellfun(@isempty, paths)), 'stable');
checks = struct('source', {}, 'staging', {}, 'final', {}, 'sourceExists', {}, 'stagingExists', {});
for k = 1:numel(paths)
    path = strrep(char(paths{k}), '\', '/');
    assert(startsWith(path, [oldRaw, '/']), 'Project contains a raw path outside this migration');
    suffix = path(numel(oldRaw)+1:end);
    staging = [stageRaw, suffix];
    checks(k) = struct('source', path, 'staging', staging, 'final', [finalRaw, suffix], ...
        'sourceExists', isfolder(path), 'stagingExists', isfolder(staging)); %#ok<AGROW>
end
assert(numel(checks) == 12 && all([checks.sourceExists]) && all([checks.stagingExists]), ...
    'Source or copied FOV raw directory is unavailable');
% Mirror the normal project-opening path adjustment in memory only.
[parent, stem] = fileparts(source);
project.setPath(parent, stem);
report = struct('success', true, 'read_only', true, 'project_saved', false, ...
    'source_mat', source, 'engine_release', release, 'matlab_version', version, ...
    'fov_count', numel(project.fov), 'roi_count', roiCount, 'io_before', ioBefore, ...
    'io_rebased_in_memory', project.io, 'raw_path_checks', checks, ...
    'compatibility_strategy', 'Retain primary raw paths and the original Sauvegarde project; no source reclamation.');
output = fullfile(work, 'project-readonly-inspection.json');
fid = fopen(output, 'w', 'n', 'UTF-8');
assert(fid >= 0);
cleanup = onCleanup(@() fclose(fid)); %#ok<NASGU>
fprintf(fid, '%s\n', jsonencode(report, PrettyPrint=true));
end
