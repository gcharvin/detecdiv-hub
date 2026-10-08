function report = inspect_legacy_project_pilot(source, output, engineRepo)
% Read MAT headers and one small project to prepare storage/path migration.
% This function does not save, relink, index, or process scientific projects.
addpath(genpath(fullfile(engineRepo, 'structure')));
addpath(fullfile(engineRepo, 'helpers'));
files = dir(fullfile(source, '*.mat'));
files = files(~cellfun(@isempty, regexp({files.name}, '(?<!\d)\d{4}(?:20)?23(?!\d)|2023', 'once')));
report = struct('source', source, 'readOnly', true, 'candidates', [], 'sample', []);
candidates = struct('file', {}, 'matBytes', {}, 'variables', {}, 'review', {});
valid = [];
for k = 1:numel(files)
    file = fullfile(source, files(k).name);
    info = whos('-file', file);
    review = '';
    matches = find(strcmp({info.class}, 'shallow'));
    if isempty(matches)
        review = 'No shallow variable in the MAT header';
    elseif any(info(matches(1)).size == 0)
        review = 'Empty shallow array; manual review required';
    else
        valid(end + 1) = k; %#ok<AGROW>
    end
    candidates(k) = struct('file', file, 'matBytes', files(k).bytes, 'variables', info, 'review', review); %#ok<AGROW>
end
report.candidates = candidates;
if ~isempty(valid)
    [~, selected] = min([files(valid).bytes]);
    k = valid(selected);
    info = candidates(k).variables;
    variable = info(find(strcmp({info.class}, 'shallow'), 1)).name;
    loaded = load(candidates(k).file, variable);
    project = loaded.(variable);
    paths = {};
    for f = 1:numel(project.fov)
        values = project.fov(f).srcpath;
        if ischar(values) || isstring(values)
            values = cellstr(values);
        end
        for ch = 1:numel(values)
            if ~isempty(values{ch})
                paths{end + 1} = char(values{ch}); %#ok<AGROW>
            end
        end
    end
    report.sample = struct('file', candidates(k).file, 'class', class(project), ...
        'projectIo', project.io, 'fovCount', numel(project.fov), ...
        'rawPaths', {unique(paths, 'stable')});
end
fid = fopen(output, 'w', 'n', 'UTF-8');
assert(fid >= 0, 'Cannot write the pilot report');
cleanup = onCleanup(@() fclose(fid)); %#ok<NASGU>
fprintf(fid, '%s\n', jsonencode(report, PrettyPrint=true));
fprintf('Read-only pilot: %d candidates, %d require review. Report: %s\n', ...
    numel(candidates), nnz(~cellfun(@isempty, {candidates.review})), output);
end
