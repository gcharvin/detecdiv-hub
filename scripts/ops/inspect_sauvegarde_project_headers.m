function report = inspect_sauvegarde_project_headers(manifestFile, output, engineRepo, sourceRoot)
% Read MAT headers only. Do not load, relink, copy, or save scientific files.
addpath(genpath(fullfile(engineRepo, 'structure')));
if nargin < 4
    sourceRoot = 'S:\';
end
manifest = jsondecode(fileread(manifestFile));
assert(manifest.read_only && manifest.complete, 'Discovery must be complete');
report = struct('sourceManifest', manifestFile, 'scientificFilesReadOnly', true, ...
    'complete', false, 'candidates', []);
rows = cell(1, numel(manifest.candidates));
% Small legacy headers first; large compressed modern MATs remain read-only.
[~, order] = sort([manifest.candidates.mat_bytes]);
for k = 1:numel(manifest.candidates)
    item = manifest.candidates(order(k));
    file = fullfile(sourceRoot, strrep(item.relative_path, '/', filesep));
    row = struct('relativePath', item.relative_path, 'matBytes', item.mat_bytes, ...
        'mtimeNs', item.mtime_ns, 'status', 'pending', 'variables', [], ...
        'projectKind', '', 'review', '', 'warning', '');
    try
        stat = dir(file);
        assert(numel(stat) == 1 && stat.bytes == item.mat_bytes, 'Source changed');
        lastwarn('');
        info = whos('-file', file);
        [warningText, ~] = lastwarn;
        row.variables = info;
        row.warning = warningText;
        shallowIndex = find(strcmp({info.class}, 'shallow'), 1);
        legacyIndex = find(strcmp({info.name}, 'timeLapse') & strcmp({info.class}, 'struct'), 1);
        if ~isempty(shallowIndex) && prod(info(shallowIndex).size) == 1
            row.status = 'valid_project_header';
            row.projectKind = 'detecdiv_shallow';
        elseif item.legacy_filename && ~isempty(legacyIndex) && prod(info(legacyIndex).size) == 1
            row.status = 'valid_project_header';
            row.projectKind = 'legacy_matlab_timelapse';
        else
            row.status = 'manual_review';
            row.review = 'No nonempty scalar shallow in MAT header; possible legacy project or another MATLAB file';
        end
    catch exception
        row.status = 'inspection_failed';
        row.review = exception.message;
    end
    rows{k} = row;
    report.candidates = [rows{1:k}];
    if mod(k, 20) == 0 || k == numel(rows)
        persist(report, output);
    end
    fprintf('%d/%d %s: %s\n', k, numel(rows), item.relative_path, row.status);
end
report.complete = true;
persist(report, output);
end

function persist(report, output)
temporary = [char(output), '.tmp'];
fid = fopen(temporary, 'w', 'n', 'UTF-8');
assert(fid >= 0, 'Cannot write header report');
cleanup = onCleanup(@() fclose(fid));
fprintf(fid, '%s\n', jsonencode(report, PrettyPrint=true));
clear cleanup;
movefile(temporary, output, 'f');
end
