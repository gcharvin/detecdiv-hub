function report = inspect_legacy_project_lineage(manifestFile, output, engineRepo)
% Inspect only the explicit pilot manifest, without saving any project.
addpath(genpath(fullfile(engineRepo, 'structure')));
addpath(fullfile(engineRepo, 'helpers'));
manifest = jsondecode(fileread(manifestFile));
assert(strcmp(manifest.owner_hub, 'alexander'));
report = struct('sourceManifest', manifestFile, 'readOnly', true, ...
    'complete', false, 'projects', []);
rows = cell(1, numel(manifest.candidates));
for k = 1:numel(manifest.candidates)
    candidate = manifest.candidates(k);
    file = char(candidate.source_windows);
    row = struct('file', file, 'fileName', candidate.file_name, ...
        'matBytes', candidate.mat_bytes, 'status', 'pending', ...
        'fovCount', 0, 'rawPaths', {{}}, 'projectIo', [], 'error', '');
    try
        assert(startsWith(file, 'S:\Alexander\analysis\'), 'Unexpected source root');
        info = dir(file);
        assert(numel(info) == 1 && info.bytes == candidate.mat_bytes, 'Source size changed');
        if candidate.manual_review
            row.status = 'manual_review';
        else
            header = whos('-file', file);
            index = find(strcmp({header.class}, 'shallow'), 1);
            assert(~isempty(index), 'No shallow variable');
            assert(prod(header(index).size) == 1, 'Expected one shallow project');
            loaded = load(file, header(index).name);
            project = loaded.(header(index).name);
            row.projectIo = project.io;
            row.fovCount = numel(project.fov);
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
            row.rawPaths = unique(paths, 'stable');
            row.status = 'inspected';
            clear loaded project;
        end
    catch exception
        row.status = 'inspection_failed';
        row.error = exception.message;
    end
    rows{k} = row;
    report.projects = [rows{1:k}];
    persist(report, output);
    fprintf('%d/%d %s: %s\n', k, numel(rows), candidate.file_name, row.status);
end
report.complete = true;
persist(report, output);
end

function persist(report, output)
temporary = [char(output), '.tmp'];
fid = fopen(temporary, 'w', 'n', 'UTF-8');
assert(fid >= 0, 'Cannot write lineage report');
cleanup = onCleanup(@() fclose(fid));
fprintf(fid, '%s\n', jsonencode(report, PrettyPrint=true));
clear cleanup;
movefile(temporary, output, 'f');
end
