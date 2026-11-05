function discover_projects(work,release)
% Read-only path discovery. Never save a project or load ROI image sidecars.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'project-discovery-request.json')));
assert(~isfile(fullfile(work,'project-discovery-receipt.jsonl')));
receipt=fopen(fullfile(work,'project-discovery-receipt.jsonl'),'w','n','UTF-8');
assert(receipt>=0);cleanup=onCleanup(@() fclose(receipt));
matched=0;unresolved=0;
for k=1:numel(request.projects)
    item=request.projects(k);
    row=struct('project_id',item.id,'name',item.name,'owner',item.owner, ...
        'status','pending','file','','fovs',0,'rois',0,'raw_paths',{{}}, ...
        'roi_paths',{{}},'matching_acquisitions',{{}},'fov_details',{{}}, ...
        'projects_saved',false);
    try
        files=cellstr(string(item.files));file='';
        for n=1:numel(files)
            if isfile(files{n}),file=files{n};break;end
        end
        assert(~isempty(file),'No accessible project main file');row.file=file;
        before=dir(file);assert(numel(before)==1);
        row.source_bytes=before.bytes;row.source_datenum=before.datenum;
        assert(~any(strcmp(item.id,cellstr(string(request.busy_project_ids)))),'Project busy at admission; inspect later');
        if endsWith(lower(file),'.json')
            document=jsondecode(fileread(file));
            paths=collectStrings(document);
            row.raw_paths=unique(paths,'stable');
            row.status='json_paths_inspected';
        else
            vars=whos('-file',file);
            ix=find(strcmp({vars.name},'shallowObj'));assert(numel(ix)==1,'Main MAT is not a recognized shallowObj layout');
            assert(vars(ix).bytes<2e9,'Large main MAT requires separate memory admission');
            loaded=load(file,'shallowObj');project=loaded.shallowObj;
            assert(isprop(project,'fov')||isfield(project,'fov'),'Project has no FOV property');
            paths={};roiPaths={};details=cell(1,numel(project.fov));
            for f=1:numel(project.fov)
                fov=project.fov(f);
                src=cellstr(string(fov.srcpath));paths=[paths reshape(src,1,[])]; %#ok<AGROW>
                for r=1:numel(fov.roi)
                    roiPaths{end+1}=char(fov.roi(r).path); %#ok<AGROW>
                end
                details{f}=struct('fov',f,'raw_paths',{src},'rois',numel(fov.roi));
                row.rois=row.rois+numel(fov.roi);
            end
            row.fovs=numel(project.fov);row.raw_paths=unique(paths,'stable');
            row.roi_paths=unique(roiPaths,'stable');row.fov_details=details;
            row.status='mat_paths_inspected';clear loaded project fov;
        end
        names={};
        for a=1:numel(request.acquisitions)
            acq=request.acquisitions(a);needle=['/' acq.name '/'];
            for n=1:numel(row.raw_paths)
                value=['/' regexprep(strrep(char(row.raw_paths{n}),'\','/'),'^/+|/+$','') '/'];
                if contains(value,needle)
                    names{end+1}=[acq.year '/' acq.name];break; %#ok<AGROW>
                end
            end
        end
        row.matching_acquisitions=unique(names,'stable');
        row.explicit_2024_reference=any(contains(string(row.raw_paths),'/2024_1/'))|| ...
            any(contains(string(row.raw_paths),'/2024_2/'))|| ...
            any(contains(string(row.raw_paths),'\2024_1\'))||any(contains(string(row.raw_paths),'\2024_2\'));
        if ~isempty(names)||row.explicit_2024_reference,matched=matched+1;end
        after=dir(file);assert(numel(after)==1 && before.bytes==after.bytes && before.datenum==after.datenum,'Main project changed while inspected');
        row.source_metadata_stable=true;
    catch error
        row.status='requires_review';row.error=error.message;unresolved=unresolved+1;
    end
    fprintf(receipt,'%s\n',jsonencode(row));
    writeJson(fullfile(work,'project-discovery-progress.json'),struct('inspected',k,'total',numel(request.projects), ...
        'matched_candidates',matched,'requires_review',unresolved,'last_project',item.name,'projects_saved',false));
end
writeJson(fullfile(work,'project-discovery-result.json'),struct('success',true,'projects_inspected',numel(request.projects), ...
    'matched_candidates',matched,'requires_review',unresolved,'projects_saved',false,'catalog_changed',false, ...
    'note','Path candidates only: aliases and raw identity, full copy hashes, scientific equivalence and both-host validation remain mandatory.'));
end

function values=collectStrings(value)
values={};
if ischar(value),values={value};
elseif isstring(value),values=cellstr(value);
elseif isstruct(value)
    fields=fieldnames(value);
    for i=1:numel(value)
        for f=1:numel(fields),values=[values collectStrings(value(i).(fields{f}))];end %#ok<AGROW>
    end
elseif iscell(value)
    for i=1:numel(value),values=[values collectStrings(value{i})];end %#ok<AGROW>
end
end

function writeJson(path,value)
temporary=[path '.tmp'];f=fopen(temporary,'w','n','UTF-8');assert(f>=0);
c=onCleanup(@() fclose(f));fprintf(f,'%s\n',jsonencode(value));clear c;movefile(temporary,path,'f');
end
