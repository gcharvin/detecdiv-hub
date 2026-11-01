function inspect_catalog_project_paths(work,release)
% Inspect declared project MATs without saving any scientific object.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'all-project-path-request.json')));
rows=cell(1,numel(request));
for k=1:numel(request)
    item=request(k);row=struct('id',item.id,'name',item.name,'file',item.file, ...
        'project_saved',false,'status','pending','raw_paths',{{}},'fovs',0,'rois',0);
    try
        if ~isfile(item.file)
            row.status='declared_mat_missing';
        else
            before=dir(item.file);info=whos('-file',item.file);
            idx=find(strcmp({info.class},'shallow'),1);
            if isempty(idx)
                row.status='no_shallow_variable';
            elseif before.bytes>2e9
                row.status='large_mat_separate_review_required';
            else
                s=load(item.file,info(idx).name);p=s.(info(idx).name);paths={};
                row.fovs=numel(p.fov);row.rois=0;
                for f=1:numel(p.fov)
                    row.rois=row.rois+numel(p.fov(f).roi);
                    values=p.fov(f).srcpath;
                    if ischar(values)||isstring(values),values=cellstr(values);end
                    paths=[paths values]; %#ok<AGROW>
                end
                row.raw_paths=unique(paths,'stable');row.io=p.io;
                row.parsed_data_empty=isempty(p.parsedData);row.parsed_data_class=class(p.parsedData);
                row.status='inspected_readonly';after=dir(item.file);
                row.mat_stable_during_read=(before.bytes==after.bytes && before.datenum==after.datenum);
                clear p s;
            end
        end
    catch error
        row.status='read_failed_requires_review';row.error_identifier=error.identifier;
    end
    rows{k}=row;
    writeJson(fullfile(work,'all-project-path-progress.json'),struct('files',k,'total',numel(request),'last_id',item.id,'last_status',row.status));
    writeJson(fullfile(work,'all-project-path-inspection.partial.json'),rows(1:k));
end
writeJson(fullfile(work,'all-project-path-inspection.json'),rows);
end
function writeJson(path,value)
tmp=[path '.tmp'];fid=fopen(tmp,'w','n','UTF-8');assert(fid>=0);
c=onCleanup(@() fclose(fid));fprintf(fid,'%s\n',jsonencode(value,PrettyPrint=true));clear c;
movefile(tmp,path,'f');
end
