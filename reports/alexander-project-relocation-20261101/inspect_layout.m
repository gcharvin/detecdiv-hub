function inspect_layout(work, release)
% Read the top MAT and representative legacy sidecars without saving objects.
addpath(genpath(fullfile(release, 'structure')));addpath(fullfile(release, 'helpers'));
base='/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis';
stem='anof_26102023_yam76s_100ms';source=fullfile(base,[stem '.mat']);
s=load(source);project=s.shallowObj;
report=struct('project_class',class(project),'io',project.io,'fovs',numel(project.fov), ...
    'parsed_data_class',class(project.parsedData),'parsed_data_fields',{{}}, ...
    'parsed_data_path_values',[],'roi_paths',{{}},'processing_fields',{{}},'sidecars',[]);
if isstruct(project.parsedData)
    report.parsed_data_fields=fieldnames(project.parsedData);
    report.parsed_data_path_values=collectPaths(project.parsedData,'parsedData');
end
roiPaths={};
for f=1:numel(project.fov)
    for r=1:numel(project.fov(f).roi)
        roiPaths{end+1}=project.fov(f).roi(r).path; %#ok<AGROW>
    end
end
report.roi_paths=unique(roiPaths,'stable');
report.processing_fields=fieldnames(project.processing);
folder=fullfile(base,stem,'26102023_yam76s_100ms_4_Pos1_1');
names={'im_26102023_yam76s_100ms_4_Pos1_1_1.mat','results_26102023_yam76s_100ms_4_Pos1_1_1.mat'};
rows=cell(1,numel(names));
for k=1:numel(names)
    path=fullfile(folder,names{k});info=whos('-file',path);
    row=struct('file',path,'header',info,'path_values',[]);
    if startsWith(names{k},'results_')
        loaded=load(path);row.path_values=collectPaths(loaded,'sidecar');
    end
    rows{k}=row;
end
report.sidecars=[rows{:}];
fid=fopen(fullfile(work,'layout-inspection.json'),'w','n','UTF-8');assert(fid>=0);
closer=onCleanup(@() fclose(fid)); %#ok<NASGU>
fprintf(fid,'%s\n',jsonencode(report,PrettyPrint=true));
end
function rows=collectPaths(value,location)
rows=struct('field',{},'values',{});
if isstruct(value)
    fields=fieldnames(value);
    for i=1:numel(value)
        for k=1:numel(fields)
            field=fields{k};child=value(i).(field);path=[location '.' field];
            if contains(lower(field),{'path','file','dir','source','src','root','folder','uri'})
                strings=flattenText(child);
                strings=strings(startsWith(strings,'/') | ~cellfun(@isempty,regexp(strings,'^[A-Za-z]:[\\/]','once')));
                if ~isempty(strings),rows(end+1)=struct('field',path,'values',{unique(strings,'stable')});end %#ok<AGROW>
            elseif isstruct(child)
                rows=[rows collectPaths(child,path)]; %#ok<AGROW>
            end
        end
    end
end
end
function texts=flattenText(value)
texts={};
if ischar(value),texts={value};elseif isstring(value),texts=cellstr(value(:));
elseif iscell(value)
    for k=1:numel(value),texts=[texts flattenText(value{k})];end %#ok<AGROW>
end
end
