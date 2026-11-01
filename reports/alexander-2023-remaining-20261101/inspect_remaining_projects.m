function inspect_remaining_projects(work,release)
% Read-only examination of existing project copies and known source aliases.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'project-inspection-request.json')));
rows=cell(1,numel(request));
for k=1:numel(request)
    item=request(k);row=struct('id',item.id,'file',item.file,'exists',isfile(item.file),'project_saved',false);
    if row.exists
        info=whos('-file',item.file);index=find(strcmp({info.class},'shallow'),1);
        assert(~isempty(index),'No shallow project in candidate MAT');
        s=load(item.file,info(index).name);p=s.(info(index).name);
        roiCount=0;roiPaths={};paths={};
        for f=1:numel(p.fov)
            roiCount=roiCount+numel(p.fov(f).roi);
            for r=1:numel(p.fov(f).roi),roiPaths{end+1}=p.fov(f).roi(r).path;end %#ok<AGROW>
            values=p.fov(f).srcpath;if ischar(values)||isstring(values),values=cellstr(values);end
            paths=[paths values]; %#ok<AGROW>
        end
        row.variable=info(index).name;row.io=p.io;row.fovs=numel(p.fov);row.rois=roiCount;
        row.roi_paths=unique(roiPaths,'stable');row.raw_paths=unique(paths,'stable');
        row.parsed_data_class=class(p.parsedData);row.parsed_data_empty=isempty(p.parsedData);
        row.processing_fields=fieldnames(p.processing);
        if ~isempty(p.fov)&&~isempty(p.fov(1).roi),a=p.fov(1).roi(1);row.roi_id=a.id;end
        clear s p;
    end
    rows{k}=row;
end
fid=fopen(fullfile(work,'project-inspection.json'),'w','n','UTF-8');assert(fid>=0);
c=onCleanup(@() fclose(fid));fprintf(fid,'%s\n',jsonencode(rows,PrettyPrint=true)); %#ok<NASGU>
end
