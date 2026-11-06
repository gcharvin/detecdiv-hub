function inspect_verified_projects_v2(work,release)
% Read-only identity/path/pixel preparation on fully hash-verified project copies.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'request.json')));rows=cell(1,numel(request.projects));
for k=1:numel(request.projects)
    item=request.projects(k);
    try
    stem=item.name;file=fullfile(item.stage,[stem '.mat']);
    assert(strcmp(fileHash(file),item.mat_sha256)&&strcmp(fileHash(item.source_mat),item.mat_sha256));
    loaded=load(file,'shallowObj');project=loaded.shallowObj;
    expected=containers.Map('KeyType','char','ValueType','logical');
    paths=cellstr(string(item.verified_paths));
    for n=1:numel(paths),expected(paths{n})=true;end
    fovs=cell(1,numel(project.fov));rois=0;dataCount=0;
    for f=1:numel(project.fov)
        fov=project.fov(f);rawPaths=cellstr(string(fov.srcpath));samples={};channels=cell(1,numel(rawPaths));
        for r=1:numel(fov.roi)
            a=fov.roi(r);relative=roiRelative(a.path,stem);
            assert(isKey(expected,[relative '/im_' a.id '.mat']),'ROI image absent from verified source/copy manifest');
            assert(isKey(expected,[relative '/results_' a.id '.mat']),'ROI results absent from verified source/copy manifest');
            dataCount=dataCount+isKey(expected,[relative '/data_' a.id '.mat']);rois=rois+1;
        end
        extracted=[];cropMatch='not_established';
        if ~isempty(fov.roi)
            a=fov.roi(1);relative=roiRelative(a.path,stem);extracted=roi(a.id,a.value);
            extracted.path=fullfile(item.stage,relative);extracted.load('Silent');
            assert(~isempty(extracted.image),'Representative copied ROI image is unreadable');
        end
        for ch=1:numel(rawPaths)
            [year,relative]=rawRelative(rawPaths{ch},request.acquisitions);
            directory=fullfile('/data/Alexander/data',year,relative);assert(isfolder(directory));
            listing=fov.srclist{ch};assert(~isempty(listing));native=dir(directory);
            names=containers.Map('KeyType','char','ValueType','double');
            for n=1:numel(native)
                if ~native(n).isdir,names(native(n).name)=n;end
            end
            offsets=zeros(1,numel(listing));
            for t=1:numel(listing)
                name=listing(t).name;assert(isempty(regexp(name,'[/\\]','once'))&&isKey(names,name),'Raw filename missing');
                actual=native(names(name));assert(actual.bytes==listing(t).bytes,'Raw size differs from saved srclist');
                offsets(t)=round((listing(t).datenum-actual.datenum)*86400);
            end
            uniqueOffsets=unique(offsets);assert(numel(uniqueOffsets)==1,'Saved/native raw timestamps require explicit review');
            frames=unique([1 ceil(numel(listing)/2) numel(listing)]);
            for t=frames
                rawFile=fullfile(directory,listing(t).name);im=imread(rawFile);assert(~isempty(im));
                samples{end+1}=struct('channel',ch,'frame',t,'year',year,'relative_path',[relative '/' listing(t).name], ...
                    'class',class(im),'size',size(im),'pixel_sha256',pixelHash(im)); %#ok<AGROW>
                if ch==1&&t==1&&~isempty(extracted)&&numel(a.value)==4&&ndims(im)==2
                    v=round(a.value);x=v(1);y=v(2);w=v(3);h=v(4);
                    if x>=1&&y>=1&&x+w-1<=size(im,2)&&y+h-1<=size(im,1)
                        if isequaln(im(y:y+h-1,x:x+w-1),extracted.image(:,:,1,1)),cropMatch='exact_xywh_first_frame';end
                    end
                    if x>=1&&y>=1&&x+w<=size(im,2)&&y+h<=size(im,1)
                        if isequaln(im(y:y+h,x:x+w),extracted.image(:,:,1,1)),cropMatch='exact_inclusive_xywh_first_frame';end
                    end
                end
                clear im;
            end
            channels{ch}=struct('old_path',rawPaths{ch},'year',year,'relative_path',relative,'saved_entries',numel(listing), ...
                'all_names_and_sizes_match',true,'constant_timestamp_offset_seconds',uniqueOffsets);
        end
        fovs{f}=struct('fov',f,'channels',{channels},'rois',numel(fov.roi),'samples',{samples},'crop_match',cropMatch);
        writeJson(fullfile(work,'progress.json'),struct('project',stem,'project_index',k,'projects',numel(request.projects),'fov',f,'fovs',numel(project.fov),'saved',false));
        writeJson(fullfile(work,sprintf('project-%02d.partial.json',k)),fovs(1:f));
        clear extracted;
    end
    assert(strcmp(fileHash(file),item.mat_sha256)&&strcmp(fileHash(item.source_mat),item.mat_sha256));
    rows{k}=struct('project_id',item.project_id,'name',stem,'mat_sha256_unchanged',item.mat_sha256,'fovs',numel(project.fov), ...
        'rois',rois,'existing_data_sidecars',dataCount,'all_roi_image_results_in_verified_manifest',true,'fov_details',{fovs},'saved',false,'success',true);
    clear loaded project;
    catch failure
        unchanged=false;
        try
            unchanged=strcmp(fileHash(fullfile(item.stage,[item.name '.mat'])),item.mat_sha256)&&strcmp(fileHash(item.source_mat),item.mat_sha256);
        catch
        end
        partial=fullfile(work,sprintf('project-%02d.partial.json',k));completedFovs=0;
        if isfile(partial),completedFovs=numel(jsondecode(fileread(partial)));end
        rows{k}=struct('project_id',item.project_id,'name',item.name,'success',false,'error',failure.message, ...
            'completed_fovs_in_partial',completedFovs,'main_original_and_copy_hashes_unchanged',unchanged,'saved',false);
        clear loaded project;
    end
    writeJson(fullfile(work,'result.partial.json'),rows(1:k));
end
writeJson(fullfile(work,'result.json'),struct('success',all(cellfun(@(r) r.success,rows)),'loop_completed',true,'projects',{rows},'saved',false,'catalog_changed',false,'source_deleted',false, ...
    'note','Read-only path/identity preparation; raw full SHA1, scientific relink equivalence, final Linux/Windows/API and retirement gates remain mandatory.'));
end

function value=normalize(value)
value=regexprep(strrep(char(value),'\','/'),'/+$','');assert(isempty(regexp(value,'(^|/)\.\.(/|$)','once')));
end

function relative=roiRelative(path,stem)
value=normalize(path);relative='';
roots={'Z:/Alexander/analysis','/DataVault/Alexander/analysis','/Volumes/DATA/Alexander/analysis', ...
    '/data/Alexander/analysis','/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis'};
for k=1:numel(roots)
    prefix=[roots{k} '/' stem];
    if strcmp(value,prefix)||startsWith(value,[prefix '/']),assert(isempty(relative));relative=[stem value(numel(prefix)+1:end)];end
end
assert(~isempty(relative),'Unreviewed ROI path prefix');
end

function [year,relative]=rawRelative(path,acquisitions)
value=normalize(path);year='';relative='';
for k=1:numel(acquisitions)
    item=acquisitions(k);
    roots={['Z:/Alexander/' item.name],['Z:/Alexander/data/' item.name], ...
        ['/data/Alexander/data/' item.name],['/data/Alexander/data/2024/' item.name], ...
        ['/data/Alexander/data/' item.year '/' item.name],['/Volumes/DATA/Alexander/data/' item.name]};
    for n=1:numel(roots)
        prefix=roots{n};
        if strcmp(value,prefix)||startsWith(value,[prefix '/'])
            assert(isempty(relative),'Ambiguous acquisition alias');year=item.year;relative=[item.name value(numel(prefix)+1:end)];
        end
    end
end
assert(~isempty(relative),'Unreviewed raw path prefix');
end

function out=fileHash(path)
md=java.security.MessageDigest.getInstance('SHA-256');f=fopen(path,'rb');assert(f>=0);c=onCleanup(@() fclose(f));
while true,b=fread(f,4*1024*1024,'*uint8');if isempty(b),break;end;md.update(b);end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end

function out=pixelHash(im)
md=java.security.MessageDigest.getInstance('SHA-256');md.update(typecast(im(:),'uint8'));
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end

function writeJson(path,value)
p=[path '.tmp'];f=fopen(p,'w','n','UTF-8');assert(f>=0);c=onCleanup(@() fclose(f));
fprintf(f,'%s\n',jsonencode(value));clear c;movefile(p,path,'f');
end
