function inspect_movie_cache_variant(work,release)
% Read-only identity/path/pixel preparation on fully hash-verified project copies.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'request.json')));assert(request.read_only&&numel(request.projects)==1);
assert(strcmp(fileHash(request.baseline_reference_file),request.baseline_reference_sha256));
baseline=containers.Map('KeyType','char','ValueType','any');lines=readlines(request.baseline_reference_file);
for z=1:numel(lines),if strlength(lines(z))==0,continue;end;b=jsondecode(char(lines(z)));if strcmp(b.project_id,request.projects.project_id),key=sprintf('%d/%d/%d',b.fov,b.channel,b.frame);assert(~isKey(baseline,key)&&b.present&&b.size_mismatch);baseline(key)=b;end;end
assert(baseline.Count==6396);rows=cell(1,numel(request.projects));
for k=1:numel(request.projects)
    if iscell(request.projects),item=request.projects{k};else,item=request.projects(k);end
    assert(isstruct(item)&&isscalar(item)&&strcmp(item.name,'movie_250124_diploids'));
    try
    stem=item.name;file=fullfile(item.stage,[stem '.mat']);
    assert(strcmp(fileHash(file),item.mat_sha256)&&strcmp(fileHash(item.source_mat),item.mat_sha256));
    loaded=load(file,'shallowObj');project=loaded.shallowObj;
    expected=containers.Map('KeyType','char','ValueType','logical');
    paths=cellstr(string(item.verified_paths));
    for n=1:numel(paths),expected(paths{n})=true;end
    fovs=cell(1,numel(project.fov));rois=0;dataCount=0;
    for f=1:numel(project.fov)
        fov=project.fov(f);rawPaths=cellstr(string(fov.srcpath));samples={};channels=cell(1,numel(rawPaths));candidateIndex=0;availableImages=0;missingImages=0;availableResults=0;missingResults=0;blankRois=0;availability=cell(1,numel(fov.roi));
        for r=1:numel(fov.roi)
            a=fov.roi(r);
            if isempty(a.path)
                assert(isempty(a.image),'Empty-path embedded image requires separate coverage');blankRois=blankRois+1;rois=rois+1;
                availability{r}=struct('roi_id',a.id,'original_path_empty',true,'original_image_empty',true,'source_layout_preserved',true);continue;
            end
            relative=roiRelative(a.path,stem);
            mat=[relative '/im_' a.id '.mat'];hdf=[relative '/im_' a.id '.h5'];hasMat=isKey(expected,mat);hasHdf=isKey(expected,hdf);
            assert(~(hasMat&&hasHdf),'Ambiguous image format requires review');
            hasImage=hasMat||hasHdf;hasResult=isKey(expected,[relative '/results_' a.id '.mat']);
            availableImages=availableImages+hasImage;missingImages=missingImages+~hasImage;availableResults=availableResults+hasResult;missingResults=missingResults+~hasResult;
            if ~hasImage,assert(isempty(a.image),'Embedded-image variant requires a separate reader');end
            placeholder=false;
            if hasImage
                imagePath=mat;if hasHdf,imagePath=hdf;end
                index=find(strcmp({item.verified_files.path},imagePath),1);assert(~isempty(index));
                placeholder=hasMat&&item.verified_files(index).bytes<=128;
                if ~placeholder&&candidateIndex==0,candidateIndex=r;end
            end
            availability{r}=struct('roi_id',a.id,'relative_directory',relative,'image_present_in_source_manifest',hasImage,'image_is_empty_placeholder',placeholder,'results_present_in_source_manifest',hasResult);
            dataCount=dataCount+isKey(expected,[relative '/data_' a.id '.mat']);rois=rois+1;
        end
        extracted=[];cropMatch='not_applicable_no_saved_roi_images';
        if candidateIndex>0
            a=fov.roi(candidateIndex);relative=roiRelative(a.path,stem);extracted=roi(a.id,a.value);extracted.path=fullfile(item.stage,relative);extracted.load('Silent');
            assert(~isempty(extracted.image),'Representative actual saved ROI image unreadable');cropMatch='not_established';
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
                actual=native(names(name));key=sprintf('%d/%d/%d',f,ch,t);assert(isKey(baseline,key));b=baseline(key);
                assert(strcmp(b.path,[relative '/' name])&&b.saved_bytes==listing(t).bytes&&b.saved_datenum==listing(t).datenum);
                assert(actual.bytes==b.actual_bytes&&actual.datenum==b.actual_datenum,'Raw native baseline changed');
                offsets(t)=round((listing(t).datenum-actual.datenum)*86400);
            end
            uniqueOffsets=unique(offsets);
            frames=unique([1 ceil(numel(listing)/2) numel(listing)]);
            for t=frames
                rawFile=fullfile(directory,listing(t).name);im=imread(rawFile);assert(~isempty(im));
                samples{end+1}=struct('channel',ch,'frame',t,'year',year,'relative_path',[relative '/' listing(t).name], ...
                    'class',class(im),'size',size(im),'pixel_sha256',pixelHash(im)); %#ok<AGROW>
                if ch==1&&t==1&&~isempty(extracted)&&numel(a.value)==4&&ndims(im)==2
                    v=round(a.value);x=v(1);y=v(2);w=v(3);h=v(4);
                    if x>=1&&y>=1&&x+w-1<=size(im,2)&&y+h-1<=size(im,1)
                        expectedImage=extracted.image(:,:,1,1);if isa(expectedImage,'gpuArray'),expectedImage=gather(expectedImage);end
                        if isequaln(im(y:y+h-1,x:x+w-1),expectedImage),cropMatch='exact_xywh_first_frame';end
                    end
                    if x>=1&&y>=1&&x+w<=size(im,2)&&y+h<=size(im,1)
                        expectedImage=extracted.image(:,:,1,1);if isa(expectedImage,'gpuArray'),expectedImage=gather(expectedImage);end
                        if isequaln(im(y:y+h,x:x+w),expectedImage),cropMatch='exact_inclusive_xywh_first_frame';end
                    end
                end
                clear im;
            end
            channels{ch}=struct('old_path',rawPaths{ch},'year',year,'relative_path',relative,'saved_entries',numel(listing), ...
                'native_reference_baseline_preserved',true,'strict_saved_cache_gate_passed',false,'timestamp_offsets_seconds',uniqueOffsets);
        end
        assert(~strcmp(cropMatch,'not_established'),'Actual extracted ROI crop requires review');
        fovs{f}=struct('unextracted_empty_path_rois',blankRois,'available_images',availableImages,'missing_image_refs_at_source',missingImages,'available_results',availableResults,'missing_result_refs_at_source',missingResults,'availability',{availability},'fov',f,'channels',{channels},'rois',numel(fov.roi),'samples',{samples},'crop_match',cropMatch);
        writeJson(fullfile(work,'progress.json'),struct('project',stem,'project_index',k,'projects',numel(request.projects),'fov',f,'fovs',numel(project.fov),'saved',false));
        writeJson(fullfile(work,sprintf('project-%02d.partial.json',k)),fovs(1:f));
        clear extracted;
    end
    assert(strcmp(fileHash(file),item.mat_sha256)&&strcmp(fileHash(item.source_mat),item.mat_sha256));
    rows{k}=struct('project_id',item.project_id,'name',stem,'mat_sha256_unchanged',item.mat_sha256,'fovs',numel(project.fov), ...
        'rois',rois,'existing_data_sidecars',dataCount,'current_source_availability_baseline_recorded',true,'missing_sidecars_not_invented',true,'fov_details',{fovs},'saved',false,'success',true);
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
    'strict_saved_cache_gate_passed',false,'raw_full_checksum_gate_passed',false,'note','Historical cache-qualified raw identity verification. Expected missing sidecars are recorded from the fully verified source manifests, not silently treated as readable or restored. Final hosts must prove the same baseline; full raw checksum and scientific relink gates remain.'));
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
