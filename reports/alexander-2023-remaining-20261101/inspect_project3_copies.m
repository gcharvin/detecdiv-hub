function inspect_project3_copies(work,release)
% Read-only preparation. No project save, relink, catalog change or deletion.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'project3-readonly-request.json')));
rows=cell(1,numel(request.projects));
for k=1:numel(request.projects)
    item=request.projects(k);stem=item.name;
    file=fullfile(item.stage,[stem '.mat']);
    prior=fileHash(file);assert(strcmp(prior,item.mat_sha256));
    sourcePrior=fileHash(item.source_mat);assert(strcmp(sourcePrior,prior));
    s=load(file);assert(isfield(s,'shallowObj'));p=s.shallowObj;
    assert(numel(p.fov)==item.fovs && strcmp(p.io.file,stem));
    expected=containers.Map('KeyType','char','ValueType','logical');
    names=item.verified_file_paths;if isstring(names),names=cellstr(names);end
    for n=1:numel(names),expected(names{n})=true;end
    roiPaths={};roiCount=0;dataCount=0;samples=cell(1,numel(p.fov));
    for f=1:numel(p.fov)
        for r=1:numel(p.fov(f).roi)
            a=p.fov(f).roi(r);directory=projectRelative(a.path,stem);
            assert(isKey(expected,[directory '/im_' a.id '.mat']));
            assert(isKey(expected,[directory '/results_' a.id '.mat']));
            dataCount=dataCount+isKey(expected,[directory '/data_' a.id '.mat']);
            roiCount=roiCount+1;roiPaths{end+1}=a.path; %#ok<AGROW>
        end
        a=p.fov(f).roi(1);relative=projectRelative(a.path,stem);
        copied=roi(a.id,a.value);copied.path=fullfile(item.stage,relative);copied.load('Silent');
        original=roi(a.id,a.value);original.path=fullfile('/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis',relative);original.load('Silent');
        assert(~isempty(copied.image) && isequaln(copied.image,original.image));
        assert(isequaln(copied.channelid,original.channelid) && isequaln(copied.results,original.results));
        paths=p.fov(f).srcpath;if ischar(paths)||isstring(paths),paths=cellstr(paths);end
        rawSamples={};cropMatch='not_established';
        for ch=1:numel(paths)
            directory=rawRelative(paths{ch},item.candidate_raw_acquisitions);
            listing=p.fov(f).srclist{ch};assert(~isempty(listing));
            frames=unique([1 ceil(numel(listing)/2) numel(listing)]);
            for t=frames
                name=listing(t).name;assert(isempty(regexp(name,'[/\\]','once')));
                relativeRaw=[directory '/' name];
                primary=fullfile('/data/Alexander/data/2023_3',relativeRaw);
                im=imread(primary);assert(~isempty(im));
                rawSamples{end+1}=struct('channel',ch,'frame',t,'relative_path',relativeRaw, ...
                    'class',class(im),'size',size(im),'pixel_sha256',pixelHash(im)); %#ok<AGROW>
                % Positive exact equality can corroborate an alias. A mismatch
                % is explicitly not an identity conclusion: extraction can
                % apply drift, registration or choose a later start frame.
                if ch==1 && t==1 && numel(a.value)==4 && ndims(im)==2
                    v=round(a.value);x=v(1);y=v(2);w=v(3);h=v(4);
                    if x>=1 && y>=1 && x+w-1<=size(im,2) && y+h-1<=size(im,1)
                        patch=im(y:y+h-1,x:x+w-1);
                        if isequaln(patch,copied.image(:,:,1,1)),cropMatch='exact_xywh_first_frame';end
                    end
                    if x>=1 && y>=1 && x+w<=size(im,2) && y+h<=size(im,1)
                        patch=im(y:y+h,x:x+w);
                        if isequaln(patch,copied.image(:,:,1,1)),cropMatch='exact_inclusive_xywh_first_frame';end
                    end
                end
            end
        end
        samples{f}=struct('fov',f,'roi',a.id,'roi_value',a.value,'roi_relative_directory',relative, ...
            'image_class',class(copied.image),'image_size',size(copied.image),'image_pixel_sha256',pixelHash(copied.image), ...
            'copied_roi_pixels_equal_original',true,'copied_results_equal_original',true, ...
            'first_raw_crop_identity_evidence',cropMatch,'raw_samples',{rawSamples});
        clear copied original im patch;
        writeJson(fullfile(work,'project3-readonly-progress.json'),struct('project',stem,'project_index',k,'fov',f,'fovs',numel(p.fov),'projects_saved',false));
    end
    assert(roiCount==item.rois);
    assert(strcmp(fileHash(file),prior) && strcmp(fileHash(item.source_mat),sourcePrior));
    rows{k}=struct('project_id',item.project_id,'name',stem,'source_mat',item.source_mat,'stage_mat',file, ...
        'mat_sha256_unchanged',prior,'original_mat_sha256_unchanged',sourcePrior,'io',p.io, ...
        'fovs',numel(p.fov),'rois',roiCount,'roi_paths',{unique(roiPaths,'stable')}, ...
        'all_roi_im_results_references_in_verified_copy_manifest',true,'existing_data_sidecars',dataCount, ...
        'samples',{samples},'projects_saved',false,'paths_rewritten',false,'catalog_changed',false);
    writeJson(fullfile(work,'project3-readonly-inspection.partial.json'),rows(1:k));
    clear s p;
end
writeJson(fullfile(work,'project3-readonly-inspection.json'),struct('success',true,'engine_release',release,'projects',{rows}, ...
    'projects_saved',false,'paths_rewritten',false,'catalog_changed',false,'source_deleted',false, ...
    'note','Read-only preparation; final raw checksum, alias review, relink, save/reopen and Linux/Windows gates remain required.'));
end

function relative=projectRelative(value,stem)
value=regexprep(strrep(char(value),'\','/'),'/+$','');
roots={['Z:/Alexander/analysis/' stem],['/DataVault/Alexander/analysis/' stem], ...
    ['/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis/' stem], ...
    ['//10.20.8.250/Sauvegarde/Alexander/analysis/' stem]};
relative='';
for k=1:numel(roots)
    root=roots{k};
    if strcmp(value,root)||startsWith(value,[root '/'])
        relative=[stem value(numel(root)+1:end)];break
    end
end
assert(~isempty(relative),'Unreviewed project path alias');
assert(isempty(regexp(relative,'(^|/)\.\.(/|$)','once')));
end

function relative=rawRelative(value,acquisitions)
value=regexprep(strrep(char(value),'\','/'),'/+$','');
if ischar(acquisitions)||isstring(acquisitions),acquisitions=cellstr(acquisitions);end
relative='';
for k=1:numel(acquisitions)
    root=['Z:/Alexander/' acquisitions{k}];
    if startsWith(value,[root '/'])
        assert(isempty(relative),'Ambiguous raw alias');relative=[acquisitions{k} value(numel(root)+1:end)];
    end
end
assert(~isempty(relative),'Unreviewed raw alias');
assert(isempty(regexp(relative,'(^|/)\.\.(/|$)','once')));
end

function out=fileHash(path)
md=java.security.MessageDigest.getInstance('SHA-256');
fid=fopen(path,'rb');assert(fid>=0);c=onCleanup(@() fclose(fid));
while true
    data=fread(fid,4*1024*1024,'*uint8');if isempty(data),break;end;md.update(data);
end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end

function out=pixelHash(im)
md=java.security.MessageDigest.getInstance('SHA-256');md.update(typecast(im(:),'uint8'));
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end

function writeJson(path,value)
tmp=[path '.tmp'];fid=fopen(tmp,'w','n','UTF-8');assert(fid>=0);
c=onCleanup(@() fclose(fid));fprintf(fid,'%s\n',jsonencode(value,PrettyPrint=true));clear c;movefile(tmp,path,'f');
end
