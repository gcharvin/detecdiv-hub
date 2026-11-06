function diagnose_saved_sizes_v2(work,release)
% Read-only diagnosis of a strict historical srclist-size assertion.
% This does not waive the failed gate, rewrite metadata, or save any project.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'request.json')));item=request.project;
assert(strcmp(item.name,'anof_060224_cres'));
file=fullfile(item.stage,[item.name '.mat']);
assert(strcmp(fileHash(file),item.mat_sha256)&&strcmp(fileHash(item.source_mat),item.mat_sha256));
loaded=load(file,'shallowObj');project=loaded.shallowObj;rows=cell(1,numel(project.fov));
for f=1:numel(project.fov)
    fov=project.fov(f);a=fov.roi(1);
    old=strrep(char(a.path),'\','/');root=['/DataVault/Alexander/analysis/' item.name '/'];
    assert(startsWith(old,root),'Unexpected ROI path in bounded diagnosis');
    extracted=roi(a.id,a.value);extracted.path=fullfile(item.stage,item.name,old(numel(root)+1:end));extracted.load('Silent');
    assert(~isempty(extracted.image));channels=cell(1,numel(fov.srcpath));samples={};cropMatches={};
    for ch=1:numel(fov.srcpath)
        old=strrep(char(fov.srcpath{ch}),'\','/');prefix='Z:/Alexander/data/060224_cres/';
        assert(startsWith(old,prefix),'Unexpected raw path in bounded diagnosis');
        relative=['060224_cres/' old(numel(prefix)+1:end)];directory=fullfile('/data/Alexander/data/2024_1',relative);
        listing=fov.srclist{ch};actual=dir(directory);names=containers.Map('KeyType','char','ValueType','double');
        for n=1:numel(actual),if ~actual(n).isdir,names(actual(n).name)=n;end;end
        mismatches=0;missing={};examples={};offsets=[];
        for t=1:numel(listing)
            name=listing(t).name;assert(isempty(regexp(name,'[/\\]','once')));
            if ~isKey(names,name),missing{end+1}=name;continue;end %#ok<AGROW>
            native=actual(names(name));offsets(end+1)=round((listing(t).datenum-native.datenum)*86400); %#ok<AGROW>
            if native.bytes~=listing(t).bytes
                mismatches=mismatches+1;
                if numel(examples)<5
                    examples{end+1}=struct('relative_path',[relative '/' name],'saved_bytes',listing(t).bytes,'actual_bytes',native.bytes, ...
                        'saved_datenum',listing(t).datenum,'actual_datenum',native.datenum); %#ok<AGROW>
                end
            end
        end
        channels{ch}=struct('channel',ch,'entries',numel(listing),'missing_names',{missing},'size_mismatches',mismatches, ...
            'timestamp_offsets_seconds',unique(offsets),'size_examples',{examples});
        available=find(cellfun(@(name) isKey(names,name),{listing.name}));
        frames=unique([1 ceil(numel(listing)/2) numel(listing) available(1:min(1,numel(available)))]);
        for t=frames
            path=fullfile(directory,listing(t).name);
            if ~isfile(path),continue;end
            info=imfinfo(path);im=imread(path);
            samples{end+1}=struct('channel',ch,'frame',t,'relative_path',[relative '/' listing(t).name], ...
                'compression',info(1).Compression,'pages',numel(info),'width',info(1).Width,'height',info(1).Height, ...
                'bits_per_sample',info(1).BitDepth,'image_class',class(im),'image_size',size(im),'pixel_sha256',pixelHash(im)); %#ok<AGROW>
            if ch==1&&numel(a.value)==4&&ndims(im)==2&&t<=size(extracted.image,4)
                v=round(a.value);x=v(1);y=v(2);w=v(3);h=v(4);
                if x>=1&&y>=1&&x+w-1<=size(im,2)&&y+h-1<=size(im,1)&&isequaln(im(y:y+h-1,x:x+w-1),extracted.image(:,:,1,t))
                    cropMatches{end+1}=['exact_xywh_frame_' num2str(t)]; %#ok<AGROW>
                end
                if x>=1&&y>=1&&x+w<=size(im,2)&&y+h<=size(im,1)&&isequaln(im(y:y+h,x:x+w),extracted.image(:,:,1,t))
                    cropMatches{end+1}=['exact_inclusive_xywh_frame_' num2str(t)]; %#ok<AGROW>
                end
            end
        end
    end
    rows{f}=struct('fov',f,'roi_id',a.id,'roi_value',a.value,'channels',{channels},'samples',{samples},'crop_matches',{cropMatches});
    writeJson(fullfile(work,'progress.json'),struct('fov',f,'fovs',numel(project.fov),'saved',false));
    writeJson(fullfile(work,'result.partial.json'),rows(1:f));clear extracted;
end
assert(strcmp(fileHash(file),item.mat_sha256)&&strcmp(fileHash(item.source_mat),item.mat_sha256));
writeJson(fullfile(work,'result.json'),struct('success',true,'project_id',item.project_id,'name',item.name,'fovs',{rows}, ...
    'mat_sha256_unchanged',item.mat_sha256,'saved',false,'strict_gate_waived',false,'source_deleted',false,'catalog_changed',false));
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
p=[path '.tmp'];f=fopen(p,'w','n','UTF-8');assert(f>=0);c=onCleanup(@() fclose(f));fprintf(f,'%s\n',jsonencode(value));clear c;movefile(p,path,'f');
end
