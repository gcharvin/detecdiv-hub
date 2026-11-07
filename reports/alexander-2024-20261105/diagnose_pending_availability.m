function diagnose_pending_availability(work,release)
% Diagnose only the two unresolved variants, without saving scientific data.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'request.json')));assert(request.read_only);
fid=fopen(fullfile(work,'references.jsonl'),'w','n','UTF-8');assert(fid>=0);cleanup=onCleanup(@() fclose(fid));rows=cell(1,numel(request.projects));
for k=1:numel(request.projects)
    if iscell(request.projects),item=request.projects{k};else,item=request.projects(k);end
    assert(any(strcmp(item.name,{'movie_250124_diploids','anof_290424_loxP15'})));
    file=fullfile(item.stage,[item.name '.mat']);assert(strcmp(fileHash(file),item.mat_sha256)&&strcmp(fileHash(item.source_mat),item.mat_sha256));
    loaded=load(file,'shallowObj');project=loaded.shallowObj;fovs=cell(1,numel(project.fov));
    for f=1:numel(project.fov)
        v=project.fov(f);channels=cell(1,numel(v.srcpath));
        for ch=1:numel(v.srcpath)
            old=char(v.srcpath{ch});normalized=regexprep(strrep(old,'\','/'),'/+','/');rel='';
            prefix=['Z:/Alexander/data/' item.acquisition '/'];
            if startsWith(normalized,prefix),rel=[item.acquisition '/' normalized(numel(prefix)+1:end)];end
            alternate=['Z:/Alexander/' item.acquisition '/'];
            if startsWith(normalized,alternate),rel=[item.acquisition '/' normalized(numel(alternate)+1:end)];end
            if startsWith(normalized,['/data/Alexander/data/2024_1/' item.acquisition '/'])
                rel=normalized(numel('/data/Alexander/data/2024_1/')+1:end);
            end
            directory=fullfile('/data/Alexander/data/2024_1',rel);exists=~isempty(rel)&&isfolder(directory);
            listing=v.srclist{ch};missing=0;different=0;offsets=[];
            actual=dir(directory);names=containers.Map('KeyType','char','ValueType','double');
            for z=1:numel(actual),if ~actual(z).isdir,names(actual(z).name)=z;end;end
            for t=1:numel(listing)
                name=listing(t).name;assert(isempty(regexp(name,'[/\\]','once')));present=exists&&isKey(names,name);bytes=[];stamp=[];
                if present,a=actual(names(name));bytes=a.bytes;stamp=a.datenum;offsets(end+1)=round((listing(t).datenum-stamp)*86400);end %#ok<AGROW>
                mismatch=present&&bytes~=listing(t).bytes;different=different+mismatch;missing=missing+~present;
                fprintf(fid,'%s\n',jsonencode(struct('project_id',item.project_id,'fov',f,'channel',ch,'frame',t,'year','2024_1','path',[rel '/' name], ...
                    'saved_bytes',listing(t).bytes,'saved_datenum',listing(t).datenum,'present',present,'actual_bytes',bytes,'actual_datenum',stamp,'size_mismatch',mismatch)));
            end
            channels{ch}=struct('old_path',old,'resolved_relative_path',rel,'directory_present',exists,'entries',numel(listing),'missing',missing, ...
                'size_mismatches',different,'timestamp_offsets_seconds',unique(offsets),'listing_class',class(listing));
        end
        paths={v.roi.path};embedded=arrayfun(@(a) ~isempty(a.image),v.roi);
        fovs{f}=struct('fov',f,'rois',numel(v.roi),'roi_paths',{unique(paths)},'embedded_images',sum(embedded),'channels',{channels}, ...
            'raw_pixels_not_replayed',true,'previously_verified_fov_reused',strcmp(item.name,'anof_290424_loxP15')&&f==1);
        writeJson(fullfile(work,'progress.json'),struct('project',item.name,'fov',f,'fovs',numel(project.fov),'saved',false));
    end
    assert(strcmp(fileHash(file),item.mat_sha256)&&strcmp(fileHash(item.source_mat),item.mat_sha256));
    rows{k}=struct('project_id',item.project_id,'name',item.name,'mat_sha256_unchanged',item.mat_sha256,'fovs',{fovs},'saved',false);
    writeJson(fullfile(work,'result.partial.json'),rows(1:k));clear loaded project;
end
clear cleanup;
writeJson(fullfile(work,'result.json'),struct('success',true,'projects',{rows},'read_only',true,'scientific_files_saved',false,'source_deleted',false, ...
    'note','Metadata diagnosis only; no strict identity gate waived, no raw pixel FOV replay, native initial/current source and destination parity still required for any historical baseline.'));
end
function out=fileHash(path)
md=java.security.MessageDigest.getInstance('SHA-256');f=fopen(path,'rb');assert(f>=0);c=onCleanup(@() fclose(f));
while true,b=fread(f,4*1024*1024,'*uint8');if isempty(b),break;end;md.update(b);end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function writeJson(path,value)
p=[path '.tmp'];f=fopen(p,'w','n','UTF-8');assert(f>=0);c=onCleanup(@() fclose(f));fprintf(f,'%s\n',jsonencode(value));clear c;movefile(p,path,'f');
end
