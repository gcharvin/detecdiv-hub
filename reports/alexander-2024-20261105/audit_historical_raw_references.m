function audit_historical_raw_references(work,release)
% Read-only complete saved/current reference baseline for two reviewed projects.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'request.json')));assert(request.read_only);
receipt=fullfile(work,'references.jsonl');assert(~isfile(receipt));fid=fopen(receipt,'w','n','UTF-8');assert(fid>=0);
cleanup=onCleanup(@() fclose(fid));rows=cell(1,numel(request.projects));
for k=1:numel(request.projects)
    item=request.projects(k);assert(any(strcmp(item.name,{'anof_060224_cres','anof_060224_cres_ARS1'})));
    file=fullfile(item.stage,[item.name '.mat']);assert(strcmp(fileHash(file),item.mat_sha256)&&strcmp(fileHash(item.source_mat),item.mat_sha256));
    loaded=load(file,'shallowObj');project=loaded.shallowObj;fovs=cell(1,numel(project.fov));
    expected=cellstr(string(item.verified_paths));
    for f=1:numel(project.fov)
        v=project.fov(f);missing=0;different=0;count=0;channels=cell(1,numel(v.srcpath));samples={};crop='not_sampled';
        % No replay of the three previously sampled cres fields; ARS1 is new.
        sample=strcmp(item.name,'anof_060224_cres_ARS1');extracted=[];
        if sample
            for q=1:numel(v.roi)
                a=v.roi(q);old=strrep(char(a.path),'\','/');prefix=['/DataVault/Alexander/analysis/' item.name '/'];assert(startsWith(old,prefix));
                rel=[item.name '/' old(numel(prefix)+1:end)];assert(ismember([rel '/im_' a.id '.mat'],expected)&&ismember([rel '/results_' a.id '.mat'],expected));
            end
            a=v.roi(1);old=strrep(char(a.path),'\','/');extracted=roi(a.id,a.value);extracted.path=fullfile(item.stage,item.name,old(numel(prefix)+1:end));extracted.load('Silent');assert(~isempty(extracted.image));
        end
        for ch=1:numel(v.srcpath)
            old=strrep(char(v.srcpath{ch}),'\','/');prefix='Z:/Alexander/data/060224_cres/';assert(startsWith(old,prefix));
            rel=['060224_cres/' old(numel(prefix)+1:end)];directory=fullfile('/data/Alexander/data/2024_1',rel);actual=dir(directory);names=containers.Map('KeyType','char','ValueType','double');
            for z=1:numel(actual),if ~actual(z).isdir,names(actual(z).name)=z;end;end
            listing=v.srclist{ch};assert(~isempty(listing));offsets=[];cmissing=0;cdifferent=0;
            for t=1:numel(listing)
                name=listing(t).name;assert(isempty(regexp(name,'[/\\]','once')));present=isKey(names,name);bytes=[];stamp=[];
                if present,raw=actual(names(name));bytes=raw.bytes;stamp=raw.datenum;offsets(end+1)=round((listing(t).datenum-stamp)*86400);end %#ok<AGROW>
                mismatch=present&&bytes~=listing(t).bytes;cmissing=cmissing+~present;cdifferent=cdifferent+mismatch;
                fprintf(fid,'%s\n',jsonencode(struct('project_id',item.project_id,'fov',f,'channel',ch,'frame',t,'year','2024_1', ...
                    'path',[rel '/' name],'saved_bytes',listing(t).bytes,'saved_datenum',listing(t).datenum,'present',present,'actual_bytes',bytes,'actual_datenum',stamp,'size_mismatch',mismatch)));
            end
            channels{ch}=struct('old_path',v.srcpath{ch},'year','2024_1','relative_path',rel,'entries',numel(listing),'missing',cmissing,'size_mismatches',cdifferent,'timestamp_offsets_seconds',unique(offsets));
            missing=missing+cmissing;different=different+cdifferent;count=count+numel(listing);
            if sample
                for t=unique([1 ceil(numel(listing)/2) numel(listing)])
                    rawFile=fullfile(directory,listing(t).name);if ~isfile(rawFile),continue;end
                    im=imread(rawFile);samples{end+1}=struct('channel',ch,'frame',t,'year','2024_1','relative_path',[rel '/' listing(t).name],'class',class(im),'size',size(im),'pixel_sha256',pixelHash(im)); %#ok<AGROW>
                    if ch==1&&t==1
                        bounds=round(a.value);x=bounds(1);y=bounds(2);w=bounds(3);h=bounds(4);
                        assert(x>=1&&y>=1&&x+w-1<=size(im,2)&&y+h-1<=size(im,1));assert(isequaln(im(y:y+h-1,x:x+w-1),extracted.image(:,:,1,1)),'ARS1 first crop requires review');crop='exact_xywh_first_frame';
                    end
                    clear im;
                end
            end
        end
        fovs{f}=struct('fov',f,'rois',numel(v.roi),'references',count,'missing',missing,'size_mismatches',different,'channels',{channels},'samples',{samples},'crop_match',crop);
        writeJson(fullfile(work,'progress.json'),struct('project',item.name,'fov',f,'fovs',numel(project.fov),'saved',false));clear extracted;
    end
    assert(strcmp(fileHash(file),item.mat_sha256)&&strcmp(fileHash(item.source_mat),item.mat_sha256));
    rows{k}=struct('project_id',item.project_id,'name',item.name,'fovs',{fovs},'main_hash_unchanged',item.mat_sha256,'saved',false);
    writeJson(fullfile(work,'result.partial.json'),rows(1:k));clear loaded project;
end
clear cleanup;
writeJson(fullfile(work,'result.json'),struct('loop_completed',true,'projects',{rows},'read_only',true,'strict_historical_cache_gate_passed',false,'raw_full_checksum_gate_passed',false,'source_deleted',false));
end
function out=fileHash(path)
md=java.security.MessageDigest.getInstance('SHA-256');f=fopen(path,'rb');assert(f>=0);c=onCleanup(@() fclose(f));
while true,b=fread(f,4*1024*1024,'*uint8');if isempty(b),break;end;md.update(b);end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function out=pixelHash(im)
md=java.security.MessageDigest.getInstance('SHA-256');md.update(typecast(im(:),'uint8'));out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function writeJson(path,value)
p=[path '.tmp'];f=fopen(p,'w','n','UTF-8');assert(f>=0);c=onCleanup(@() fclose(f));fprintf(f,'%s\n',jsonencode(value));clear c;movefile(p,path,'f');
end
