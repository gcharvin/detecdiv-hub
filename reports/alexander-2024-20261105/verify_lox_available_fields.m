function verify_lox_available_fields(work,release)
% Read available odd-position fields only; absent fields remain unresolved.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
q=jsondecode(fileread(fullfile(work,'request.json')));p=q.project;
assert(strcmp(p.name,'anof_290424_loxP15'));
file=fullfile(p.stage,[p.name '.mat']);
assert(strcmp(fileHash(file),p.mat_sha256)&&strcmp(fileHash(p.source_mat),p.mat_sha256));
loaded=load(file,'shallowObj');project=loaded.shallowObj;assert(numel(project.fov)==30);
prior=jsondecode(fileread(q.prior_fov_file));assert(isscalar(prior)&&prior.fov==1);
assert(numel(prior.samples)==12&&numel(prior.channels)==4);
rows=cell(1,15);newSourceSamples=0;targetSamples=0;
for slot=1:15
    f=2*slot-1;fv=project.fov(f);raw=cellstr(string(fv.srcpath));assert(numel(raw)==4);
    channels=cell(1,4);samples={};
    for ch=1:4
        old=regexprep(strrep(raw{ch},'\','/'),'/+$','');
        prefix='Z:/Alexander/data/290424_loxP15/';
        assert(startsWith(old,prefix)&&isempty(regexp(old,'(^|/)\.\.(/|$)','once')));
        relative=['290424_loxP15/' old(numel(prefix)+1:end)];
        assert(endsWith(relative,['/Pos' num2str(f)]));
        source=fullfile('/data/Alexander/data/2024_1',relative);
        target=fullfile('/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105/2024_1',relative);
        listing=fv.srclist{ch};assert(~isempty(listing));
        if f==1
            assert(strcmp(prior.channels(ch).relative_path,relative));
            assert(prior.channels(ch).saved_entries==numel(listing));
            frames=prior.samples([prior.samples.channel]==ch);
            assert(numel(frames)==3);
            for n=1:numel(frames)
                a=frames(n);assert(a.frame>=1&&a.frame<=numel(listing));
                assert(strcmp(a.relative_path,[relative '/' listing(a.frame).name]));
                im=imread(fullfile(target,listing(a.frame).name));
                assert(strcmp(class(im),a.class)&&isequal(size(im),reshape(a.size,1,[]))&&strcmp(pixelHash(im),a.pixel_sha256));
                samples{end+1}=a;targetSamples=targetSamples+1; %#ok<AGROW>
            end
            channels{ch}=prior.channels(ch);
        else
            native=dir(source);names=containers.Map('KeyType','char','ValueType','double');
            for n=1:numel(native),if ~native(n).isdir,names(native(n).name)=n;end,end
            offsets=zeros(1,numel(listing));
            for t=1:numel(listing)
                name=listing(t).name;assert(isempty(regexp(name,'[/\\]','once'))&&isKey(names,name));
                actual=native(names(name));assert(actual.bytes==listing(t).bytes);
                offsets(t)=round((listing(t).datenum-actual.datenum)*86400);
            end
            assert(numel(unique(offsets))==1,'Unreviewed timestamp offsets');
            for t=unique([1 ceil(numel(listing)/2) numel(listing)])
                name=listing(t).name;a=imread(fullfile(source,name));b=imread(fullfile(target,name));
                assert(~isempty(a)&&isequaln(a,b)&&strcmp(class(a),class(b)));
                samples{end+1}=struct('channel',ch,'frame',t,'year','2024_1','relative_path',[relative '/' name], ...
                    'class',class(a),'size',size(a),'pixel_sha256',pixelHash(a)); %#ok<AGROW>
                newSourceSamples=newSourceSamples+1;targetSamples=targetSamples+1;
            end
            channels{ch}=struct('year','2024_1','relative_path',relative,'saved_entries',numel(listing), ...
                'all_names_and_sizes_match',true,'constant_timestamp_offset_seconds',unique(offsets));
        end
    end
    rows{slot}=struct('fov',f,'channels',{channels},'samples',{samples},'prior_source_samples_reused',f==1);
    writeJson(fullfile(work,'progress.json'),struct('available_fovs_complete',slot,'available_fovs',15,'new_source_samples',newSourceSamples,'target_samples',targetSamples,'saved',false));
    writeJson(fullfile(work,'result.partial.json'),rows(1:slot));
end
assert(newSourceSamples==168&&targetSamples==180);
assert(strcmp(fileHash(file),p.mat_sha256)&&strcmp(fileHash(p.source_mat),p.mat_sha256));
writeJson(fullfile(work,'result.json'),struct('available_fields_verification_success',true,'read_only',true, ...
    'available_fovs',15,'missing_fovs_unresolved',2:2:30,'new_source_samples',newSourceSamples,'target_samples',targetSamples, ...
    'prior_fov1_source_samples_reused',12,'main_hashes_unchanged',true,'fov_details',{rows}, ...
    'strict_project_access_gate_passed',false,'cleanup_gate_passed',false,'source_deleted',false,'scientific_files_saved',false));
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
