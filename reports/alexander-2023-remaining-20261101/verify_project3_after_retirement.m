function verify_project3_after_retirement(work,release,host)
% Verify real project/ROI/raw readers after the primary duplicates are gone.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'project3-host-request.json')));
assert(request.source_manifest_retired && ~isempty(request.source_retirement_receipt_sha256));
canonical='/homes2/maliavko/DetecdivHub';root=canonical;
if strcmp(host,'windows'),assert(ispc);root='//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub';else,assert(isunix);end
projects=cell(1,numel(request.projects));
for projectIndex=1:numel(request.projects)
    item=request.projects(projectIndex);folder=[root '/projects/' item.name];file=fullfile(folder,[item.name '.mat']);
    assert(strcmp(fileHash(file),item.mat_sha256));node=load(file);p=node.shallowObj;
    assert(strcmp(strrep(p.io.path,'\','/'),[canonical '/projects/' item.name]));p.io.path=folder;
    assert(numel(p.fov)==item.fovs);rawReads=0;roiReads=0;
    for fovIndex=1:numel(p.fov)
        sample=item.baseline_samples(fovIndex);
        paths=p.fov(fovIndex).srcpath;
        for channelIndex=1:numel(paths)
            path=strrep(paths{channelIndex},'\','/');
            assert(startsWith(path,[canonical '/raw/.incoming-alexander-2023-20261008/2023_3/']));
            p.fov(fovIndex).srcpath{channelIndex}=mapped(path);
            list=p.fov(fovIndex).srclist{channelIndex};
            if isfield(list,'folder')
                for listIndex=1:numel(list),list(listIndex).folder=mapped(list(listIndex).folder);end
            end
            p.fov(fovIndex).srclist{channelIndex}=list;
        end
        pixels=p.fov(fovIndex).readImage(1,1);
        entries=sample.raw_samples;index=find([entries.channel]==1 & [entries.frame]==1,1);assert(~isempty(index));
        assert(strcmp(pixelHash(pixels),entries(index).pixel_sha256));rawReads=rawReads+1;
        current=p.fov(fovIndex).roi(1);current.path=mapped(current.path);
        assert(isfile(fullfile(current.path,['im_' current.id '.mat'])) && isfile(fullfile(current.path,['results_' current.id '.mat'])));
        if fovIndex==1
            a=roi(current.id,current.value);a.path=current.path;a.load('Silent');
            assert(strcmp(pixelHash(a.image),sample.image_pixel_sha256));roiReads=roiReads+1;clear a;
        end
        clear pixels;
    end
    assert(strcmp(fileHash(file),item.mat_sha256));
    projects{projectIndex}=struct('project_id',item.project_id,'name',item.name,'fovs',item.fovs, ...
        'raw_reads_verified',rawReads,'roi_image_reads_verified',roiReads,'main_mat_unchanged',true,'project_saved',false);
    writeJson(fullfile(work,['post-' host '-progress.json']),struct('project',item.name,'project_index',projectIndex));
    clear node p;
end
writeJson(fullfile(work,['post-' host '-verification.json']),struct('success',true,'host',host,'projects',{projects}, ...
    'source_retirement_receipt_sha256',request.source_retirement_receipt_sha256,'project_saved',false));
    function out=mapped(value)
        value=strrep(char(value),'\','/');assert(strcmp(value,canonical)||startsWith(value,[canonical '/']));
        out=[root value(numel(canonical)+1:end)];
    end
end
function out=fileHash(path)
md=java.security.MessageDigest.getInstance('SHA-256');fid=fopen(path,'rb');assert(fid>=0);c=onCleanup(@() fclose(fid));
while true,data=fread(fid,4*1024*1024,'*uint8');if isempty(data),break;end;md.update(data);end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function out=pixelHash(value)
md=java.security.MessageDigest.getInstance('SHA-256');md.update(typecast(value(:),'uint8'));
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function writeJson(path,value)
tmp=[path '.tmp'];fid=fopen(tmp,'w','n','UTF-8');assert(fid>=0);c=onCleanup(@() fclose(fid));
fprintf(fid,'%s\n',jsonencode(value,PrettyPrint=true));clear c;movefile(tmp,path,'f');
end
