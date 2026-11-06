function inspect_gilles_roi_layout(work,release)
% Inspect the pre-existing empty-path variant without inventing sidecars.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
r=jsondecode(fileread(fullfile(work,'request.json')));p=r.project;assert(strcmp(p.name,'281124_Alex'));
file=fullfile(p.stage,[p.name '.mat']);assert(strcmp(fileHash(file),p.mat_sha256)&&strcmp(fileHash(p.source_mat),p.mat_sha256));
loaded=load(file,'shallowObj');project=loaded.shallowObj;rows=cell(1,numel(project.fov));paths=cellstr(string(p.verified_paths));
for f=1:numel(project.fov)
    v=project.fov(f);details=cell(1,numel(v.roi));
    for q=1:numel(v.roi)
        a=v.roi(q);matches=paths(endsWith(paths,['/im_' a.id '.mat']));resultMatches=paths(endsWith(paths,['/results_' a.id '.mat']));
        details{q}=struct('id',a.id,'path',a.path,'value',a.value,'embedded_image_class',class(a.image),'embedded_image_size',size(a.image),'embedded_image_empty',isempty(a.image), ...
            'matching_image_paths',{matches},'matching_result_paths',{resultMatches});
    end
    rows{f}=struct('fov',f,'rois',numel(v.roi),'blank_roi_paths',sum(cellfun(@(a) isempty(a.path),details)),'roi_details',{details});
end
assert(strcmp(fileHash(file),p.mat_sha256)&&strcmp(fileHash(p.source_mat),p.mat_sha256));
out=struct('success',true,'project_id',p.project_id,'name',p.name,'fovs',{rows},'main_hash_unchanged',p.mat_sha256,'io',project.io,'read_only',true,'saved',false,'identity_or_host_gate_passed',false);
f=fopen(fullfile(work,'result.json'),'w','n','UTF-8');assert(f>=0);c=onCleanup(@() fclose(f));fprintf(f,'%s\n',jsonencode(out));
end
function out=fileHash(path)
md=java.security.MessageDigest.getInstance('SHA-256');f=fopen(path,'rb');assert(f>=0);c=onCleanup(@() fclose(f));
while true,b=fread(f,4*1024*1024,'*uint8');if isempty(b),break;end;md.update(b);end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
