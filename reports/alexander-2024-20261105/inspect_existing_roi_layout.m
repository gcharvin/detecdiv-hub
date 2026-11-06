function inspect_existing_roi_layout(work,release)
% Readonly diagnosis of explicitly bounded missing/partial derived sidecars.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'request.json')));assert(request.read_only);rows=cell(1,numel(request.projects));
for k=1:numel(request.projects)
    item=request.projects(k);assert(any(strcmp(item.name,{'anof_230124_RMng','anof_250724_dbud4stest','anof_290424_loxP15'})));
    assert(strcmp(fileHash(item.file),item.main_sha256));loaded=load(item.file,'shallowObj');project=loaded.shallowObj;
    paths=cellstr(string(item.verified_paths));if isempty(item.verified_paths),paths={};end
    fovs=cell(1,numel(project.fov));
    for f=1:numel(project.fov)
        v=project.fov(f);details=cell(1,numel(v.roi));
        for q=1:numel(v.roi)
            a=v.roi(q);im=paths(endsWith(paths,{['/im_' a.id '.mat'],['/im_' a.id '.h5']}));
            results=paths(endsWith(paths,['/results_' a.id '.mat']));data=paths(endsWith(paths,['/data_' a.id '.mat']));
            details{q}=struct('id',a.id,'path',a.path,'embedded_image_empty',isempty(a.image),'embedded_image_class',class(a.image),'embedded_image_size',size(a.image), ...
                'verified_image_files',{im},'verified_result_files',{results},'verified_data_files',{data});
        end
        fovs{f}=struct('fov',f,'rois',numel(v.roi),'raw_paths',{v.srcpath},'roi_details',{details}, ...
            'embedded_image_count',sum(cellfun(@(a) ~a.embedded_image_empty,details)), ...
            'verified_image_count',sum(cellfun(@(a) numel(a.verified_image_files),details)), ...
            'verified_result_count',sum(cellfun(@(a) numel(a.verified_result_files),details)));
        writeJson(fullfile(work,'progress.json'),struct('project',item.name,'fov',f,'fovs',numel(project.fov),'saved',false));
    end
    assert(strcmp(fileHash(item.file),item.main_sha256));
    rows{k}=struct('project_id',item.project_id,'name',item.name,'main_hash_unchanged',item.main_sha256,'fovs',{fovs},'original_saved',false,'copy_or_identity_gate_passed',false);
    writeJson(fullfile(work,'result.partial.json'),rows(1:k));clear loaded project;
end
writeJson(fullfile(work,'result.json'),struct('success',true,'projects',{rows},'read_only',true,'scientific_saved',false,'source_deleted',false));
end
function out=fileHash(path)
md=java.security.MessageDigest.getInstance('SHA-256');f=fopen(path,'rb');assert(f>=0);c=onCleanup(@() fclose(f));
while true,b=fread(f,4*1024*1024,'*uint8');if isempty(b),break;end;md.update(b);end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function writeJson(path,value)
p=[path '.tmp'];f=fopen(p,'w','n','UTF-8');assert(f>=0);c=onCleanup(@() fclose(f));fprintf(f,'%s\n',jsonencode(value));clear c;movefile(p,path,'f');
end
