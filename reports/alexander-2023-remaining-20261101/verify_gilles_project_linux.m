function verify_gilles_project_linux(work,release,phase)
% Read-only validation of the existing JSON project, including partial results.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
assert(any(strcmp(phase,{'before','after'})));
stem='190423_nh_sir2_tween';base='/data/Gilles/analysis/projects';
file=fullfile(base,[stem '.json']);metadataBefore=dir(file);
% Also verify the historical MAT alias resolves to its sibling JSON.
[p,msg]=shallowLoad(fullfile(base,[stem '.mat'])); %#ok<ASGLU>
assert(isa(p,'shallow') && numel(p.fov)==30);
assert(strcmp(p.projectId,'67556ff2-8555-478c-a957-1c952a4f49d7'));
old='/data/Alexander/data/2023_1';
new='/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_1';
if strcmp(phase,'before'),expected=old;else,expected=new;end
rois=0;samples=cell(1,numel(p.fov));
for f=1:numel(p.fov)
    current=p.fov(f);rois=rois+numel(current.roi);
    for ch=1:numel(current.srcpath)
        assert(startsWith(current.srcpath{ch},[expected '/190423_nh_sir2_tween/']));
        assert(isfolder(current.srcpath{ch}));
    end
    path=current.srcpath{1};pos=regexp(path,'/Pos(\d+)$','tokens','once');assert(~isempty(pos));
    name=sprintf('img_channel000_position%03d_time000000000_z000.tif',str2double(pos{1}));
    raw=fullfile(path,name);a=imread(raw);
    rawOld=strrep(raw,new,old);rawNew=strrep(raw,old,new);
    b=imread(rawOld);c=imread(rawNew);assert(isequaln(a,b) && isequaln(a,c));
    sample=struct('fov',f,'id',current.id,'roiCount',numel(current.roi), ...
        'rawPath',raw,'rawClass',class(a),'rawSize',size(a),'rawPixelsSha256',digestPixels(a), ...
        'rawPixelsMatchPrimaryAndHomes2',true,'roiSamplePresent',false,'imageFilesPresent',0);
    assert(~isempty(current.roi));folder=fullfile(base,stem,current.id);
    assert(isfolder(folder));
    % Extraction was interrupted historically: empty ROI.path/status values
    % can coexist with HDF5 files. Keep that metadata and inventory unchanged.
    files=[dir(fullfile(folder,'im_*.h5'));dir(fullfile(folder,'im_*.mat'))];
    sample.imageFilesPresent=numel(files);
    inventory=struct('name',{files.name},'bytes',{files.bytes},'datenum',{files.datenum});
    sample.imageInventorySha256=digestBytes(unicode2native(jsonencode(inventory),'UTF-8'));
    if ~isempty(files)
        [~,imageStem]=fileparts(files(1).name);id=imageStem(4:end);
        idx=find(strcmp({current.roi.id},id),1);assert(~isempty(idx));
        source=current.roi(idx);r=roi(source.id,source.value);r.path=folder;r.load('Silent');
        assert(strcmp(r.path,folder) && ~isempty(r.image));
        sample.roiSamplePresent=true;sample.roiId=r.id;sample.roiClass=class(r.image);
        sample.roiSize=size(r.image);sample.roiPixelsSha256=digestPixels(r.image);
        clear r;
    end
    samples{f}=sample;clear a b c;
    writeJson(fullfile(work,['gilles-json-' phase '-progress.json']),struct('fovs',f,'total',numel(p.fov)));
end
assert(rois==15281);metadataAfter=dir(file);
assert(metadataBefore.bytes==metadataAfter.bytes && metadataBefore.datenum==metadataAfter.datenum);
report=struct('success',true,'phase',phase,'project_saved',false,'project',file, ...
    'projectId',p.projectId,'fovs',numel(p.fov),'rois',rois,'samples',{samples}, ...
    'project_retained_under_gilles',true,'historical_mat_alias_loads_json',true,'engine_release',release);
if strcmp(phase,'after')
    before=jsondecode(fileread(fullfile(work,'gilles-json-before-verification.json')));
    assert(before.success && before.fovs==report.fovs && before.rois==report.rois);
    for f=1:numel(samples)
        if iscell(before.samples),b=before.samples{f};else,b=before.samples(f);end
        a=samples{f};
        assert(strcmp(b.id,a.id) && b.roiCount==a.roiCount && b.imageFilesPresent==a.imageFilesPresent);
        assert(strcmp(b.rawPixelsSha256,a.rawPixelsSha256) && strcmp(b.imageInventorySha256,a.imageInventorySha256));
        assert(b.roiSamplePresent==a.roiSamplePresent);
        if a.roiSamplePresent,assert(strcmp(b.roiPixelsSha256,a.roiPixelsSha256));end
    end
    report.baseline_images_and_raw_reads_unchanged=true;
end
writeJson(fullfile(work,['gilles-json-' phase '-verification.json']),report);
end
function out=digestPixels(value)
out=digestBytes(typecast(value(:),'uint8'));
end
function out=digestBytes(bytes)
md=java.security.MessageDigest.getInstance('SHA-256');md.update(typecast(uint8(bytes(:)),'int8'));
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function writeJson(path,value)
tmp=[path '.tmp'];fid=fopen(tmp,'w','n','UTF-8');assert(fid>=0);
c=onCleanup(@() fclose(fid));fprintf(fid,'%s\n',jsonencode(value,PrettyPrint=true));clear c;
movefile(tmp,path,'f');
end
