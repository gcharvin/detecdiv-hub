function verify_gilles_windows(work,release)
% Read the retained Gilles JSON and verified homes2 raws in the actual session.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
stem='190423_nh_sir2_tween';base='//10.20.11.250/data/Gilles/analysis/projects';
file=fullfile(base,[stem '.json']);before=dir(file);
[p,msg]=shallowLoad(fullfile(base,[stem '.mat']),'ProjectDir',fullfile(base,stem)); %#ok<ASGLU>
assert(isa(p,'shallow') && numel(p.fov)==30);
assert(strcmp(p.projectId,'67556ff2-8555-478c-a957-1c952a4f49d7'));
canonical='/homes2/maliavko/DetecdivHub';unc='//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub';
linux=jsondecode(fileread(fullfile(work,'gilles-json-linux-before-verification.json')));
assert(linux.success && strcmp(linux.phase,'before'));
samples=cell(1,30);roiCount=0;
for f=1:numel(p.fov)
    current=p.fov(f);roiCount=roiCount+numel(current.roi);
    for ch=1:numel(current.srcpath)
        current.srcpath{ch}=mapped(current.srcpath{ch});
        assert(startsWith(current.srcpath{ch},[unc '/raw/']) && isfolder(current.srcpath{ch}));
    end
    for k=1:numel(current.roi)
        if ~isempty(current.roi(k).path)
            current.roi(k).path=mapped(current.roi(k).path);
            assert(startsWith(strrep(current.roi(k).path,'\','/'),[base '/' stem '/']));
        end
    end
    source=current.srcpath{1};pos=regexp(strrep(source,'\','/'),'/Pos(\d+)$','tokens','once');assert(~isempty(pos));
    name=sprintf('img_channel000_position%03d_time000000000_z000.tif',str2double(pos{1}));
    raw=fullfile(source,name);pixels=imread(raw);
    if iscell(linux.samples),b=linux.samples{f};else,b=linux.samples(f);end
    sample=struct('fov',f,'id',current.id,'roiCount',numel(current.roi),'rawPath',raw, ...
        'rawPixelsSha256',digestPixels(pixels),'roiSamplePresent',false);
    assert(strcmp(sample.rawPixelsSha256,b.rawPixelsSha256));
    folder=fullfile(base,stem,current.id);
    files=[dir(fullfile(folder,'im_*.h5'));dir(fullfile(folder,'im_*.mat'))];sample.imageFilesPresent=numel(files);
    assert(numel(files)==b.imageFilesPresent && sample.roiCount==b.roiCount);
    if ~isempty(files)
        [~,imageStem]=fileparts(files(1).name);id=imageStem(4:end);
        idx=find(strcmp({current.roi.id},id),1);assert(~isempty(idx));
        item=current.roi(idx);r=roi(item.id,item.value);r.path=folder;r.load('Silent');
        assert(strcmp(r.path,folder) && ~isempty(r.image));
        sample.roiId=r.id;sample.roiPixelsSha256=digestPixels(r.image);sample.roiSamplePresent=true;
        assert(strcmp(r.id,b.roiId) && strcmp(sample.roiPixelsSha256,b.roiPixelsSha256));clear r;
    end
    assert(sample.roiSamplePresent==b.roiSamplePresent);samples{f}=sample;
    clear pixels;
end
assert(roiCount==15281);after=dir(file);assert(before.bytes==after.bytes && before.datenum==after.datenum);
r=struct('success',true,'project_saved',false,'project_retained_under_gilles',true, ...
    'historical_mat_alias_loads_json',true,'fovs',30,'rois',roiCount,'samples',{samples}, ...
    'partial_existing_results_preserved',true,'engine_release',release);
fid=fopen(fullfile(work,'gilles-json-windows-verification.json'),'w','n','UTF-8');assert(fid>=0);
c=onCleanup(@() fclose(fid));fprintf(fid,'%s\n',jsonencode(r,PrettyPrint=true)); %#ok<NASGU>
    function out=mapped(value)
        out=strrep(char(string(value)),'\','/');
        if startsWith(out,[canonical '/']),out=[unc out(numel(canonical)+1:end)];
        elseif startsWith(out,'/data/'),out=['//10.20.11.250/data/' out(7:end)];end
        assert(~startsWith(out,'/data/Alexander/data/2023_1'),'Stale primary raw source');
    end
end
function out=digestPixels(value)
md=java.security.MessageDigest.getInstance('SHA-256');md.update(typecast(value(:),'int8'));
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
