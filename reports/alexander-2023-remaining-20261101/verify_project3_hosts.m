function verify_project3_hosts(work,release,host)
% Read-only host tests. Translate unpublished project paths only in memory.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
assert((strcmp(host,'windows') && ispc)||(strcmp(host,'linux') && isunix));
request=jsondecode(fileread(fullfile(work,'project3-host-request.json')));
canonical='/homes2/maliavko/DetecdivHub';
if ispc,hostRoot='//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub';else,hostRoot=canonical;end
projectRoot='';incomingRoot='';projects=cell(1,numel(request.projects));
for projectIndex=1:numel(request.projects)
    item=request.projects(projectIndex);stem=item.name;
    projectRoot=[canonical '/projects/' stem];incomingRoot=[hostRoot '/projects/.incoming-' stem '-20261101'];
    file=fullfile(incomingRoot,[stem '.mat']);assert(strcmp(fileHash(file),item.mat_sha256));
    loaded=load(file);p=loaded.shallowObj;
    assert(strcmp(strrep(p.io.path,'\','/'),projectRoot) && strcmp(p.io.file,stem));
    assert(numel(p.fov)==item.fovs);p.io.path=incomingRoot;
    expectedData=containers.Map('KeyType','char','ValueType','logical');
    dataPaths=item.data_sidecar_paths;if isstring(dataPaths),dataPaths=cellstr(dataPaths);end
    if ischar(dataPaths),dataPaths=cellstr(dataPaths);end
    for pathIndex=1:numel(dataPaths),expectedData(dataPaths{pathIndex})=true;end
    rois=0;roiFiles=0;dataFiles=0;rawFiles=0;samples=cell(1,numel(p.fov));
    for fovIndex=1:numel(p.fov)
        sample=item.baseline_samples(fovIndex);assert(sample.fov==fovIndex);
        values=p.fov(fovIndex).srcpath;assert(iscell(values));
        for channelIndex=1:numel(values)
            v=strrep(values{channelIndex},'\','/');
            assert(startsWith(v,[canonical '/raw/.incoming-alexander-2023-20261008/2023_3/']));
            p.fov(fovIndex).srcpath{channelIndex}=mapped(v);assert(isfolder(p.fov(fovIndex).srcpath{channelIndex}));
            list=p.fov(fovIndex).srclist{channelIndex};assert(isstruct(list) && ~isempty(list));
            assert(all(cellfun(@(n) isempty(regexp(n,'[/\\]','once')),{list.name})),'Unexpected absolute srclist name');
            if isfield(list,'folder')
                folders=unique({list.folder});assert(numel(folders)==1);
                assert(strcmp(regexprep(strrep(folders{1},'\','/'),'/+$',''),regexprep(v,'/+$','')));
                [list.folder]=deal(mapped(folders{1}));
            end
            p.fov(fovIndex).srclist{channelIndex}=list;
        end
        for roiIndex=1:numel(p.fov(fovIndex).roi)
            current=p.fov(fovIndex).roi(roiIndex);v=strrep(current.path,'\','/');
            assert(startsWith(v,[projectRoot '/' stem '/']),'ROI path is not canonical for this project');
            relative=v(numel(projectRoot)+2:end); % manifest paths start with stem/FOV
            current.path=mapped(v);assert(isfolder(current.path));
            assert(isfile(fullfile(current.path,['im_' current.id '.mat'])));
            assert(isfile(fullfile(current.path,['results_' current.id '.mat'])));
            rois=rois+1;roiFiles=roiFiles+2;
            dataRelative=[relative '/data_' current.id '.mat'];
            if isKey(expectedData,dataRelative)
                assert(isfile(fullfile(current.path,['data_' current.id '.mat'])));
                dataFiles=dataFiles+1;roiFiles=roiFiles+1;
            end
        end
        current=p.fov(fovIndex).roi(1);assert(strcmp(current.id,sample.roi));
        a=roi(current.id,current.value);a.path=current.path;a.load('Silent');
        assert(strcmp(strrep(a.path,'\','/'),strrep(current.path,'\','/')) && ~isempty(a.image));
        assert(strcmp(class(a.image),sample.image_class) && isequal(size(a.image),double(sample.image_size(:)')));
        assert(strcmp(pixelHash(a.image),sample.image_pixel_sha256),'ROI pixels differ from pre-relink original baseline');
        rawSamples=sample.raw_samples;
        for rawIndex=1:numel(rawSamples)
            entry=rawSamples(rawIndex);
            path=[hostRoot '/raw/.incoming-alexander-2023-20261008/2023_3/' entry.relative_path];
            pixels=imread(path);assert(strcmp(class(pixels),entry.class) && isequal(size(pixels),double(entry.size(:)')));
            assert(strcmp(pixelHash(pixels),entry.pixel_sha256),'Raw pixels differ from original baseline');
            rawFiles=rawFiles+1;
        end
        % Exercise the protected engine's FOV reader, beyond direct imread.
        pixels=p.fov(fovIndex).readImage(1,1);assert(~isempty(pixels));
        first=find([rawSamples.channel]==1 & [rawSamples.frame]==1,1);assert(~isempty(first));
        assert(strcmp(pixelHash(pixels),rawSamples(first).pixel_sha256),'Engine FOV reader differs from original raw baseline');
        samples{fovIndex}=struct('fov',fovIndex,'roi',current.id,'roi_pixels_equal_original_baseline',true, ...
            'raw_pixels_equal_original_baseline',true,'engine_fov_reader_verified',true,'legacy_roi_load_keeps_mapped_path',true);
        writeJson(fullfile(work,['project3-host-' host '-progress.json']),struct('project',stem,'project_index',projectIndex, ...
            'fov',fovIndex,'fovs',item.fovs,'rois_verified',rois,'raw_reads_verified',rawFiles,'project_saved',false));
        clear a pixels;
    end
    assert(rois==item.rois && dataFiles==numel(dataPaths));
    assert(strcmp(fileHash(file),item.mat_sha256),'Read-only host validation modified the shared MAT');
    processorFiles=item.processors;
    for processorIndex=1:numel(processorFiles)
        recipe=processorFiles(processorIndex);path=fullfile(incomingRoot,recipe.relative_path);
        assert(strcmp(fileHash(path),recipe.sha256));node=load(path);assert(isfield(node,'processObj'));
        saved=strrep(node.processObj.path,'\','/');assert(startsWith(saved,[projectRoot '/' stem '/processor/']));
        assert(isfolder(mapped(saved)));assert(strcmp(fileHash(path),recipe.sha256));clear node;
    end
    projects{projectIndex}=struct('project_id',item.project_id,'name',stem,'fovs',item.fovs,'rois',rois, ...
        'roi_files_present',roiFiles,'data_sidecars_present',dataFiles,'raw_sample_reads_verified',rawFiles, ...
        'processors_verified',numel(processorFiles),'main_mat_sha256_unchanged',item.mat_sha256, ...
        'samples',{samples},'project_saved',false);
    writeJson(fullfile(work,['project3-host-' host '.partial.json']),projects(1:projectIndex));
    clear loaded p;
end
writeJson(fullfile(work,['project3-host-' host '-verification.json']),struct('success',true,'host',host, ...
    'engine_release',release,'projects',{projects},'project_saved',false,'source_deleted',false,'catalog_changed',false, ...
    'path_policy','Canonical final project paths translated to incoming folders in memory only; raw namespace unchanged.'));

    function out=mapped(value)
        normalized=strrep(char(value),'\','/');
        assert(~startsWith(normalized,'Z:') && ~startsWith(normalized,'/DataVault/') && ~startsWith(normalized,'/data/Alexander/data/2023_3'));
        if strcmp(normalized,projectRoot)||startsWith(normalized,[projectRoot '/'])
            out=[incomingRoot normalized(numel(projectRoot)+1:end)];
        elseif strcmp(normalized,canonical)||startsWith(normalized,[canonical '/'])
            out=[hostRoot normalized(numel(canonical)+1:end)];
        else,error('Unexpected path outside the reviewed personal home');end
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
