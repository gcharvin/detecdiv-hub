function verify_year2_final_hosts(work,release,host)
% Readonly final-folder reads with exact, immutable availability baselines.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
assert((strcmp(host,'windows')&&ispc)||(strcmp(host,'linux')&&isunix));
control=jsondecode(fileread(fullfile(work,'controller-request.json')));
assert(posixtime(datetime('now','TimeZone','UTC'))<control.own_lease_deadline_unix);
q=jsondecode(fileread(fullfile(work,'host-request.json')));assert(q.read_only&&strcmp(q.project_namespace,'final'));
assert(control.raw_year2_full_sha1_and_source_stability_verified&&control.filesystem_publication_verified);
canonical='/homes2/maliavko/DetecdivHub';rawPrefix=[canonical '/raw/.incoming-alexander-2024-20261105/'];
if ispc,hostRoot='//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub';dataRoot='//10.20.11.250/DATA';else,hostRoot=canonical;dataRoot='/data';end
rawSpecs=containers.Map('KeyType','char','ValueType','any');rawCache=containers.Map('KeyType','char','ValueType','any');
for z=1:numel(q.raw_directories),r=at(q.raw_directories,z);rawSpecs([r.year '/' r.path])=r;end
projects=cell(1,numel(q.projects));totalFovs=0;totalRois=0;totalRaw=0;projectRoot='';
for projectIndex=1:numel(q.projects)
    item=at(q.projects,projectIndex);stem=item.name;projectRoot=[canonical '/projects/' stem];
    location=[hostRoot '/projects/' stem];file=fullfile(location,[stem '.mat']);
    assert(isfolder(location)&&strcmp(fileHash(file),item.main_sha256));
    loaded=load(file);p=loaded.shallowObj;assert(strcmp(normal(p.io.path),projectRoot)&&strcmp(p.io.file,stem));
    assert(numel(p.fov)==numel(item.fovs));p.io.path=location;
    if strcmp(stem,'281124_Alex'),assert(strcmp(normal(p.parsedData.folder),projectRoot));p.parsedData.folder=location;end
    files=containers.Map('KeyType','char','ValueType','any');directories=containers.Map('KeyType','char','ValueType','logical');
    for z=1:numel(item.files),record=at(item.files,z);assert(~isKey(files,record.path));files(record.path)=record;end
    directoryNames=cellstr(string(item.directories));for z=1:numel(directoryNames),directories(directoryNames{z})=true;end
    missingAllowed=cellstr(string(item.expected_missing_raw_paths));missingObserved={};
    sizeMismatches=0;rois=0;rawReads=0;roiReads=0;imagePresent=0;resultPresent=0;dataPresent=0;absentImages=0;blankPaths=0;fieldResults=cell(1,numel(p.fov));
    for f=1:numel(p.fov)
        assert(posixtime(datetime('now','TimeZone','UTC'))<control.own_lease_deadline_unix);
        baseline=at(item.fovs,f);assert(baseline.fov==f);currentFov=p.fov(f);
        assert(numel(currentFov.roi)==baseline.rois&&iscell(currentFov.srcpath)&&numel(currentFov.srcpath)==numel(baseline.channels));
        for ch=1:numel(currentFov.srcpath)
            channel=at(baseline.channels,ch);relative=channel.relative_path;
            saved=normal(currentFov.srcpath{ch});assert(strcmp(saved,[rawPrefix channel.year '/' relative]));
            currentFov.srcpath{ch}=mapped(saved);actual=rawDirectory([channel.year '/' relative]);
            listing=currentFov.srclist{ch};assert(isstruct(listing)&&numel(listing)==channel.saved_entries);
            for t=1:numel(listing)
                name=listing(t).name;assert(isempty(regexp(name,'[/\\]','once')));relativeFile=[channel.year '/' relative '/' name];
                if isKey(actual,name)
                    sizeMismatches=sizeMismatches+(actual(name)~=listing(t).bytes);
                    assert(~ismember(relativeFile,missingAllowed),'Unexpected recovery/change of historical absence');
                else
                    assert(ismember(relativeFile,missingAllowed),'New absent raw reference');missingObserved{end+1}=relativeFile; %#ok<AGROW>
                end
            end
            if isfield(listing,'folder')
                folders=unique({listing.folder});assert(numel(folders)==1&&strcmp(normal(folders{1}),saved));
                [listing.folder]=deal(mapped(saved));currentFov.srclist{ch}=listing;
            end
        end
        representative=0;
        for r=1:numel(currentFov.roi)
            current=currentFov.roi(r);rois=rois+1;
            if isempty(current.path)
                assert(ismember(r,baseline.blank_roi_indices)&&isempty(current.image));blankPaths=blankPaths+1;continue;
            end
            assert(~ismember(r,baseline.blank_roi_indices));saved=normal(current.path);
            assert(startsWith(saved,[projectRoot '/' stem '/']));relative=saved(numel(projectRoot)+2:end);
            current.path=mapped(saved);assert(isfolder(current.path)==isKey(directories,relative),'ROI directory availability changed');
            assert(isempty(regexp(current.id,'[/\\]','once')));
            mat=[relative '/im_' current.id '.mat'];hdf=[relative '/im_' current.id '.h5'];
            hasMat=checkPayload(mat);hasHdf=checkPayload(hdf);assert(~(hasMat&&hasHdf));hasImage=hasMat||hasHdf;
            imagePresent=imagePresent+hasImage;absentImages=absentImages+~hasImage;
            resultPresent=resultPresent+checkPayload([relative '/results_' current.id '.mat']);
            dataPresent=dataPresent+checkPayload([relative '/data_' current.id '.mat']);
            if ~hasImage,assert(isempty(current.image),'Unexpected embedded-image variant');end
            if strcmp(item.image_policy,'empty_mat_placeholder')
                assert(hasMat&&~hasHdf);record=files(mat);placeholder=fullfile(location,mat);
                assert(record.bytes==128&&strcmp(fileHash(placeholder),'239d020f09a8ad2cf79c4ab1a2de31d39cd41edac8823924ef1cf6e1ed749a33')&&isempty(whos('-file',placeholder)));
                assert(isempty(current.image));
            elseif strcmp(item.image_policy,'hdf5'),assert(hasHdf&&~hasMat);end
            if hasImage&&representative==0
                if hasHdf,record=files(hdf);else,record=files(mat);end
                if record.bytes>128,representative=r;end
            end
        end
        assert(blankPaths>=0);samples=baseline.samples;
        for z=1:numel(samples)
            entry=at(samples,z);channel=at(baseline.channels,entry.channel);listing=currentFov.srclist{entry.channel};
            assert(strcmp(entry.year,channel.year)&&strcmp(entry.relative_path,[channel.relative_path '/' listing(entry.frame).name]));
            pixels=imread([hostRoot '/raw/.incoming-alexander-2024-20261105/' entry.year '/' entry.relative_path]);
            assert(strcmp(class(pixels),entry.class)&&isequal(size(pixels),double(entry.size(:)'))&&strcmp(pixelHash(pixels),entry.pixel_sha256),'Raw pixels differ from original baseline');
            rawReads=rawReads+1;
        end
        engineImage=currentFov.readImage(1,1);first=[];
        for z=1:numel(samples),entry=at(samples,z);if entry.channel==1&&entry.frame==1,first=entry;break;end,end
        assert(~isempty(first)&&strcmp(pixelHash(engineImage),first.pixel_sha256),'Protected engine FOV reader mismatch');
        if representative>0
            current=currentFov.roi(representative);a=roi(current.id,current.value);a.path=current.path;a.load('Silent');
            assert(strcmp(normal(a.path),normal(current.path))&&~isempty(a.image));im=a.image;if isa(im,'gpuArray'),im=gather(im);end
            bounds=round(current.value);assert(numel(bounds)==4);x=bounds(1);y=bounds(2);w=bounds(3);h=bounds(4);
            if strcmp(baseline.crop_match,'exact_inclusive_xywh_first_frame'),w=w+1;h=h+1;else,assert(strcmp(baseline.crop_match,'exact_xywh_first_frame'));end
            engineCpu=engineImage;if isa(engineCpu,'gpuArray'),engineCpu=gather(engineCpu);end
            assert(x>=1&&y>=1&&x+w-1<=size(engineCpu,2)&&y+h-1<=size(engineCpu,1));
            assert(isequaln(engineCpu(y:y+h-1,x:x+w-1),im(:,:,1,1)),'ROI/raw crop differs from original identity baseline');roiReads=roiReads+1;
            clear a im engineCpu;
        else
            if strcmp(item.image_policy,'empty_mat_placeholder')
                assert(strcmp(baseline.crop_match,'not_applicable_preexisting_placeholder')&&numel(currentFov.roi)==1);
            elseif strcmp(baseline.crop_match,'not_applicable_preexisting_unextracted')
                assert(strcmp(stem,'anof_061224_X5152moth')&&numel(baseline.blank_roi_indices)==numel(currentFov.roi));
            else,assert(strcmp(baseline.crop_match,'not_applicable_no_saved_roi_images'),'Previously extracted image unavailable');end
        end
        fieldResults{f}=struct('fov',f,'roi_representative_read',representative>0,'raw_sample_reads',numel(samples),'engine_reader_verified',true,'availability_baseline_preserved',true);
        writeJson(fullfile(work,['host-' host '-progress.json']),struct('project',stem,'project_index',projectIndex,'fov',f,'fovs',numel(p.fov),'rois_checked',rois,'raw_reads',rawReads,'project_saved',false));
        clear engineImage pixels;
    end
    assert(isequal(sort(unique(missingObserved(:))),sort(unique(missingAllowed(:))))&&sizeMismatches==item.expected_saved_size_mismatches);
    for z=1:numel(item.processors)
        proc=at(item.processors,z);path=fullfile(location,proc.path);assert(strcmp(fileHash(path),proc.sha256));node=load(path);assert(isfield(node,'processObj'));
        assert(startsWith(normal(node.processObj.path),[projectRoot '/' stem '/processor/'])&&isfolder(mapped(node.processObj.path)));assert(strcmp(fileHash(path),proc.sha256));clear node;
    end
    typed=struct('typed_json_verified',0,'run_loaders_verified',0,'external_files_verified',0,'historical_run_resumption_validated',false);
    if strcmp(stem,'281124_Alex'),typed=typedGilles(item,location);end
    assert(strcmp(fileHash(file),item.main_sha256),'Shared main changed during readonly validation');
    projects{projectIndex}=struct('project_id',item.project_id,'name',stem,'fovs',numel(p.fov),'rois',rois,'raw_sample_reads',rawReads,'roi_representative_reads',roiReads, ...
        'image_payloads_present',imagePresent,'preexisting_placeholder_payloads',double(strcmp(item.image_policy,'empty_mat_placeholder')),'images_previously_absent',absentImages,'results_present',resultPresent,'data_present',dataPresent,'blank_paths_preserved',blankPaths, ...
        'historical_missing_raw_paths_preserved',numel(missingAllowed),'historical_cache_size_mismatches_preserved',sizeMismatches,'processors_verified',numel(item.processors), ...
        'main_sha256_unchanged',item.main_sha256,'typed_paths',typed,'field_results',{fieldResults},'availability_baseline_preserved',true,'project_saved',false);
    totalFovs=totalFovs+numel(p.fov);totalRois=totalRois+rois;totalRaw=totalRaw+rawReads;
    writeJson(fullfile(work,['host-' host '.partial.json']),projects(1:projectIndex));clear loaded p;
end
assert(numel(projects)==q.expected_projects&&totalFovs==q.expected_fovs&&totalRois==q.expected_rois&&totalRaw==q.expected_raw_samples);
assert(sum(cellfun(@(v) v.blank_paths_preserved,projects))==q.expected_blank_roi_paths);
writeJson(fullfile(work,['host-' host '-verification.json']),struct('success',true,'host',host,'project_namespace','final','projects',{projects}, ...
    'project_saved',false,'source_deleted',false,'catalog_changed',false,'availability_policy','Exact original availability, including explicitly preserved missing data. Missing data was not restored or declared readable.'));

    function mapOut=rawDirectory(directoryKey)
        if isKey(rawCache,directoryKey),mapOut=rawCache(directoryKey);return;end
        assert(isKey(rawSpecs,directoryKey));rawDefinition=rawSpecs(directoryKey);mapOut=containers.Map('KeyType','char','ValueType','double');
        for specIndex=1:numel(rawDefinition.files),specEntry=at(rawDefinition.files,specIndex);assert(~isKey(mapOut,specEntry.name));mapOut(specEntry.name)=specEntry.bytes;end
        nativeRows=dir([hostRoot '/raw/.incoming-alexander-2024-20261105/' directoryKey]);nativeRows=nativeRows(~[nativeRows.isdir]);
        nativeRows=nativeRows(~ismember({nativeRows.name},{'@eaDir','#recycle','#snapshot','.snapshot','@Recycle','.rsync-partial'}));
        assert(numel(nativeRows)==mapOut.Count,'Raw directory inventory changed');
        for nativeIndex=1:numel(nativeRows),assert(isKey(mapOut,nativeRows(nativeIndex).name)&&mapOut(nativeRows(nativeIndex).name)==nativeRows(nativeIndex).bytes,'Raw directory payload differs from native baseline');end
        rawCache(directoryKey)=mapOut;
    end
    function existsExpected=checkPayload(payloadRelative)
        existsExpected=isKey(files,payloadRelative);payloadHostPath=fullfile(hostRoot,'projects',stem,payloadRelative);
        assert(isfile(payloadHostPath)==existsExpected,'ROI sidecar availability changed');
        if existsExpected,payloadRecord=files(payloadRelative);payloadInfo=dir(payloadHostPath);assert(isscalar(payloadInfo)&&~payloadInfo.isdir&&payloadInfo.bytes==payloadRecord.bytes);end
    end
    function checks=typedGilles(spec,projectLocation)
        assert(numel(spec.typed_json)==9&&numel(spec.external_dependencies)==3);
        checks=struct('typed_json_verified',0,'run_loaders_verified',0,'external_files_verified',0,'historical_run_resumption_validated',false);
        hub=struct('pathMappings',struct('remoteRoot',{canonical,'/data'},'localRoot',{hostRoot,dataRoot}));
        for typedIndex=1:numel(spec.typed_json)
            record=at(spec.typed_json,typedIndex);jsonFile=fullfile(projectLocation,record.path);
            assert(strcmp(fileHash(jsonFile),record.sha256));doc=jsondecode(fileread(jsonFile));
            for changeIndex=1:numel(record.changes)
                changeRecord=at(record.changes,changeIndex);value=fieldAt(doc,changeRecord.field);
                assert(isequal(value,changeRecord.after));verifyReference(value);
            end
            if endsWith(record.path,'/run.json')
                [run,msg]=pipelineRunLoad(jsonFile);assert(isempty(msg)&&isa(run,'pipelineRun'));
                run=detecdiv_paths_localize_run(run,hub);
                for refName={'templatePath','projectPath'}
                    key=refName{1};expected=referencePath(doc.(key));assert(strcmp(normal(run.(key)),normal(expected)));
                end
                for refName={'pipelineRef','targetRef'}
                    key=refName{1};names={'path'};if strcmp(key,'targetRef'),names={'projectPath','classiPath'};end
                    for refIndex=1:numel(names)
                        member=names{refIndex};if isfield(doc.(key),member)
                            saved=doc.(key).(member);
                            if isempty(saved)&&strcmp(key,'pipelineRef')&&strcmp(member,'path'),saved=doc.templatePath;end
                            if isempty(saved)&&strcmp(key,'targetRef')&&strcmp(member,'projectPath'),saved=doc.projectPath;end
                            expected=referencePath(saved);assert(strcmp(normal(run.(key).(member)),normal(expected)));
                        end
                    end
                end
                if isfield(doc,'ctx'),assert(isequaln(run.ctx,doc.ctx));end
                % Event reconciliation may update the IN-MEMORY progress/outputs.
                % The complete persisted JSON hash must remain exact; no run is executed.
                checks.run_loaders_verified=checks.run_loaders_verified+1;clear run;
            end
            assert(strcmp(fileHash(jsonFile),record.sha256));checks.typed_json_verified=checks.typed_json_verified+1;
        end
        assert(checks.run_loaders_verified==7);
        for depIndex=1:numel(spec.external_dependencies)
            dep=at(spec.external_dependencies,depIndex);actual=referencePath(dep.canonical_path);info=dir(actual);
            assert(isscalar(info)&&~info.isdir&&info.bytes==dep.bytes&&strcmp(fileHash(actual),dep.sha256));
            checks.external_files_verified=checks.external_files_verified+1;
        end
    end
    function verifyReference(value)
        if isempty(value),return;end
        actual=referencePath(value);assert(isfile(actual)||isfolder(actual),'Active typed reference is unavailable on this host');
    end
    function actual=referencePath(value)
        if isempty(value),actual='';return;end
        value=normal(value);
        if strcmp(value,canonical)||startsWith(value,[canonical '/']),actual=mapped(value);
        elseif strcmp(value,'/data')||startsWith(value,'/data/'),actual=[dataRoot value(6:end)];
        else,error('Unclassified typed canonical reference');end
    end
    function mappedPath=mapped(canonicalValue)
        canonicalNormalized=normal(canonicalValue);assert(strcmp(canonicalNormalized,canonical)||startsWith(canonicalNormalized,[canonical '/']));mappedPath=[hostRoot canonicalNormalized(numel(canonical)+1:end)];
    end
end

function out=normal(value)
out=regexprep(strrep(char(value),'\','/'),'/+$','');assert(isempty(regexp(out,'(^|/)\.\.(/|$)','once')));
end
function value=at(values,index)
if iscell(values),value=values{index};else,value=values(index);end
end
function out=fileHash(path)
md=java.security.MessageDigest.getInstance('SHA-256');f=fopen(path,'rb');assert(f>=0);c=onCleanup(@() fclose(f));
while true,b=fread(f,4*1024*1024,'*uint8');if isempty(b),break;end;md.update(b);end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function out=pixelHash(value)
if isa(value,'gpuArray'),value=gather(value);end
md=java.security.MessageDigest.getInstance('SHA-256');md.update(typecast(value(:),'uint8'));out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function writeJson(path,value)
p=[path '.tmp'];f=fopen(p,'w','n','UTF-8');assert(f>=0);c=onCleanup(@() fclose(f));fprintf(f,'%s\n',jsonencode(value));clear c;movefile(p,path,'f');
end

function value=fieldAt(doc,path)
parts=cellstr(string(path));value=doc;
for i=1:numel(parts),assert(isstruct(value)&&isscalar(value)&&isfield(value,parts{i}));value=value.(parts{i});end
end
