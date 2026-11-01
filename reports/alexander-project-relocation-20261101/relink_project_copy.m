function relink_project_copy(work, release)
% Rewrite only verified copies. Scientific values and user annotations must
% survive a save/reload comparison after canonical path normalization.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
stem='anof_26102023_yam76s_100ms';
container=['/homes2/maliavko/DetecdivHub/projects/' stem];
dataRoot=[container '/' stem];
sourceBase='/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis';
oldRaw='/data/Alexander/data/2023_2';newRaw='/homes2/maliavko/DetecdivHub/raw/2023_2';
file=fullfile(container,[stem '.mat']);
guard=jsondecode(fileread(fullfile(work,'copy-state.json')));
assert(strcmp(guard.phase,'verified_copy_ready_for_path_rewrite'));
assert(guard.full_content_verification.files==11450);
assert(isfile(file));
roots={['/DataVault/Alexander/analysis/' stem],dataRoot; ...
       [sourceBase '/' stem],dataRoot; ...
       ['//10.20.8.250/Sauvegarde/Alexander/analysis/' stem],dataRoot; ...
       oldRaw,newRaw; ...
       '//10.20.11.250/DATA/Alexander/data/2023_2',newRaw};
mode='';seen=containers.Map('KeyType','char','ValueType','any');nextId=1;phaseLabel='snapshot_before_rewrite';
changedCount=0;changeExamples=struct('field',{},'before',{},'after',{});
% Always begin from the independently verified pre-rewrite MAT. This makes
% an interrupted sidecar audit safely restartable without losing provenance.
baseline=['/homes2/maliavko/DetecdivHub/.migration-backups/alexander-project-20261101/' stem '.mat'];
assert(isfile(baseline));loaded=load(baseline);assert(isa(loaded.shallowObj,'shallow'));
before=takeSnapshot(loaded);
phaseLabel='rewrite_paths';
reset('rewrite');loaded=visit(loaded,'mat',false,0);
changes=changedCount;examples=changeExamples;
assert(changes>=5737,'Expected raw, ROI and project paths were not all rebased');
phaseLabel='audit_rewritten_paths';reset('audit');visit(loaded,'mat',false,0);
assert(changedCount==0,'An actionable legacy path remains in the copied project');
phaseLabel='compare_before_save';assert(isequaln(before,takeSnapshot(loaded)),'Non-path project values changed during rewrite');
temporary=fullfile(container,[stem '.relocated-tmp.mat']);
assert(~isfile(temporary));
phaseLabel='save_copied_mat';writeJson(fullfile(work,'relink-progress.json'),struct('phase',phaseLabel));
save(temporary,'-struct','loaded','-v7');
reopened=load(temporary);
phaseLabel='compare_reopened_project';
assert(isequaln(before,takeSnapshot(reopened)),'Save/reload changed scientific values or annotations');
movefile(temporary,file,'f');
clear loaded reopened before;

% These small legacy result sidecars may hold nested path-bearing fields.
% Images are deliberately never resaved or converted.
sidecars=dir(fullfile(dataRoot,'**','results_*.mat'));
assert(numel(sidecars)==5724);
modifiedSidecars={};
for k=1:numel(sidecars)
    path=fullfile(sidecars(k).folder,sidecars(k).name);s=load(path);
    prior=takeSnapshot(s);reset('rewrite');s=visit(s,'sidecar',false,0);
    n=changedCount;
    if n>0
        assert(isequaln(prior,takeSnapshot(s)),'Sidecar scientific values changed');
        tmp=[path '.relocated-tmp.mat'];assert(~isfile(tmp));save(tmp,'-struct','s','-v7');
        check=load(tmp);assert(isequaln(prior,takeSnapshot(check)),'Sidecar save/reload changed values');
        movefile(tmp,path,'f');modifiedSidecars{end+1}=path; %#ok<AGROW>
    end
    if mod(k,250)==0,writeJson(fullfile(work,'relink-progress.json'),struct('phase','sidecar_path_audit','files',k,'total',numel(sidecars)));end
end
loaded=load(file);p=loaded.shallowObj;
assert(strcmp(p.io.path,container) && strcmp(p.io.file,stem));
assert(numel(p.fov)==12);
roiCount=0;roiFiles=0;
samples=struct('fov',{},'roi',{},'path',{},'imageClass',{},'imageSize',{},'roiPixelsMatchOriginal',{},'rawPixelsMatchOriginal',{});
for f=1:numel(p.fov)
    for r=1:numel(p.fov(f).roi)
        current=p.fov(f).roi(r);roiCount=roiCount+1;
        assert(startsWith(strrep(current.path,'\','/'),[dataRoot '/']),'ROI path remains outside the copied project');
        assert(isfile(fullfile(current.path,['im_' current.id '.mat'])));
        assert(isfile(fullfile(current.path,['results_' current.id '.mat'])));
        roiFiles=roiFiles+2;
    end
    current=p.fov(f).roi(1);
    a=roi(current.id,current.value);a.path=current.path;
    a.load('Silent');
    assert(strcmp(a.path,current.path),'Legacy image loading replaced the rebased ROI path');
    reset('audit');visit(a,'loadedROI',false,0);
    assert(changedCount==0,'Legacy image loading reintroduced an active old path');
    b=roi(current.id,current.value);
    b.path=strrep(current.path,dataRoot,fullfile(sourceBase,stem));
    b.load('Silent');
    assert(~isempty(a.image) && isequaln(a.image,b.image));
    assert(isequaln(a.channelid,b.channelid) && isequaln(a.results,b.results));
    values=p.fov(f).srcpath;assert(iscell(values));
    for ch=1:numel(values),assert(startsWith(values{ch},[newRaw '/']) && isfolder(values{ch}));end
    list=p.fov(f).srclist{1};assert(isstruct(list) && ~isempty(list));
    name=list(1).name;
    if startsWith(name,'/'),rawNew=name;else,rawNew=fullfile(values{1},name);end
    rawOld=strrep(rawNew,newRaw,oldRaw);
    rawA=imread(rawNew);rawB=imread(rawOld);assert(isequaln(rawA,rawB));
    samples(f)=struct('fov',f,'roi',current.id,'path',current.path,'imageClass',class(a.image), ...
        'imageSize',size(a.image),'roiPixelsMatchOriginal',true,'rawPixelsMatchOriginal',true); %#ok<AGROW>
    clear a b rawA rawB;
end
assert(roiCount==5724 && roiFiles==11448);
report=struct('success',true,'source_preserved',true,'copied_project_file',file,'source_project_file',fullfile(sourceBase,[stem '.mat']), ...
    'project_path_changes',changes,'path_change_examples',examples,'scientific_values_and_annotations_preserved',true, ...
    'full_project_save_reopen_verified',true,'fovs',12,'rois',roiCount,'roi_sidecar_files_present',roiFiles, ...
    'result_sidecars_audited',numel(sidecars),'result_sidecars_rewritten',{modifiedSidecars}, ...
    'legacy_image_mats_rewritten',false,'legacy_image_path_policy','roi.load excludes id/path; validated on a representative ROI from each FOV', ...
    'samples',samples,'engine_release',release);
writeJson(fullfile(work,'linux-relink-verification.json'),report);

    function snapshot=takeSnapshot(value)
        reset('snapshot');snapshot=visit(value,'mat',false,0);
    end
    function reset(newMode)
        mode=newMode;seen=containers.Map('KeyType','char','ValueType','any');nextId=1;
        changedCount=0;changeExamples=struct('field',{},'before',{},'after',{});
    end
    function out=visit(value,location,isPath,depth)
        assert(depth<80,'Unexpected persistent object cycle or depth');
        if isnumeric(value)||islogical(value)||ischar(value)||isstring(value)
            out=value;
            if (ischar(value)||isstring(value)) && isPath
                if ischar(value)
                    if isrow(value)||isempty(value),out=mapText(value,location);end
                else
                    for z=1:numel(value),out(z)=string(mapText(char(value(z)),location));end
                end
                if strcmp(mode,'audit'),out=value;end
            end
            if strcmp(mode,'snapshot'),out=struct('kind',class(value),'size',size(out),'value',{out});end
            return
        end
        if iscell(value)
            if all(cellfun(@simple,value(:)))
                out=value;
                if isPath
                    for z=1:numel(value)
                        if ischar(value{z}) && (isrow(value{z})||isempty(value{z})),out{z}=mapText(value{z},location);
                        elseif isstring(value{z})
                            for t=1:numel(value{z}),out{z}(t)=string(mapText(char(value{z}(t)),location));end
                        end
                    end
                    if strcmp(mode,'audit'),out=value;end
                end
                if strcmp(mode,'snapshot'),out=struct('kind','simpleCell','size',size(value),'classes',{cellfun(@class,value,'UniformOutput',false)},'value',{out});end
                return
            end
            out=cell(size(value));for z=1:numel(value),out{z}=visit(value{z},location,isPath,depth+1);end
            if strcmp(mode,'snapshot'),out=struct('kind','cell','size',size(value),'value',{out});end
            return
        end
        if istable(value)
            names=value.Properties.VariableNames;parts=struct();out=value;
            for z=1:numel(names)
                field=names{z};child=visit(value.(field),[location '.' field],pathField(field,isPath),depth+1);
                if strcmp(mode,'snapshot'),parts.(field)=child;elseif strcmp(mode,'rewrite'),out.(field)=child;end
            end
            if strcmp(mode,'snapshot')
                out=struct('kind',class(value),'size',size(value),'names',{names},'rowNames',{value.Properties.RowNames}, ...
                    'units',{value.Properties.VariableUnits},'descriptions',{value.Properties.VariableDescriptions}, ...
                    'description',value.Properties.Description,'dimensionNames',{value.Properties.DimensionNames}, ...
                    'userData',visit(value.Properties.UserData,[location '.UserData'],false,depth+1),'variables',parts);
            else
                priorChanges=changedCount;
                userData=visit(value.Properties.UserData,[location '.UserData'],false,depth+1);
                if strcmp(mode,'rewrite') && changedCount>priorChanges,out.Properties.UserData=userData;end
            end
            return
        end
        if isstruct(value)
            fields=sort(fieldnames(value));out=value;columns=struct();
            for q=1:numel(fields)
                field=fields{q};values={value.(field)};priorChanges=changedCount;
                transformed=visit(values,[location '.' field],pathField(field,isPath),depth+1);
                if strcmp(mode,'snapshot'),columns.(field)=transformed;
                elseif strcmp(mode,'rewrite') && changedCount>priorChanges,[out.(field)]=transformed{:};end
            end
            if strcmp(mode,'snapshot'),out=struct('kind','struct','size',size(value),'fields',{fields},'columns',columns);end
            return
        end
        if isobject(value) && ~isdatetime(value) && ~isduration(value) && ~iscalendarduration(value) && ~iscategorical(value)
            if isa(value,'containers.Map')
                keys=sort(value.keys);parts=cell(size(keys));out=value;
                for z=1:numel(keys),parts{z}=visit(value(keys{z}),[location '.' char(string(keys{z}))],false,depth+1);if strcmp(mode,'rewrite'),out(keys{z})=parts{z};end;end
                if strcmp(mode,'snapshot'),out=struct('kind','containers.Map','keys',{keys},'values',{parts});end
                return
            end
            out=value;items=cell(size(value));
            for z=1:numel(value)
                node=value(z);name=class(node);id=0;
                if isa(node,'handle')
                    if ~isvalid(node),items{z}=struct('invalid',name);continue;end
                    if isKey(seen,name)
                        group=seen(name);index=find(group.objects==node,1);
                        if ~isempty(index),items{z}=struct('reference',group.ids(index),'class',name);continue;end
                    else,group=struct('objects',[],'ids',[]);end
                    id=nextId;nextId=nextId+1;
                    if mod(nextId,1000)==0,writeJson(fullfile(work,'relink-progress.json'),struct('phase',phaseLabel,'objects',nextId));end
                    if isempty(group.objects),group.objects=node;else,group.objects(end+1)=node;end
                    group.ids(end+1)=id;seen(name)=group;
                end
                meta=metaclass(node);properties=meta.PropertyList;names={};
                for q=1:numel(properties)
                    prop=properties(q);if iscell(properties),prop=properties{q};end
                    if prop.Transient||prop.Dependent||prop.Constant||~ischar(prop.GetAccess)||~strcmp(prop.GetAccess,'public'),continue;end
                    names{end+1}=prop.Name; %#ok<AGROW>
                end
                names=sort(names);parts=struct();
                for q=1:numel(names)
                    field=names{q};child=node.(field);priorChanges=changedCount;
                    transformed=visit(child,[location '.' field],pathField(field,false),depth+1);
                    if strcmp(mode,'snapshot'),parts.(field)=transformed;
                    elseif strcmp(mode,'rewrite') && changedCount>priorChanges && ~isa(child,'handle'),node.(field)=transformed;
                    end
                end
                items{z}=struct('class',name,'id',id,'properties',parts);
            end
            if strcmp(mode,'snapshot'),out=struct('kind','object','class',class(value),'size',size(value),'items',{items});end
            return
        end
        out=value;
        if strcmp(mode,'snapshot'),out=struct('kind',class(value),'size',size(value),'value',{value});end
    end
    function yes=pathField(field,parent)
        protected=any(strcmpi(field,{'notes','comments','description','history','tag','message','label'}));
        yes=~protected && (parent||~isempty(regexpi(field,'path|file|dir|source|src|folder|root|uri','once')));
    end
    function out=mapText(text,location)
        out=text;if isempty(text),return;end
        if text(1)~='/' && text(1)~='\' && ~(numel(text)>=2 && text(2)==':'),return;end
        normalized=strrep(text,'\','/');normalized=regexprep(normalized,'/+$','');
        if endsWith(location,'.io.path') && any(strcmp(normalized,{'/DataVault/Alexander/analysis',sourceBase,container}))
            out=container;
        else
            for q=1:size(roots,1)
                old=roots{q,1};caseInsensitive=startsWith(old,'//');
                matches=strcmp(normalized,old)||startsWith(normalized,[old '/']);
                if caseInsensitive,matches=strcmpi(normalized,old)||startsWith(normalized,[old '/'],'IgnoreCase',true);end
                if matches,out=[roots{q,2} normalized(numel(old)+1:end)];break;end
            end
        end
        if ~strcmp(text,out) && ~strcmp(mode,'snapshot')
            changedCount=changedCount+1;
            if numel(changeExamples)<50,changeExamples(end+1)=struct('field',location,'before',text,'after',out);end
        end
    end
    function yes=simple(value)
        yes=isnumeric(value)||islogical(value)||ischar(value)||isstring(value)||isdatetime(value)||isduration(value)||iscalendarduration(value)||iscategorical(value);
    end
end
function writeJson(path,value)
tmp=[path '.tmp'];fid=fopen(tmp,'w','n','UTF-8');assert(fid>=0);
cleaner=onCleanup(@() fclose(fid));fprintf(fid,'%s\n',jsonencode(value,PrettyPrint=true));clear cleaner;
movefile(tmp,path,'f');
end
