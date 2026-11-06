function relink_2024_batch1(work,release)
% Only 23 backed-up metadata MAT copies in twelve reviewed, unpublished stages.
% Canonical scientific visitor adapted from the prior verified implementation;
% GPU snapshots compare gathered contents AND retain gpuArray class/type.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
guard=jsondecode(fileread(fullfile(work,'request.json')));
assert(~guard.source_deletion_allowed&&~guard.catalog_cutover_allowed);
assert(numel(guard.files)==23&&numel(guard.projects)==12);
assert(guard.type_inventory_gpu_values==4258&&guard.type_inventory_function_handles==0&&guard.type_inventory_nonpublic_properties==0);
mode='';seen=[];nextId=1;phaseLabel='preflight';changedCount=0;changeExamples=[];roots={};container='';ioParents={};
reports=cell(1,numel(guard.files));
for fileIndex=1:numel(guard.files)
    recipe=guard.files(fileIndex);stem=recipe.stem;
    project=guard.projects(find(strcmp({guard.projects.project_id},recipe.project_id),1));
    container=project.final_container;ioParents=project.io_parent_aliases;
    roots=cell(numel(project.mappings),2);
    for q=1:numel(project.mappings),roots(q,:)={project.mappings(q).old,project.mappings(q).target};end
    assert(endsWith(recipe.relative_path,'.mat')&&~startsWith(string(recipe.relative_path),'/'));
    [~,base]=fileparts(recipe.relative_path);assert(~startsWith(base,'im_'),'Image MAT must never be relinked');
    assert(posixtime(datetime('now','TimeZone','UTC'))<guard.lease_deadline_unix);
    assert(strcmp(fileHash(recipe.backup),recipe.sha256)&&strcmp(fileHash(recipe.target),recipe.sha256));
    loaded=load(recipe.backup);
    phaseLabel='scientific_snapshot_before';before=takeSnapshot(loaded);
    phaseLabel='rewrite_paths_in_memory';reset('rewrite');loaded=visit(loaded,'metadata',false,0);
    changes=changedCount;examples=changeExamples;assert(changes==recipe.expected_path_changes,'Unexpected number of path changes');
    phaseLabel='audit_canonical_paths';reset('audit');visit(loaded,'metadata',false,0);assert(changedCount==0);
    phaseLabel='compare_scientific_values_before_save';assert(isequaln(before,takeSnapshot(loaded)),'Scientific values/types/annotations changed');
    temporary=[recipe.target '.homes2-relink-tmp.mat'];assert(~isfile(temporary));
    assert(posixtime(datetime('now','TimeZone','UTC'))<guard.lease_deadline_unix);
    phaseLabel='save_and_reopen_unpublished_metadata';save(temporary,'-struct','loaded','-v7');reopened=load(temporary);
    assert(isequaln(before,takeSnapshot(reopened)),'Scientific save/reopen equivalence failed');
    reset('audit');visit(reopened,'metadata',false,0);assert(changedCount==0);
    assert(posixtime(datetime('now','TimeZone','UTC'))<guard.lease_deadline_unix);
    assert(strcmp(fileHash(recipe.backup),recipe.sha256)&&strcmp(fileHash(recipe.target),recipe.sha256));
    movefile(temporary,recipe.target,'f');
    reports{fileIndex}=struct('project_id',recipe.project_id,'relative_path',recipe.relative_path,'backup',recipe.backup, ...
        'before_sha256',recipe.sha256,'after_sha256',fileHash(recipe.target),'path_changes',changes,'path_change_examples',examples, ...
        'scientific_values_types_and_annotations_preserved',true,'gpu_class_and_content_preserved',true,'save_reopen_verified',true, ...
        'original_preserved',true,'catalog_changed',false,'source_deleted',false);
    writeJson(fullfile(work,sprintf('modified-%02d.json',fileIndex)),reports{fileIndex});
    writeJson(fullfile(work,'progress.json'),struct('phase','copied_metadata_relinked','files_complete',fileIndex,'total',numel(guard.files),'project',stem,'source_deleted',false));
    clear loaded reopened before;
end
writeJson(fullfile(work,'result.json'),struct('success',true,'files',{reports},'projects',12,'engine_release',release, ...
    'phase','copied_paths_relinked_science_verified_raw_and_host_gates_pending','original_projects_preserved',true, ...
    'image_files_rewritten',false,'catalog_changed',false,'source_deleted',false));

    function snapshot=takeSnapshot(value)
        reset('snapshot');snapshot=visit(value,'mat',false,0);
    end
    function reset(newMode)
        mode=newMode;seen=containers.Map('KeyType','char','ValueType','any');nextId=1;
        changedCount=0;changeExamples=struct('field',{},'before',{},'after',{});
    end
    function out=visit(value,location,isPath,depth)
        assert(depth<80,'Unexpected persistent object cycle or depth');
        if isa(value,'gpuArray')
            out=value;
            if strcmp(mode,'snapshot')
                cpu=gather(value);assert(isequal(size(cpu),size(value)),'GPU contents unavailable');
                out=struct('kind','gpuArray','underlying_class',class(cpu),'size',size(cpu),'sparse',issparse(cpu),'value',{cpu});
            end
            return
        end
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
                    if mod(nextId,1000)==0,writeJson(fullfile(work,'visitor-progress.json'),struct('phase',phaseLabel,'objects',nextId));end
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
        if endsWith(location,'.io.path') && (any(strcmp(normalized,cellstr(string(ioParents))))||strcmp(normalized,container))
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
        yes=~isa(value,'gpuArray')&&(isnumeric(value)||islogical(value)||ischar(value)||isstring(value)||isdatetime(value)||isduration(value)||iscalendarduration(value)||iscategorical(value));
    end
end
function writeJson(path,value)
tmp=[path '.tmp'];fid=fopen(tmp,'w','n','UTF-8');assert(fid>=0);
cleaner=onCleanup(@() fclose(fid));fprintf(fid,'%s\n',jsonencode(value,PrettyPrint=true));clear cleaner;
movefile(tmp,path,'f');
end

function out=fileHash(path)
md=java.security.MessageDigest.getInstance('SHA-256');fid=fopen(path,'rb');assert(fid>=0);c=onCleanup(@() fclose(fid));
while true,data=fread(fid,4*1024*1024,'*uint8');if isempty(data),break;end;md.update(data);end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
