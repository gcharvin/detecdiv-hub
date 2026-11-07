function audit_gilles_main_typed(work,release)
% Read-only path plan for fully verified, identity-corroborated project copies.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
requestFile=fullfile(work,'request.json');requestHash=fileHash(requestFile);
request=jsondecode(fileread(requestFile));assert(request.read_only&&numel(request.projects)==1&&strcmp(request.projects.name,'281124_Alex'));
seen=[];roots={};container='';pathCount=0;changeCount=0;unknownCount=0;changes=[];unknown=[];
receipt=fullfile(work,'receipt.jsonl');assert(~isfile(receipt));fid=fopen(receipt,'w','n','UTF-8');assert(fid>=0);
cleanup=onCleanup(@() fclose(fid));projects=cell(1,numel(request.projects));
for projectIndex=1:numel(request.projects)
    item=request.projects(projectIndex);container=item.final_container;
    roots=cell(numel(item.mappings),2);
    for mapIndex=1:numel(item.mappings),roots(mapIndex,:)={item.mappings(mapIndex).old,item.mappings(mapIndex).target};end
    total=0;totalPaths=0;filesNeedingRebase=0;filesWithUnknown=0;imageFilesExcluded=0;otherFiles={};
    for fileIndex=1:numel(item.verified_files)
        record=item.verified_files(fileIndex);[~,basename,ext]=fileparts(record.path);
        if strcmpi(ext,'.mat')&&startsWith(basename,'im_'),imageFilesExcluded=imageFilesExcluded+1;continue;end
        if ~any(strcmpi(ext,{'.mat','.json'})),otherFiles{end+1}=record.path;continue;end %#ok<AGROW>
        file=fullfile(item.stage,record.path);assert(strcmp(fileHash(file),record.sha256),'Verified copy changed');
        if strcmpi(ext,'.mat'),document=load(file);else,document=jsondecode(fileread(file));end
        seen=containers.Map('KeyType','char','ValueType','any');pathCount=0;changeCount=0;unknownCount=0;
        changes=struct('field',{},'before',{},'after',{});unknown=struct('field',{},'value',{});
        scan(document,'metadata',false,0);clear document;
        assert(strcmp(fileHash(file),record.sha256),'File changed while read-only inspected');
        fprintf(fid,'%s\n',jsonencode(struct('project_id',item.project_id,'relative_path',record.path,'sha256_unchanged',record.sha256, ...
            'absolute_path_values',pathCount,'recognized_rebase_values',changeCount,'unresolved_path_values',unknownCount, ...
            'rebase_examples',changes,'unresolved_examples',unknown,'file_saved',false)));
        total=total+1;totalPaths=totalPaths+pathCount;filesNeedingRebase=filesNeedingRebase+(changeCount>0);filesWithUnknown=filesWithUnknown+(unknownCount>0);
        if mod(total,100)==0
            writeJson(fullfile(work,'progress.json'),struct('project',item.name,'project_index',projectIndex,'files_audited',total, ...
                'current_relative_path',record.path,'files_needing_rebase',filesNeedingRebase,'files_with_unresolved_paths',filesWithUnknown,'saved',false));
        end
    end
    projects{projectIndex}=struct('project_id',item.project_id,'name',item.name,'metadata_files_audited',total, ...
        'image_mat_files_excluded',imageFilesExcluded,'absolute_path_values',totalPaths,'files_needing_rebase',filesNeedingRebase, ...
        'files_with_unresolved_paths',filesWithUnknown,'other_files_needing_type_review',{otherFiles},'saved',false);
    writeJson(fullfile(work,'result.partial.json'),projects(1:projectIndex));
end
clear cleanup;assert(strcmp(fileHash(requestFile),requestHash));
writeJson(fullfile(work,'result.json'),struct('success',true,'projects',{projects},'saved',false,'source_deleted',false,'catalog_changed',false, ...
    'note','Read-only candidate mapping plan, not approval to rebase unresolved paths or a scientific save/reopen receipt.'));

    function scan(value,location,isPath,depth)
        assert(depth<80,'Unexpected persistent object cycle or depth');
        if isnumeric(value)||islogical(value)||isdatetime(value)||isduration(value)||iscalendarduration(value)||iscategorical(value),return;end
        if ischar(value)
            if isPath && (isrow(value)||isempty(value)),inspectText(value,location);end
            return
        end
        if isstring(value)
            if isPath,for z=1:numel(value),inspectText(char(value(z)),location);end;end
            return
        end
        if iscell(value)
            for z=1:numel(value),scan(value{z},location,isPath,depth+1);end
            return
        end
        if istable(value)
            names=value.Properties.VariableNames;
            for z=1:numel(names),field=names{z};scan(value.(field),[location '.' field],pathField(field,isPath),depth+1);end
            scan(value.Properties.UserData,[location '.UserData'],false,depth+1);return
        end
        if isstruct(value)
            names=sort(fieldnames(value));
            for z=1:numel(names),field=names{z};scan({value.(field)},[location '.' field],pathField(field,isPath),depth+1);end
            return
        end
        if isa(value,'containers.Map')
            keys=value.keys;
            for z=1:numel(keys),scan(value(keys{z}),[location '.MapValue'],false,depth+1);end
            return
        end
        if isobject(value)
            for z=1:numel(value)
                node=value(z);name=class(node);
                if isa(node,'handle')
                    assert(isvalid(node),'Invalid persistent handle requires review');
                    if isKey(seen,name)
                        group=seen(name);if any(group==node),continue;end
                        group(end+1)=node;seen(name)=group;
                    else,seen(name)=node;end
                end
                meta=metaclass(node);props=meta.PropertyList;
                for q=1:numel(props)
                    if iscell(props),prop=props{q};else,prop=props(q);end
                    if prop.Transient||prop.Dependent||prop.Constant||~ischar(prop.GetAccess)||~strcmp(prop.GetAccess,'public'),continue;end
                    field=prop.Name;scan(node.(field),[location '.' field],pathField(field,false),depth+1);
                end
            end
        end
    end
    function yes=pathField(field,parent)
        protected=any(strcmpi(field,{'notes','comments','description','history','tag','message','label'}));
        yes=~protected && (parent||~isempty(regexpi(field,'path|file|dir|source|src|folder|root|uri','once')));
    end
    function inspectText(text,location)
        if isempty(text),return;end
        if text(1)~='/' && text(1)~='\' && ~(numel(text)>=2 && text(2)==':'),return;end
        pathCount=pathCount+1;v=regexprep(strrep(text,'\','/'),'/+$','');target='';
        if (endsWith(location,'.io.path') || strcmp(location,'metadata.shallowObj.parsedData.folder')) && any(strcmp(v,cellstr(string(item.io_parent_aliases))))
            target=container;
        else
            for q=1:size(roots,1)
                old=roots{q,1};match=strcmp(v,old)||startsWith(v,[old '/']);
                if startsWith(old,'//'),match=strcmpi(v,old)||startsWith(v,[old '/'],'IgnoreCase',true);end
                if match,target=[roots{q,2} v(numel(old)+1:end)];break;end
            end
        end
        if isempty(target)
            unknownCount=unknownCount+1;
            if numel(unknown)<30,unknown(end+1)=struct('field',location,'value',text);end
        else
            changeCount=changeCount+1;
            if numel(changes)<30,changes(end+1)=struct('field',location,'before',text,'after',target);end
        end
    end
end

function out=fileHash(path)
md=java.security.MessageDigest.getInstance('SHA-256');fid=fopen(path,'rb');assert(fid>=0);c=onCleanup(@() fclose(fid));
while true,data=fread(fid,4*1024*1024,'*uint8');if isempty(data),break;end;md.update(data);end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function writeJson(path,value)
tmp=[path '.tmp'];fid=fopen(tmp,'w','n','UTF-8');assert(fid>=0);c=onCleanup(@() fclose(fid));
fprintf(fid,'%s\n',jsonencode(value,PrettyPrint=true));clear c;movefile(tmp,path,'f');
end
