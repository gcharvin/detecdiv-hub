function audit_relink_value_types(work,release)
% Readonly scientific-type readiness; no MAT, project or backup is saved.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'request.json')));assert(request.read_only);
seen=[];classes=[];gpu=[];functionsFound=[];private=[];rows=cell(1,numel(request.files));
for k=1:numel(request.files)
    item=request.files(k);assert(strcmp(fileHash(item.target),item.sha256));
    loaded=load(item.target);seen=containers.Map('KeyType','char','ValueType','any');
    classes=containers.Map('KeyType','char','ValueType','double');gpu={};functionsFound={};private={};
    scan(loaded,'metadata',0);clear loaded;
    assert(strcmp(fileHash(item.target),item.sha256));
    rows{k}=struct('project_id',item.project_id,'path',item.relative_path,'sha256_unchanged',item.sha256, ...
        'classes',{classes.keys},'class_counts',{classes.values},'gpu_values',{gpu},'function_handles',{functionsFound}, ...
        'nonpublic_persistent_properties',{private},'saved',false);
    writeJson(fullfile(work,'result.partial.json'),rows(1:k));writeJson(fullfile(work,'progress.json'),struct('files_complete',k,'total',numel(request.files),'saved',false));
end
writeJson(fullfile(work,'result.json'),struct('success',true,'files',{rows},'read_only',true,'saved',false,'source_deleted',false, ...
    'note','Readiness inventory only. Any private persistent state or function closure must have explicit equivalence/path coverage before scientific save.'));
    function scan(value,where,depth)
        assert(depth<80,'Unexpected persistent cycle or depth');
        name=class(value);if isKey(classes,name),classes(name)=classes(name)+1;else,classes(name)=1;end
        if isa(value,'gpuArray')
            cpu=gather(value);assert(isnumeric(cpu)||islogical(cpu));
            assert(isequal(size(cpu),size(value)),'GPU content unavailable');
            gpu{end+1}=struct('field',where,'class','gpuArray','underlying_class',class(cpu),'size',size(cpu),'content_sha256',numericHash(cpu));return %#ok<AGROW>
        end
        if isnumeric(value)||islogical(value)||ischar(value)||isstring(value)||isdatetime(value)||isduration(value)||iscalendarduration(value)||iscategorical(value),return;end
        if isa(value,'function_handle')
            description=functions(value);
            functionsFound{end+1}=struct('field',where,'type',description.type,'function',description.function,'definition_file',description.file, ...
                'workspace_present',isfield(description,'workspace')&&~isempty(description.workspace));return %#ok<AGROW>
        end
        if iscell(value)
            for z=1:numel(value),scan(value{z},where,depth+1);end;return
        end
        if istable(value)
            for n=value.Properties.VariableNames,scan(value.(n{1}),[where '.' n{1}],depth+1);end
            scan(value.Properties.UserData,[where '.UserData'],depth+1);return
        end
        if isstruct(value)
            fields=sort(fieldnames(value));for z=1:numel(fields),scan({value.(fields{z})},[where '.' fields{z}],depth+1);end;return
        end
        if isa(value,'containers.Map')
            keys=value.keys;for z=1:numel(keys),scan(value(keys{z}),[where '.MapValue'],depth+1);end;return
        end
        if isobject(value)
            for z=1:numel(value)
                node=value(z);
                if isa(node,'handle')
                    assert(isvalid(node),'Invalid persisted handle needs explicit coverage');
                    if isKey(seen,name),group=seen(name);if any(group==node),continue;end;group(end+1)=node;seen(name)=group;
                    else,seen(name)=node;end
                end
                meta=metaclass(node);props=meta.PropertyList;
                for q=1:numel(props)
                    if iscell(props),prop=props{q};else,prop=props(q);end
                    if prop.Transient||prop.Dependent||prop.Constant,continue;end
                    if ~ischar(prop.GetAccess)||~strcmp(prop.GetAccess,'public')
                        private{end+1}=struct('class',name,'field',[where '.' prop.Name]);continue %#ok<AGROW>
                    end
                    scan(node.(prop.Name),[where '.' prop.Name],depth+1);
                end
            end
        end
    end
end
function out=numericHash(value)
assert(~issparse(value),'Sparse GPU value needs explicit adapter');md=java.security.MessageDigest.getInstance('SHA-256');
parts={real(value)};if ~isreal(value),parts{2}=imag(value);end
for k=1:numel(parts)
    part=parts{k};if islogical(part),bytes=uint8(part(:));else,bytes=typecast(part(:),'uint8');end;md.update(bytes);
end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function out=fileHash(path)
md=java.security.MessageDigest.getInstance('SHA-256');f=fopen(path,'rb');assert(f>=0);c=onCleanup(@() fclose(f));
while true,b=fread(f,4*1024*1024,'*uint8');if isempty(b),break;end;md.update(b);end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function writeJson(path,value)
p=[path '.tmp'];f=fopen(p,'w','n','UTF-8');assert(f>=0);c=onCleanup(@() fclose(f));fprintf(f,'%s\n',jsonencode(value));clear c;movefile(p,path,'f');
end
