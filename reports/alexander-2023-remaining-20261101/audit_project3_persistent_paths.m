function audit_project3_persistent_paths(work,release)
% Read-only audit of every non-image MAT in the verified project copies.
% Original images, project objects, scientific values and annotations are not saved.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
control=jsondecode(fileread(fullfile(work,'project3-persistent-paths-request.json')));
input=fullfile(work,'project3-readonly-request.json');
assert(strcmp(fileHash(input),control.input_request_sha256));
assert(strcmp(fileHash(fullfile(work,'project3-readonly-inspection.json')),control.identity_receipt_sha256));
request=jsondecode(fileread(input));assert(request.read_only);
seen=[];roots={};container='';pathCount=0;changeCount=0;unknownCount=0;changes=[];unknown=[];
receipt=fullfile(work,'project3-persistent-paths-receipt.jsonl');assert(~isfile(receipt));
fid=fopen(receipt,'w','n','UTF-8');assert(fid>=0);
cleaner=onCleanup(@() fclose(fid));projects=cell(1,numel(request.projects));
for k=1:numel(request.projects)
    item=request.projects(k);stem=item.name;
    container=['/homes2/maliavko/DetecdivHub/projects/' stem];
    canonical=[container '/' stem];
    source='/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis';
    roots={['Z:/Alexander/analysis/' stem],canonical; ...
        ['/DataVault/Alexander/analysis/' stem],canonical; ...
        [source '/' stem],canonical; ...
        ['//10.20.8.250/Sauvegarde/Alexander/analysis/' stem],canonical};
    acqs=item.candidate_raw_acquisitions;
    if ischar(acqs)||isstring(acqs),acqs=cellstr(acqs);end
    for n=1:numel(acqs)
        target=['/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_3/' acqs{n}];
        roots(end+1,:)={['Z:/Alexander/' acqs{n}],target}; %#ok<AGROW>
        roots(end+1,:)={['/data/Alexander/data/2023_3/' acqs{n}],target}; %#ok<AGROW>
    end
    verifiedNames=item.verified_file_paths;if isstring(verifiedNames),verifiedNames=cellstr(verifiedNames);end
    total=0;totalPaths=0;filesNeedingRebase=0;filesWithUnknown=0;imageFilesExcluded=0;
    for n=1:numel(verifiedNames)
        [~,basename,ext]=fileparts(verifiedNames{n});
        if ~strcmpi(ext,'.mat'),continue;end
        if startsWith(basename,'im_'),imageFilesExcluded=imageFilesExcluded+1;continue;end
        file=fullfile(item.stage,verifiedNames{n});prior=dir(file);assert(numel(prior)==1 && ~prior.isdir);
        value=load(file);seen=containers.Map('KeyType','char','ValueType','any');
        pathCount=0;changeCount=0;unknownCount=0;
        changes=struct('field',{},'before',{},'after',{});unknown=struct('field',{},'value',{});
        scan(value,'mat',false,0);clear value;
        after=dir(file);assert(prior.bytes==after.bytes && prior.datenum==after.datenum);
        row=struct('project_id',item.project_id,'relative_path',verifiedNames{n},'bytes',prior.bytes, ...
            'absolute_path_values',pathCount,'recognized_rebase_values',changeCount,'unresolved_path_values',unknownCount, ...
            'rebase_examples',changes,'unresolved_examples',unknown,'file_saved',false);
        fprintf(fid,'%s\n',jsonencode(row));
        total=total+1;totalPaths=totalPaths+pathCount;
        filesNeedingRebase=filesNeedingRebase+(changeCount>0);filesWithUnknown=filesWithUnknown+(unknownCount>0);
        if mod(total,100)==0
            writeJson(fullfile(work,'project3-persistent-paths-progress.json'),struct('project',stem,'project_index',k, ...
                'files_audited',total,'current_relative_path',verifiedNames{n},'files_needing_rebase',filesNeedingRebase, ...
                'files_with_unresolved_paths',filesWithUnknown,'files_saved',false));
        end
    end
    projects{k}=struct('project_id',item.project_id,'name',stem,'non_image_mat_files_audited',total, ...
        'image_mat_files_excluded',imageFilesExcluded,'absolute_path_values',totalPaths, ...
        'files_needing_rebase',filesNeedingRebase,'files_with_unresolved_paths',filesWithUnknown,'files_saved',false);
    writeJson(fullfile(work,'project3-persistent-paths.partial.json'),projects(1:k));
end
clear cleaner;
assert(strcmp(fileHash(input),control.input_request_sha256));
writeJson(fullfile(work,'project3-persistent-paths.json'),struct('success',true,'projects',{projects}, ...
    'engine_release',release,'files_saved',false,'project_paths_rewritten',false,'catalog_changed',false,'source_deleted',false, ...
    'note','Read-only candidate path plan; unresolved paths require review before any rewrite.'));

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
        if endsWith(location,'.io.path') && any(strcmp(v,{'Z:/Alexander/analysis','/DataVault/Alexander/analysis','/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis'}))
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
