function audit_gilles_hdf5_metadata(work,release)
% Read headers, attributes and bounded string datasets; never write image data.
assert(isfolder(release));request=jsondecode(fileread(fullfile(work,'request.json')));assert(request.read_only);
rows=cell(1,numel(request.files));links={};pathValues={};references={};datasets=0;attributes=0;stringDatasets=0;
for k=1:numel(request.files)
    item=request.files(k);assert(strcmp(fileHash(item.target),item.sha256));
    info=h5info(item.target);links={};pathValues={};references={};datasets=0;attributes=0;stringDatasets=0;
    walkGroup(info,item.target);
    assert(strcmp(fileHash(item.target),item.sha256));
    rows{k}=struct('path',item.relative_path,'sha256_unchanged',item.sha256,'datasets',datasets,'attributes',attributes, ...
        'string_datasets_read',stringDatasets,'links_requiring_review',{links},'path_values_requiring_review',{pathValues}, ...
        'references_requiring_review',{references},'scientific_arrays_read',false,'saved',false, ...
        'success',isempty(links)&&isempty(pathValues)&&isempty(references));
    writeJson(fullfile(work,'result.partial.json'),rows(1:k));
    writeJson(fullfile(work,'progress.json'),struct('files_complete',k,'total',numel(request.files),'saved',false));
end
writeJson(fullfile(work,'result.json'),struct('success',all(cellfun(@(r) r.success,rows)),'files',{rows},'read_only',true,'saved',false, ...
    'source_deleted',false,'note','Full-file hashes guard headers/attribute inspection. External storage, virtual datasets, links, object references or path-like strings require review; numeric image datasets are not rewritten or loaded.'));

    function walkGroup(group,file)
        inspectAttributes(group.Attributes,group.Name);
        for n=1:numel(group.Links),links{end+1}=struct('group',group.Name,'link',group.Links(n));end %#ok<AGROW>
        for n=1:numel(group.Datasets)
            ds=group.Datasets(n);path=strrep([group.Name '/' ds.Name],'//','/');datasets=datasets+1;
            fid=H5F.open(file,'H5F_ACC_RDONLY','H5P_DEFAULT');cf=onCleanup(@() H5F.close(fid));
            did=H5D.open(fid,path);cd=onCleanup(@() H5D.close(did));
            plist=H5D.get_create_plist(did);cp=onCleanup(@() H5P.close(plist));
            layout=H5P.get_layout(plist);external=H5P.get_external_count(plist);
            if external>0||layout==H5ML.get_constant_value('H5D_VIRTUAL')
                references{end+1}=struct('dataset',path,'layout',layout,'external_file_count',external); %#ok<AGROW>
            end
            clear cp cd cf;
            inspectAttributes(ds.Attributes,path);
            if strcmp(ds.Datatype.Class,'H5T_REFERENCE')
                references{end+1}=struct('dataset',path,'type',ds.Datatype.Class); %#ok<AGROW>
            elseif strcmp(ds.Datatype.Class,'H5T_STRING')
                assert(prod(ds.Dataspace.Size)<=100000,'Large string dataset requires explicit review');
                inspectText(h5read(file,path),path);stringDatasets=stringDatasets+1;
            elseif ~any(strcmp(ds.Datatype.Class,{'H5T_INTEGER','H5T_FLOAT','H5T_ENUM','H5T_BITFIELD'}))
                references{end+1}=struct('dataset',path,'type',ds.Datatype.Class); %#ok<AGROW>
            end
        end
        for n=1:numel(group.Groups),walkGroup(group.Groups(n),file);end
    end
    function inspectAttributes(list,where)
        for n=1:numel(list)
            attributes=attributes+1;a=list(n);
            if strcmp(a.Datatype.Class,'H5T_REFERENCE')
                references{end+1}=struct('attribute',[where '@' a.Name],'type',a.Datatype.Class); %#ok<AGROW>
            else,inspectText(a.Value,[where '@' a.Name]);end
        end
    end
    function inspectText(value,where)
        if iscell(value),for z=1:numel(value),inspectText(value{z},where);end;return;end
        if isstring(value),for z=1:numel(value),inspectText(char(value(z)),where);end;return;end
        if ischar(value)&&~isempty(regexp(value,'[A-Za-z]:[/\\]|/(data|homes2|Volumes|DataVault|mnt)/|^\\\\','once'))
            pathValues{end+1}=struct('field',where,'value',value); %#ok<AGROW>
        end
    end
end
function out=fileHash(path)
md=java.security.MessageDigest.getInstance('SHA-256');f=fopen(path,'rb');assert(f>=0);c=onCleanup(@() fclose(f));
while true,b=fread(f,4*1024*1024,'*uint8');if isempty(b),break;end;md.update(b);end
out=lower(reshape(dec2hex(typecast(md.digest(),'uint8'),2).',1,[]));
end
function writeJson(path,value)
p=[path '.tmp'];f=fopen(p,'w','n','UTF-8');assert(f>=0);c=onCleanup(@() fclose(f));fprintf(f,'%s\n',jsonencode(value));clear c;movefile(p,path,'f');
end
