function export_project3_raw_listing(work,release)
% Read-only evidence for legacy aliases. Does not establish content identity.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'2023_3-project-candidate-plan.json')));
items=request.candidate_projects;rows=cell(1,numel(items));
for k=1:numel(items)
    item=items(k);before=dir(item.source_mat);assert(~isempty(before));
    info=whos('-file',item.source_mat);idx=find(strcmp({info.class},'shallow'),1);assert(~isempty(idx));
    s=load(item.source_mat,info(idx).name);p=s.(info(idx).name);
    listingFile=fullfile(work,['project3-srclist-' item.project_id '.jsonl']);
    assert(~isfile(listingFile),'Do not overwrite existing evidence');
    fid=fopen(listingFile,'w','n','UTF-8');assert(fid>=0);guard=onCleanup(@() fclose(fid));
    entries=0;
    for f=1:numel(p.fov)
        paths=p.fov(f).srcpath;if ischar(paths)||isstring(paths),paths=cellstr(paths);end
        lists=p.fov(f).srclist;assert(iscell(lists));
        assert(numel(paths)==numel(lists),'Channel/path correspondence requires review');
        for ch=1:numel(lists)
            list=lists{ch};assert(isstruct(list));
            for t=1:numel(list)
                entry=list(t);assert(isfield(entry,'name'));
                row=struct('fov',f,'channel',ch,'legacy_directory',paths{ch},'name',entry.name);
                if isfield(entry,'bytes'),row.bytes=entry.bytes;end
                if isfield(entry,'datenum'),row.datenum=entry.datenum;end
                if isfield(entry,'folder'),row.folder=entry.folder;end
                fprintf(fid,'%s\n',jsonencode(row));entries=entries+1;
            end
        end
    end
    clear guard;
    after=dir(item.source_mat);assert(before.bytes==after.bytes && before.datenum==after.datenum);
    rows{k}=struct('project_id',item.project_id,'name',item.name,'source_mat',item.source_mat, ...
        'source_mat_bytes',before.bytes,'source_mat_datenum',before.datenum,'source_stable',true, ...
        'project_saved',false,'fovs',numel(p.fov),'listing_entries',entries,'listing_file',listingFile);
    writeJson(fullfile(work,'project3-listing-export-progress.json'),rows(1:k));
    clear s p;
end
writeJson(fullfile(work,'project3-listing-export.json'),rows);
end
function writeJson(path,value)
tmp=[path '.tmp'];fid=fopen(tmp,'w','n','UTF-8');assert(fid>=0);
c=onCleanup(@() fclose(fid));fprintf(fid,'%s\n',jsonencode(value,PrettyPrint=true));clear c;
movefile(tmp,path,'f');
end
