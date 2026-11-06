function review_legacy_paths_v3(work,release)
% Read unknown legacy main layouts only; never save/relink scientific files.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
request=jsondecode(fileread(fullfile(work,'request.json')));
global DD_INVALID_GRAPHICS_SKIPPED;
assert(~isfile(fullfile(work,'receipt.jsonl')));
f=fopen(fullfile(work,'receipt.jsonl'),'w','n','UTF-8');assert(f>=0);cleanup=onCleanup(@() fclose(f));
candidates=0;unresolved=0;
for k=1:numel(request.projects)
    item=request.projects(k);row=struct('id',item.id,'name',item.name,'owner',item.owner, ...
        'file',item.file,'status','pending','matching_acquisitions',{{}},'matching_strings',{{}},'saved',false);
    try
        assert(~any(strcmp(item.id,cellstr(string(request.busy_project_ids)))),'Busy project deferred');
        before=dir(item.file);assert(numel(before)==1 && before.bytes<128*1024*1024,'Missing or large main needs separate admission');
        lastwarn('');variables=whos('-file',item.file);[message,~]=lastwarn;
        assert(~isempty(variables)&&~contains(message,'unknown MAT-file error'),'Unreadable MAT variable directory');
        lastwarn('');data=load(item.file);[message,~]=lastwarn;
        assert(~contains(message,'unknown MAT-file error'),'MAT contains unreadable variables');
        assert(numel(fieldnames(data))==numel(variables),'Not all variables were loaded');
        DD_INVALID_GRAPHICS_SKIPPED=0;values=stringsIn(data,0);row.invalid_graphics_handles_ignored=DD_INVALID_GRAPHICS_SKIPPED;
        row.variables={variables.name};row.classes={variables.class};
        names={};matched={};
        for a=1:numel(request.acquisitions)
            acq=request.acquisitions(a);needle=['/' acq.name '/'];
            for n=1:numel(values)
                normalized=['/' regexprep(strrep(values{n},'\','/'),'^/+|/+$','') '/'];
                if contains(normalized,needle)||contains(normalized,'/Alexander/data/2024_1/')||contains(normalized,'/Alexander/data/2024_2/')
                    names{end+1}=[acq.year '/' acq.name];matched{end+1}=values{n}; %#ok<AGROW>
                end
            end
        end
        row.matching_acquisitions=unique(names,'stable');row.matching_strings=unique(matched,'stable');
        after=dir(item.file);assert(numel(after)==1 && before.bytes==after.bytes && before.datenum==after.datenum,'File changed while reading');
        row.source_metadata_stable=true;row.status='legacy_strings_inspected';
        if ~isempty(names),candidates=candidates+1;end
        clear data values;
    catch error
        row.status='requires_review';row.error=error.message;unresolved=unresolved+1;
    end
    fprintf(f,'%s\n',jsonencode(row));
    writeJson(fullfile(work,'progress.json'),struct('inspected',k,'total',numel(request.projects),'candidates',candidates,'unresolved',unresolved,'saved',false));
end
writeJson(fullfile(work,'result.json'),struct('success',true,'inspected',numel(request.projects),'candidates',candidates,'unresolved',unresolved,'saved',false, ...
 'note','Legacy string inventory only; incomplete/unreadable layouts remain unresolved, candidates require scientific identity.'));
end

function out=stringsIn(value,depth)
global DD_INVALID_GRAPHICS_SKIPPED;
assert(depth<64,'Deep object needs separate path review');out={};
if ischar(value)
    if isrow(value),out={value};else,out=reshape(cellstr(value),1,[]);end
elseif isstring(value),out=reshape(cellstr(value),1,[]);
elseif isstruct(value)
    names=fieldnames(value);
    for i=1:numel(value)
        for j=1:numel(names),out=[out stringsIn(value(i).(names{j}),depth+1)];end %#ok<AGROW>
    end
elseif iscell(value)
    for i=1:numel(value),out=[out stringsIn(value{i},depth+1)];end %#ok<AGROW>
elseif isa(value,'matlab.graphics.primitive.Line')
    % Only textual/user metadata can contain paths; numeric plot data is ignored.
    for i=1:numel(value)
        if ~isvalid(value(i)),DD_INVALID_GRAPHICS_SKIPPED=DD_INVALID_GRAPHICS_SKIPPED+1;continue;end
        fields={'UserData','DisplayName','Tag'};
        for j=1:numel(fields),out=[out stringsIn(value(i).(fields{j}),depth+1)];end %#ok<AGROW>
    end
elseif isobject(value)
    error('Legacy live object requires a class-specific path reader: %s',class(value));
end
end

function writeJson(path,value)
p=[path '.tmp'];f=fopen(p,'w','n','UTF-8');assert(f>=0);c=onCleanup(@() fclose(f));
fprintf(f,'%s\n',jsonencode(value));clear c;movefile(p,path,'f');
end
