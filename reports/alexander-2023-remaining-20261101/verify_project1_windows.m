function verify_project1_windows(work,release)
% Apply the worker's existing mapping only in memory. Never save the project.
addpath(genpath(fullfile(release,'structure')));addpath(fullfile(release,'helpers'));
stem='anof_190423_nh_sir2_tween';
canonical='/homes2/maliavko/DetecdivHub';
unc='//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub';
container=[unc '/projects/' stem];file=fullfile(container,[stem '.mat']);
originalStat=dir(file);s=load(file);p=s.shallowObj;
assert(strcmp(p.io.path,[canonical '/projects/' stem]));
p.setPath(container,stem);
assert(numel(p.fov)==28);n=0;
samples=struct('fov',{},'roi',{},'path',{},'imageClass',{},'imageSize',{}, ...
    'rawClass',{},'rawSize',{},'rawPath',{},'legacyLoadPreservesMappedPath',{},'roiAndRawReadable',{});
for f=1:numel(p.fov)
    for ch=1:numel(p.fov(f).srcpath)
        p.fov(f).srcpath{ch}=mapped(p.fov(f).srcpath{ch});
        assert(isfolder(p.fov(f).srcpath{ch}));
    end
    for ch=1:numel(p.fov(f).srclist)
        list=p.fov(f).srclist{ch};
        if isfield(list,'folder')
            for k=1:numel(list),list(k).folder=mapped(list(k).folder);end
        end
        p.fov(f).srclist{ch}=list;
    end
    for r=1:numel(p.fov(f).roi)
        a=p.fov(f).roi(r);n=n+1;
        path=strrep(a.path,'\','/');assert(startsWith(path,[container '/' stem '/']));
        assert(isfile(fullfile(a.path,['im_' a.id '.mat'])));
        assert(isfile(fullfile(a.path,['results_' a.id '.mat'])));
        assert(isfile(fullfile(a.path,['data_' a.id '.mat'])));
    end
    current=p.fov(f).roi(1);a=roi(current.id,current.value);a.path=current.path;
    a.load('Silent');assert(strcmp(a.path,current.path) && ~isempty(a.image));
    list=p.fov(f).srclist{1};name=list(1).name;
    if startsWith(name,'/')||startsWith(name,'\'),raw=mapped(name);else,raw=fullfile(p.fov(f).srcpath{1},name);end
    pixels=imread(raw);assert(~isempty(pixels));
    samples(f)=struct('fov',f,'roi',a.id,'path',a.path,'imageClass',class(a.image), ...
        'imageSize',size(a.image),'rawClass',class(pixels),'rawSize',size(pixels),'rawPath',raw, ...
        'legacyLoadPreservesMappedPath',true,'roiAndRawReadable',true); %#ok<AGROW>
    clear a pixels;
end
assert(n==14386);after=dir(file);
assert(after.bytes==originalStat.bytes && after.datenum==originalStat.datenum);
r=struct('success',true,'project_saved',false,'fovs',28,'rois',n, ...
    'roi_sidecar_files_present',3*n,'project',file,'engine_release',release,'samples',samples);
fid=fopen(fullfile(work,'project1-windows-verification.json'),'w','n','UTF-8');assert(fid>=0);
c=onCleanup(@() fclose(fid));fprintf(fid,'%s\n',jsonencode(r,PrettyPrint=true)); %#ok<NASGU>
    function out=mapped(value)
        out=strrep(char(string(value)),'\','/');
        if startsWith(out,[canonical '/'])||strcmp(out,canonical),out=[unc out(numel(canonical)+1:end)];end
        assert(~startsWith(out,'/data/Alexander/data/2023_1'),'Stale primary raw path');
    end
end
