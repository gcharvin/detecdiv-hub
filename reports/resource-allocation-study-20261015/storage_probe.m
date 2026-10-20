% Local synthetic probe only. No NAS paths, Hub requests or job data.
maxNumCompThreads(1);
study=fileparts(mfilename('fullpath'));
fixture=fullfile(study,'storage-fixture');
assert(~isfolder(fixture),'Refuse to reuse an existing fixture folder.');
mkdir(fixture);
guard=onCleanup(@()removeFixture(fixture,study)); %#ok<NASGU>
rng(851);
H=16; W=16; C=6; T=851; N=2;
truth=cell(1,N);
for r=1:N, truth{r}=randi([0 65000],[H W C T],'uint16'); end
rows=struct([]);
for cut=[88 64 44]
 for format={'legacy_mat_full','hdf5_partial'}
  mode=format{1};readBytes=0;writeBytes=0;
  files=cell(1,N);
  for r=1:N
   if strcmp(mode,'legacy_mat_full')
    files{r}=fullfile(fixture,sprintf('roi%d.mat',r));
   else
    files{r}=fullfile(fixture,sprintf('roi%d.h5',r));
    if isfile(files{r}), delete(files{r}); end
   end
  end
  tic;
  if strcmp(mode,'hdf5_partial')
   for r=1:N
    for c=1:C
     h5create(files{r},sprintf('/raw%d',c),[H W 1 T], ...
      'Datatype','uint16','ChunkSize',[H W 1 16]);
    end
   end
  end
  starts=1:cut:T;
  for b=1:numel(starts)
   first=starts(b);last=min(T,first+cut-1);ix=first:last;
   for r=1:N
    if strcmp(mode,'legacy_mat_full')
     if b==1
      image=zeros(H,W,C,T,'uint16');
     else
      image=load(files{r},'image').image;
      readBytes=readBytes+numel(image)*2;
     end
     image(:,:,:,ix)=truth{r}(:,:,:,ix);
     save(files{r},'image','-v7','-nocompression');
     writeBytes=writeBytes+numel(image)*2;
     clear image;
    else
     for c=1:C
      h5write(files{r},sprintf('/raw%d',c),truth{r}(:,:,c,ix), ...
       [1 1 1 first],[H W 1 numel(ix)]);
     end
     writeBytes=writeBytes+H*W*C*numel(ix)*2;
    end
   end
  end
  seconds=toc;
  fileBytes=0;
  for r=1:N
   if strcmp(mode,'legacy_mat_full')
    actual=load(files{r},'image').image;
   else
    actual=zeros(H,W,C,T,'uint16');
    for c=1:C, actual(:,:,c,:)=h5read(files{r},sprintf('/raw%d',c)); end
   end
   assert(isequal(actual,truth{r}),'Pixel mismatch');
   fileBytes=fileBytes+dir(files{r}).bytes;
  end
  row=struct('format',mode,'framesPerBlock',cut,'blocks',numel(starts), ...
   'seconds',seconds,'logicalImageBytesRead',readBytes, ...
   'logicalImageBytesWritten',writeBytes,'finalFileBytes',fileBytes, ...
   'pixelsExact',true,'roiCount',N,'shape',[H W C T]);
  rows=[rows row]; %#ok<AGROW>
  fprintf('%s cut=%d blocks=%d seconds=%.3f imageIO_MiB=%.2f\n', ...
   mode,cut,numel(starts),seconds,(readBytes+writeBytes)/2^20);
 end
end
proof=struct('scope','local synthetic arrays, no production data', ...
 'matlabVersion',version,'compression','disabled in both formats', ...
 'caveat','Single warm-cache workstation probe; logical image bytes, not measured NAS traffic or future production speedup.', ...
 'rows',rows);
fid=fopen(fullfile(study,'storage_results.json'),'w');assert(fid>0);
fwrite(fid,jsonencode(proof),'char');fclose(fid);
% Exercise the existing universal ROI streaming implementation, unchanged.
core='C:/Users/Gilles/Documents/MATLAB/DetecDiv';
addpath(genpath(core));
tests=runtests(fullfile(core,'structure','io','tests','testRoiStreamingFrameSave.m'));
assertSuccess(tests);
fprintf('UNIVERSAL_ROI_STREAMING_TESTS=%d PASSED\n',numel(tests));

function removeFixture(folder,study)
assert(strcmpi(fileparts(char(java.io.File(folder).getCanonicalPath())), ...
 char(java.io.File(study).getCanonicalPath())));
assert(endsWith(folder,'storage-fixture'));
if isfolder(folder), rmdir(folder,'s'); end
end
