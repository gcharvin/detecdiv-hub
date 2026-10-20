function verify_alex_logeb2_851
% Independent read-only verification of every final ROI and extraction image.
maxNumCompThreads(1);
release='/home/charvin-admin/repos/DetecDiv-jobs/releases/dce3ca768bcae597224424620abc46ede0788a12';
restoredefaultpath; addpath(genpath(release));
assert(startsWith(which('roi'),release),'Unexpected roi implementation');
root='/data/Alexander/nobackup/analysis/anof_071026_X54445_LOGEB_2';
runPath=fullfile(root,'pipeline','alex_logeb2_full_auto_gpu_20261015');
inventory=jsondecode(fileread(fullfile(runPath,'initial_roi_inventory.json')));
inputRows=inventory.rows;
assert(numel(inputRows)==1437 && numel(unique([inputRows.fov]))==28);
runState=jsondecode(fileread(fullfile(runPath,'run.json')));
assert(strcmp(runState.status,'done'));
report=runState.outputs.report;
assert(numel(report.nodeRuns)==2 && all(strcmp({report.nodeRuns.status},'done')));
boundaries=[1 88 89 176 177 264 265 352 353 440 441 528 529 616 617 704 705 792 793 851];
rawNames={'channel000_z000','channel000_z001','channel000_z002','channel001_z001','channel002_z001','channel003_z001'};
expectedNames=[rawNames {'seg'}];
rows=repmat(struct('flatIndex',0,'fov',0,'roiId','','complete',false, ...
 'reason','','dataOnlyReloadEqual',false,'fullReloadEqual',false, ...
 'rawPixelsEqual',false,'boundariesEqual',false,'imageSize',[], ...
 'probabilitySize',[],'fluorescenceSize',[],'elapsedSeconds',0),numel(inputRows),1);
detail=fullfile(runPath,'final_verification_rows.jsonl');
assert(~isfile(detail),'Verification already has a journal; inspect before rerunning');
fid=fopen(detail,'w'); assert(fid>0); guard=onCleanup(@() fclose(fid)); %#ok<NASGU>
started=tic;
for i=1:numel(inputRows)
 one=tic; item=inputRows(i); row=rows(i);
 row.flatIndex=item.flatIndex; row.fov=item.fov; row.roiId=char(item.roiId);
 try
  T=double(item.frames); assert(T==851);
  assert(isequal(double(item.rawChannelFrames(:)'),[851 851 851 214 214 214]));
  folder=char(item.folder); rid=row.roiId;
  sidecar=fullfile(folder,['results_' rid '.mat']);
  h5=fullfile(folder,['im_' rid '.h5']);
  mat=fullfile(folder,['im_' rid '.mat']);
  assert(isfile(sidecar) && isfile(h5) && isfile(mat),'Required artifact missing');
  saved=load(sidecar,'results'); assert(isfield(saved,'results'));
  z=saved.results;
  assert(isfield(z,'div_1') && numel(z.div_1.id)==T && numel(z.div_1.labels)==T,'Incomplete division predictions or labels');
  assert(isequal(size(z.div_1.prob),[6 T]),'Unexpected probability dimensions');
  assert(isfield(z,'RLS') && isfield(z.RLS,'from_div_1'),'Missing RLS');
  assert(isfield(z,'signal') && isfield(z.signal,'full') && isfield(z.signal,'onemask'),'Missing fluorescence outputs');
  fluo=z.signal.full.from_full_with100maxPixels.meanfluo;
  assert(isequal(size(fluo),[3 T]),'Unexpected fluorescence dimensions');
  row.probabilitySize=size(z.div_1.prob); row.fluorescenceSize=size(fluo);
  info=h5info(h5); names={info.Datasets.Name};
  assert(numel(names)==7 && numel(unique(names))==7 && all(ismember(expectedNames,names)),'Missing or duplicate image channel');
  value=double(item.value(:)'); expectedSize=[value(4) value(3) 7 T];
  for c=1:7
   k=find(strcmp(names,expectedNames{c}),1);
   dims=info.Datasets(k).Dataspace.Size;
   assert(isequal(double(dims(:)'),[expectedSize(1:2) 1 T]),'Incomplete HDF5 channel dimensions');
  end
  r=roi(rid,value); r.path=folder;
  r.load('data'); assert(isequaln(r.results,z),'load(data) results differ from sidecar');
  row.dataOnlyReloadEqual=true;
  r.results=[]; r.clear; r.load('Silent');
  assert(isequaln(r.results,z),'Full load results differ from sidecar');
  row.fullReloadEqual=true;
  assert(isequal(size(r.image),expectedSize),'Wrong full image size');
  assert(isequal(r.channelid,1:7) && isequal(r.findChannelID('seg','exact'),7),'Wrong channel mapping');
  for c=1:6
   assert(isequal(r.findChannelID(rawNames{c},'exact'),c),'Raw channel mapping changed');
  end
  legacy=load(mat,'roiobj');
  assert(isfield(legacy,'roiobj') && isa(legacy.roiobj,'roi'));
  assert(isequal(size(legacy.roiobj.image),[expectedSize(1:2) 6 T]),'Incomplete legacy extraction');
  % Compare all pixels, channel by channel to bound temporary comparison memory.
  for c=1:6
   assert(isequal(r.image(:,:,c,:),legacy.roiobj.image(:,:,c,:)),'Raw pixels changed after analysis');
  end
  row.rawPixelsEqual=true;
  for frame=boundaries
   assert(isequal(r.image(:,:,1:6,frame),legacy.roiobj.image(:,:,1:6,frame)),'Block boundary mismatch');
   assert(any(reshape(r.image(:,:,1:6,frame),[],1)~=0),'Completely empty block boundary frame');
  end
  row.boundariesEqual=true; row.imageSize=size(r.image);
  row.complete=true; row.reason='all_results_reload_and_full_raw_pixels_verified';
 catch ME
  row.reason=ME.message;
  fprintf('VERIFY_FAILED %d %s: %s\n',i,row.roiId,row.reason);
 end
 clear r legacy saved z info fluo;
 row.elapsedSeconds=toc(one); rows(i)=row;
 fprintf(fid,'%s\n',jsonencode(row));
 if mod(i,10)==0 || i==1 || i==numel(inputRows)
  progress=struct('status','verifying','checkedCount',i,'totalCount',numel(inputRows), ...
   'passedCount',sum([rows(1:i).complete]),'failedCount',sum(~[rows(1:i).complete]),'elapsedSeconds',toc(started));
  writeJson(fullfile(runPath,'final_verification_progress.json'),progress);
  fprintf('VERIFY_PROGRESS %d/%d passed=%d failed=%d seconds=%.1f\n',i,numel(inputRows),progress.passedCount,progress.failedCount,progress.elapsedSeconds);
 end
end
passed=all([rows.complete]);
manifest=struct('jobId','8d23a039-3082-46a7-913a-1feb097522f1', ...
 'status','failed','verifiedAtUtc',char(datetime('now','TimeZone','UTC','Format','yyyy-MM-dd''T''HH:mm:ss''Z''')), ...
 'coreCommit','dce3ca768bcae597224424620abc46ede0788a12','roiCode',which('roi'), ...
 'matlabVersion',version,'frames',851,'roiCount',numel(rows),'fovCount',numel(unique([rows.fov])), ...
 'completedCount',sum([rows.complete]),'failedCount',sum(~[rows.complete]), ...
 'blockBoundaryFrames',boundaries,'rawSourceAcquisitions',[851 851 851 214 214 214], ...
 'fullRawPixelComparison',true,'dataOnlyAndFullReload',true,'nodeRuns',report.nodeRuns, ...
 'elapsedSeconds',toc(started),'rows',rows);
if passed, manifest.status='final_verified'; end
writeJson(fullfile(runPath,'final_results_verification_851.json'),manifest);
fprintf('FINAL_VERIFIED passed=%d failed=%d total=%d\n',manifest.completedCount,manifest.failedCount,numel(rows));
assert(passed,'Final verification failed; see final_results_verification_851.json');
end

function writeJson(path,value)
staged=[path '.verification-tmp'];
fid=fopen(staged,'w'); assert(fid>0);
fprintf(fid,'%s\n',jsonencode(value)); fclose(fid);
[ok,msg]=movefile(staged,path,'f'); assert(ok,msg);
end
