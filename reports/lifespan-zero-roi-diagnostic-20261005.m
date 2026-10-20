root = 'X:\Gilles\analysis\projects\x2026_04_28_Scerev_Calb_Cglab_1';
a = jsondecode(fileread(fullfile(root,'pipeline','lifespan_1','run.json')));
b = jsondecode(fileread(fullfile(root,'pipeline','lifespan_3','run.json')));
patches = {a.ctx.roiPattern.pattern.image, b.ctx.roiPattern.pattern.image, imresize(b.ctx.roiPattern.pattern.image,0.5)};
patchNames = {'run1_80px','run3_130px','run3_half_65px'};
rows = struct('position',{},'patch',{},'maxCorrelation',{},'detectionsAtPoint5',{},'imageSize',{});
for pos = [0 11 15]
    filename = sprintf('X:/Antoine/raw_data/2026_04_28_Scerev_Calb_Cglab_1/Pos%d/img_channel000_position%03d_time000000004_z001.tif',pos,pos);
    image = imread(filename);
    for k = 1:numel(patches)
        patch = patches{k}; c = normxcorr2(patch,image);
        valid = c(size(patch,1)+1:size(image,1),size(patch,2)+1:size(image,2));
        pp = regionprops(c>0.5,'Centroid'); n = 0;
        if ~isempty(pp)
            xy = round(cat(1,pp.Centroid));
            n = sum(xy(:,1)-size(patch,2)>=1 & xy(:,2)-size(patch,1)>=1 & xy(:,1)<=size(image,2) & xy(:,2)<=size(image,1));
        end
        rows(end+1) = struct('position',pos,'patch',patchNames{k},'maxCorrelation',max(valid,[],'all'),'detectionsAtPoint5',n,'imageSize',size(image));
        fprintf('Pos%d %s max=%.6f detections=%d image=%dx%d\n',pos,patchNames{k},max(valid,[],'all'),n,size(image));
    end
end
fid = fopen('C:\Users\Gilles\Documents\MATLAB\detecdiv-hub\reports\lifespan-zero-roi-20261005.json','w'); fprintf(fid,'%s',jsonencode(rows,PrettyPrint=true)); fclose(fid);

