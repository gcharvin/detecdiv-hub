function legacy_matlab_jpg_preview(config_path)
config = jsondecode(fileread(config_path));
sourcePath = char(string(config.source_path));
resultPath = char(string(config.result_path));
fps = max(1, double(config.fps));
maxFrames = max(1, double(config.max_frames));
maxDimension = max(64, double(config.max_dimension));

result = struct( ...
    'status', 'failed', ...
    'error', '', ...
    'frame_count', 0, ...
    'source_width', 0, ...
    'source_height', 0, ...
    'encoded_width', 0, ...
    'encoded_height', 0, ...
    'channel_labels', {{}} ...
);

try
    frameDir = char(string(config.frame_dir));
    files = collect_legacy_image_files(sourcePath);
    if isempty(files)
        error('legacy_matlab_jpg_preview:NoFiles', 'No JPEG files found under %s.', sourcePath);
    end

    channelKeys = string({files.channel_key});
    uniqueKeys = unique(channelKeys, 'stable');
    channelLabels = cell(1, numel(uniqueKeys));
    channelFiles = cell(1, numel(uniqueKeys));
    for idx = 1:numel(uniqueKeys)
        key = uniqueKeys(idx);
        mask = channelKeys == key;
        channelFiles{idx} = files(mask);
        channelLabels{idx} = char(channel_label_from_key(key));
    end
    channelBinningFactors = channel_binning_factors(config, numel(channelFiles));

    counts = cellfun(@numel, channelFiles);
    totalFrames = max(counts);
    selectedIndices = sample_indices(totalFrames, maxFrames);

    if ~exist(frameDir, 'dir')
        mkdir(frameDir);
    end

    sourceWidth = 0;
    sourceHeight = 0;
    encodedWidth = 0;
    encodedHeight = 0;

    for frameIndex = selectedIndices
        channelFrames = cell(1, numel(channelFiles));
        rawWidths = zeros(1, numel(channelFiles));
        rawHeights = zeros(1, numel(channelFiles));
        for channelIndex = 1:numel(channelFiles)
            framesForChannel = channelFiles{channelIndex};
            sourceIndex = min(frameIndex, numel(framesForChannel));
            imagePath = fullfile(framesForChannel(sourceIndex).folder, framesForChannel(sourceIndex).name);
            [frame, rawWidth, rawHeight] = read_legacy_frame(imagePath, maxDimension);
            if sourceWidth == 0
                sourceWidth = rawWidth;
                sourceHeight = rawHeight;
            end
            channelFrames{channelIndex} = frame;
            rawWidths(channelIndex) = rawWidth;
            rawHeights(channelIndex) = rawHeight;
        end

        referenceIndex = first_known_binning_index(channelBinningFactors);
        if referenceIndex < 1
            referenceIndex = 1;
        end
        referenceBinning = max(1, channelBinningFactors(referenceIndex));
        referenceWidth = rawWidths(referenceIndex);
        referenceHeight = rawHeights(referenceIndex);

        targetWidths = zeros(1, numel(channelFiles));
        targetHeights = zeros(1, numel(channelFiles));
        for channelIndex = 1:numel(channelFiles)
            binningFactor = max(1, channelBinningFactors(channelIndex));
            scaleFactor = binningFactor / double(referenceBinning);
            if channelBinningFactors(channelIndex) <= 0
                scaleFactor = infer_scale_from_reference(
                    rawWidths(channelIndex),
                    rawHeights(channelIndex),
                    referenceWidth,
                    referenceHeight
                );
            end
            targetWidths(channelIndex) = max(1, round(rawWidths(channelIndex) * scaleFactor));
            targetHeights(channelIndex) = max(1, round(rawHeights(channelIndex) * scaleFactor));
        end

        targetWidth = max(targetWidths);
        targetHeight = max(targetHeights);
        for channelIndex = 1:numel(channelFrames)
            channelFrames{channelIndex} = resize_frame_to_target(channelFrames{channelIndex}, targetHeight, targetWidth);
        end

        if numel(channelFrames) > 1
            frame = cat(2, channelFrames{:});
        else
            frame = channelFrames{1};
        end
        encodedHeight = size(frame, 1);
        encodedWidth = size(frame, 2);
        framePath = fullfile(frameDir, sprintf('frame_%06d.tif', frameIndex));
        imwrite(frame, framePath, 'tif');
    end

    result.status = 'ok';
    result.frame_count = numel(selectedIndices);
    result.source_width = sourceWidth;
    result.source_height = sourceHeight;
    result.encoded_width = encodedWidth;
    result.encoded_height = encodedHeight;
    result.channel_labels = channelLabels;
catch ME
    result.error = getReport(ME, 'basic', 'hyperlinks', 'off');
end

fid = fopen(resultPath, 'w');
fwrite(fid, jsonencode(result), 'char');
fclose(fid);
end

function files = collect_legacy_image_files(sourcePath)
files = dir(fullfile(sourcePath, '**', '*.jpg'));
files = [files; dir(fullfile(sourcePath, '**', '*.jpeg'))];
if isempty(files)
    return;
end

filtered = struct('folder', {}, 'name', {}, 'channel_key', {});
for idx = 1:numel(files)
    fullName = fullfile(files(idx).folder, files(idx).name);
    if contains(fullName, [filesep '.detecdiv-previews' filesep]) || endsWith(fullName, [filesep '.detecdiv-previews'])
        continue;
    end
    channelKey = channel_key_from_folder(files(idx).folder);
    filtered(end + 1) = struct('folder', files(idx).folder, 'name', files(idx).name, 'channel_key', channelKey); %#ok<AGROW>
end

if isempty(filtered)
    files = filtered;
    return;
end

[~, order] = sort(string({filtered.folder}) + "/" + string({filtered.name}));
files = filtered(order);
end

function channelKey = channel_key_from_folder(folderPath)
[~, folderName] = fileparts(folderPath);
channelKey = lower(string(folderName));
if strlength(channelKey) == 0
    channelKey = "channel";
end
end

function label = channel_label_from_key(key)
tokens = regexp(char(key), 'ch(\d+)', 'tokens', 'once');
if ~isempty(tokens)
    label = sprintf('Ch %s', tokens{1});
else
    label = char(key);
end
end

function indices = sample_indices(totalCount, maxFrames)
if totalCount <= 0
    indices = 1;
    return;
end
if totalCount <= maxFrames
    indices = 1:totalCount;
    return;
end
if maxFrames <= 1
    indices = 1;
    return;
end
indices = unique(round(linspace(1, totalCount, maxFrames)));
indices(indices < 1) = 1;
indices(indices > totalCount) = totalCount;
end

function [frame, rawWidth, rawHeight] = read_legacy_frame(imagePath, maxDimension)
image = imread(imagePath);
rawHeight = size(image, 1);
rawWidth = size(image, 2);
if ndims(image) == 3
    image = 0.2989 * double(image(:, :, 1)) + 0.5870 * double(image(:, :, 2)) + 0.1140 * double(image(:, :, 3));
else
    image = double(image);
end
image = normalize_uint8(image);
image = resize_to_limit(image, maxDimension);
frame = image;
end

function out = normalize_uint8(image)
minValue = min(image(:));
maxValue = max(image(:));
if maxValue <= minValue
    out = zeros(size(image), 'uint8');
    return;
end
scaled = (image - minValue) ./ (maxValue - minValue);
out = uint8(round(255 * max(0, min(1, scaled))));
end

function out = resize_to_limit(image, maxDimension)
[height, width] = size(image);
largest = max(height, width);
if largest <= maxDimension
    out = image;
    return;
end
scale = maxDimension / double(largest);
targetHeight = max(1, round(height * scale));
targetWidth = max(1, round(width * scale));
[xGrid, yGrid] = meshgrid(linspace(1, width, targetWidth), linspace(1, height, targetHeight));
resized = interp2(double(image), xGrid, yGrid, 'linear', 0);
out = uint8(round(max(0, min(255, resized))));
end

function out = resize_frame_to_target(image, targetHeight, targetWidth)
[height, width] = size(image);
if height == targetHeight && width == targetWidth
    out = image;
    return;
end
[xGrid, yGrid] = meshgrid(linspace(1, width, targetWidth), linspace(1, height, targetHeight));
resized = interp2(double(image), xGrid, yGrid, 'linear', 0);
out = uint8(round(max(0, min(255, resized))));
end

function binningFactors = channel_binning_factors(config, channelCount)
binningFactors = ones(1, channelCount);
if ~isfield(config, 'channel_settings')
    return;
end
channelSettings = config.channel_settings;
if isempty(channelSettings)
    return;
end
for idx = 1:min(channelCount, numel(channelSettings))
    entry = channelSettings(idx);
    if isstruct(entry)
        factor = parse_binning_factor(entry);
        if factor > 0
            binningFactors(idx) = factor;
        end
    end
end
end

function factor = parse_binning_factor(entry)
factor = 0;
if ~isstruct(entry)
    return;
end
candidateFields = {'binning_factor', 'binning', 'Binning', 'Binning factor', 'Binning Factor'};
for idx = 1:numel(candidateFields)
    fieldName = candidateFields{idx};
    if ~isfield(entry, fieldName)
        continue;
    end
    value = entry.(fieldName);
    if isstring(value) || ischar(value)
        token = regexp(char(value), '(\d+)', 'tokens', 'once');
        if ~isempty(token)
            factor = max(1, str2double(token{1}));
            return;
        end
    elseif isnumeric(value) && isscalar(value) && isfinite(value)
        factor = max(1, round(double(value)));
        return;
    end
end
end

function scaleFactor = infer_scale_from_reference(rawWidth, rawHeight, referenceWidth, referenceHeight)
scaleFactor = 1;
if rawWidth <= 0 || rawHeight <= 0 || referenceWidth <= 0 || referenceHeight <= 0
    return;
end
widthRatio = double(referenceWidth) / double(rawWidth);
heightRatio = double(referenceHeight) / double(rawHeight);
if abs(widthRatio - heightRatio) > 0.15 * max([1, abs(widthRatio), abs(heightRatio)])
    return;
end
if widthRatio >= 1 && heightRatio >= 1
    candidate = mean([widthRatio, heightRatio]);
    rounded = round(candidate);
    if abs(candidate - rounded) <= 0.2 * max(1, candidate)
        scaleFactor = max(1, rounded);
    end
end
end

function index = first_known_binning_index(binningFactors)
index = find(binningFactors > 0, 1, 'first');
if isempty(index)
    index = 0;
end
end
