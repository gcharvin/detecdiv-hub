# Test Plan: Orphan Dataset Detection

This document validates the orphan raw dataset detection logic added to `project_indexing.py`.

## Key Functions

### 1. `looks_like_raw_dataset_dir(path: Path) -> bool`
- **Purpose**: Determine if a directory looks like a raw dataset
- **Location**: `project_indexing.py:1558-1585`
- **Detection Priority**:
  1. Zarr (`.ome.zarr`, `.zarr` with `.zarray` metadata)
  2. NDTiff (`NDTiff.index` file)
  3. Legacy MATLAB timelapse (has `.mat` files + position dirs)
  4. Micro-Manager (has markers: `metadata.txt`, `acquisitionmetadata.txt`, `displaysettings.txt`)
     - Special case: Returns FALSE if directory name starts with `Pos*` AND parent has MM markers

### 2. `iter_orphan_raw_candidates(root, project_dirs, max_depth=5)`
- **Purpose**: Walk directory tree and yield raw dataset candidates not inside project directories
- **Location**: `project_indexing.py:1588-1627`
- **Key Logic**:
  - Line 1620-1621: **Skip Pos* directories if parent has MM markers**
  - Line 1616: Skip any directory inside a project directory
  - Recursively search up to max_depth (default 5)

## Test Scenarios

### Scenario 1: Micro-Manager Parent with Positions
**Directory Structure**:
```
/data/Anais/Test_Wafers/SampleA/
├── metadata.txt                     ← MM marker
├── DisplaySettings.json             ← MM marker
├── Pos0/
│   ├── metadata.txt
│   ├── img_000_Default.tif
│   └── img_001_Default.tif
└── Pos1/
    ├── metadata.txt
    ├── img_000_Default.tif
    └── img_001_Default.tif
```

**Expected Result**:
- ✅ One raw dataset indexed: `SampleA` (parent directory)
- ✅ Zero datasets indexed: Pos0, Pos1 (skipped by iter_orphan_raw_candidates line 1620)
- ✅ Frame count validation: If parent has ≥10 frames across positions, SUCCESS
- ❌ Frame count validation: If parent has <10 frames, ValueError, skipped

**Actual Code Paths**:
1. `iter_orphan_raw_candidates()` yields `SampleA` (calls looks_like_raw_dataset_dir)
2. `looks_like_raw_dataset_dir(SampleA)` → TRUE (has metadata.txt = MM marker)
3. `raw_dataset_ingest.py` reads metadata, counts total frames ≥10 → SUCCESS
4. Pos0/Pos1 are NOT yielded by iter_orphan_raw_candidates (line 1620)

---

### Scenario 2: Legacy MATLAB Timelapse
**Directory Structure**:
```
/data/Anais/Test_Wafers/MyTimelapse/
├── MyTimelapse-project.mat          ← Legacy marker
├── timelapse_id.mat                 ← Legacy marker
├── MyTimelapse-pos0/
│   ├── img_*.tif (15 frames)
│   └── metadata.txt
└── MyTimelapse-pos1/
    ├── img_*.tif (15 frames)
    └── metadata.txt
```

**Expected Result**:
- ✅ One raw dataset indexed: `MyTimelapse` (parent directory)
- ✅ Zero datasets indexed: position subdirectories
- ✅ Frame count: Combined frames from all positions ≥10 → SUCCESS

**Actual Code Paths**:
1. `is_legacy_matlab_timelapse_dataset_dir(MyTimelapse)` → TRUE (has .mat + pos* subdirs)
2. `looks_like_raw_dataset_dir(MyTimelapse)` → TRUE (legacy MATLAB check)
3. Pos0/Pos1 subdirectories NOT detected as datasets (not MM markers, not legacy structure)

---

### Scenario 3: Snapshot Files (Should Be Excluded)
**Directory Structure**:
```
/data/Anais/Test_Wafers/Snapshot1/
├── metadata.txt                     ← MM marker
├── img_000_Default.tif              ← Only 1 frame!
└── img_001_Default.tif

/data/Anais/Test_Wafers/Snapshot2/
├── metadata.txt
└── img_000_Default.tif              ← Only 1 frame!
```

**Expected Result**:
- ❌ Zero datasets indexed (both fail frame count validation)
- ❌ Frame count: 1 < 10 minimum → ValueError
- ✅ Silently skipped (ValueError caught, not counted as failure)

**Actual Code Paths**:
1. `iter_orphan_raw_candidates()` yields `Snapshot1`, `Snapshot2`
2. `raw_dataset_ingest.py` reads metadata, frame_count=1
3. Check at line ~X: `if frame_count < 10: raise ValueError("few frames")`
4. Exception caught in project_indexing.py, skipped silently

---

## Validation Checklist

Run indexing with `scan_orphan_raw=True` on `/data/Anais/` and verify:

- [ ] **Orphan Micro-Manager datasets detected**: Count > 0
- [ ] **Position directories NOT indexed separately**: Should be 0 Pos* datasets
- [ ] **Snapshot files excluded**: Small files (<10 frames) should NOT appear in database
- [ ] **Legacy MATLAB timelapses detected**: If any exist in Test Wafers, should be indexed
- [ ] **Indexing job result**:
  - Check `indexed_raw_datasets` > 0
  - Check `failed_raw_datasets` = number of frames that failed validation
  - Total indexed + failed ≈ number of orphan candidates found

## To Test Manually

```bash
# 1. Run indexing with orphan scan enabled
curl -X POST http://webserver-labo:5100/api/indexing \
  -H "Content-Type: application/json" \
  -d '{
    "root": "/data/Anais",
    "owner": "antoine",
    "visibility": "private",
    "scan_orphan_raw": true,
    "queue_previews": false
  }'

# 2. Check indexing job status
curl http://webserver-labo:5100/api/indexing-jobs | jq '[-1]'

# 3. Once completed, verify raw datasets
curl "http://webserver-labo:5100/api/raw-datasets?owner=antoine" | jq '.[] | .name, .data_format, .frame_count'
```

## Known Issues Fixed

1. **1201 Pos* datasets indexed**: Fixed by skipping Pos* in `iter_orphan_raw_candidates()` (line 1620)
2. **90 small snapshots indexed**: Fixed by frame count validation in `raw_dataset_ingest.py`
3. **Individual positions as datasets**: Fixed by parent detection in `looks_like_raw_dataset_dir()` (line 1577-1583)
