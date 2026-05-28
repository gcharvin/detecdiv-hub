# Misc Storage Inventory

`misc_storage_items` is the third storage catalog beside DetecDiv projects and
raw datasets. It is intended for large storage trees that should be visible to
the hub without pretending they are DetecDiv objects.

Typical examples:

- legacy experiment folders that contain many datasets but no DetecDiv project
- classifier repositories and training sets
- old/no-backup folders
- common lab data, movies, software, and archives

The misc inventory is deliberately shallow. It records significant folders and
files under a source path, with a bounded depth and a timeout per size probe.
Items that match already-cataloged project or raw dataset paths are skipped by
default to avoid double-counting.

## Queue A Scan

Use the storage-visible worker, not the API container, for server paths:

```bash
POST /storage/misc-inventory/jobs
{
  "source_path": "/data",
  "storage_root_name": "data_misc",
  "root_type": "misc_root",
  "host_scope": "server",
  "min_size_bytes": 10737418240,
  "max_depth": 2,
  "du_timeout_sec": 45,
  "include_cataloged": false
}
```

For a targeted legacy area such as Theo/Basile collaboration data:

```bash
POST /storage/misc-inventory/jobs
{
  "source_path": "/data/Collab-Theo_Basile/Manips",
  "storage_root_name": "collab_theo_basile_manips_misc",
  "owner_user_key": "Basile",
  "min_size_bytes": 10737418240,
  "max_depth": 2
}
```

## List Results

```bash
GET /storage/misc-items?min_size_bytes=10737418240&limit=500
GET /storage/misc-items?owner_user_key=anais
GET /storage/misc-items?category=legacy_project_candidate
```

The main fields are:

- `total_bytes`
- `scan_status`: `measured`, `timeout`, or `error`
- `category`: heuristic classification such as `candidate_raw_dataset`,
  `legacy_project_candidate`, `classifier_training`, `no_backup`, or `misc`
- `lifecycle_tier`, `archive_status`, and backup fields for future policy work

## Relationship To Project/Raw Indexing

If a misc item is classified as `candidate_raw_dataset` or
`legacy_project_candidate`, that is a prompt to run the existing project/raw
indexer on that specific subtree. Misc inventory should not replace raw/project
indexing; it is the accounting layer for everything else.
