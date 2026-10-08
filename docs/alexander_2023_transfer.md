# Alexander 2023 — approved migration, 2026-10-08

The user reports Alexander's agreement to move his 2023 data. Start with the
three explicit primary-NAS trees `/data/Alexander/data/2023_{1,2,3}`; the native
2026-10-01 inventory measured 15,866,515,072,652 payload bytes. The target is
the per-user home `/homes2/maliavko/DetecdivHub/raw` on `10.20.8.250`.
The primary legacy home and the read-only `Sauvegarde` projects are not silently
included in this first physical copy. They require their own lineage/migration
items; the source folders and their files remain preserved.

## Phase launched here: copy and full verification only

`scripts/ops/launch_alexander_2023_transfer.py` captures a private, live catalog
snapshot and records an approved `StorageMigrationBatch` with `copy_verify_only`
strategy. No `materialize` call is used: that endpoint creates catalog/experiment
placeholders, not a physical file transfer. Existing raw IDs, external keys,
owners, projects, experiments, links and preferred locations remain unchanged.
The initial audit found 121 raw records, including equivalent locations and
some Gilles-owned entries. Preserve them; do not deduplicate or reassign by name.

The committed standalone runner is copied to:

`/home/charvin-admin/storage-transfers/alexander-2023-20261008/runner.py`

It runs in detached tmux session `alex2023-transfer`, independently of the SSH
connection and without restarting or modifying any Hub/worker runtime.
State, logs and the catalog snapshot are private, mode 0700/0600 operational
files. No DSM password or private key is copied into the repo or manifest.

Each year follows this sequence:

1. Validate exact NAS mounts, per-user `maliavko` write identity, and no shadow
   mounts. Capture a sorted native source inventory; reject symlinks, special
   files and traversal errors. Synology metadata/recycle/snapshot folders are
   excluded, not scientific payloads.
2. Check capacity with at least a 2 TB margin. Copy with resumable rsync into
   `raw/.incoming-alexander-2023-20261008/<year>`. The initial bandwidth cap is
   60 MiB/s, with low CPU/idle I/O priority. Incomplete copies never become
   preferred catalog locations.
3. Compare every included source/destination file using full rsync SHA1
   checksums in dry-run mode. An empty discrepancy report is required.
   The SHA256 inventory digest describes path/size/mtime metadata, not image
   content; do not confuse it with the separate full-content verification.
4. Repeat native inventory and require an identical source inventory, then
   leave the verified copy in staging. Record `verified_in_staging` per year
   and finally `verified_ready_for_catalog_cutover`.

Neither source deletion nor database cutover is in the runner. Those safety
flags are pinned false in the manifest. Any error leaves sources/catalog intact
and records `failed_safe`. Rerunning the same pinned runner/manifest resumes
partial copies under an exclusive flock, but an existing final destination
requires explicit inspection. A host reboot stops the task; tmux protects
against SSH disconnection, not reboot. No unrequested scheduler is installed.

## Required gates before actual cutover and reclamation

- Re-read the completed verification receipts and compare the live catalog
  with the original snapshot: preserve IDs/owners/relationships, tolerate no
  unreviewed concurrent catalog changes.
- Check active jobs/locks using every affected raw/project. No active job is
  interrupted or silently rerouted.
- Publish validated copies and add locations to the existing raw records;
  change preferred locations transactionally, retaining original locations as
  rollback references. Never create replacement raw records using path hashes.
- Resolve all affected project/FOV source paths. Relink only verified copied
  MATs, test a reopen, and retain `Sauvegarde` originals. Pending legacy aliases
  are not proof of a source identity, and the 2023 projects are not all linked.
- Test Linux **and Windows** access to `/homes2`: current Windows workers may
  only know `/data` and `/archive`. Coordinate required worker/client mappings
  with the separate worker thread; do not modify workers here.
- Verify backup/restore coverage and compatibility paths before retiring old
  data. The secondary provider remains inactive; this copy is not an automatic
  activation or quota/backup-policy change.

Only after these gates may the approved migration reclaim source storage.
Copying alone frees no primary-NAS space. Never delete `Sauvegarde` or blindly
replace an entire storage-root prefix spanning unmoved years.

## Read-only status

```powershell
ssh detecdiv-server 'cat /home/charvin-admin/storage-transfers/alexander-2023-20261008/state.json'
ssh detecdiv-server 'tail -c 2000 /home/charvin-admin/storage-transfers/alexander-2023-20261008/2023_1-copy.log'
```

The authoritative physical phase is `state.json`, not a guessed ETA. The Hub
batch records launch/copying; audit synchronization and preferred-location
changes are separate, reviewed operations after the physical receipts exist.
