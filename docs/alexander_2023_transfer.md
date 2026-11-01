# Alexander 2023 — approved migration, 2026-10-08

## Project relocated and approved source retirement completed, 2026-11-01

The active `anof_26102023_yam76s_100ms` project is now its verified, relinked
homes2 copy. The original Sauvegarde MAT/tree is intact and retained as a
readonly nonpreferred location. All 11,450 files were fully content-checked;
47,965 path values in the copied MAT were rebased, preserving scientific values
and annotations through save/reopen comparison. Linux and the actual limited
Windows worker session validated 12 FOVs, 5,724 ROIs, every sidecar path and
representative image reads in each FOV. Project identity and relationships
are unchanged. Project batch: `268a32d9-0a87-4d26-b4a1-02e41838d1d2`.

The user then explicitly approved removing the verified 2023_2 primary duplicates
while retaining homes2 data and the original Sauvegarde project. Exactly
425,112 files / 3,583,194,799,325 logical bytes were unlinked from the explicit
manifest after fresh inventories and per-file type/size/mtime gates. Actual
free space increased by **3,577,917,788,160 bytes (3.578 TB)**; primary-NAS
available space was **4,921,802,833,920 bytes (4.922 TB)** at completion.
Only excluded NAS `@eaDir` metadata and its ancestors remain in that source
year tree. No NAS snapshot, backup, 2023_1 or 2023_3 payload was deleted.

A verified 5.875-GB API projection retains 335 MP4s and 19 display-setting JSON
files outside the retired tree at
`/data/.detecdiv-hub-api-cache/alexander-2023_2`. The API container lacks a
homes2 mount; this preserves existing content handlers without deployment.
The 21 dead source locations were retired after the complete unlink receipt;
their IDs/paths remain in batch `459a5303-c15e-458d-bd48-ac10be19f037`.
All 337 positions, raw identities, ownership and relationships were preserved,
along with all unrelated locations. The temporary project lease was released.

No worker or API restart occurred. Existing engine defaults, staged 2023_1
pilot, inactive/planned secondary provider/account and disabled Hub automatic
backup setting are unchanged. No additional automatic raw backup is claimed.
The historical Sauvegarde MAT retains its original saved raw paths; the active
homes2 MAT is the usable relinked project. Reversing source retirement requires
copying data back from homes2; the earlier preferred-flag rollback alone is
no longer sufficient.

Evidence and operational boundaries:
`reports/alexander-project-relocation-20261101/README.md`. The earlier sections
below record the retention state at each historical gate.

## 2023_2 raw locations switched, 2026-11-01

This section records the 18:35 UTC raw-only phase, before the separate project
relocation and explicitly approved source retirement.

The 21 existing Alexander raw records prefer read locations under
`/homes2/maliavko/DetecdivHub/raw/2023_2`. The copied year directory was published
from `.incoming-alexander-2023-20261008/2023_2` using an atomic no-replace rename;
the original primary-NAS files and their catalog locations are retained.
No raw ID, owner, external key, metadata, project/experiment link or position
was replaced. Post-transaction verification checked 337 unchanged positions,
all 21 API detail views and all 2,554 unrelated locations.

Fresh native inventories of both NAS trees found 425,112 regular files and
3,583,194,799,325 bytes, with identical file paths and sizes. The full source
metadata inventory exactly matched the initial receipt. Destination timestamps
on 223 files differed by at most one microsecond, consistent with the copy's
CIFS timestamp precision. The earlier full SHA1 content comparison had exit 0
and no discrepancy output; no new full-payload checksum pass was run here.

The original linked project `anof_26102023_yam76s_100ms` reopened read-only in
MATLAB: 12 FOVs and 5,724 ROIs. All 12 original and published raw paths were also
readable from the limited Windows worker session. This phase intentionally
retains that Sauvegarde MAT and its primary-NAS raw references as compatibility
sources. No MAT was saved, relinked or relocated. Before any source reclamation,
copy and verify the project, relink only its copy, test reopening on the required
clients, and establish backup/restore coverage. Original-source retention remains
in force until deletion is explicitly requested and reviewed.

The original copy-only manifest was not changed. A separate audited batch,
`b2530427-9baf-4276-a5da-855ce3831394`, records this raw-only cutover and retains
the old preferred-location IDs for rollback. Its receipts, plan, preserved
catalog snapshots and verification are in
`reports/alexander-2023-cutover-20261101/`. The original batch links to it;
neither batch authorizes deleting sources. The independent 2023_1 pilot
`190423_nh_sir2_tween_4` remains at its prior staging location, and 2023_3 was
not copied by this phase. Secondary-provider activation and quota/backup policy
changes were not included.

## Windows access gate passed, 2026-11-01

The Windows worker now resolves `/homes2/maliavko/DetecdivHub` to the secondary
NAS personal `home` share through its verified DNS name,
`//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub`, authenticated as `maliavko`.
The mapping was loaded by an admission-guarded idle restart. Validation in the
worker's limited session passed for all 21 copied 2023_2 raw directories, a
copied image header and temporary-file read/write. Native NAS ownership checks
confirmed `maliavko`; test probes were removed. The evidence is in
`reports/homes2-access-audit-20261101/`.

At completion of the Windows access gate, these 21 raws still preferred the
primary NAS; their subsequent raw-only cutover is recorded above. The earlier
separate `190423_nh_sir2_tween_4` pilot already prefers
its copied location in the 2023_1 staging tree; preserve that reference when
planning publication or renames. Do not treat the old runner's `failed_safe`
or database batch's `copying` status as a current transfer process. The resumed
2023_2 SHA1 check has a separate successful receipt (exit 0, empty discrepancy
log); fresh source-stability and catalog checks are required before cutover.

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
- Resolve all affected project/FOV source paths. For a raw-location cutover
  retaining compatibility sources, verify that the original project reopens
  and its source paths remain readable alongside the new raw locations.
  Before retiring any original source, relink only verified copied MATs, test
  a reopen, and retain `Sauvegarde` originals. Pending legacy aliases are not
  proof of a source identity, and the 2023 projects are not all linked.
- Test Linux **and Windows** access to `/homes2`. The native Windows worker's
  per-user mapping and read/write validation passed on 2026-11-01 as recorded
  above. This is not a deployment of path mappings to every other client, nor
  validation of every Windows filesystem job kind.
- Verify backup/restore coverage and compatibility paths before retiring old
  data. The secondary provider remains inactive; this copy is not an automatic
  activation or quota/backup-policy change.

Only after these gates may the approved migration reclaim source storage.
Copying alone frees no primary-NAS space. Never delete `Sauvegarde` or blindly
replace an entire storage-root prefix spanning unmoved years.

## Read-only status

```powershell
ssh detecdiv-server 'cat /home/charvin-admin/storage-transfers/alexander-2023-cutover-20261101/cutover-status.json'
ssh detecdiv-server 'cat /home/charvin-admin/storage-transfers/alexander-2023-20261008/state.json'
ssh detecdiv-server 'tail -c 2000 /home/charvin-admin/storage-transfers/alexander-2023-20261008/2023_1-copy.log'
```

The new `cutover-status.json` records the completed 2023_2 raw-location phase.
The original runner's `state.json` remains a historical copy-phase receipt;
its old `failed_safe` state is not evidence of a current running transfer or a
failed resumed checksum pass. Preferred-location changes are journaled in the
separate cutover batch rather than silently rewriting that old manifest.
