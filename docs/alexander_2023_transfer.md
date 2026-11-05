# Alexander 2023 — approved migration, 2026-10-08

## Alexander 2023 migration fully completed, 2026-11-05 12:39 UTC

All three scientific source manifests and all three temporary API cache payloads
are retired from `/data`, with full per-file receipts and final native empty
payload audits. Original projects and backups remain preserved. The readonly
homes2 mount is installed on **webserver-labo**, persistently exposed readonly
to the API with its unchanged image. PostgreSQL and worker PIDs/fingerprints
are unchanged; unrelated calculations continued.

All 1,926 API media references now point to homes2. Full actual response streams
passed both before and after cache retirement; Linux and actual limited Windows
session reads passed. The final cache retirement recovered **28,880,097,280
measured bytes**, bringing the sum of measured deletion-window gains to
**10,311,325,290,496 bytes (10.311 TB)**. This sum is not a multi-day net NAS
usage measurement. Scientific files, identities, owners, notes, ACLs/groups,
positions, relationships, original locations and raw staging namespaces remain
preserved. All fourteen own cache-phase leases were released; batch
`468070ce-1489-456e-9175-27f188156e41` is `api_cache_retired_verified`.

Completion evidence: `reports/alexander-api-cache-retirement-20261105/README.md`.
No live migration work remains; the six-hour follow-up has been deleted. Do not replay
completed controllers or diagnostic tasks. Older pending/blocker entries below
are historical and superseded by this completion record.

## 2023_3 source retirement verified complete, 2026-11-05

At 10:41 UTC the full verified 2023_3 manifest was retired: 708,590 files /
5,967,100,516,555 logical bytes. Measured primary free space increased by
**5,967,222,644,736 bytes** to 10,990,913,290,240 bytes. The combined measured
deletion-stage gain across 2023_1/2/3 is **10,282,445,193,216 bytes**.
Post-removal Linux/actual Windows project/raw readers and all 798 complete API
response streams passed. At 10:51 UTC exactly 47 dead raw location rows were
retired and all eleven own leases released. All original project files and
catalog identities, annotations, ACLs/groups and relationships remain intact.

No Alexander 2023 scientific source payload remains on `/data`. The broader
final objective still includes **28,981,723,684 bytes of live API caches** for
years 1/2/3. They remain necessary until the VM's readonly homes2 serving path
is installed and verified. A concrete mount installer is staged and validated;
the VM requires interactive sudo, which the agent cannot supply. Do not remove
the caches or describe that broader objective as complete until serving and
verified cache retirement are finished. Receipts and remaining action are in
the continuation report README.

## 2023_3 catalog switched and bounded retirement running, 2026-11-05

Both final-folder host validations passed. At 10:28 UTC the same eleven
projects and 24 raws/775 positions were transactionally switched to homes2.
Original Sauvegarde locations remain readonly; identities, owners, notes, ACLs,
groups and links are preserved. All 798 API content responses were fully
streamed and hashed after their references were switched to the verified cache.
Two destination timestamp exceptions passed exact source/destination SHA1
comparisons before metadata-only correction; the complete native recheck passed.
The authorized exact-manifest retirement started at 10:32 UTC, with per-file
checks/receipts and own-lease deadline enforcement. Its result, actual recovered
space and final source-location removal still require the terminal/post checks.
See the continuation report; never replay a partially executed retirement.

## 2023_3 raw verified and final project folders published, 2026-11-05

Full raw SHA1 and source stability succeeded at 08:41 UTC for 708,590 files /
5,967,100,516,555 bytes. At 09:51 all eleven relinked copies were renamed to
their final homes2 project folders under fresh admission and own leases.
Original Sauvegarde projects remain intact. Complete final-folder Linux and
actual Windows-session validation is running before preferred catalog/API
cutover and manifest-driven source retirement. No 2023_3 source has yet been
deleted. Current tasks, receipts and lease closure are documented in
`reports/alexander-2023-remaining-20261101/README.md`.

## Final scope and standing source-retirement authorization, 2026-11-01

After completion of the 2023_2 retirement, the user clarified:

> il faut à terme surpprimer tout 2023 sur /data une fois que tout est transféré

The final objective covers Alexander's three explicit source trees:
`/data/Alexander/data/2023_1`, `2023_2` and `2023_3`. All their scientific
payloads must ultimately reside on verified homes2 storage rather than retain
primary-NAS duplicates. This supersedes any earlier copy-only/source-retention
restriction for those trees once the validation conditions below are met.
It is standing authorization to retire the verified duplicates; no repeated
deletion approval is needed for the same scope and conditions. It does not
authorize deleting untransferred/unverified files, other users' data, other
years, original Sauvegarde projects, NAS snapshots or backups.

Complete the remaining work in this order, reclaiming verified lots promptly:

1. Finish and fully verify remaining 2023_1 and 2023_3 transfers. Preserve the
   working location of the already staged 2023_1 pilot during any publication.
2. Resolve every affected project/raw relationship. Copy and verify affected
   projects, relink only their copies, and validate saved/reopened paths plus
   image access on Linux and the actual Windows worker session. Keep original
   Sauvegarde project files intact.
3. Switch the existing catalog records and live dependent URIs to working
   destinations, preserving identities, ownership, annotations and links.
4. Check source stability, verified destination contents, jobs and leases;
   retire only the corresponding verified source manifests, audit the action,
   measure reclaimed space, and remove dead source-location references.
5. Provide API access to homes2 preview/display-setting files, verify all
   existing content handlers, switch their live URIs, then retire the temporary
   5,874,728,850-byte cache at
   `/data/.detecdiv-hub-api-cache/alexander-2023_2`. Until that serving change
   passes, the cache must remain available.

Completion means no Alexander 2023 scientific payload or required API serving
copy remains on `/data`. The already completed 2023_2 scientific retirement
does not by itself complete this broader objective. Retained NAS-managed
`@eaDir` metadata/empty ancestors are accounted for separately and must not
trigger a blind recursive cleanup. Backup/provider activation remains a
separate configuration change; do not describe it as enabled by this approval.

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

### Follow-up, 2026-11-03 13:28 UTC

The raw copy completed at 10:02 UTC; full SHA1 and source stability are still
pending. All 32 copied MATs completed relink with scientific save/reopen gates
(524,082 path changes), all eleven own leases were released, and live catalog
identity/metadata/location digests remain unchanged. Linux and actual limited
Windows-session tests now verify all ROI/data paths, 123 FOVs and 1,728 raw
sample reads using the protected engine; both passed the first six FOVs.
An API projection prepares the 798 served assets with three-way primary/homes2/
cache hashes, without changing URIs. The continuation README documents live
tasks, private receipts, final-folder publication and remaining retirement gates.
No 2023_3 source has been retired; preserve all original projects.

### Follow-up, 2026-11-03 03:37 UTC

The raw copy reached 84.13% (5.020 / 5.967 TB). The completed metadata audit
covered 98,611 non-image MATs with no unresolved paths. A guarded relink task
now prepares the 11 main MATs and 21 processor MATs in unpublished copies:
tmux `alex2023-3-relink-20261103`, private `project3-relink/`, batch
`46b50e1d-bf24-4284-9d3d-fbe4df5de158`. Fresh original inventories match;
backups precede every write, with exact content/scientific save/reopen gates.
Its eleven own leases have a four-hour bound and a terminal-receipt watcher.
Incoming folders and final canonical saved paths must be translated in memory
until filesystem publication. No preferred location or source data was changed
at this checkpoint. Full raw, host/API and retirement gates remain pending;
the continuation README documents the new receipts and one-shot recovery rules.

### Follow-up, 2026-11-02 21:33 UTC

Raw copy reached 68.10% (4.063 / 5.967 TB). The project preparation audit
completed with exit 0: exact raw crop/ROI equality in all 123 FOVs corroborates
the five legacy acquisition aliases; original/copy ROI pixels and results
agree, and main MAT hashes are unchanged. All 53,623 ROI image/results
references resolve against the verified copy manifests. A further read-only
audit of all non-image MAT paths runs in tmux
`alex2023-3-persistent-paths-20261102`. Its failed first attempt and corrected
retry are preserved/documented in the continuation report. No relink or
publication is done by these audits; raw checksums, save/reopen, Windows/API
and retirement gates still remain. The six-hour follow-up is still active.

### Follow-up, 2026-11-02 afternoon UTC

The raw 2023_3 copy reached 52.78% (3.150 / 5.967 TB) at 15:38 UTC; full
checksums and source stability are still pending. All eleven project copies
completed with full content hashes and stable source inventories: 152,277
files / 1,492,353,838,054 bytes. A separate read-only audit is running in tmux
`alex2023-3-project-inspection-20261102` to prepare path mapping and ROI/raw pixel
baselines for 123 FOVs / 53,623 ROIs. Do not duplicate it; inspect its private
progress, partial/final receipts and exit file first. Project copies have not
been relinked or published. Originals and 2023_3 primary data remain preserved.
The report README records the current gates, completed-copy proof and the
798-file API continuity inventory. Six-hour autonomous follow-up is active.

### Continuation checkpoint, 2026-11-01 evening UTC

The completed 2023_2 operation is unchanged. The follow-up is tracked in
`reports/alexander-2023-remaining-20261101/README.md` and private compute work
`/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101`.

- 2023_1: the prior full-content verified copy retains all 750,255 original
  files / 6,316,035,396,988 bytes. The current primary tree is an unchanged
  residual subset, 87,364 files / 737,182,660,223 bytes; the earlier absence of
  the other source files is not space reclaimed by this continuation.
- Keep the existing homes2 staging namespace: 70 preferred raw locations
  already reference it. The copied Alex project path rewrite passed Windows
  validation in the actual limited worker session and the complete Linux
  sidecar/save/reopen audit (28 FOVs / 14,386 ROIs). Its original remains on
  Sauvegarde. The linked Gilles project was found as a v3 JSON manifest and
  retained under /data/Gilles: 301 raw references in 32 metadata files were
  relinked, with original backups and unchanged scientific values/HDF5 arrays.
  Linux before/after and actual Windows reads passed (30 FOVs / 15,281 ROIs).
- At 22:25 UTC the 87,364 verified residual source files were retired. Measured
  free space increased by 737,304,760,320 bytes, to 5,648,026,640,384 bytes.
  Source rows 3703/4288 were removed, preferred homes2 location 5512 retained,
  and all other raw identities/locations/links preserved. Both own leases
  were released. Batch: `7ec51c52-e14a-4ac5-ab74-85bd4ee30590`.
- API continuity for 2023_1 is verified for 710 existing previews and 64
  display settings using a temporary 11.052-GB projection on /data. The 156
  already-missing references under `160623_test` were left unchanged. Retire
  all temporary projections only after the API can serve homes2 directly.
- 2023_3: a detached copy/full-checksum runner is active, copying 708,590
  files / 5,967,100,516,555 bytes. It cannot delete sources, publish preferred
  raw locations or rewrite projects. At 22:34 UTC, 324,619,727,042 bytes (5.44%)
  had transferred. Eleven dependent Alexander project copies are separately
  preparing as independent maliavko-owned clones with full content checks,
  preserving Sauvegarde originals. Legacy alias identity and final project/
  Windows checks are still required before source retirement.

The GitGuardian alert on commit `dcf7560` was a confirmed false positive on
two POSIX username-to-UID lookups. Clarification was pushed as `7779e1f`;
see `reports/alexander-project-relocation-20261101/gitguardian-false-positive-20261101.md`.
No credential was contained in the two reported occurrences.

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
