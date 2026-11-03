# Alexander 2023 migration continuation — 2026-11-01

## Follow-up, 2026-11-03 09:36–13:28 UTC

**Raw copy completed at 10:02 UTC.** The existing runner is now
`verifying_full_content`, with checksum log
`2023_3/verify-20261103T100210Z.log`. Its heartbeat is current. An empty log
while rsync is still running is not a successful gate. Require exit 0, the
terminal full-SHA1 flag and a complete identical fresh source inventory before
publication/retirement. No 2023_3 payload has been deleted or made preferred.

**All 32 copied MATs completed relink at 06:05:47 UTC**, with exit 0 and
524,082 path changes. Every file passed exact audited change counts and
canonical scientific/annotation comparisons before/after/save/reopen. All
Sauvegarde originals and backups remain. Evidence:
`project3-relink-completion-verification.json`. The watcher released all eleven
own leases at 06:06:45 UTC and exited; do not restart it or the relink controller.
Closure receipt: `project3-relink-lease-finalization.json`. A separate live
database review confirmed unchanged identity, metadata and location digests for
all eleven projects: `project3-relink-catalog-preservation.json`. Batch remains
`copied_paths_relinked_pending_raw_and_host_gates`.

Read-only filesystem/engine tests now run on **both hosts** using
`verify_project3_hosts.m` and `run_project3_host_validation.py`:

- Linux tmux **`alex2023-3-host-linux-20261103`**, private directory
  **`project3-host-validation/`** under the compute work root.
- Windows task **`DetecDiv Alexander Project3 Host Verify 20261103`**, running
  as actual limited **`GMGM\Charvin-Admin`, session 1**. Private files:
  `C:/ProgramData/DetecDivHub/homes2-connection-private/project3-host-validation-20261103`.

At 13:24 UTC both had passed six FOVs of the first project without errors.
They check every ROI image/results path, all **44,944 actually existing data
sidecars**, all 21 processor paths, main/processor MAT hashes and representative
ROI pixels against the original baseline in every one of 123 FOVs. All **1,728
raw sample images** must match original pixel hashes; the protected engine's
FOV reader is exercised too. Final canonical paths are translated to incoming
folders **in memory only**, and no shared MAT is saved. Windows runs one MATLAB
thread below normal priority after a 15.3-GB free-memory preflight. No worker
restart/configuration/credential/default-engine change was performed.

Before doing anything else, inspect `project3-host-<host>-state.json`,
`project3-host-<host>-progress.json`, `.partial.json`, `-verification.json`,
`-matlab.log`, and Linux `controller.exit` in the respective private directory.
`project3_windows_host_ops.py status` retrieves Windows state into ignored local
`runtime/project3-host-windows-status.json`; it does not re-run a test.
Successful host state is `host_reads_verified_in_staging`. Keep full receipts,
compare both hosts' project/FOV/ROI/raw counts, then remove only the temporary
Windows diagnostic task/scripts with the guarded `cleanup` operation. Requests
and full samples are private; launch summaries are committed. Do not duplicate
these tests or change their pinned scripts while they run. Final publication
still needs the real final-folder paths checked after the incoming-folder rename.

An API continuity projection is also preparing in tmux
**`alex2023-3-api-projection-20261103`**, private **`project3-api-projection/`**.
It copies the 798 known preview/settings files (12,055,348,763 bytes) to
`/data/.detecdiv-hub-api-cache/alexander-2023_3`, checking **primary source,
homes2 and cache SHA1 equality** for every file and stable input metadata. At
13:28 UTC it had verified 100 files; no API URI has changed. Inspect `state.json`,
`controller.exit`, `controller.log`, and `receipt.jsonl`. Required terminal phase:
`verified_ready_for_api_reference_update`. A partial cache must be reviewed,
never overwritten by blindly replaying this one-shot script. Transactionally
switch only still-matching API references and verify all real content handlers
before source retirement. This extra cache also belongs to the eventual
off-/data cleanup after the API can actually serve homes2 directly.

Remaining gates: complete raw checksum/source stability; both host receipts;
fresh jobs/locks and original/catalog preservation; final-folder publication
and preferred locations on the same IDs; API continuity; a separately bounded,
tested 2023_3 native manifest retirement and measured physical space recovery.
Source deletion remains authorized only after all gates. Originals stay intact.

## Follow-up, 2026-11-03 03:37–03:51 UTC

At 03:54 UTC both native gates had completed successfully for all 32 files,
including independent maliavko-owned backups. The 21 processor MATs were already
relinked with exact change counts and successful scientific before/after/save/
reopen comparisons; the eleven main MATs were next. Full snapshot evidence:
`project3-relink-preparation-verification.json`. MATLAB PID 3122948 at launch;
inspect receipts rather than treating this interim count as terminal completion.

Raw 2023_3 copy reached **5,019,920,590,245 / 5,967,100,516,555 bytes
(84.13%)** at 03:37 UTC. Full SHA1 and source stability remain pending. No
2023_3 source has been retired; primary free space was 5,587,621,289,984 bytes.

The corrected persistent-path audit completed with exit 0: **98,611 non-image
MATs**, no unresolved absolute path values. It excluded all 53,623 image MATs.
Only **32 MATs** need rebasing: eleven main projects and 21 processor MATs.
All other result/data/labeled MATs have no actionable paths and remain unchanged.
`project3-persistent-paths-verification.json` contains the completed audit,
receipt digest and all 32 recipes/examples. The launcher also rechecked that
the receipt covers exactly every expected non-image MAT in the copy manifests.

Unpublished-copy relinking is now running in detached tmux
**`alex2023-3-relink-20261103`**, private subdirectory **`project3-relink/`**.
Batch: `46b50e1d-bf24-4284-9d3d-fbe4df5de158`.
The controller checked no affected queued/running/cancelling jobs, no active
indexing, Alexander ownership, and acquired only its own eleven edit leases.
They expire at **2026-11-03 07:45:18 UTC**. Project identity/metadata/location
digests are retained in the batch and private `admission.json`. Existing preferred
locations, owners, notes, ACLs, groups, raw links and positions are not changed.

Before writing any MAT, the controller repeats every complete native Sauvegarde
inventory and requires byte-for-byte equality with the original copy manifest.
All eleven fresh inventories passed. Native source/copy hash, regular-file,
independent-inode and maliavko ownership checks passed for the 32 inputs.
It then created all 32 backups (3,350,294,871 bytes) under
`/homes2/maliavko/DetecdivHub/.migration-backups/alexander-2023-remaining-20261101/2023_3-relink-20261103`.
At 03:51 the native three-way source/copy/backup hash gate was still running;
no copied MAT had yet been rewritten.

`relink_project3_copies.m` uses the proven persistent-value visitor from the
2023_1 operation, with explicit mappings for these projects/acquisitions.
For each allowed MAT it verifies the input and backup hashes, canonicalizes
paths for before/after scientific-value comparisons, requires the exact audited
change count, saves to a new temporary MAT, reopens and compares every persistent
non-path value/annotation, audits remaining old paths, then replaces only that
copied MAT. Processor files are handled first. Every successful replacement gets
a separate `modified-NN.json` receipt with old/new hashes. No image file is saved.
Lease expiry is checked before every replacement. No automatic retry/rollback
is allowed after a partial write; preserve receipts, temporary files and backups.

**Important:** copies stay in `.incoming-<stem>-20261101` during this phase.
Saved paths use the future final canonical project folders. Until validated
filesystem publication, translate final-folder prefixes to the incoming folders
in memory when inspecting a copied project. Do not make its catalog location
preferred yet. Full copied-raw verification, Linux/actual Windows filesystem
reads, API continuity, publication and source retirement remain separate gates.

Inspect `project3-relink/state.json`, `controller.exit`, `controller.log`,
`matlab.log`, `matlab.exit`, `progress.json`, `result.json`, `modified-NN.json`,
and native input/backup receipts before doing more. Both controllers are one-shot;
never replay them over partially relinked copies. Terminal success phase:
`copied_paths_relinked_science_verified_raw_and_host_gates_pending`.

A local hidden one-shot watcher (PID 17584 at launch) polls terminal receipts
every minute and then releases only those eleven own leases and records the
terminal result in the same audit batch. It changes no preferred location and
deletes no source. Its logs and private admission/status are under local
`runtime/project3-relink-*`; final closure receipt is
`project3-relink-lease-finalization.json`. If the watcher stops, inspect the server
task first, then close only its recorded own leases after terminal exit. Do not
kill or alter an unrelated job. Admission summary, launch and watcher launch
receipts are retained alongside this report. No worker/API/default-engine change.

## Follow-up, 2026-11-02 21:33 UTC

2023_3 is still copying, now **4,063,322,667,444 / 5,967,100,516,555 bytes
(68.10%)**. No source deletion or catalog/path mutation has occurred. Primary
free space was 5,629,685,805,056 bytes. The existing copy runner is healthy;
do not restart it or infer completion before its full SHA1/source-stability gates.

The first read-only project audit completed with exit 0. Every one of the
**123 FOVs** has an exact first-frame raw crop equal to the representative saved
ROI image. Representative copied ROI pixels/results also equal the Sauvegarde
originals in every FOV. These content checks corroborate all five legacy Z:
acquisition aliases alongside the earlier 469,830 metadata matches. The audit
recorded 1,728 raw pixel samples, resolved all 53,623 ROI image/results references
against verified copy manifests, and preserved all main MAT hashes. Evidence:
`project3-identity-readonly-verification.json`; full private receipt
`project3-readonly-inspection.json`, SHA256
`4e21373fee4510e94a76f722c8a8815633803f6e17b9943602782c66f553830e`.
This does not replace the complete raw source/destination checksum, save/reopen
comparison after relink, or the actual Windows-session validation.

The next read-only audit, `audit_project3_persistent_paths.m`, runs in detached
tmux **`alex2023-3-persistent-paths-20261102`**. It scans every non-image MAT in
the verified copies, including main, labeled, processor, data and results MATs.
It records candidate canonical rebases and unresolved absolute path values;
notes/history remain protected, and image MATs are not rewritten or converted.
Inspect `project3-persistent-paths-progress.json`, `.partial.json`, `.json`,
`.log`, `.exit` and `project3-persistent-paths-receipt.jsonl` before doing more.
No relink/publication is performed by this audit. Review all unresolved paths
before preparing writes, backups and canonical scientific-value comparisons.

The first scanner attempt stopped before any receipt or science write because
its nested function shared the outer `names` variable. The corrected outer
variables are distinct (`verifiedNames`/`basename`). All failed-run evidence,
including the original script, is preserved in private
`project3-persistent-paths-attempt1`; the initial launch receipt is retained
locally as `attempt1-project3-persistent-paths-launch.json`. The retry requires
that exact failed script hash, exit 1 and a zero-byte receipt. The corrected
script SHA256 is `6df3d7bd7ec5e50394aa6292863f24a69a3eadfdb5babce7e8286767563efd04`.
No source/copy is recreated or overwritten by restarting this read-only audit.

The existing six-hour heartbeat prompt now points at these completed and active
audits, with its schedule/thread unchanged. Project backups/relink, scientific
save/reopen comparisons, full homes2 raw checks, Linux/Windows reads, API
continuity, catalog cutover and manifest-bounded retirement are still pending.

## Follow-up, 2026-11-02 15:38–15:47 UTC

The raw copy remains healthy and active: 3,149,612,890,352 of
5,967,100,516,555 bytes (52.78%) had transferred at 15:38 UTC. This is copy
progress, not the subsequent full SHA1 comparison or source-stability gate.
Primary free space was 5,637,885,267,968 bytes; no 2023_3 source was retired.

All eleven independent project copies completed at 10:19 UTC: **152,277 files /
1,492,353,838,054 bytes**. A separate review re-read every verification receipt,
checked its pinned digest and every file's successful source/destination hash
and ownership result, and compared the complete source inventories before/after.
Evidence: `project3-copy-completion-verification.json`. Sauvegarde originals
remain intact. No project has been published or relinked by this phase.

`inspect_project3_copies.m` is now running read-only in detached tmux
`alex2023-3-project-inspection-20261102`, with the protected d83c4dbc engine,
one MATLAB thread and low scheduling/I/O priority. It checks all 53,623 ROI
image/results references against the verified manifests, compares representative
copied/original ROI pixels and results in all 123 FOVs, records primary raw
first/middle/last pixel baselines for each channel, and preserves both main
MAT hashes. It does not save a project, mutate annotations, publish locations
or delete sources. Exact raw/crop matches are recorded as positive identity
evidence; an absent match remains unresolved (drift or extraction frame selection
can affect it). This is preparation, not the final relink/reopen/Windows gate.
The first project has no data sidecars; another has only a partial set. Preserve
these existing layouts rather than requiring a fabricated uniform three-file
layout for every ROI. Labeled/processor MATs also need a path audit before cutover.

Private compute files: `project3-readonly-progress.json`,
`project3-readonly-inspection.partial.json`, `project3-readonly-inspection.json`,
`project3-readonly-inspection.log` and `.exit`. Inspect these and the tmux session
before any new launch; **do not duplicate the running audit**. Its request and
launcher are one-shot and fail on existing evidence files. Admission found no
affected active jobs or leases; unrelated jobs were left running. Runtime
assumptions from the older idle-poller checkpoint below are historical.

A fresh read-only catalog inventory found 24 raw records / 775 positions, with
775 preview and 23 display-setting URIs. All 798 referenced files occur in the
source manifest (12,055,348,763 bytes). This is a plan for API continuity, not
a fresh content check or URI update. Private plan:
`2023_3/api-media-readonly-plan-20261102.json`; summary and digests:
`project3-api-readonly-evidence.json`. Preserve these handlers before retirement.
The API still cannot see homes2 directly.

The user-requested six-hour follow-up is active in the existing Codex thread,
automation `migration-alexander-2023-3-contr-le-6-h`. It must continue actual
verification/cutover/retirement work when gates permit, preserving project
originals, and stop when the approved scope is complete. Its local execution
requires the workstation and Codex app to remain available; detached NAS-copy
and inspection tasks continue independently of this workstation.

Checkpoint: 22:34 UTC. The user authorized retiring verified Alexander 2023
raw duplicates from /data while preserving original projects and working paths.

## 2023_1 complete: residual source retired

The homes2 copy retains all 750,255 original files / 6,316,035,396,988 bytes.
The original full SHA1 comparison completed on 2026-10-12. Fresh native
inventories preserved every original path and size. A single destination
image timestamp exception predates that full comparison; see
`copy-preservation-gates.json`. No new data was inferred from directory names.

Only 87,364 files / 737,182,660,223 bytes remained on the primary NAS at the
start of this continuation. They were an unchanged subset of the original
manifest; the other 662,891 files were already absent. Do not attribute their
earlier removal to this operation. The residual destination files were checked
again immediately before retirement, including actual maliavko ownership and
modification times (maximum source/destination difference: 96 nanoseconds).

After both affected projects passed Linux and actual Windows-session tests,
manifest-driven native unlink removed exactly those 87,364 files. Each unlink
rechecked type, size and timestamp through no-follow directory descriptors.
No regular source payload remains. NAS system folders and nonempty ancestors
were retained, along with the year root. No recursive deletion was used.

Measured primary free space increased from 4,910,721,880,064 to
**5,648,026,640,384 bytes**, a **737,304,760,320-byte increase**. The phase had
previously added an 11.052-GB temporary API cache; the net increase from the
post-2023_2 checkpoint is therefore smaller than this deletion-only measurement.

Retirement batch: `7ec51c52-e14a-4ac5-ab74-85bd4ee30590` (22:25 UTC).
Only dead source location rows 3703 and 4288 were retired. Existing raw ID
`094444cd-1aa8-49d6-b6ab-d7c7df730b96`, preferred homes2 location 5512,
positions, ownership and links remain unchanged. All 2,572 unrelated raw
locations were preserved. Both temporary retirement leases were released.

Seventy preferred raw locations already use the homes2 staging namespace.
Keep it stable; renaming it would require updating all project/catalog paths.
The six phantom `160623_test` records describe data already absent before this
operation; they were not merged with real calibration data or silently removed.

## Both affected projects verified and originals preserved

Alexander project `anof_190423_nh_sir2_tween`, ID
`1bbe0337-ba93-4c15-a7e9-39ae6df47979`: 28 FOVs / 14,386 ROIs.
The copied MAT was relinked on homes2, then scientific values/annotations were
compared across save/reopen. All 28,772 results/data sidecars were audited.
Representative ROI/raw pixels from every FOV matched the originals. Windows
validation passed in limited `GMGM\Charvin-Admin`, session 1, without saving.
Preferred location 4246 and original readonly Sauvegarde location 2761 remain.
Original MAT SHA256 remains
`c7c6b30c1882d2ad7f1fb91276b2565bfac0e38177b45e31c5712dc8b9cea874`.
Project batch: `6c5f88a0-da46-4bf8-aa37-6e4026a7ec9b`.

Gilles project `190423_nh_sir2_tween`, ID
`0e485efc-6b23-4faa-a316-4faea283477d`, was found at
`/data/Gilles/analysis/projects/190423_nh_sir2_tween.json` (schema v3).
The earlier user's wording meant “m'appartient”, not a drive M:. That false
lead is resolved. The actual project stayed in Gilles's folder throughout.

Only 301 raw references across the main manifest, 30 FOV metadata JSONs and
run profiles were rebased. All 32 original JSON files are preserved under
`/data/Gilles/analysis/projects/.detecdiv-path-backups/20261101-homes2-190423_nh_sir2_tween`.
Live files were updated in place to preserve their existing inodes, NAS owner,
mode and ACLs. Canonical JSON comparisons preserved every non-path value.
No HDF5/scientific array or historical pipeline record was rewritten.

Linux before/after and actual Windows tests passed: 30 FOVs / 15,281 declared
ROIs. All 14,340 existing FOV image files retained their inventory; representative
HDF5 images and raw pixels agreed on both operating systems. Interrupted
extraction metadata and the two existing extra FOVs were preserved. The whole
retained project contains 14,360 HDF5 files including pipeline assets, with
1,287,710,869,264 HDF5 bytes; it was not moved into Alexander's home.

The current registration API requires a .mat alias. Location 2746 and that
alias were deliberately retained: the pinned engine's `shallowLoad` resolves
it to the sibling JSON, verified on both hosts. Metadata now records the actual
JSON manifest explicitly, and catalog FOV/ROI/footprint counts were refreshed.
Project identity, ownership, notes, privacy, ACLs, groups and raw link remain.
Project batch: `369692d0-be0f-4f3c-94e6-150310c42276`.

## API continuity and remaining final-scope cache work

All 710 previews and 64 display-setting handlers passed after source retirement.
The temporary `/data/.detecdiv-hub-api-cache/alexander-2023_1` contains 774
SHA1-verified files / 11,051,646,071 bytes; batch
`23a0fd56-b832-49da-9013-65fcad5052bc`. The 150 previews and six settings under
already-absent `160623_test` remain unchanged and require separate review.

The earlier 2023_2 cache retains 5,874,728,850 bytes. Both caches must eventually
be removed from /data after a working homes2 serving path is installed. The API
currently sees only a readonly Docker bind of /data through the VM's existing
SSHFS bridge. No homes2 mount/service/container change was made. The current
VM SSH identity cannot use noninteractive sudo; its root-key probe did not
verify key absence. Do not read or copy credential contents to work around it.

## 2023_3 raw and project copies running independently

The raw copy/full-verification runner is detached in tmux
`alex2023-3-copy-20261101`, copying 708,590 files / 5,967,100,516,555 bytes with
a 60-MiB/s cap and low I/O priority. At 22:34 UTC, 324,619,727,042 bytes (5.44%
of the whole payload) had transferred. Ignore rsync's incremental-file-list
percentage. Full SHA1 comparison and a fresh source stability inventory follow.
The runner cannot delete sources, rewrite projects or publish raw locations.
Raw copy batch: `a8c8e0da-72b6-42c5-9644-2028709c9707`.

A read-only scan examined 153 declared MATs: 151 loaded, the Gilles MAT alias
above and an unrelated 2026 MAT were absent. Eleven 2023_3 project candidates
were identified. Their 469,830 saved raw-list entries all match primary names,
sizes and timestamps with the same one-hour timezone offset. This is alias
evidence, not a content checksum; final identity/path review is still required.

All eleven projects are Alexander-owned and had no active job/edit lease.
Their original Sauvegarde sources are legacy MAT projects, with no JSON sibling.
`prepare_project3_copies.py` runs separately in tmux
`alex2023-3-project-copies-20261101`. It creates maliavko-owned placeholders,
independent Btrfs reflinks, hashes every source/destination file, and compares
complete source inventories before/after each project copy. Sources stay intact.
Copies remain under `.incoming-<project>-20261101` and are not published/relinked.
At the checkpoint the first project's cloning was progressing normally; its
full content verification and the remaining ten copies were still pending.

Btrfs subvolumes report different device numbers: an over-strict preflight
initially stopped before any clone. Its receipts were preserved. Resume was
allowed only after verifying every existing placeholder was empty and owned by
maliavko; FICLONE itself validates filesystem compatibility. No partial payload
was overwritten. The prepared request pins the exact runner SHA256.

## Audit, execution and operational state

Use `python -X utf8` for Windows operational controllers. A Windows cp1252 source
read initially corrupted the authorization text and stopped retirement before
any native action. Its original gate/log were preserved; the corrected gate
was revalidated and the correction recorded in the existing audit batch.

The actual native unlink routine passed isolated fixtures for unexpected files,
modified files, symlink directories, hardlinks and the exact valid-manifest case.
All full Linux/Windows receipts and native retirement proofs are retained here.
The confirmed GitGuardian false positive was documented separately and clarified
in commit `7779e1f`; no password was contained in its two reported occurrences.

Large catalog/path inventories and complete API projection plans are excluded
from Git. Their private archive/hashes are in `private-checkpoint-manifest.json`.
Private compute root:
`/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101`.
Inspect `2023_1/retirement-state.json`, `2023_3/state.json`, and
`projects3/state.json`; receipts are more authoritative than stale interim logs.
Never blindly replay a one-shot finalization or partially failed copy.

An optional one-shot local status bridge mirrors raw/project-copy progress into
the raw audit batch every five minutes. It installs no scheduler/service and
sends no messages. It can stop if this workstation sleeps, while server copies
continue. Tmux survives SSH disconnection, not a compute reboot.

At 22:34 UTC the API/database health was OK, all seven pollers were idle and
retained their original PIDs/fingerprints, and temporary Windows diagnostic
tasks/scripts were removed. No API/worker restart, engine-default change,
provider activation, quota change or backup-policy change occurred.
