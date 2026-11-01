# Alexander 2023_1 / 2023_3 continuation — 2026-11-01

The user authorized transferring 2023_1, then 2023_3, and reclaiming primary
space only after the copies and affected project paths are verified. This
operation is separate from the completed 2023_2 retirement. **No new source
file has been deleted by this continuation at the checkpoint recorded here.**

## Security interruption resolved

The GitGuardian notification for earlier commit `dcf7560` was investigated
using the exact two GitHub links in the notification email. Both refer to POSIX
username-to-UID lookups, not passwords. Clarification and evidence were pushed
as `7779e1f`; see the preceding operation's
`gitguardian-false-positive-20261101.md`. No credential was copied or rotated.

## 2023_1 preserved copy and remaining primary data

The original copy/full SHA1 comparison completed on 2026-10-12. A fresh native
secondary-NAS inventory contains all 750,255 original paths and sizes:
6,316,035,396,988 bytes. All modification times agree within one microsecond
except one image whose destination timestamp is 2026-10-08, **before** that
successful full SHA1 comparison. This is not evidence of a later content write.
See `copy-preservation-gates.json` and `changed-image-investigation.json`.

The current primary tree contains only 87,364 files / 737,182,660,223 bytes.
These files are an unchanged subset of the original manifest: no new or
modified source file was found. The other 662,891 source files were already
absent before this operation. Current homes2-only locations for their existing
raw IDs date from 2026-10-13. Do not attribute that earlier source removal or
its space recovery to this continuation; its precise deletion mechanism has
not been established here. The old copy runner's source-retention flags are
historical and do not describe the present primary tree.

Seventy existing preferred raw locations already use the verified homes2
staging directory. Keep this working namespace stable during the project
checks; a directory rename without updating all those references would break
existing readers. The six `160623_test` records point to a different, already
absent dataset and must not be merged with the real `160623` calibration data.

## Project path checks and source-retirement dependency

Alexander's active `anof_190423_nh_sir2_tween` copy already exists under homes2.
Its MAT matched the original Sauvegarde MAT and an independent pre-rewrite
backup, SHA256:
`c7c6b30c1882d2ad7f1fb91276b2565bfac0e38177b45e31c5712dc8b9cea874`.
The copied project contains 28 FOVs and 14,386 ROIs, with image, data and results
sidecars. `relink_project1.m` rebases the copied MAT only, audits both data and
results sidecars, compares canonical scientific values across save/reopen,
and tests representative image/raw reads against the originals. Original
Sauvegarde files remain untouched. Any modified sidecar is backed up first.

Project ID: `1bbe0337-ba93-4c15-a7e9-39ae6df47979`. The operation holds its own
edit lease `fe48ef68-3959-4a90-9b36-337f46bcc28d`, initially expiring
2026-11-01T22:39:45Z. Release only this lease after validation/catalog update;
do not release unrelated leases. Windows validation passed in the actual
limited worker session on 2026-11-01T21:40:02Z: 28 FOVs, 14,386 ROIs and all
43,158 image/data/results paths readable. The temporary diagnostic task/scripts
were removed; the worker stayed running. Linux's full sidecar audit is still
running at this checkpoint.

`wait_project1_finalization.py` is a one-shot local continuation of that running
validation, not a scheduler/service. It polls for the Linux exit receipt and
then invokes `finalize_project1.py` only on exit 0. The finalizer also requires
the successful Windows receipt, unchanged original hash and project identity,
no active affected jobs and its unexpired own lease. It updates project
metadata/audit and releases only that lease. **It cannot delete any raw source.**
It stops before 22:33 UTC if validation has not finished. A workstation restart
stops this optional continuation; inspect its private runtime status and the
catalog receipt before resuming. Never replay a committed finalization.

**External project location still requires clarification.** The same raw has
an existing link to Gilles's project `190423_nh_sir2_tween`, ID
`0e485efc-6b23-4faa-a316-4faea283477d`. Its declared
`/data/Gilles/analysis/projects/190423_nh_sir2_tween.mat` is already absent.
The earlier chat calls its location "M", without proving that this means a
drive letter. Its actual catalog root is `hub_gilles_10ef868b`, prefix
`/data/Gilles`, and its registration metadata mentions an X: client path.
No M: drive is mounted on this administration workstation. The user has been
asked where the current MAT resides. Keep the 737-GB primary source while that dependent project's path
has not been validated or its disposition clarified. Do not treat its absence
as authorization to discard a real local project. Another missing localdev
catalog MAT was observed read-only and is likewise not silently reassigned.

## API media continuity

The API container mounts /data, not homes2. A fully SHA1-checked temporary
projection at `/data/.detecdiv-hub-api-cache/alexander-2023_1` preserves 774
files / 11,051,646,071 bytes from the verified homes2 copy. Existing IDs were
preserved: 710 preview artifacts and 64 display-setting records. All 774 actual
API content handlers passed after the URI update; audit batch
`23a0fd56-b832-49da-9013-65fcad5052bc`.

The remaining 150 preview and six settings references are all under the
already absent `160623_test` tree. They were missing on both NAS views before
this operation and remain unchanged. None was a live primary file overlooked
by the projection. These records require a separate catalog/data review.
All temporary API projections must eventually be retired after a working
homes2 serving solution is established, as required by the user's final scope.

## 2023_3 detached copy/full verification

The fresh native source inventory contains 708,590 files / 5,967,100,516,555
bytes, metadata SHA256
`4abdfbfff41264b9bc069c3968ac742ac06fa9b7c52127bbf4369a7d95306fd3`.
`continue_2023_3.py` is running in tmux `alex2023-3-copy-20261101` on compute,
with the existing maliavko SMB mount, an initial 60-MiB/s cap, low priority,
and at least a 2-TB free-space margin. The runner reuses the immutable verified
copy helpers, not the old all-year runner's main routine. It cannot delete
sources, publish a year, relink a project or change preferred raw locations.
After copying it performs a full rsync SHA1 comparison and fresh source
stability inventory. Only then may project/catalog/Windows gates be completed
and a separately audited manifest retirement occur.

Copy audit batch: `a8c8e0da-72b6-42c5-9644-2028709c9707`.
The source and existing catalog locations remain intact during this copy.
The tmux process survives SSH disconnects, not a compute reboot; inspect state
and receipts before resuming. Do not blindly replay one-shot scripts.

An optional one-shot local process `copy3_status_reporter.py` mirrors the
standalone copy phase into that audit batch every five minutes and exits on
completion/failure. It never changes data. It installs no scheduler/service,
and sends no messages. If this workstation sleeps or restarts, the server copy
continues; the status bridge can be resumed later. Its local runtime files are
ignored by Git. Its terminal verified status explicitly leaves project and
catalog gates pending rather than claiming source retirement complete.

## Dependency discovery and live status

A separate read-only MATLAB task examines 153 declared catalog MATs to discover
raw dependencies, including old drive aliases. Already handled project copies
are excluded. The scan finished with exit 0: 151 inspected, two declared MATs
already absent (the Gilles project above and unrelated Alexander 2026 project
`anof_071026_X54445_LOGEB`). Eleven candidate projects refer to five of the six
2023_3 acquisitions. Their saved raw listings were exported read-only. All
469,830 listing entries match current primary filenames, sizes and saved
timestamps after the same one-hour timezone offset. This is useful alias
evidence, not a content checksum; content identity still requires review.
No 2023_3 project has been rewritten or published.
Candidate acquisition-name matches are **not** automatically
treated as proven relocated identities. Preserve raw/project IDs, owners,
annotations and relationships; investigate legacy aliases before rewriting.

Full catalog snapshots, the 153-project path scan, complete API projection plan
and raw-location listings are intentionally excluded from Git. Their hashes
and private compute archive are recorded in `private-checkpoint-manifest.json`:
`/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101/private-checkpoint`.
Scripts and the focused verification receipts remain reviewable here.

Full native inventories, state and diagnostic logs remain private on compute:

`/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101`

Useful files there:

- `2023_3/state.json`, `2023_3/copy.log`, later the dated verification log;
- `project1-relink-progress.json`, `project1-relink.exit`,
  `project1-linux-relink-verification.json`;
- `all-project-path-progress.json`, `all-project-path-inspection.json`;
- `2023_1/copy-preservation-gates.json`, `2023_1/api-media-state.json`.

The old `2023_1/state.json` source-change/target-inventory diagnostic does not
represent a fresh full-tree source copy. Use the separate preservation gate,
residual manifest and prior full comparison together. No API/worker restart,
engine-default change, provider activation or backup-policy change occurred.
