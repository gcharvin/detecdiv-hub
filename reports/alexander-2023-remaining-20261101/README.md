# Alexander 2023 migration continuation — 2026-11-01

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