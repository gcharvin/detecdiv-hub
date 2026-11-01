# Alexander project relocation and approved 2023_2 retirement — 2026-11-01

This operation follows the separately recorded homes2 Windows-access gate and
21-record raw catalog cutover. It preserves the project identity
`aee22015-d2a7-4c78-a715-bf957c722c72` and the original Sauvegarde project.
Completion and measured reclaimed space are recorded in the final receipts;
launch receipts alone do not demonstrate completion.

**Completed.** The source-retirement batch
`459a5303-c15e-458d-bd48-ac10be19f037` finalized at 19:51:22 UTC. Exactly
425,112 source files were retired. Measured free space increased by
3,577,917,788,160 bytes (3.578 TB), leaving 4,921,802,833,920 bytes available
(4.922 TB). The full per-file unlink receipt exactly matches the approved
manifest. A final native homes2 inventory finished at 19:52:58 UTC with all
425,112 destination files / 3,583,194,799,325 bytes and the identical metadata
digest. The final 19:53 UTC catalog check confirmed all identities, links,
337 positions, 2,554 unrelated locations and all seven worker PIDs/fingerprints.
The temporary project lease and Windows diagnostic task were released/removed.

The project cutover committed at 19:45:01 UTC, audited batch
`268a32d9-0a87-4d26-b4a1-02e41838d1d2`, new preferred location `4257`.
Linux and Windows validations both passed. The copy has 47,965 rebased path
values. All 5,724 result sidecars were audited and needed no changes; all image
MATs remain byte-identical. The copied MAT SHA256 is
`60fe1a608a40b7e559145cc799e71f33ed5e33e80473606b1f172a2d99774faa`.
The source MAT SHA256 remains
`ef4bdb4155e4866ecef3d8fbf1c2466a644c85d7063d943488dab625613bdc96`.
The measured copied project is 152,861,985,450 bytes after reserialization.

The user explicitly approved the following separate source-retirement question:

> Après validation du projet sur homes2, autorises-tu la suppression des copies
> devenues redondantes de 2023_2 sur `/data` (3,58 To), en conservant le projet
> original sur Sauvegarde et les données vérifiées sur homes2 ?

Answer: **Oui, retirer ces doublons vérifiés de /data**.

## Project copy and path conversion

The source is the top-level `anof_26102023_yam76s_100ms.mat` and paired data
directory under `/mnt/detecdiv-secondary-sauvegarde/Alexander/analysis`.
Its 11,450 regular files total 152,861,817,319 bytes. All were copied as
independent inodes on the secondary NAS and fully compared with their sources
using SHA256/SHA1. Empty targets were created through maliavko's personal SMB
mount before native Btrfs cloning, preserving target ownership. The source
inventory remained unchanged. Independent reflinks preserve the original
project while avoiding a second physical image payload on that NAS.

The published container is:

`/homes2/maliavko/DetecdivHub/projects/anof_26102023_yam76s_100ms`

It contains the same-named MAT and paired data directory. An independently
verified, pre-rewrite top MAT is also retained under
`DetecdivHub/.migration-backups/alexander-project-20261101/`.

`relink_project_copy.m` uses the existing pinned MATLAB engine release
`d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7`. It changes only paths belonging to
this project and 2023_2. Canonical snapshots compare scientific values and
annotations before conversion and after save/reload, normalizing only the
approved path roots. Every results sidecar is audited. The 5,724 legacy
image MATs remain byte-identical: the pinned ROI loader explicitly preserves
the receiving ROI's id/path. Representative image/result loading and original
pixel comparisons test this behavior for all 12 FOVs.

The initial diagnostic was interrupted after its main MAT save/reopen checks
and during the results audit. The optimized restart always reads the verified
pre-rewrite MAT, making that interruption restartable. Only the operation's
own MATLAB process was signalled; no worker was stopped.

Windows validation runs MATLAB R2025b under the actual worker's limited
`GMGM\Charvin-Admin` session 1. It applies the existing homes2-to-personal-UNC
mapping in memory, checks every ROI sidecar path, and reads representative
ROI/raw images for each FOV. It never saves UNC paths into the shared project.
Linux validation uses R2024b and the same pinned engine release.
The first Windows diagnostic stopped on its own empty-structure report
initialization after reading the first FOV. Correcting the diagnostic and
rerunning it passed all 12 FOVs; neither attempt saved the project.

The catalog cutover keeps the project ID, key, owner, privacy, annotations,
groups, ACLs and exact raw link. The old readonly location is retained;
the new verified location becomes preferred. Directory size is measured.

## API projection retained on /data

The API container mounts `/data` readonly and currently has no homes2 mount.
Directly replacing preview/display-setting URIs with homes2 paths would break
its file-content handlers. Therefore 335 existing MP4s and 19 display-setting
JSON files are retained as independent, fully SHA1-checked files under:

`/data/.detecdiv-hub-api-cache/alexander-2023_2`

This projection totals 5,874,728,850 bytes. A separate audited transaction keeps
all artifact/raw IDs and changes only their content URIs. All 354 handlers are
checked inside the real API container. This preserves current content routes
without a mount change, service restart or new serving implementation.
Future generation directly into homes2 still requires an API-visible storage
projection or an explicit homes2 serving solution.

## Source retirement boundaries

`refresh_raw_inventory.py` makes fresh native inventories of the primary source
and final homes2 destination. The source matches the original metadata digest
`bfe6b368e2c8d0a2de897c6351733060e2e0e62870b30a391afd14a40f52e063`.
Both trees have 425,112 regular files and 3,583,194,799,325 bytes. The previous
full SHA1 comparison succeeded; destination timestamp differences on 223 files
are within one microsecond. Metadata stability keeps that comparison applicable
without rereading 3.58 TB for another full checksum pass.

After both MATLAB validations and project cutover, `retirement_catalog.py`
checks active jobs, indexing and project leases; verifies the API dependencies;
records all original location IDs and paths; and acquires its own temporary
project lease. `retire_verified_raws.py` is then permitted to unlink **only**
the approved manifest under native root
`/volume1/DATA/Alexander/data/2023_2`.

The native process checks the complete source tree first, then checks regular
file type, size and mtime again immediately before each unlink. Parent paths
are opened relative to a fixed root with no-follow directory descriptors.
Symlinks, unexpected files, changed files and unexpected hardlinks stop it.
It removes empty directories only; excluded NAS system directories, snapshots
and backups are never recursively deleted. A per-file receipt and final free
space measurements remain on compute.

Only after the complete unlink receipt is verified are the 21 dead source
location rows retired. Their historical IDs/paths remain in the migration
batch. Preferred homes2 locations, 337 positions and raw identity/ownership/
relationships are preserved. The operation releases its own project lease.

The original Sauvegarde MAT/tree is preserved as historical evidence, including
its original saved raw paths. The active project is the relinked homes2 copy.
After source retirement, restoring primary paths requires copying back from
homes2; flipping old preferred flags alone is no longer a rollback.

No 2023_1 or 2023_3 data is retired by this operation. Hub automatic backups
remain disabled and the secondary provider/account inactive/planned. The user
authorized source retirement with the verified homes2 data retained; this
operation does not claim to establish an additional automatic raw backup.

## Evidence and status

Compact receipts are checked into this directory. Full manifests, per-file
hash/unlink receipts, diagnostic logs and progress are kept privately at:

`/home/charvin-admin/storage-transfers/alexander-project-relocation-20261101`

Important completion receipts are `linux-project-final-verification.json`,
`windows-status.json`, `project-catalog-cutover.json`,
`raw-retirement-result.json` and `raw-retirement-catalog-finalization.json`.

The Python scripts are operational records with one-shot guards. Do not replay
them as a general migration command or blindly retry a partial retirement.
