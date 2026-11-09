# Current Deployment
## Resolved-resource execution preflight, 2026-11-09

DetecDiv `b9e1a650df3ef87626403b880c104fac9b479646` extends the binding repair:
both pipeline runners check every enabled selected node for unresolved resource
inputs before the first node executes. Dry-run performs the same check and
returns actionable validation errors. The structured runner also retains its
per-node guard. Regression tests cover whole-graph checks, disabled nodes,
struct/cell run overrides and producer output-name changes. Twenty-three tests
passed, one platform-specific alias test skipped. Already submitted recovery
job `8aa837fd-b962-47b5-bc5d-199ff656df53` retains its `0cab1d9c` pin. The new SHA is pushed to both remotes, verified in protected releases on both compute hosts and published as the default; no worker restarted.

## Abhilasha resource-binding recovery, 2026-11-09

The channel recovery job `4df3e2fd-c66b-4add-b1c1-8a31083696ed` failed on
2026-11-08 at 12:23 CET in computeRLS. Submitted `node_params` were applied again
after symbolic binding resolution and restored literal `@resource:` tokens.
The CNN fallback used the intended CombinedChannel and persisted div_1;
lineage skipped its missing mask and metrics lacked the lineage mask despite
node_done events. Fresh MATLAB inspection confirms 160-frame extracted images,
Cellpose masks and div_1 on sample positions 1, 13, 27 and 32. Preserve these
outputs. Recovery is limited to lineage, metrics and RLS with explicit existing
segmentation/classification inputs; no raw extraction or segmentation restart.
The former four-hour monitor was removed after diagnosing the failed job.


Recovery job `8aa837fd-b962-47b5-bc5d-199ff656df53`, run
`pipelineTemplate_bindings_20261109_5ca7bf5f`, was submitted with only the
three missing processors selected. It is pinned to DetecDiv
`0cab1d9cc6f4f4eb679529e9210f75988773819c`, pushed to both remotes, protected
on both hosts and published as the default without worker restart. This includes
`5ca7bf5f` (apply run overrides before resolving once, runtime unresolved-input
guard and active metric-channel preload limits) and the same override fix in
the legacy runner. Twenty-three targeted tests passed; one Windows alias test
was skipped. The broad contract smoke suite has two pre-existing channel-count
failures, reproduced on the unchanged baseline. Actual partial-run validation
and Hub preflight both passed with no warnings/errors. A fresh MATLAB inventory
verified div_1 with160rows in all1488extractedROIs. Input SHA256:
`ae4eb0993d4af96986bd13bfadfb4919af41e95f05dec7d5c6b8871bc4f46159`.
External inputs are explicitly bound to results_cellposeSAM_cell and div_1;
internal bindings resolve to MotherLineageViterbi_cell and channel_quantification.
A new four-hour heartbeat monitors this job and verifies persisted outputs before
reporting success. Receipts: `reports/abhilasha-bindings-20261109`.
## ROI loading and Score compatibility release, 2026-11-06

DetecDiv `140850e451d44ff2cd3243b27e08e9c30e036bd5` is pushed to GitHub and
GitLab `unstable` and published as the default for future MATLAB jobs. Twenty
MATLAB tests passed for the bounded HDF5 read retry, registered classifier
plugin discovery, explicit all-zero intensity-frame notice and compatibility
with older open Score editors. Protected release worktrees were verified at:

- Linux: `/home/charvin-admin/repos/DetecDiv-jobs/releases/140850e451d44ff2cd3243b27e08e9c30e036bd5`
- Windows: `C:\Users\Charvin-Admin\Documents\GitHub\DetecDiv-jobs\releases\140850e451d44ff2cd3243b27e08e9c30e036bd5`

No worker restarted or shared source HEAD moved. Already submitted jobs,
including Abhilasha's recovery below, retain their existing code pins. The
preceding channel parsing fix is included in this release.

## Abhilasha Zarr channel names release, 2026-11-06

**15:16 CET update, superseding the deferred-only instructions below:** Gilles
explicitly authorized identifying the blocker, killing the old job/worker,
submitting the corrected run and monitoring it long term. The deferred
webserver controller PID 4137788 was stopped first. Original worker @3 PID
911503 had no MATLAB child and was waiting on SMB with Zarr chunk files open;
raw reingestion was enabled despite an existing indexed source link. The Hub
cancel route recorded Gilles's request, then SIGKILL stopped only that verified
worker PID at 14:12:19 UTC. systemd replaced it with PID 328227. Normal orphan
recovery finalized old job `09f697c4-5a47-4932-a986-c18dba06a291` as failed and
released its server lock. A newly acquired Abhilasha desktop edit lease was
handed off through the normal admin release route, with audit metadata, before
new-job preflight and creation. No other worker/job was stopped.

Recovery job **`4df3e2fd-c66b-4add-b1c1-8a31083696ed`** is now running on
Linux with the exact release and validated snapshot below. It reuses indexed
raw dataset `3d707760-f50f-4358-892e-5dcecc99bf20` and its existing project
link, with `ingest_raw_dataset=false`; this skips the redundant catalog scan,
not the MATLAB dataloader. Its isolated supervisor PID is 328796; at 14:16 UTC
MATLAB had started, loaded the lightweight project, and written real logs.
At 14:17 UTC the runner passed initial pipeline validation and started
`dataloader_5`, actively parsing the OME-Zarr. Scientific outputs remain to be
verified. Local receipts
include `request-immediate.json`, `submission.json`, `submit_immediate.py`.
The Codex heartbeat **`suivre-la-reprise-hyper7-d-abhilasha`** checks every ten
minutes, stays quiet during ordinary progress, diagnoses failures and verifies
persisted outputs before reporting success. It does not authorize another
automatic retry or killing any other job. Do not restart the old deferred
controller or submit a duplicate recovery.

DetecDiv `00aaa6b4e016fe61301e33f51a2918778dc8c404` is pushed to GitHub and
GitLab `unstable` and published as the default MATLAB release. The deployment
script verified protected worktrees on Linux at
`/home/charvin-admin/repos/DetecDiv-jobs/releases/00aaa6b4e016fe61301e33f51a2918778dc8c404`
and Windows at
`C:\Users\Charvin-Admin\Documents\GitHub\DetecDiv-jobs\releases\00aaa6b4e016fe61301e33f51a2918778dc8c404`.
No worker restarted and no shared source HEAD moved. Thirteen focused MATLAB
tests passed; readonly parsing of all 32 Hyper7 positions verified channel
names and their C/Z identities.

Gilles authorized a corrected retry while explicitly preserving the running
Abhilasha job `09f697c4-5a47-4932-a986-c18dba06a291`. At 13:56 UTC it remains
running, pinned to `a7cf13fb25908fe45eda43be9582a481e08a1c0c`, with its lock
untouched. Do not cancel or replace it. The corrected recovery is prepared,
not submitted: run `pipelineTemplate_channels_20261106_00aaa6b4`, separate
input snapshot under the project's `pipeline/<run-id>/input/pipeline.json`,
SHA256 `dacc6bfcc9a5780f6ce8c206538735137f7a2649b6a13f530781d6a3976414b7`.
It preserves the submitted motif, FOV 1:32, frames 1:160 and scientific settings;
only exact generic channel references were mapped to acquisition names.
MATLAB dry-run passes; Hub preflight reports only the original job's lock.

A one-shot controller on **webserver-labo** in
`/home/charvin-admin/ops/abhilasha-channels-20261106/` (PID 4137788 at launch)
records `state.json`. It waits up to 24 hours, submits at most one new job via
the existing Hub service after a failed original and all project writers exit,
and checks the snapshot checksum and ordinary Hub preflight/locks. It never
signals workers, cancels jobs or releases locks. Original success stops without
repeating the analysis; cancellation stops for a new instruction. Inspect this
controller/state before any manual retry; do not create a duplicate. Local
receipts are in `reports/abhilasha-channels-20261106/`.

## Alexander 2024 migration started, 2026-11-05

2026-11-07 15:24 UTC: 33 unpublished project copies have verified path
corrections (59 MATs plus9 active-reference JSONs,1,427,052 exact changes).
Scientific/type/annotation/save/reopen comparisons passed; GPU postflights
for both GPU-bearing batches match original contents and classes. Movie/qp
completed in a preserved partial attempt; a separate bounded Gilles-only
recovery completed after correcting its scientific snapshot root adapter.
All own relink leases are released; originals/backups, HDF5 images, archival
JSON/logs and all catalog identities/locations remain preserved. See the latest
report checkpoint and partial/recovery receipts; do not replay either writer.
LoxP15's15 initially absent raw fields remain unresolved. Raw 2024_1 SHA1 is
still active;2024_2 has not begun. No filesystem publication, preferred
location/API cutover or2024 source retirement, zero2024 bytes reclaimed.

2026-11-07 11:21 UTC: all four metadata batches completed successfully,
30 unpublished project copies/56 MATs/1410163 exact path changes with scientific
save-reopen checks. All own leases released; catalog identities/metadata/
locations unchanged. Batch2 after-hash GPU corroboration is now running; never
replay completed writers/watchers. Raw 2024_1 full SHA1 still active, 2024_2
not started; no cutover/source retirement and zero 2024 bytes reclaimed.
Read the newest report checkpoint and preserve unresolved lox raw absences.


2026-11-07 09:13 UTC update: metadata batch4 finished successfully (four
projects/six MATs/110444 paths), own leases released and DB identity/metadata/
locations independently unchanged. Completed batches1+4 now cover sixteen
project copies,29 MATs; batches2/3 continue. Raw full SHA1 still pending;
no preferred location/API/source retirement. Do not replay finished batch4.

2026-11-07 09:01 UTC checkpoint: raw 2024_1 full SHA1 still running, no
terminal checksum/stability or 2024 source retirement. First twelve project
copies now also have exact post-save parity for all 4258 GPU descriptors.
Three new disjoint unpublished metadata writers are running for 9+5+4 projects
(18+9+6 MATs), with fresh own leases, native original/backup gates and bounded
scientific save/reopen checks. Do not replay any live writer or its hidden
own-lease watcher. No preferred location/API reference changed.
New native parity proves loxP15 already lacked 53280 raw refs across fifteen
even positions in the initial manifest; these fields remain inaccessible,
not recovered. Gilles was asked about another archive while other work continues.
Movie cache discrepancies were separately corroborated by 36 pixel reads and
two exact ROI crops; its two blank ROI slots are preserved. Read the newest
report README before any operation; zero 2024 bytes reclaimed.


2026-11-07 02:40 UTC checkpoint: raw 2024_1 copy finished at 02:22 UTC
(5.178 TB), full SHA1 now running; source stability and 2024_2 still pending.
The first twelve unpublished project copies completed their metadata relink:
23 MATs / 636618 exact changes, scientific/save-reopen equivalence passed,
backups/originals preserved. All twelve own leases released and DB identity,
metadata and locations independently confirmed unchanged. Saved GPU descriptor
corroboration is running readonly; do not replay the completed writer/watcher.
Identity batch5 and metadata batch4 are complete; next bounded metadata/type/
HDF5 and availability-variant audits are running. No preferred location/API
switch or 2024 source retirement; zero 2024 bytes reclaimed. Read the newest
report README for authoritative receipts and remaining gates.


2026-11-06 21:01 UTC checkpoint: 2024_1 copying at 85.16%, no raw full SHA1
or source retirement. Original project-copy runner stopped safely before
project29's missing associated source folder; no stage existed. Its 28 receipts
are preserved. Bounded continuation of the final three projects completed:
31 full standard copies plus two special copies verified. The available MAT
of loxP15 was separately copied/hash-verified without inventing its absent
folder; complete project access remains under explicit baseline review.
Actual metadata relink of twelve unpublished copies is now running in
alex2024-relink-batch1-20261106: 23 MATs / 636618 planned changes, 23 native
verified backups (3.715 GB), twelve original inventories unchanged, twelve
own leases until 2026-11-07 02:47 UTC. At the checkpoint 12/23 files have
scientific/save-reopen receipts, not terminal success. Hidden watcher releases
only those own leases after terminal completion. No preferred location or
API reference changed. Read the newest report README before any action;
never replay the old copy runner or the live metadata relink.


2026-11-06 14:41 UTC checkpoint: raw 2024_1 still copying normally, 70.03%,
23 standard project copies verified; no full SHA1, relink or source retirement.
The expanded nine-basename search is complete over 9.91 million payload names:
no same-acquisition copy found, only unrelated 2025 namesakes. Historical
native availability parity remains verified (15 old absences/45 old cache-size
exceptions), not image recovery. Metadata batch2 and FIG scan are finished;
first twelve projects now have 55,636 metadata files audited and 23 MATs planned
for future relink. Cre16 and both special-layout identities are verified.
Gilles has 58 HDF5 image sidecars, not missing MAT images; two real ROI/raw
crops match and all originals/Hub owners remain preserved. Identity batch4,
metadata batch3 and scientific type readiness audits are running. Special-layout
metadata finished, but Gilles main and 14 pipeline execution snapshots need
typed path classification; no blind rewrite of history or external models.
Type readiness must cover actual GPU content before any save.
Read the newest migration report checkpoint for all task states and gates.
Earlier migration descriptions below are historical.


2026-11-06 10:50 UTC: 2024_1 copy healthy at 3.169 TB / 5.178 TB (61.20%),
16 standard project copies verified. Native readonly parity of all 13,158 saved
references in cres/ARS1 is complete: 15 absent names (9+6) and 45 historical
cached-size differences (27+18) were already in the initial source state;
source current native metadata and homes2 presence/sizes match that state.
This is not recovery or full raw SHA1. Empty ROI layout is now verified and
its bounded identity continuation is finished: 412 pre-existing unextracted
ROI preserved, 308 extracted, no metadata saved. Thirteen projects have full
identity preparation, separate from the two historical-baseline projects.
New persistent metadata batch2 (nine projects), identity batch3 (project16),
and expanded filename search are running. Older completed audits must not be
replayed. No relink, cutover, source deletion or 2024 space recovery yet.
Read the newest report README checkpoint for exact receipts and task states.

2026-11-06 09:58 UTC: 2024_1 copy healthy at 3.025 TB / 5.178 TB (58.42%).
Fifteen standard project copies and both special-layout copies are fully hash
verified; the standard runner continues. Twelve projects have full readonly
identity preparation (154 FOV, 43,847 ROI). Both identity audits are finished;
the second completed its loop with nine successes and two review rows, not
all-project success. The first audit's fourth project has nine historical raw
references already absent and 27 cached-size differences already in the initial
source manifest; preserve that evidence and diagnose without inventing data.
First-three-project persistent MAT/JSON and seven FIG audits are also finished,
with four MATs planned for future corrections. No actual relink, raw SHA1 terminal
success, cutover or source retirement. See the latest report README checkpoint;
do not replay finished audits or successful FOVs. Older status below is historical.

2026-11-06 02:29 UTC: raw 2024_1 remains healthy in copy, 1.956 TB / 5.178 TB,
37.77%; no raw SHA1 gate or source retirement yet. Nine standard project copies
are fully hash/ownership/stability verified, with the existing 32-project runner
continuing. Both nonstandard copies (qp and Gilles) are now fully hash verified,
106 files / 5,293,672,634 bytes, originals/Hub owners preserved, unpublished.
A read-only alias/srclist/ROI/raw pixel preparation is running for the first
nine verified copies in `identity-audit-20261106-batch1/`, tmux
`alex2024-identity-audit-batch1-20261106`. All legacy layout reviews (initial,
bounded v2 and v3) and the exception-copy controller are finished; do not replay.
Remaining unreadable/missing main records still need relevance classification.
Read the latest checkpoint in the report README for actual scripts/receipts
and layout-specific gates (qp image placeholder, no results/data in either
exception layout). No path relink, publication or preferred-location change.

20:29 UTC follow-up: raw 2024_1 copy is healthy at 960.20 GB / 5,177.81 GB
(18.54%). The main project discovery finished: 34 candidates (33 Alexander,
one Gilles), with unresolved legacy/missing layouts requiring classification.
Thirty-two standard-layout Alexander project copies are now being independently
cloned/fully hashed under owned unpublished homes2 stages in tmux
`alex2024-project-copies-20261105`, private `project-copies/`. A separate new
read-only format review is running for unknown legacy layouts in
`alex2024-legacy-path-review-20261105`, private `legacy-path-review/`.
Do not replay the completed main/supplemental discovery or duplicate these
active tasks. Two nonstandard-layout candidates need separate preparation.
No relink, cutover or source retirement has occurred. The report README has
the pinned hashes, receipts and remaining gates.

The user authorized migration of 2024_1 then 2024_2 to homes2, associated project
inventory/relinking, and verified `/data` duplicate retirement while preserving
all original projects. Batch `0382c23e-ad54-4ec9-9ad8-53cc79b64d97` is
`copying_and_project_discovery`. Detached compute work is under
`/home/charvin-admin/storage-transfers/alexander-2024-20261105`.
Tmux `alex2024-copy-20261105` started the actual 2024_1 copy at 14:27:53 UTC;
it requires complete SHA1 and native source stability before starting 2024_2.
The two source years total 1,706,691 files / 11,034,331,124,398 logical bytes.
No source has been removed and no preferred location or project file changed.

Independent read-only project path discovery is running in
`alex2024-project-discovery-20261105`, covering 3,326 catalog main files,
Alexander's 154 first. No project/raw links exist yet for the 115 selected raws;
candidate paths require scientific identity proof. Deferred busy projects,
unknown layouts and uncataloged main files must be resolved before retirement.
Use `reports/alexander-2024-20261105/status.py` and read that report's README
before taking action. Never replay a partially executed controller.
Six-hour autonomous follow-up:
`alexander-2024-migration-et-lib-ration-data`.

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

## Alexander 2023 final migration scope, confirmed 2026-11-01

The user requires eventual retirement of all Alexander 2023 data from `/data`
after transfer and validation on homes2. This is standing authorization for
verified source retirement across `/data/Alexander/data/2023_{1,2,3}`; it does
not permit deleting any untransferred or unverified file. Preserve the original
Sauvegarde projects. Complete the remaining 2023_1 and 2023_3 transfers, relink
and validate all affected project copies, and update catalog references before
retiring their verified source manifests. The separate staged 2023_1 pilot
must retain a working catalog location throughout publication.

The final scope also includes retiring the temporary 2023_1 and 2023_2 API caches
from `/data/.detecdiv-hub-api-cache/alexander-2023_{1,2}` after the API can serve their
verified homes2 equivalents. The existing cache is an interim dependency,
not the final storage arrangement. Do not remove it while its URIs are live.
The 2023_2 scientific payload has already been retired as recorded below.
Detailed conditions and the user's instruction: `docs/alexander_2023_transfer.md`.

## Alexander 2023_1 retired; 2023_3 raw/project copies active, 2026-11-01

Latest follow-up, 2026-11-05 10:51 UTC: **2023_3 source retirement is complete
and fully verified**. Exactly 708,590 files / 5,967,100,516,555 logical bytes were
removed with complete receipts. Available space increased by
5,967,222,644,736 bytes; cumulative measured deletion gains for all three years
are 10,282,445,193,216 bytes. Post-removal Linux/actual Windows reads and all 798
complete API content streams passed. Exactly 47 dead raw source locations were
retired, all eleven own leases released, originals and identities preserved.
Batch `67c091e6-b021-4f9e-8048-33a9192e1bc7` is `source_retired_verified`.
Do not repeat any completed copy/relink/test/cutover/retirement. Remaining final
scope is the 28,981,723,684-byte live API caches across years 1/2/3. Their
readonly homes2 mount preparation is staged, but VM sudo requires the user;
no mount/API deployment or cache removal has occurred. See the report README.

Latest follow-up, 2026-11-05 10:33 UTC: both final-folder host validations
passed. At 10:28 the same eleven projects and 24 raw identities/775 positions
were switched transactionally to verified homes2 locations, with original
readonly project locations preserved. All 798 real API responses were fully
streamed and hashed successfully after their URI projection update. Two raw
destination mtime exceptions were audited, content-hash checked, corrected
without data changes, and the complete 708,590-file native metadata check
passed. Exact manifest retirement is now running in tmux
`alex2023-3-source-retirement-20261105`, batch
`67c091e6-b021-4f9e-8048-33a9192e1bc7`. Read its actual state/receipts first;
do not replay any copy, relink, publication, cutover or completed host test.
Own retirement leases expire 14:27 UTC. Post-retirement checks and catalog
source-location retirement remain. API caches (~29 GB) still require a
privileged readonly homes2 mount on the VM before their eventual removal.

Latest follow-up, 2026-11-05 09:55 UTC: raw full SHA1 and exact source-stability
inventories passed at 08:41 UTC for all 708,590 files / 5,967,100,516,555 bytes.
All eleven project copies were published to their final filesystem folders at
09:51 under fresh own leases, with originals preserved. Final-folder Linux and
actual limited Windows-session read tests are now running in distinct tasks.
Catalog preferred locations and API references remain unchanged. No 2023_3
source was deleted. Read the continuation README for active task names, private
receipts, own-lease watcher and next cutover/retirement gates. Do not repeat the
completed raw checksum, incoming tests, relink or filesystem publication.

Latest follow-up, 2026-11-03 15:13 UTC: complete Linux and actual limited
Windows-session read tests passed for all eleven projects (123 FOVs / 53,623
ROIs / 1,728 raw samples per host). The diagnostic Windows task/scripts were
cleaned up; full receipts are preserved. The 798-file API projection also passed
three-way hashes, with no URI changes. Raw full SHA1/source stability remain
pending. Do not restart completed tasks. Final-folder checks, catalog/API cutover
and manifest-bounded source retirement are still required; no 2023_3 source was
deleted. Evidence: continuation README and completion-verification receipts.

Latest follow-up, 2026-11-03 13:28 UTC: the full 2023_3 copy completed at
10:02 UTC and is undergoing the complete SHA1 comparison, followed by source
stability. All 32 copied MATs completed relink/save/reopen comparisons at 06:05
(524,082 path changes); all eleven own leases were released at 06:06. Original
projects and all catalog identity/metadata/location digests remain unchanged.
Read-only Linux and actual limited Windows-session tests are now running across
123 FOVs / 53,623 ROIs and 1,728 raw sample reads. Both passed the first six
FOVs at 13:24. Linux tmux `alex2023-3-host-linux-20261103`; Windows task
`DetecDiv Alexander Project3 Host Verify 20261103`. Incoming folders are still
unpublished; canonical final paths are mapped in memory for these diagnostics.
An independently three-way SHA1-verified API media cache is preparing in tmux
`alex2023-3-api-projection-20261103` (798 files / 12.055 GB); no URI changed.
See the continuation report for private receipt paths and guarded cleanup.
Do not duplicate tasks or retire 2023_3 before all remaining gates complete.

Latest follow-up, 2026-11-03 03:37 UTC: raw copy reached 5,019,920,590,245
of 5,967,100,516,555 bytes (84.13%). The metadata audit completed with exit 0:
98,611 non-image MATs, zero unresolved paths; 32 files need rebasing (11 main
projects, 21 processors). Relinking of those unpublished copies is preparing
in tmux `alex2023-3-relink-20261103`, batch
`46b50e1d-bf24-4284-9d3d-fbe4df5de158`. All eleven fresh original inventories
matched; native input hashes/ownership passed and 32 backups were created.
At 03:51 UTC the backup hash gate was still running, before any MAT rewrite.
At 03:54 UTC that gate passed and the 21 processor MATs had been relinked with
verified scientific save/reopen comparisons; the eleven main MATs were next.
Eleven own leases expire 07:45 UTC; a hidden local watcher releases only those
leases on terminal exit. Read the continuation README for receipts and recovery
gates. Copied MATs will use final canonical paths while files remain incoming;
they are not yet preferred/published. No 2023_3 source retirement occurred.

Latest follow-up, 2026-11-02 21:33 UTC: raw copy reached 4,063,322,667,444
of 5,967,100,516,555 bytes (68.10%). The first read-only project audit completed
(exit 0): all 123 FOVs have exact raw-crop/ROI pixel equality corroborating
the legacy Z: aliases; copied ROI pixels/results equal originals, 1,728 raw
samples were recorded, 53,623 ROI image/results references resolve in the
verified manifests, and every main MAT remains unchanged. The next read-only
audit scans all non-image MATs in tmux `alex2023-3-persistent-paths-20261102`;
its first failed attempt and corrected retry are documented in the report README.
Do not duplicate either task. No 2023_3 project relink, catalog cutover or source
retirement has occurred. The final raw, save/reopen, Windows and API gates remain.

Follow-up, 2026-11-02 15:38 UTC: 2023_3 raw copy reached 3,149,612,890,352
bytes of 5,967,100,516,555 (52.78%); full SHA1/source stability still follow.
All eleven independent project copies completed at 10:19 UTC, with full matching
hashes for 152,277 files / 1,492,353,838,054 bytes and stable source inventories.
Sauvegarde originals remain intact; projects are not relinked or published yet.
A read-only preparation audit now runs in tmux
`alex2023-3-project-inspection-20261102`, comparing ROI samples against originals
and recording raw pixel baselines across 123 FOVs / 53,623 ROIs. Do not launch a
duplicate. No affected job/edit lease was active at admission; unrelated jobs
remain untouched. Source retirement is still gated on complete raw verification,
reviewed identities, project relink/save/reopen, Linux/Windows and API checks.
API continuity planning identified 798 referenced files / 12,055,348,763 bytes;
no URI changed. Primary free space at this check: 5,637,885,267,968 bytes.
Detailed evidence and live audit paths are in the continuation report README.

At 22:25 UTC, the verified residual 2023_1 source was retired: exactly 87,364
regular files / 737,182,660,223 logical bytes. Measured primary free space grew
by **737,304,760,320 bytes**, to **5,648,026,640,384 bytes**. No scientific source
payload remains in 2023_1; NAS system metadata/nonempty ancestors were retained.
The original full homes2 copy has 750,255 files / 6,316,035,396,988 bytes. The
other 662,891 source files were already absent before this continuation and
must not be counted as new space reclaimed here.

Both affected projects passed Linux and actual limited Windows-session checks:

- Alexander's `anof_190423_nh_sir2_tween`: 28 FOVs / 14,386 ROIs, copied MAT
  relinked, scientific values/annotations compared across save/reopen, all
  28,772 data/results sidecars audited, representative ROI/raw pixel equality.
  Preferred homes2 location 4246 and original readonly Sauvegarde location 2761
  remain. Project batch `6c5f88a0-da46-4bf8-aa37-6e4026a7ec9b`.
- Gilles's `190423_nh_sir2_tween` remains under `/data/Gilles/analysis/projects`
  as a v3 JSON project: 30 FOVs / 15,281 declared ROIs. Only 301 raw references
  in 32 JSON files were changed; original metadata backups, live inodes/owners/
  modes/ACLs, HDF5 results, annotations and historical runs were preserved.
  The existing .mat catalog alias is retained because the registration API
  requires it; the pinned engine resolves it to the sibling JSON on both hosts.
  Metadata explicitly records the actual JSON manifest. Project batch
  `369692d0-be0f-4f3c-94e6-150310c42276`. The old mention of M: was a mistaken
  interpretation of the user's wording, not an actual missing drive.

Retirement batch `7ec51c52-e14a-4ac5-ab74-85bd4ee30590` retired only dead source
locations 3703/4288. Raw ID, preferred homes2 location 5512, ownership, links,
positions and all 2,572 unrelated locations were preserved. Both project leases
were released. Keep the working staging namespace: 70 existing preferred raw
locations and the verified project paths already reference it.

All 774 actual API preview/settings handlers passed after retirement. The
2023_1 temporary projection retains 11,051,646,071 bytes alongside the earlier
2023_2 cache (5,874,728,850 bytes). The 156 already-missing references under
phantom `160623_test` were left unchanged. The API still mounts only /data;
homes2 serving and eventual cache retirement remain pending, without a service
or credential change in this phase.

2023_3 raw copy/full verification runs in tmux `alex2023-3-copy-20261101`:
708,590 files / 5,967,100,516,555 bytes. At 22:34 UTC, 324,619,727,042 bytes
(5.44%) had transferred. The copy cannot delete sources or publish locations.
Eleven Alexander project copies are also preparing as independent maliavko-owned
Btrfs clones with full per-file hashes and source stability inventories, in
tmux `alex2023-3-project-copies-20261101`. Their originals remain on Sauvegarde;
no 2023_3 MAT is rewritten or preferred location changed by this copy task.
Raw copy, legacy alias review and final Linux/Windows project gates must finish
before any 2023_3 primary retirement.

At 22:34 UTC, API/database health was OK and all seven pollers were idle with
unchanged PIDs/fingerprints. No API/worker restart, engine-default change,
provider activation, quota change or backup-policy change occurred. Evidence:
`reports/alexander-2023-remaining-20261101/README.md`; private compute work:
`/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101`.

## Alexander project on homes2 and 2023_2 space reclaimed, 2026-11-01

The linked project `anof_26102023_yam76s_100ms` now prefers the independently
verified copy at
`/homes2/maliavko/DetecdivHub/projects/anof_26102023_yam76s_100ms/anof_26102023_yam76s_100ms.mat`.
Its original Sauvegarde MAT and all original project files remain intact;
original readonly catalog location `2765` is retained, new preferred location
`4257`. Project ID/key, Alexander ownership, privacy, notes, ACLs, groups and
the exact raw link are preserved. The copied project has 12 FOVs and 5,724 ROIs.
All 11,450 copied files passed full content comparison before relinking;
47,965 saved path values were rebased. Scientific values and annotations passed
canonical before/after save/reopen comparisons. All 5,724 results sidecars were
audited; none required rewriting. Legacy image MATs remain byte-identical.

MATLAB validation passed on Linux R2024b and in the actual limited Windows
worker session using R2025b, both with pinned engine release
`d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7`. Every ROI image/results path was
checked, with representative ROI/raw reads in every FOV and Linux pixel
comparisons against the originals. Windows path translation was in memory;
the shared MAT retains canonical server paths. Project cutover batch:
`268a32d9-0a87-4d26-b4a1-02e41838d1d2` (19:45 UTC).

After those validations, the user explicitly authorized retiring the verified
2023_2 duplicates from `/data`. Native manifest-driven unlink removed exactly
425,112 regular files (3,583,194,799,325 logical bytes), with another type/size/
mtime check before every unlink. No source payload file remains. NAS `@eaDir`
metadata and their nonempty ancestors were retained, along with the year root.
Measured primary-NAS free space increased by **3,577,917,788,160 bytes**, from
1,343,885,045,760 to **4,921,802,833,920 bytes**. The 21 dead source-location
rows were subsequently retired; raw identities, preferred homes2 locations,
337 positions, links and all 2,554 unrelated locations were preserved. The
temporary project lease was released. Retirement batch:
`459a5303-c15e-458d-bd48-ac10be19f037`, finalized 19:51 UTC.

The API container still mounts `/data`, not homes2. Its 335 preview MP4s and
19 display-setting JSON files remain available through a verified 5,874,728,850
byte cache at `/data/.detecdiv-hub-api-cache/alexander-2023_2`. All artifact/raw
IDs were preserved and all 354 content handlers checked after retirement.
Cache audit batch: `c2eaf913-4f9f-42ee-9c0f-b5df6d2beb18`. Future preview
generation directly on homes2 still needs an API-visible serving solution.

No API/service/worker restart or engine-default change occurred in this phase.
Windows remains PID `3720` / `b35b0444d5f0`; all six Linux pollers retain their
previous PIDs and `a2649a68b316`. Temporary Windows diagnostics were removed.
2023_1 (including its staged pilot) and 2023_3 were not retired. Hub automatic
backups remain disabled; secondary provider/account activation was not changed.
The retained original Sauvegarde MAT has historical primary raw paths; use the
relinked homes2 copy. Restoring retired primary raw paths requires a copy back
from homes2, rather than merely restoring preferred flags.

Evidence: `reports/alexander-project-relocation-20261101/`. Full private
manifests and receipts:
`/home/charvin-admin/storage-transfers/alexander-project-relocation-20261101`.
Earlier sections below describe historical gates, not the current retention
state of the retired 2023_2 primary copies.

## Alexander 2023_2 raw catalog cutover, 2026-11-01

Historical raw-only phase at 18:35 UTC: the same 21 Alexander raw records were
transactionally switched
to preferred read locations under `/homes2/maliavko/DetecdivHub/raw/2023_2`.
The verified copy was first published from its incoming directory by an atomic,
no-replace rename on the secondary NAS. All original primary-NAS locations and
files remain intact. No source storage was reclaimed.

Fresh native NAS inventories matched all 425,112 file paths and sizes,
3,583,194,799,325 payload bytes. The source inventory, including modification
times, exactly matched its initial inventory. Destination timestamp differences
were within one microsecond on 223 files. The separate earlier
full-content SHA1 verification receipt remains valid; this phase did not reread
the entire payload for new content checksums.

The linked original Sauvegarde project reopened read-only in MATLAB using the
currently published release `d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7`: 12 FOVs,
5,724 ROIs. Both original and published FOV paths were checked from the Windows
worker session. No project MAT was saved or relocated: its old raw paths remain
usable through the retained primary sources. Migrating those project internals
and establishing backup/restore coverage remain prerequisites to any future
source retirement, which is not authorized by this source-retaining cutover.

Post-transaction checks validated all 21 API detail views, unchanged raw
identity/ownership/metadata/links, 337 preserved positions, the unchanged linked
project, and all 2,554 other storage locations. Audit batch:
`b2530427-9baf-4276-a5da-855ce3831394`. Evidence and rollback location references:
`reports/alexander-2023-cutover-20261101/`. Live read-only status:
`/home/charvin-admin/storage-transfers/alexander-2023-cutover-20261101/cutover-status.json`.

No service or worker was restarted during this catalog phase. Windows remains
PID `3720` / `b35b0444d5f0`; all six Linux pollers report `a2649a68b316`. The
existing 2023_1 pilot location was preserved, and 2023_3 was not transferred.
The secondary provider/account remain inactive/planned; Hub backups remain
disabled. These settings were not activated implicitly by the cutover.

## Alexander homes2 Windows access, 2026-11-01

The Windows worker now maps `/homes2/maliavko/DetecdivHub` to
`//srv-data-pb-lan1.u-strasbg.fr/home/DetecdivHub`. That DNS name was checked
to resolve exclusively to the secondary NAS, `10.20.8.250`. SMB authentication
uses DSM user `maliavko`; its credential is saved in the existing worker
account's Windows Credential Manager. The earlier IP-address SMB connections
and all nine existing worker path mappings were preserved.

At 18:17 UTC, the Windows worker was restarted only after its target admission
lock confirmed no active jobs, a fresh idle heartbeat, and an empty scientific
child tree. Its new PID is `3720`, with unchanged fingerprint `b35b0444d5f0`.
Admission was restored after a fresh heartbeat and validation in the limited
`GMGM\Charvin-Admin` token in session 1. All 21 copied 2023_2 raw directories,
a copied PNG header, and temporary-file write/read passed. Both NAS-side probe
owners were `maliavko`; both probes were removed. The database tunnel, original
worker task and unlimited task duration were preserved. No API or Linux worker
deployment accompanied this change.

This access change initially left the 21 2023_2 raws on the primary NAS. Their
subsequent source-retaining catalog cutover is recorded above. The secondary
provider/account remain inactive/planned, and no scientific source was deleted.
Evidence: `reports/homes2-access-audit-20261101/summary.json` and the detailed
activation, session verification, and NAS ownership receipts in that directory.

## Reconciled Hub deployment, 2026-10-20

At 21:07 UTC (23:07 Europe/Paris), the applicable runtime changes from Hub
commit `01204c96829b0b676384e7f182ae54074b56aa8d` were installed with host
adaptations preserved. The API image was rebuilt after updating one shared
service file; its actual healthy fingerprint is `15fed3245ae8`. Force-stop
columns, route, UI, public proxy and recent API logs were verified.

The native Windows worker is active at `b35b0444d5f0`, PID `93632`, after a
guarded idle activation of five files. Its temporary activation task removed
itself. The database tunnel and unlimited worker-task duration were preserved.

Linux source fingerprint is `a2649a68b316`, with two shared service files
updated. Idle pollers `@1,@2,@6` are active at that fingerprint. Pollers
`@3,@4,@5` remain on their previous loaded code while their three analyses run;
their PIDs, starts, parameters and advancing heartbeats were preserved.
Full runtime activation is therefore still pending for these three pollers.
The user timer `detecdiv-hub-reconciled-activation-20261020.timer` checks every
30 seconds, reloads only an idle poller with an empty child tree, and stops
itself when every fingerprint matches. User lingering is enabled, so this
continues after SSH logout. The previous maintenance timer was superseded.

Tests of the exact staged host variants: 59 passed on Linux, 59 on Windows,
and two synthetic backup-file tests passed inside the API environment. The
matching staged NVIDIA 580.173.02 libraries were checked without GPU allocation.
Admission was restored. MATLAB's default remains
`6ed9c3e62c10a6bab3d1fa361adbfe54f9355c6b`.
Details and evidence are in `docs/reports/hub-reconciled-deployment-2026-10-20.md`
and `reports/hub-reconciled-deployment-20261020/`.

## Repository reconciliation, 2026-10-20

The local backlog was reviewed, tested and grouped into commits without
deploying or restarting services. The 515 initially pending files included
489 untracked files, mostly historical operational evidence; they did not
represent 515 new product changes. Earlier October 15-19 commits remain in
the branch history. GitLab was one commit behind GitHub before reconciliation.
The detailed inventory and test results are in
`docs/reports/hub-reconciliation-2026-10-20.md`.

Read-only before/after checks preserved all seven poller PIDs/fingerprints and
the three active jobs' start times and parameter hashes. The published MATLAB
default remains `6ed9c3e62c10a6bab3d1fa361adbfe54f9355c6b`. This Git reconciliation
does not activate different worker code or replay historical operation scripts.

## Live GPU transition audit, 2026-10-20

At 16:26 UTC, both existing Abhilasha jobs remained running with unchanged
parameters and start times. Their pinned releases already contain CPU fallbacks
for the only selected GPU classifiers: Cellpose uses CPU if CUDA availability
is false and CNN/LSTM retries classification on CPU after a GPU exception.
No preceding unprotected GPU initialization was found in those classifier paths.
A new process in the same Python environment reproduced CUDA error 804 with the
incompatible system libraries, reported `torch.cuda.is_available()=False`, and
successfully computed on CPU. This validates the existing fallback for the
observed failure, without repairing already mapped libraries or guaranteeing
GPU execution. No running process, job parameter or protected release changed.

The hourly Alex monitor now also checks these two jobs' classification
transition, reporting a CPU fallback or failure while preserving their running
calculations. Alex `e28d8d5e-3601-4c8d-8d69-ae67fe4c8974` was still running and
had progressed beyond drift frame 202. Evidence is in
`reports/alex-memory-recovery-20261020/live-gpu-transition-protection.json` and
`gpu-transition-audit-summary.json`.

## Alexander project memory recovery, 2026-10-20

Four new attempts of `anof_151026_X544rep` failed the legacy extractor's
`ReadPoolMemory` check. Their actual allocation was 24576 MiB, not 16 GiB;
the latter was usable image RAM after `MemoryHigh`, existing processes and the
MATLAB planner's safety reserve. The last attempt needed 15.29 GiB of client
buffers plus 2.48 GiB per reader, exceeding 16.20 GiB usable.

The API now supports validated project defaults in
`metadata_json.pipeline_run_resources`, copied into new jobs' `resources` with
provenance. This project's permanent default is now 49152 MiB (48 GiB); the
global profile and existing jobs retain their allocations. The urgent recovery
has an explicit, audited 30720-MiB exception so it fits the free shared capacity.
API fingerprint is
`d5d9b6785981`. The API source was backed up, verified and rebuilt on
`webserver-labo`; no compute poller was restarted. Local validation: 27 tests
passed. Details are in `docs/worker_resource_scheduling.md`.

At Gilles's explicit request for immediate restart, after confirmation that
Alexander had left and could not close MATLAB, his exact `GC-DELL01-308` client
lease was administratively released with audit and replaced atomically by the
new server-job lock. Job `4bd92189-f80c-4a28-8ad1-00d6fe863d3e` started on Linux
poller `@2` at 15:30:02 UTC, preserving the failed attempt's code pin, scientific
selection, two nodes and `resume`/`skip` policy. Its service has `MemoryMax=30 GiB`
and `MemoryHigh=27 GiB`. It passed the planner with 20.51 GiB usable and two read
workers, then failed independently at CUDA initialization in drift correction.
Previous run metadata/logs are backed up and the two other scientific jobs were
preserved. Hourly thread heartbeat
`suivi-horaire-du-job-x544rep-d-alex` monitors this run. Evidence and the
idempotent operation helper are in `reports/alex-memory-recovery-20261020/`.

The CUDA failure came from an unattended NVIDIA package upgrade to 580.178.04,
while the loaded kernel driver remains 580.173.02. An official Ubuntu package
for 580.173.02 was downloaded, SHA256-verified and extracted without installing
or changing system libraries. Its libraries reside under the worker checkout's
`.nvidia-runtime/580.173.02`. `worker.nvidia_runtime` selects only a staged
runtime matching the loaded kernel and adds its library directory to new MATLAB
children. A real R2024b `gpuDevice`/`gpuArray`/`gather` test passed. The extractor's
unconditional GPU use is now also accounted for in admission; it is not labeled
as deep learning.

Only idle Linux pollers `@1,@2,@5,@6` were reloaded, with fresh fingerprint
`558d898f9a97`. Occupied `@3,@4` were preserved. The existing user activation
timer's manifest was updated to this fingerprint and the timer recreated; it
will reload those pollers only once idle with an empty child tree. This does
not repair driver libraries already loaded in an active MATLAB/Python process.
The other two jobs currently advance in CPU extraction and may still be exposed
when they first use CUDA. No GPU reset or server reboot was performed.

Successor `e28d8d5e-3601-4c8d-8d69-ae67fe4c8974` was submitted with the corrected
runtime and an explicit 30-GiB recovery budget; the hourly monitor now follows
this successor. Windows was checked but has only about 32 GiB physical RAM and
14 GiB available, so it cannot safely take this 30/48-GiB request. The system
`nvidia-smi` still needs the matching library path until a controlled maintenance
reboot synchronizes the installed and loaded drivers.

At 15:53:44 UTC, the successor had completed drift for frames 1-101 of the
first FOV and entered ROI cropping, with empty stderr. Its actual MATLAB PID
409923 used 3646 MiB of GPU memory. The new plan admitted two readers with
20.54 GiB usable RAM, proving both previous failure points were passed.

The GPU-admission correction was also deployed as a single-file change on the
idle native Windows worker, preserving its host adaptations. Its new poller PID
is 27664, fingerprint `5ca30c1b891c`; 15 Windows tests passed. The database tunnel
and the three guest MATLAB sessions were preserved, and admission was restored.
Evidence is copied into `reports/alex-memory-recovery-20261020/windows/`.

## Windows worker recovery, 2026-10-20

The Windows poller was absent, with its last heartbeat at 13:21:16 UTC.
No Python/MATLAB process matched the abandoned Hub job
`f4150ff1-f1bf-4c89-8483-1b7e26c8726e`; its stdout also stopped advancing.
The remaining three MATLAB processes belong to the separate `guest` interactive
session. Their PIDs, creation times and advancing CPU times were preserved.
The old Hub job was explicitly reconciled as failed, with recovery provenance,
logs and existing outputs retained; it was not requeued automatically.

Task Scheduler event 329 confirms that the worker's scheduled task had exceeded
its default `PT72H` limit on October 19 at 09:48 Paris time. The worker subsequently
continued heartbeating until October 20 at 15:21 Paris time, so this event alone
does not establish the exact cause of the later Python process disappearance.
The task limit is now explicitly `PT0S` (unlimited), preserving its principal,
action and other settings. The registration helper was corrected locally and on
the Windows host so future registrations also set an unlimited duration.

Under a brief admission drain, the previously verified merged worker update was
activated with no existing managed worker/job process to stop. The scheduled
task was started in its existing `Charvin-Admin` interactive context. New poller
PID `45124` advertises fingerprint `a1fffd69452e`, with advancing fresh heartbeats.
Admission was restored; its force-stop activation watcher subsequently removed
itself after successful verification. The database tunnel PID `21408` and Linux
analysis poller PID `2571587` were preserved. No API or Linux service was updated.

After recovery the previously queued Windows job
`68f5bb54-f98d-4975-a84f-1515be8988f2` was dispatched to MATLAB and failed pipeline
validation because its linked HOCT classifier is not runnable in server mode.
The worker handled that job failure and returned to idle; repairing the scientific
pipeline dependency is separate work. Evidence and task/source backups:
`reports/windows-worker-recovery-20261020/` and host-local
`.deploy-backups/windows-worker-recovery-20261020/`.

## Alexander force-stop demonstration, 2026-10-20

At the user's request, legacy job `c0fc44c4-4380-4c48-8f9d-8ad2b3286e6e`
was stopped by validating and terminating only its managed compute children.
Its worker finalized cancellation and released the project locks. Linux poller
`@1` was then reloaded only after becoming idle with an empty child tree;
fresh PID `388226` advertises fingerprint `3327f21659f7`.

Private empty demo project `43eea751-5d1b-4dde-bb57-e6468843f04e` belongs to
`alexander`. Its one-core, 4096-MiB job
`2718583f-01f9-4f70-a0e4-926fb72a23ad`, run
`alex_force_stop_demo_20261020`, used the existing pinned MATLAB release and
processed no experimental data. Alexander invoked **Force stop** himself at
13:51:26 UTC. Process exit was confirmed after 0.615 seconds: MATLAB and both
Python descendants exited, and worker PID `388226` remained alive and idle.

The other Linux analysis retains PID `2571587`, its original allocation,
parameters and start time, with an advancing heartbeat. Windows job metadata
is unchanged, but its heartbeat was already stale at the initial snapshot
(13:21:16 UTC), and registered worker PID `31820` was absent during final
read-only host inspection. Its registry therefore does not establish current
process health. No Windows process was modified during this operation; the
Windows condition requires separate diagnosis. Admission was restored on both
targets. Evidence is in `reports/alex-force-stop-demo-20261020/`.

## Job force-stop rollout, 2026-10-20

The central database has the four nullable job force-stop audit/control columns
from `db/migrations/20261020_job_force_stop.sql`. The rebuilt API on
`webserver-labo` is healthy with fingerprint `4246616565fb`. The Run Monitor
offers **Force stop**, with confirmation, authenticated actor audit, and explicit
process-exit confirmation. Interface messages, including the Assistant and
restore hints, are in English. Project/Assistant script versions were bumped
to avoid stale proxy/browser assets. Remote-specific API changes were preserved.

Linux pollers `@2,@3,@5,@6` were reloaded only after fresh idle registry entries
and empty process trees, under the target admission lock. Their active fingerprint
is `3327f21659f7`. Busy `@1` (PID 227710, cooperative cancellation pending) and
`@4` (PID 2571587) were preserved. The user timer
`detecdiv-force-stop-activation-20261020.timer` checks every 30 seconds and reloads
each remaining poller only once idle, then stops after fresh fingerprint checks.

The native Windows poller remains busy on PID 31820, fingerprint `957aa209126d`.
Its merged update is staged and verified under
`.deploy-staging/job-force-stop-20261020`, intended fingerprint `a1fffd69452e`.
The SYSTEM task **DetecDiv Hub Force Stop Activation 20261020** checks each minute
and activates only after no active target jobs and an empty managed child tree;
it unregisters after fresh heartbeat/fingerprint verification. The database
tunnel and unrelated MATLAB sessions are preserved. Both new watchers supersede
the preceding ROI-memory activation watchers; the staged Windows update includes
their pending memory changes and preserves its host-specific adaptations.

Two empty-project synthetic jobs exercised real MATLAB execution on Linux,
with two Python descendants including a detached child. API test
`897ae1c1-9788-4a1a-840b-fa614ba2ee07` confirmed termination in 0.95 seconds.
The user accepted the browser confirmation for
`d20d4fce-bf96-415b-8b6e-fa47eb802bee`; termination was confirmed in 1.01 seconds.
Both jobs are `cancelled`, logs remain attached, all synthetic processes exited,
project locks were released, and their worker PIDs remain alive and idle.
The three pre-existing scientific jobs retained their parameter hashes,
allocations, start times and worker PIDs, with advancing heartbeats. No MATLAB
release or processing plugin was changed. Temporary ops sessions were revoked.

Remote staged tests: 32 passed on each compute host, excluding one pre-existing
unrelated cancellation-actor test whose older deployed route differs from the
local version. Native Windows containment was tested in the staged runtime;
its occupied production poller has not yet run a new job with this code.
Evidence, manifests, rollback copies and the browser screenshot are recorded in
`reports/job-force-stop-deployment-20261020/` and host-local
`.deploy-backups/job-force-stop-20261020/`. Prepared busy pollers remain pending
activation; files on disk do not establish activation of imported modules.

## Automatic ROI extraction memory rollout, 2026-10-19

The Linux worker tree now includes automatic admission for runs selecting only
the standard streaming `roiExtract.process`: preferred 49152 MiB, conservative
minimum 24576 MiB (or a higher configured pipeline profile), smaller admitted
budgets when shared capacity is occupied, and queuing below the minimum.
The existing MATLAB block/FOV planners consume the job's applied service limits.
Explicit requests and mixed/legacy/non-dynamic runs keep fixed budgets.

Five idle Linux pollers (`@1,@2,@3,@5,@6`) and the independent worker manager
were reloaded. Fresh poller fingerprint is `c367c1f32f1e`. Worker `@4` continues
its existing analysis with PID 2571587 and fingerprint `c0f13002d996`; it must
not be interrupted. A user systemd timer
`detecdiv-roi-memory-activation-20261019.timer` checks every 30 seconds and
reloads it only after a fresh idle heartbeat and an empty child-process tree.
It stops itself when all poller fingerprints match.

Windows remains on active fingerprint `957aa209126d` while its analysis runs
(PID 31820). The merged update is prepared and verified under
`.deploy-staging/roi-auto-memory-20261019` in its Hub checkout; intended
fingerprint is `89081645c071`. Host-specific claim compatibility, diagnostics,
target assignment and GPU/path adaptations are preserved. Automatic memory
remains disabled on Windows, retaining its current 24576-MiB pipeline default.
The SYSTEM task `DetecDiv Hub ROI Memory Activation 20261019` checks each
minute, activates only after the target has no active jobs and the managed
Python tree has no children, verifies fresh fingerprints on subsequent checks,
and unregisters itself after completion. The database tunnel and unrelated
MATLAB sessions are preserved.

Both activation paths briefly reserve admission under the target-row lock and
restore its preceding drain state. Original worker files are retained under
`.deploy-backups/roi-auto-memory-20261019`. No API rebuild, database migration,
MATLAB release change or scientific job submission was needed.

Verification: 65 targeted tests passed in isolated runtime copies on each
compute host, and 83 local Python tests passed. A read-only production-policy
probe confirmed Linux allocations of 48 and 30 GiB, and queuing with less than
24 GiB remaining. The Windows staged policy retained fixed 24-GiB requests.
Both active jobs kept their PIDs, parameter hashes, allocations and start times,
with advancing heartbeats. Evidence and activation helpers:
`reports/roi-auto-memory-deployment-20261019/`. Busy-poller activation remains
pending until those analyses finish; prepared files alone are not activation.

## Native Windows GPU activation, 2026-10-16

`windows-10-20-11-56` now declares `supports_gpu=true` for its RTX 2000 Ada
(16380 MiB). Its target-specific admission budget is 14336 MiB, preserving
display headroom; one concurrent job remains the limit. Windows poller
fingerprint `957aa209126d` includes that budget and external classifier package
path mapping, preserving its existing local adaptations.

The isolated phase CellposeSAM runtime is
`C:/ProgramData/DetecDivHub/cellpose-gpu-v1/Scripts/python.exe`, selected by
`DETECDIV_PHASE_CELLPOSE_PYTHON`. Validation passed in `GMGM\Charvin-Admin`'s
actual worker session: MATLAB GPU detection, Python CUDA forward/backward,
CellposeSAM inference with pinned weights, 100 input fingerprints and shared
output access. Temporary installation/validation tasks are removed after use.

The existing pilot job `43637ab4-4044-443d-b8f9-3372935f8356` was reserved on
Windows while drained; no duplicate job was created. Its first real attempt
stopped before training because client path specificity overrode trusted worker
mapping. DetecDiv `594217dd37bfa9b2b529f7f2204253d9901340bf` corrects that
priority, is prepared on both hosts, and is the published default. Running jobs
keep their earlier pins. The pilot uses a versioned Windows launcher (HOCT
`45f68ffc19e6faee2d727506a6bd4fa3b41f3a82`) with unchanged scientific protocol
and Python pilot hashes. See `reports/windows-gpu-activation-20261016/` and
`docs/windows_worker.md` for evidence and environment details.

After the complete dry run passed in the worker session, the same job was
requeued at 07:57 UTC with its first failure retained in `attempt_history`.
The actual GPU training started at 07:58 UTC and completed its first epoch
without error. At final verification the RTX reported 87% utilization and
9847 MiB total VRAM use; exactly one pilot launch was present. The original
scientific run request and frozen protocol hash are unchanged. Summary:
`reports/windows-gpu-activation-20261016.json`.

## Alex fresh-ROI extraction validation, 2026-10-15

DetecDiv `dce3ca768bcae597224424620abc46ede0788a12` is the published
MATLAB default, prepared and verified on both compute hosts. DetecDiv-plugins
`2b13ecb7f43085f2b0e211e95dd1d109ec5c0e0e` updates the extraction adapter
to 0.1.8. Its explicit `imagesToRoi` contract lets semantic validation and typed
resource binding project the raw channels through a processor extraction node.
The standard ROI extractor retains its existing-project inventory policy.

Alex's new `anof_071026_X54445_LOGEB_2` project exposes six raw source channels
but newly identified ROIs still advertise only `Channel 1`. Previously the
combined extraction/classification dry run rejected this provisional inventory.
Earlier first-project runs already exposed six ROI channels at validation time,
masking this missing transfer declaration. The analysis still requires six
channels; this fix changes no image processing, pool sizing, block size or saves.

Both of Alex's clean core/plugin checkouts under
`X:/Alexander/code/DetecDiv_2` and the shared extraction package are updated;
an immutable plugin release is retained under pipeline `assets/releases/<SHA>`.
All six Linux workers and native Windows use the prepared default/shared package
for new jobs. Existing jobs and pipeline definitions were preserved. Three new
core tests, four Alex pipeline tests and 29 existing contract tests passed on
local Windows R2026a, Linux R2024b and native Windows R2025b. The exact new
project passed dry-run validation. Windows account `GMGM\Charvin-Admin`
verified the shared package version and changed file hashes through a temporary
verification task, subsequently removed. Evidence:
`reports/alex-channel-contract-deployment-20261015.json` and
`reports/alex-channel-contract-validation-20261015.json`.

## Alex extraction read pool, 2026-10-15

DetecDiv-plugins `700c95ac2bbabf4c0e04e4afd2ff81bcb5cdbc29`
(`saveCroppedImagesXConservative` 0.1.7) is published on GitHub and GitLab.
The extraction adapter explicitly opens a process pool before the historical
frame-reading `parfor`, independently of `Pool.AutoCreate`. CPU admission uses
the job environment, matching Hub attempt allocation and MATLAB thread ceiling;
RAM admission reserves the raw
block/crop/drift buffers and each process's baseline and serialized image cache.
Workers use one compute thread each. An owned pool closes on success or failure;
an incompatible caller-owned pool is preserved and reported. The historical
script, fixed `cut=88`, scientific calculations and universal `roi.save` are
unchanged. For Alex's actual 2048-by-2048 six-channel inputs, the 24-GiB budget
admits two read workers within the eight-CPU ceiling.

The exact plugin SHA is prepared in detached worktrees on both compute hosts:
Linux `/home/charvin-admin/repos/DetecDiv-plugins-releases/<SHA>` and Windows
`C:/Users/Charvin-Admin/Documents/GitHub/DetecDiv-plugins-releases/<SHA>`.
The shared extraction package under
`/data/matlab/pipelines/Alex_Xtra_FullChannels_Conservative/assets/plugins/processor`
is updated for all six Linux workers and native Windows; an immutable copy is
under that pipeline's `assets/releases/<SHA>/plugins/processor`. Alex's clean
`X:/Alexander/code/DetecDiv_2/DetecDiv-plugins` checkout was fast-forwarded to
the same SHA. The live classification package, pipeline definition and active
job payload were verified unchanged. No global core release or worker service
was changed, and no analysis was restarted.

Six read-pool tests passed on Linux R2024b, local Windows R2026a and native
Windows R2025b, including real process workers, exact multi-block TIFF crops,
CPU/RAM limits (including a job smaller than the Windows worker capacity),
existing-pool preservation and failure cleanup. Eleven existing
ROI persistence tests passed on Linux and local Windows. Six actual frames
(1, 88, 89, 176, 440, 517), across all six channels, were read on two processes
and compared pixel-for-pixel with sequential reads. In the actual Windows
worker account `GMGM\Charvin-Admin`, a temporary verification task loaded the
shared UNC package, verified all 18 file hashes and started/closed two workers;
the task was removed afterwards. Evidence:
`reports/alex-readpool-deployment-20261015.json`.

## Windows Cellpose scratch paths, 2026-10-15

DetecDiv release
`79a3166c1c1826709b36c33c194c51623d399833` is now the published default on
both MATLAB targets. On Windows, Cellpose moves only its per-ROI temporary
files to a short, unique temporary directory when the project UNC work path
approaches the legacy Windows path limit. It first uses the job-scoped MATLAB
temporary root; if the final filename would exceed 240 characters, it tries
the normal local Windows user temporary directory (`LOCALAPPDATA/Temp`). The
selected directory is checked for write access. Scientific outputs remain in
their existing project locations. Five checks passed on native Windows R2025b
as `GMGM/Charvin-Admin` and on Linux R2024b, including MATLAB/Python JSON and
MAT round trips at 238 characters. With a 191-character job temporary root,
Windows selected a 113-character config path in the user temporary directory.
Both protected releases were verified and published; no worker restart was
needed. The independent Windows verification task was removed after completion;
desktop MATLAB sessions were preserved. Jobs already submitted retain their
existing code pins. Evidence: `reports/cellpose-short-paths-20261015.json`.

Hyper7 recovery `87f55cec-264d-4d1a-b575-4adc979d7f5c` (`h7r_1015`) was
submitted through normal Hub preflight and creation services with automatic
routing and this corrected SHA. It started on Linux at 13:04 UTC. It preserves
the failed `2f53acb2` run's FOVs 1-50, frames 1-50 and five selected analysis
nodes. Three metrics parameters changed in the mutable template since that run;
explicit overrides restore the saved source values. Evidence:
`reports/abhilasha-hyper7-shortpath-retry-20261015.json`.

## Cellpose failed-inference metadata rollback, 2026-10-15

Release `6c86c83cda16a7accaa2e4a5d20cd74507e52286` was published earlier,
prepared and verified in protected
release directories on Linux and native Windows. This extends `07422ee8` by
restoring the ROI image cache, logical channel mapping, and display metadata
when Cellpose inference raises an error or cancellation. The Hub can then save
the project after a node failure without exporting newly prepared but
uncommitted Cellpose output channels. Existing masks and prior completed ROIs
are preserved.

The source was reproduced with a first inference that creates a blank output
plane before failing: without rollback, the normal light-project exporter
records its name although the HDF5 store still contains only raw input. This
matches Windows attempt `f9d102cc-0011-44ac-925d-2235c82078ed`, submitted on
October 13 and failed on October 15 at 05:51 CEST after Python could not open
its configuration path. Both the blank-output creation and the subsequent
project metadata save are visible in the execution path/logs. Output
preallocation dates to February; September's output-cache reload change later
made the metadata-only result fatal on resume.

Eleven related tests passed on Windows R2026a, including two new regressions
that failed before the fix. Both new regressions also passed on Linux R2024b.
No workers were restarted or API images rebuilt. Already submitted/running
jobs keep their existing code pins. Evidence:
`reports/abhilasha-ghost-source-20261015.json` and
`reports/abhilasha-ghost-origin-job-20261015.json`.

## Cellpose metadata-only output fix, 2026-10-15

MATLAB release `07422ee84108dbb26f670520adcb5888348ce04d` was published earlier
today (previously `bd1d64ec03c6e91d972dd609e45780964e0d6203`). Its protected release was
prepared and verified on Linux and native Windows with
`scripts/deploy_detecdiv_release.ps1`. Shared checkouts and active job pins were
preserved; no API rebuild or worker restart was needed.

CellposeSAM now materializes an output plane when project/display metadata
advertises it but the readable HDF5 store has no corresponding dataset. It
reuses the logical channel identity, preserves existing on-disk masks for
partial reruns, and still propagates unreadable/corrupt-store errors. Four
output-channel regression tests passed on Linux R2024b; nine related tests
passed locally on Windows R2026a. Hub pilot
`2ec1cd2d-1b51-49a5-bb66-3d3dbe26f0f1` completed successfully on the actual
first Hyper7 ROI from failed job `e9dd1130-6815-4c75-bbba-55344c2301d9`:
50/50 frames contain masks; the HDF5 store retains its six original datasets
alongside the new segmentation dataset. Full Hyper7 recovery is
`2f53acb2-370c-4683-bc20-721ddcf115f1`, pinned to the corrected SHA.

The separate Diamide failure `00c9a5ef-2cdb-4bb9-ace3-6710a7288878` coincided
with local disk exhaustion (another MATLAB job logged ENOSPC at the same
minute). Subsequent disk cleanup restored approximately 79 GiB free (later
cleanup increased this to approximately 207 GiB). Its 40
completed FOVs remain present; FOVs 45–46 are partial and 47–50 are pending.
After Abhilasha closed the project and its client edit lease cleared, recovery
`bd3c852e-8728-4d65-a646-d5472cd1cf48` was submitted through the normal Hub
service and started. It skips existing extraction outputs, reuses the saved
ROI detection, and continues downstream processing. No client locks were
overridden. Evidence: `reports/abhilasha-recovery-live-20261015.json` and
`reports/abhilasha-failures-20261015.json`.

## Alex worker fixes: fleet activation completed, 2026-10-15

Hub fixes `538aa238` (MATLAB cancellation even with exit code zero, cancellation
actor audit) and `4c714c64` (explicit GPU inference by external processors) are
published. The API cancellation route is active on `webserver-labo`, with healthy
API/database and fingerprint `c4a3fc462f1f`. All six Linux pollers `@1` through
`@6` now report runtime fingerprint `c0f13002d996`. `@2` activated at the
2026-10-14 21:28 UTC check and `@6` at the 2026-10-15 03:58 UTC check, after their
analyses finished. Native Windows is also active with verified fingerprint
`8c31b3db2d99` (interpreter PID 53756, venv launcher PID 44188). Its previous
job finished before activation. Its raw-ingestion adaptation and other local
changes were preserved; staging passed 11 checks in its actual Python runtime.
The final administration regression suite passed 19 tests, including real
PowerShell guards for orphan pollers and active MATLAB child processes.

`scripts/finish_alex_worker_fix_rollout.py` reported `complete: true` after Windows
activation. It checks every baseline worker and additional
registered instances, reserves admission only during an idle reload, checks
actual child processes, validates source hashes, reloads the specific poller or
Windows scheduled worker task, confirms the new heartbeat fingerprint, and
restores admission. Admission lock waits are bounded at one second (SQL operations
at ten seconds), avoiding both indefinite waits and repeated NOWAIT contention.
On Windows the task launcher can leave Python children alive; activation checks
all managed Python process trees for active children, stops only verified idle
pollers, and starts one scheduled worker. The actual deployed hashes are:

- Linux executor: `0464da6627d90d99a8cf1ccec89da9243254e0a08d32be205d13e5514c75d146`
- Windows executor: `ba6fa9beb28c80ff48d9d431d5654aa74299c9f8666ddf7df903c7906e227e95`
- Resource module on both hosts: `6450a97cf1ae178e6a1a36b665807ddee10773b9fbd97ae98aec51d4f4862ef5`

The worker task and database tunnel are both running; exactly one Windows poller
remains (its venv launcher plus interpreter). Admission was restored on both
targets and no rollout marker remains. No active analysis or MATLAB release was
interrupted or changed. Evidence: `reports/alex-worker-fix-rollout-20261015.json`.
Windows staging and backups live under its Hub checkout in
`.deploy-staging/alex-terminal-gpu-20261014` and
`.deploy-backups/alex-terminal-gpu-20261014`. The Alex heartbeat is disabled after
this verified fleet completion; the 517-frame scientific run remains complete.

This file is the source of truth for the current live deployment target and
runtime topology for this repository.

Use the `detecdiv-hub-ops` skill for the operational sequence:
restart commands, verification steps, and deployment classification.
This file should describe what is live; the skill should describe how to act on
it.

## Stable Deployment Target

As of 2026-04-26, the primary DetecDiv Hub deployment is the VM:

- VM: `webserver-labo`
- Internal URL: `http://detecdiv-hub.detecdiv.internal/`
- Web UI: `http://detecdiv-hub.detecdiv.internal/web/`
- Health check: `http://detecdiv-hub.detecdiv.internal/health`
- VM address on the libvirt network: `192.168.122.185`

For installation details and remote worker configuration, see
`docs/deployment_installation.md`.

The old API process on `detecdiv-server` may still exist for rollback/history,
but it is no longer the deployment target to assume by default.

## Runtime Split

The production-like split is:

- `webserver-labo`
  - PostgreSQL, in Docker Compose
  - FastAPI app, in Docker Compose
  - static web UI served by the FastAPI app
  - read-only `/data` bridge mounted at boot from `detecdiv-server` for
    project ops browsing and preview serving
- `detecdiv-server` / `GC-CALCUL-306`
  - compute worker only
  - MATLAB access
  - direct visibility of server project/dataset storage
- `CG-PCDELL01-306` / `10.20.11.56`
  - supplemental native Windows worker, one job at a time
  - local MATLAB R2025b and Python environments
  - accesses shared jobs only when they are unassigned; jobs explicitly
    targeted at `detecdiv-server` stay there

The VM should not be assumed to have direct access to project or dataset file
storage. Any server-side filesystem work, including project-root indexing, must
run through the worker on `detecdiv-server`.

## Current Services

On `webserver-labo`, the Compose deployment lives at:

```bash
/home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
```

Useful checks:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
docker-compose ps
curl -sS http://127.0.0.1:8000/health
```

On `detecdiv-server`, the workers currently run from:

```bash
/home/charvin-admin/repos/detecdiv-hub-webvm
```

The active worker services are:

```bash
systemctl list-units 'detecdiv-worker@*.service' --all --no-pager
systemctl status detecdiv-worker@1.service detecdiv-worker@2.service detecdiv-worker@3.service --no-pager
```

The systemd override that points the worker to the VM database is:

```bash
/etc/systemd/system/detecdiv-worker@.service.d/10-webvm-db.conf
```

Since 2026-09-28 the compute pool uses `detecdiv-workers.slice` with a shared
96 GiB RAM, 6 GiB swap, and 36 CPU-core budget. The remaining approximately
29 GiB physical RAM and 2 GiB swap stay outside the pool for the 16 GiB Hub VM
and host services. Three workers receive 32 GiB RAM, 2 GiB swap, and 12 CPU cores
each was the initial static arrangement, now superseded by per-job sizing.
Idle workers have 1 CPU / 512 MiB RAM. Their quotas grow to the admitted job's
CPU/RAM request and proportional swap share, then return to idle limits.
The manager keeps six workers ready by default and grows the pool automatically
from admissible queued resource demand, up to 36 instances. It shrinks after
60 seconds of stable lower demand without stopping active jobs. Changing the
baseline changes ready slots, not job sizes. Shared budgets
remain persisted in `/etc/systemd/system/detecdiv-worker-resources.conf`.

`detecdiv-worker-manager.service` runs on the compute host and automatically
adds/removes idle instances under the same admission lock as resource claims.
The admin worker-count control sets the ready-worker baseline (default six).
Explicit manual mode retains the drain-before-reconfiguration workflow. Its database
drop-in inherits only environment directives from the worker override, never
the worker's ExecStart. It must remain outside the worker pool slice so scaling
does not stop the manager itself. The former global `max_concurrent_jobs=1`
emergency setting has been cleared; worker slots and resource budgets now bound
concurrency. See `docs/worker_resource_scheduling.md` for limits and verification.

The boot-time VM orchestration service now lives in the adjacent `Webserver`
repository because it belongs to the VM host layer, not the hub control plane.

## Windows MATLAB Worker Deployment Policy

### MATLAB code isolation rollout (2026-09-29, API and Windows active)

Hub commits `6afec5b` and `eee5902` add per-job detached Git worktrees,
submission-time SHA pins, and queued-job routing hardening. Both are pushed to
GitHub and GitLab. API image `3ca3a9abcf8c` is now running on `webserver-labo`;
health and authenticated release retrieval succeed. API fingerprint:
`0d93ee7f38ba`. Original files are backed up under
`.deploy-backups/codeversions-6afec5b` in the operational tree.

The published default release is
`e33264d90f671fed4d27fe5d7c77171d95a5e9c4`. Preparation worktrees at that SHA
were verified on both hosts. Linux shared source HEAD remains
`36bfde6c9bcbc413ceead6cf723f5f6aa10d3b27`; Windows source HEAD equals the
published SHA. Their HEADs need not match for isolated jobs.

Windows launcher files are installed, preserving the trusted path-mapping
patch. Its poller was restarted while admission was reserved, with no active
Windows job or MATLAB process. Fingerprint: `d99cba4190bf`. Windows now has
`matlab_code_isolation_ready=true`, and admission has been restored. The active
Cellpose training debug task was informed of the rollout and publication API.

Linux launcher files are installed. Pollers `@2`, `@4`, `@5`, and `@6` were
reloaded idle at 16:05 CEST; `@1` reloaded after its job finished. Fresh pollers
report `b005cc07895f`. Old `@3` (PID 150213) continues job
`a4bdf595-28be-4e2b-b138-91b243a7bac2` unchanged. Linux readiness remains unset
until this final poller reloads. Versioned jobs can already run on Windows;
explicit Linux requests are refused until Linux is ready.

The one-shot helper `scripts/finish_matlab_code_activation.py` runs on the
administration workstation (initial PID 51188). It waits for old `@3` to become
idle, reserves admission during its reload, then marks Linux ready only after
all six baseline pollers report the expected fingerprint. Logs:
`C:\Users\Gilles\AppData\Local\Temp\detecdiv-codeversions-finish.log` and the
adjacent `.err` file. Keep the workstation running until completion. Inspect
live heartbeat state before changing readiness manually.

Future processing releases are prepared in separate worktrees on both hosts
and published through `PUT /matlab-code/release`. No shared-checkout pull or
worker restart is needed for a release change. Legacy targets without isolation
retain the idle-only policy. See `docs/matlab_code_versions.md` for procedures.

### Shared MATLAB release cache (2026-09-29)

Worker changes `b574d69`, `f7126ca` (executor file only), and `a1295ad` are
installed on both compute hosts. Each SHA now has one protected checkout at
`DetecDiv-jobs/releases/<SHA>`. Job attempts use persistent directories at
`DetecDiv-jobs/work/<job-id>/<attempt-id>`, including logs and private temporary
files. Only the four launcher files were deployed; unrelated storage changes
that landed in the same Git commit were not part of this rollout.

Windows is active with fingerprint `463c764ab270` and admission restored.
Repeated preparation returned the same release path successfully. NTFS uses
a deny-write/delete ACL created through .NET without the SYNCHRONIZE denial
introduced by icacls; Linux files have mode 444 and directories 555. Linux
cache reuse took approximately 1.7 seconds including Python startup and Git
validation. The default release remains `e33264d90f671fed4d27fe5d7c77171d95a5e9c4`.

Five idle Linux pollers now report `7389e458c980`. Old `@3` still owns its
original running job and was not signalled. The updated one-shot activation
helper (initial local PID 19028) waits for it, retries interrupted SSH access,
and records its admission reservation for recovery. Logs are now
`C:\Users\Gilles\AppData\Local\Temp\detecdiv-shared-release-final.log`
and the adjacent `.err` file. The old helpers have stopped. Keep the
administration workstation running until the final readiness message.

Code-cache GC runs during job preparation with a 30-day retention window.
Published/default versions, queued/active jobs, failed/cancelled resumable jobs,
recent completed jobs, and locally modified code are retained. Historical
per-job worktrees are preserved until eligible; job logs are not automatically
deleted. No scientific job was launched for this deployment.

### MATLAB temporary-path correction (2026-09-30)

The Linux rerun `9b85ac9a-39a3-4197-a284-44344f435799` used DetecDiv release
`6c6e414a` but exited before writing any MATLAB output. An isolated
`matlab -batch "disp(version)"` reproduced exit 1 with the long attempt-local
`TMPDIR`; the same command succeeded with a short symlink to that directory.
Hub commit `37fe12c` makes Linux MATLAB jobs use a private
`/tmp/dd-matlab/<attempt-id>` alias while retaining temporary files in the
persistent attempt directory. The alias is removed when the job ends.

The changed Hub launcher file was copied to both worker hosts. All six Linux
pollers were restarted while idle and report fingerprint `68dc9b448e76`;
the manager stayed active. The Windows copy was synchronized, but its running
poller was not restarted because the new branch applies only on Linux. The
published DetecDiv release is `6c6e414a4a3b52a905f3aab7639cd0d7a935f7d5`.
The dose pipeline was resubmitted as job
`1535b10c-cc50-4301-a2d2-cbeff504f2f6`; by 07:34 CEST it had passed
MATLAB startup and entered the data loader. Its final result remains pending.

### ROI extraction memory follow-up (2026-09-30)

The dose job above and pulse job `ed821a11-0b68-42c1-b58a-16f647c5a638`
subsequently failed during `roiextract_3` with an apparent 0.8 GiB headroom
below `memory.high`. Both were pinned to the older `6c6e414a` DetecDiv release.
The raw frames are 2048 x 2048, five uint16 channels (40 MiB per frame), and
the extractor caps ordinary raw blocks at 512 MiB. Live cgroup measurements
showed that clean file cache from raw reads and HDF5 writes occupied many GiB.

DetecDiv `4ab5613d` excludes clean active and inactive file cache from cgroup
usage, while retaining shmem, dirty, and writeback pages. DetecDiv
`cc6bce4f64bac7be343ceda23bdde961bf27a5f6` also permits a single frame
above the 512 MiB ordinary block cap when the real memory budget allows it.
MATLAB memory-planner tests passed. The latter SHA was prepared on both Linux
and Windows workers and published as the default release; no polling worker
restart was needed. It also includes `fe2a1535`, which preserves submitted FOV
selections and deduplicates lightweight project imports.

At 14:26 CEST, the replacement Linux jobs were running with 24 GiB each:
`fd8c8501-1480-44c7-949e-aca993263580` for dose (FOV 41-50, entered ROI
extraction) and `3ae55b05-88df-4e4c-864e-6ef90022a3f4` for pulse (raw ingest
and dependency preflight complete). Their final results remain pending. The
thread heartbeat `surveiller-reprises-abhilasha-roi` checks them every 10 minutes.

The Windows worker is an additional queue consumer, not a replacement for the
Linux storage-visible workers. Keep its Hub checkout at
`C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub` and its MATLAB
processing checkout at
`C:\Users\Charvin-Admin\Documents\GitHub\DetecDiv`. The Linux worker's
DetecDiv checkout is configured through `DETECDIV_HUB_MATLAB_REPO_ROOT` (the
current deployment default is `/home/charvin-admin/repos/DetecDiv`). These are
separate repositories with separate purposes; do not copy MATLAB project
internals into the Hub repository.

For a processing release, prepare and verify the same SHA in the release cache
on both hosts, then publish it through `PUT /matlab-code/release`. Source HEADs
may differ, and running jobs retain their pinned release. Do not modify/remove
code directories used by a job. Legacy targets without isolation still require
idle, clean, fast-forward-only synchronization of their source checkouts.
The Hub worker code itself follows its own deployment revision and procedure;
reload polling workers only when idle.

From the administration workstation, run
`.\scripts\deploy_detecdiv_release.ps1 <full-sha>` for the DetecDiv release
sequence on the current Linux and Windows workers. Its host paths and remotes are in
`ops/detecdiv_release_targets.json`; see `docs/matlab_code_versions.md`.

The Windows MATLAB license was reported renewed on 2026-09-28; the unattended
`matlab.exe -batch "disp(version)"` check succeeded under
`GMGM\Charvin-Admin`. The Windows worker `.env` was updated on 2026-09-29 to
claim unassigned jobs of all kinds, including `pipeline_run`, `legacy_matlab`,
and `archive_raw_dataset`; `restore_raw_dataset` remains excluded. Its readiness
check reports three path mappings: `/data`, legacy `X:\`, and `/archive`.
Raw-data ingestion remains on the Linux storage-visible worker.

The Windows archive share is `\\10.20.11.251\archive`, mounted as `Y:` at its
share root (`Y:\`). Read and write/delete probes succeeded in the RDP
interactive session through both `Y:\` and the UNC path. SSH and RDP have
different Windows logon IDs; the SSH key session had no `Y:` mapping and could
not use that SMB connection. The worker task runs in the signed-in interactive
session. The `.env` update does not alter a worker that is already running.
At the 2026-09-29 check, one Abhilasha `pipeline_run` was active on Windows with
a fresh heartbeat, so the worker was left running until the job finishes; no
archive jobs were queued at that snapshot. Check the queue and active jobs
before restarting. Once enabled, an unassigned archive job with
`mark_archived=true` can delete its source after a successful copy. Keep restore
excluded until a separate restore test.

Incoming SSH on the Windows PC currently accepts the dedicated public key,
but command and SFTP sessions fail before shell startup. Server DEBUG3 logs show
`LsaLogonUser()` failing to create an S4U token for `GMGM\Charvin-Admin`
(`0xC00000BB`). Build `26200.9457` maps to Windows 11 25H2 KB5129195; an open
Win32-OpenSSH report describes the same S4U failure after KB5074109. Treat the
update link as a strong lead, not a confirmed root cause. Keep RDP as the
administration path. The same key opened an interactive SSH shell as local
account `detecdiv-ops`; `whoami /groups` confirmed enabled local Administrators
membership and high integrity, so this is a working elevated SSH path. The
domain secure channel also tests healthy. Focus remaining diagnosis on S4U and
AD group-read prerequisites. Do not rotate the SSH key, edit its ACL, or remove
Windows updates to address this server-side token failure. See
[`docs/windows_worker_ssh_strategy.md`](docs/windows_worker_ssh_strategy.md).

Follow-up inspection over the working local-admin SSH account confirmed that
`sshd` runs automatically as `LocalSystem`, using OpenSSH 9.5p2. Domain
controller discovery and clock synchronization to
`srv-data-com01.gmgm.lab` (`10.20.1.150`) succeed. The ActiveDirectory
PowerShell module is not installed, so `Charvin-Admin` account flags and AD
group memberships remain unverified. A read-only
`net user Charvin-Admin /domain` query from the local-admin SSH account returned system error 5
(Access denied), so a domain-authorized identity is needed for that inspection.
`DEBUG3`/`LOCAL0` logging is still active in `sshd_config`; no remote
configuration or service changes were made during this inspection.

### Alex legacy ROI results persistence (2026-10-14)

DetecDiv `da89797e0df58971f59dea1fedd746f7b8ddc867` is published as the
default for new MATLAB jobs. The protected release directory at that full SHA
was verified on both Linux and Windows. `roi.load` restores explicitly saved
`results_<roi-id>.mat` sidecars; the universal `roi.save` is unchanged.

DetecDiv-plugins `528bde9ceb6ab4df556692ea76489653e0210881` is pushed to
GitHub and GitLab. Both conservative Alex wrappers are version 0.1.1. Their
26 files were deployed and checked in
`/data/matlab/pipelines/Alex_Xtra_FullChannels_Conservative/assets/plugins/processor`;
the shared pipeline JSON is version 1.1. Previous packages and pipeline JSON
are retained in `.deploy-backups/20261014-results-528bde9`.

Alex's source checkout at `/data/Alexander/code/DetecDiv_2/DetecDiv`
(`X:\Alexander\code\DetecDiv_2\DetecDiv`) was updated fast-forward to the
same DetecDiv SHA. Its apparent dirty state consisted only of CRLF formatting;
`core.autocrlf=true` and `core.filemode=false` preserve that Windows checkout
format on the Linux administration host. The tracked checkout is clean.

Three jobs were active before deployment, in isolated code directories. No
worker was restarted and no active job's code directory was modified.
Regression checks passed: 11 Windows integration/save tests, nine Linux
integration tests, and three additional legacy-results loader tests.

After Alex closed his client edit lease on `GC-DELL01-308`, job
`8fe920f8-c614-4df3-b5a1-b0ef5a986979` ran on Linux with the pinned release
above. Run `alex_results_fix_30frames_20261014` processed FOV 1 / ROI 1 /
frames 1:30 and finished successfully at 11:11 CEST. Its two nodes completed
without errors. A fresh MATLAB process confirmed identical persisted
`roi.results` through both `roi.load('data')` and full `roi.load`: fields
`div_1`, `RLS`, and `signal`; probability size 6 x 30; fluorescence size 3 x 30;
image size 171 x 171 x 7 x 30; channel mapping 1:7.
The report is in the run folder as `results_reload_verification.json`.
Pre-test artifacts are retained under the project's
`diagnostic_backups/20261014-results-30frames-after-close`. The temporary chat
heartbeat is paused after successful verification. No client lock was forced.

A later adapter-only update, DetecDiv-plugins
`153403b87f1f43a797069b213b8125def737ba0e`, is pushed to GitHub and GitLab.
The extraction wrapper is version 0.1.2; Xtra remains 0.1.1. The shared
pipeline is version 1.2 and passes the existing historical `cut=256` option.
The added adapter code is marked with `ADAPTATION PIPELINE 2026-10-14`.
`saveCroppedImages_X.m` and `computeDrift_X.m` were verified byte-identical to
the previously deployed persistence fix. An intermediate adaptive-memory
change was reverted before deployment. Ten integration tests pass on both
Windows and Linux. Backups are in `.deploy-backups/20261014-cut256-153403b`.

Job `75a7de1f-6630-42c2-bb5e-0e597c32565f`, run
`alex_xtra_full_all_frames_20261014`, is submitted on Linux for Alex's latest
selection: FOV 1 / ROI 1 / all 517 frames, blocks 256 + 256 + 5. It pins the
same DetecDiv release and requests 32768 MiB through the supported per-job
resource override, included atomically before submission commit. A synthetic
32-frame allocation probe on the server measured roughly two resident raw
buffers during drift-style copy-on-write; extrapolation for 256 frames gives
24 GiB for raw buffers before MATLAB and pool overhead. At verification time
the job is queued because the resource manager reports `memory_capacity`;
the two existing Linux jobs continue undisturbed. No allocation or capacity
limits were lowered to force admission. Pre-run ROI artifacts and the project
header are backed up under `diagnostic_backups/20261014-before-all-frames`.

The queued 32-GiB job above was subsequently cancelled at the user's request.
DetecDiv-plugins `a0143f09a28947692a0c30021b15f7b109a5d964` is pushed to
GitHub and GitLab and deployed into the shared Alex pipeline. Extraction
version 0.1.3 / pipeline version 1.3 fix the block size at 176 frames by default
(`floor(2*265/3)`) in the pipeline params, `setparam`, and legacy script. The
only new legacy-script behavior change is its block-size default, marked
`ADAPTATION MEMOIRE 2026-10-14`; scientific calculations and universal
`roi.save` are unchanged. Ten integration tests pass on Windows and Linux.
The prior version is retained in `.deploy-backups/20261014-cut176-a0143f0`.

The first 24-GiB replacement, `99b96d64-3812-4ee0-a2ea-1a58ed49bd5d`, run
`alex_xtra_full_all_frames_cut176_20261014`, started on Linux with
`requested_by=alexander`, as explicitly requested by Gilles. Submission is
still authenticated as Gilles, and the client context records the authorized
resubmission. The original FOV 1 / ROI 1 selection and all 517 frames are
retained, in blocks 176 + 176 + 165. The worker's actual allocation was
verified as 24576 MiB; the core release remains
`da89797e0df58971f59dea1fedd746f7b8ddc867`. Existing jobs and worker services
were left running. This attempt then failed before raw extraction because the legacy routine
counted `srclist{1}` before lazy source metadata had been hydrated.

DetecDiv-plugins `7eaf0ade17b86d743b6045b99f93ff4dbb9cf945` is pushed to
GitHub and GitLab and deployed as extraction version 0.1.4 / pipeline 1.4.
The adapter now calls `fov.readImage(1,1)` for selected FOVs with an empty raw
file list before invoking the legacy all-frame routine. The new regression
reproduces lazy Micro-Manager source discovery; eleven tests pass on Windows
and Linux. The historical default remains 176, with no further legacy-script
change. Backup: `.deploy-backups/20261014-cut176-lazy-7eaf0ad`.

Job `ad0d3610-9c71-4693-99c7-f50af0c80bf1`, run
`alex_xtra_full_all_frames_cut176_retry_20261014`, is running on Linux as
`alexander`, with an actual 24576-MiB allocation, FOV 1 / ROI 1, all frames,
and `cut=176`. The same core SHA remains pinned. Full-run completion and
all-frame results reload remain unverified while the job executes.

Alex's local installation under `X:\Alexander\code\DetecDiv_2` (Linux
`/data/Alexander/code/DetecDiv_2`) now includes the conventional sibling
`DetecDiv-plugins` checkout at
`7eaf0ade17b86d743b6045b99f93ff4dbb9cf945`, branch main. Both core and
plugin Git checkouts are clean; core remains
`da89797e0df58971f59dea1fedd746f7b8ddc867`. The core ROI results loader was
compared with the committed development copy after LF/CRLF normalization.
Fresh MATLAB sessions on Windows using the actual X: installation and on
Linux verified automatic sibling discovery, both conservative processor
entrypoints resolving inside Alex's new checkout, and `setparam.cut=176`.
No personal plugin preferences or tracked core startup files were modified.
The running server job continues to use its existing code directories.

After the 176-frame run stalled with 23.6 GiB anonymous RAM and a full
1.5-GiB swap allowance, Gilles explicitly requested cancellation and a fixed
88-frame replacement. Job `ad0d3610-9c71-4693-99c7-f50af0c80bf1` is confirmed
cancelled. The cooperative token could not interrupt the blocked legacy code;
only its verified MATLAB PID 2452179 (matching job work directory and worker
cgroup) was terminated with SIGTERM. No worker service or other job was stopped.

DetecDiv-plugins `18ce411149c9fba85c5ab6d19540dc5d535241ac` is pushed to
GitHub and GitLab, with extraction version 0.1.5. The shared pipeline version
1.5, legacy default, and setparam all use `cut=88`. The only historical-script
change since the preceding release is that fixed block size. Eleven tests
pass on Windows and Linux. Pipeline backups are retained in
`.deploy-backups/20261014-cut88-18ce411`. Alex's clean sibling plugin checkout
at `X:\Alexander\code\DetecDiv_2\DetecDiv-plugins` was updated fast-forward to
the exact SHA; fresh Windows MATLAB resolved the local module and confirmed
`cut=88`. The universal ROI save implementation is unchanged.

Job `4fa7df14-9e80-4c89-b2b0-7e1e4393186b`, run
`alex_xtra_full_all_frames_cut88_20261014`, is running on Linux under
`requested_by=alexander`, with 24576 MiB actually allocated, FOV 1 / ROI 1,
and all 517 frames (five blocks of 88 plus 77). It retains the explicit
DetecDiv pin `da89797e0df58971f59dea1fedd746f7b8ddc867`; no global core release
was republished or rolled back. Its extraction log shows active progress
through the first block. The existing heartbeat is active every 30 minutes
and now tracks this new job. Its fresh-session `verify_results_517.m` and
final report will cover all six block boundaries, classification, HDF5/MAT
raw-channel equality, and exact roi.results reload. Completion remains to
be verified by the monitor.

Final monitoring on 2026-10-14 found a status inconsistency for the 88-frame
job: Hub Job.status is `done`, but both MATLAB result and run.json are
`cancelled`. The job-specific cancel token was created at 11:02:47 UTC
(13:02:47 CEST); API access logs corroborate a POST to this exact job's cancel
endpoint. The requester identity was not inferred from the proxy address.
MATLAB honoured the cancellation after legacy extraction and returned code
0; the worker therefore marked done despite result.status=cancelled. This
worker status defect is diagnosed, not changed during monitoring.

All six extraction blocks are logged. Fresh MATLAB inspection verified the
persisted/extracted image dimensions 171 x 171 x 6 x 517 and nonempty frames
at every block boundary. Classification did not start, and roi.results.div_1
still contains the preceding 30 predictions. The full cold-reload verifier
failed its 517-prediction assertion. The run's results_reload_verification.json
records `status=incomplete` and `completeVerificationPassed=false`; do not
report the full run as successful. The heartbeat is now paused after diagnosis
and notification. No job was resubmitted and no scientific data was modified
by this final monitoring pass.

### Alex monitored recovery and terminal-status fix (2026-10-14)

API access logs confirm seven cancellation requests for the first 88-frame
job between 11:02 and 11:06 UTC. The original 30 predictions were preview
results retained because classification never ran. The old API did not retain
requester identity, so no person was inferred from the proxy address.

Hub `538aa238a9d131949774bcd7b06d0e11a14d4de7` rejects MATLAB cancelled,
failed/error or incomplete results even when the process exits with code zero;
the API records the authenticated cancellation actor. The API route was
verified and rebuilt on webserver-labo; health and DB checks passed. The worker
executor was verified on Linux and loaded by restarting only idle instances
@1,@3,@4,@5 through their Restart=always service policy. Direct remote testing
confirmed a zero-exit cancelled result raises PipelineRunCancelled. Busy
Linux @2/@6 and the Windows worker were not restarted or updated during their
ongoing analyses; Windows remains pending for this Hub runtime fix.

Hub `4c714c64413e87cfb68681a528a83e84318f92ef` reserves GPU memory for custom
processors with explicit GPU execution settings. It is deployed on Linux;
idle @3,@4,@5 reloaded it. @1 started Alex's new job before this resource-rule
activation and was preserved. Future recovery submissions explicitly declare
GPU use. Existing jobs remain undisturbed.

Alex independently resubmitted `b38095c1-ec3a-403f-8787-5588e6e030ae` from
GC-DELL01-308 at 13:32 CEST, reusing run ID
`alex_xtra_full_all_frames_cut88_20261014`. Its FOV 1 / ROI 1 / all 517-frame
selection and cut 88 match the request, so no duplicate was submitted. It pins
core `bd1d64ec03c6e91d972dd609e45780964e0d6203`, already prepared, and is
currently running on @1. At inspection its six extraction blocks had been
read and the 517-frame HDF5 channels were being saved; classification and
final results validation are still pending.

Hub `8852501e63bd8431d96c434fd29afd775d5aea3e` adds the durable campaign
CLI and recovery runbook. Forty-four focused regression checks pass. The
campaign state lives at the project's
`pipeline/monitor_recovery_20261014/campaign.json`; actual Hub inspection
through the CLI was verified. The existing 30-minute heartbeat is active and
now authorized to diagnose, test, commit/deploy and retry technical failures,
with project-lock/preflight checks, fresh stop paths and no duplicate jobs.
It records attempts and permits at most three additional automatic submissions.
Human cancellation remains explicit. It can reuse proven extraction and retry
classification only. No scientific algorithm/model or universal roi.save was
changed. The job-specific fresh-session verifier is
`verify_results_517_b38095c1.m` in the run folder, with report
`results_reload_verification_b38095c1.json` identifying the actual job and SHA.

The adopted Alex job completed both nodes successfully: extraction 1118.075
seconds and classification 51.330 seconds. A fresh MATLAB session verified
job b38095c1-ec3a-403f-8787-5588e6e030ae: 517 predictions/labels,
probabilities 6 x 517, fluorescence 3 x 517, images 171 x 171 x 7 x 517,
unique channel mapping 1:7, raw channels identical to the legacy MAT, block
boundaries nonempty, and exact roi.results reload through data-only and full
load. The passed report results_reload_verification_b38095c1.json is also the
canonical results_reload_verification.json; the previous incomplete report is
archived as results_reload_verification_4fa7df14.json. The campaign state is
complete / verification_status=passed. Its heartbeat is paused after verified
success; the recovery CLI remains installed. No technical retry was needed
after adopting Alex's concurrent submission. Now-idle @1 reloaded the pending
GPU rule. Other busy Linux and Windows workers remain undisturbed.

Fresh Windows MATLAB using Alex's actual X:/Alexander/code/DetecDiv_2/DetecDiv
installation also passed exact data-only/full roi.results reload, probability
6 x 517, image 171 x 171 x 7 x 517, and channel mapping 1:7. This consumer
verification is recorded in both passed reports and in campaign.json.

## Data State

### ROI pattern library release (2026-10-06)

DetecDiv commit `9535f6e46babd15efc439895038a8bb2bfd07e15` is pushed to
GitHub and GitLab and published as the default for new MATLAB jobs, replacing
`501775949bf659930506d2e6386ab97087574822`. The exact SHA is verified in:

- Linux: `/home/charvin-admin/repos/DetecDiv-jobs/releases/9535f6e46babd15efc439895038a8bb2bfd07e15`
- Windows: `C:\Users\Charvin-Admin\Documents\GitHub\DetecDiv-jobs\releases\9535f6e46babd15efc439895038a8bb2bfd07e15`

The desktop pipeline editor now provides a thumbnail library popup, separate
pattern creation/testing in workflow2, and explicit per-run selection. Named
presets live beside the pipeline, replacements require confirmation and a
revision check, and runs embed their own motif pixels and preset provenance.
Structured runtime overrides are preserved when saving a run. Eight library
tests and fourteen existing pattern tests passed on temporary projects; the
popup was rendered and inspected. App Designer code is synchronized. Desktop
MATLAB applications must reload to use the new UI. No worker restart was needed;
previously submitted jobs retain their pinned code version.

### ROI pattern consistency release (2026-10-05)

DetecDiv commit `759de8853eb70ce2eeb099d6ad8ad5e8374b5381` is pushed to
GitHub and GitLab, verified in protected release directories on both Linux
and Windows, and published as the default for new MATLAB jobs. It replaces
default `5a71907dca8d51af0f52180fa81a90764c3b4be5` without restarting workers.
Existing submitted jobs retain their original SHA.

This release invalidates outdated ROI pattern previews, preserves the source
patch when browsing other FOVs/frames and closing either editor, pins pattern
settings in submitted node overrides, restricts downstream ROI lists to the
selected positions, and explicitly fails empty generation attempts. The local
App Designer applications are synchronized; existing desktop MATLAB sessions
must reload the applications to use the updated editor code. Four existing
regression tests and ten new consistency tests passed, including GUI callbacks
against temporary synthetic projects. The affected scientific project was not
modified or rerun during the fix.

The PostgreSQL database from the former local `detecdiv-server` deployment was
restored into the VM PostgreSQL container on 2026-04-26.

The VM is now the main database to use for ongoing DetecDiv Hub work unless an
explicit rollback is requested.

The VM guest also mounts `/data` read-only through `detecdiv-server` so the API
container can serve raw-dataset preview MP4s and the project ops browser can
inspect storage-backed folders without logging into the storage host directly.

## Pending Live Schema Migrations

Before deploying synchronized physical locations for Labguru yeast stocks, apply:

```bash
psql "$DETECDIV_HUB_DATABASE_URL" -f db/migrations/20260911_labguru_yeast_storage.sql
```

This adds the synchronized Labguru storage tree, boxes, and physical stocks.
The same API-side synchronization that refreshes YeastStrains also refreshes
these records; compute workers remain uninvolved.

Before deploying incremental Labguru Yeast strains synchronization and
creation-date ordering, apply:

```bash
psql "$DETECDIV_HUB_DATABASE_URL" -f db/migrations/20260911_labguru_yeast_created_external_at.sql
```

This adds the Labguru-side creation timestamp and backfills it from the already
imported payloads without repeating local rows.

Before deploying the Labguru Yeast strains catalog and instant-search page,
apply the migration:

```bash
psql "$DETECDIV_HUB_DATABASE_URL" -f db/migrations/20260911_labguru_yeast_strains.sql
```

It creates the reconciled local Labguru yeast inventory used by the dedicated
`External accounts > Yeast strains` page. Synchronization runs as a short
FastAPI background task because it only needs the Labguru API and PostgreSQL;
compute workers are not involved.

Before deploying the Micro-Manager acquisition-widget position-description
changes from commit `a05b313`, apply the migration:

```bash
psql "$DETECDIV_HUB_DATABASE_URL" -f db/migrations/20260514_raw_dataset_position_description.sql
```

It adds `raw_dataset_positions.description`, used to persist per-position
annotations entered in the acquisition widget and exposed on raw dataset
details.

Before deploying worker CPU reporting, apply:

```bash
psql "$DETECDIV_HUB_DATABASE_URL" -f db/migrations/20260926_worker_cpu_usage.sql
```

This adds the per-worker CPU topology and live job-usage fields to
`worker_instances`.

## Agent Rules

Future agents should assume:

- The VM deployment is the primary runtime.
- New deployment work should target `webserver-labo`.
- Compute and storage-visible worker work should target `detecdiv-server`.
- The local checkout, the deployed API copy, and the deployed worker copy are
  separate states; never assume a local edit is live.
- Do not assume the remote worker copy is a clean git checkout. Verify the
  remote state before claiming a deploy.
- If a change touches code imported by both API and worker, treat it as a
  cross-layer deploy and update both hosts.
- Do not reintroduce API-side filesystem scans for server paths.
- Do not assume the VM can see project storage.
- The current ready pool uses `detecdiv-worker@1` through `@6`; additional
  numbered instances start automatically when resource demand permits.
  Per-worker heartbeat state is stored in the `worker_instances` table.
- VM autostart and host-level reboot orchestration are documented in
  `../Webserver`, not here.

## Rollback Note

Rollback is possible by removing the worker systemd override and restarting the
old local services on `detecdiv-server`, but that should be treated as an
explicit incident/rollback action, not as the default development target.




