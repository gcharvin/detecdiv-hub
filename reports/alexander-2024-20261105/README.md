# Alexander 2024 migration — started 2026-11-05

The user explicitly authorized copying **2024_1 then 2024_2** to homes2,
inventorying and relinking associated project copies, and retiring verified
`/data` duplicates after complete copy/access validation, **keeping original
projects**. This authorization persists: do not ask for it again. No other
year/user, original project, NAS snapshot, backup policy, quota, default engine
or SMB credentials may be changed. The completed 2023 migration is separate;
never replay any of its one-shot scripts.

## Latest follow-up — 2026-11-06, 10:50 UTC

Gilles reaffirmed autonomous continuation, verification of the nine historical
missing images, retention of original projects, and retirement only after
verified copies/access, with final notice of actual recovered space.
**This checkpoint supersedes the older audit states below.**

2024_1 copy is healthy at **3,168,635,187,429 bytes / 61.1964%**. Sixteen
standard project copies are fully verified; project 17 `anof_210624_s148s_SCON`
is still in verification. Both original transfer runners continue. 2024_2 has
not started. No raw terminal SHA1 success, scientific path write, publication,
preferred-location change or source retirement has occurred; no 2024 space
has been reclaimed yet.

### Complete native availability baseline for the historical images

`historical-reference-baseline-20261106/` completed with exit 0, writing every
saved/current reference (not only discrepancy examples): **13,158 entries**
across `anof_060224_cres` and `anof_060224_cres_ARS1`. It confirms the first
project's nine absent GFP filenames and 27 cached-size differences, plus
**six absent GFP filenames and 18 cached-size differences in ARS1**. These are
15 distinct absent names and 45 old size differences, not new transfer loss.
The two ARS1 fields also have exact first-frame raw/ROI crop equality; the
three fields already sampled for the first project were not replayed.

The separate readonly **native** validator `historical-reference-parity-20261106/`
completed with exit 0: all 13,158 paths agree with the original native manifest.
Current source presence, size, mtime_ns, inode and UID/GID match the initial
inventory; homes2 presence/size match it and present files belong to maliavko.
All 15 absences are identical before/current-source/current-target. No source
or scientific project file was written. See
`historical-reference-baseline-verification.json` and
`historical-reference-parity-verification.json`.

- Baseline result SHA256: `4b4832b751f5ec63938522fd60f6fdacdb568dcad4c0fa3992194a14a7b86bc5`
- Complete references JSONL SHA256: `7c590a9c5950d711c7a517aeee28568577f150409cbccffd63f6ad07a5aff255`
- Native parity result SHA256: `9d4ac5089262555b46a728ad5c4bd3e071aa7a7c787a1c22f5536b0f6308a4b6`
- Source native stat receipt: `692325e57761f4d672cb712cfa7e84b02c1f75b75530b917064453f60bb1765e`
- Target native stat receipt: `5681c0083bf5a0c876d9fac3eb0738ed95dd5aae5d8260f6011d46eef59177e8`

These receipts establish **explicit historical availability parity**, not image
recovery, matching the old saved cache, or a complete raw content checksum.
Preserve every scientific cached entry and original project. Final host checks
must enforce the frozen baseline exactly, including expected absences; any
additional missing/changed reference fails. Do not pretend the old strict
srclist assertion passed. The full physical raw SHA1/stability and all other
scientific relink/final-host/catalog/API/retirement gates still apply.

### Expanded filename lookup still running, do not duplicate

Tmux **`alex2024-historical-name-search-20261106`**, private
`historical-name-search-20261106/`, script SHA256
`206031b80dc109339e781d976f81a7dcd4d6f996f95cba94cd5a48cae8126a8d`.
It scans exact nine initial basenames in primary `/volume1/DATA/Alexander`
and secondary `/volume1/Sauvegarde/Alexander`, readonly and no-follow, excluding
hidden/NAS metadata, snapshots and recycle directories. Read controller exit/log,
only tails of primary/secondary receipt JSONL, and result.partial/result JSON.
The primary scan has found homonymous files under unrelated **2025 acquisitions**;
those are candidates by name only, not the missing 2024 images. Never copy or
substitute unrelated raw pixels based on a filename match. This search does
not alter any file, snapshot, backup policy, mount or credential.
The first launcher stopped at its Python executable preflight after preparing
only script/request; no process/receipt existed. A bounded continuation verified
those exact two files and script hash before the first real launch.

### Empty ROI paths are a verified pre-existing unextracted layout

`blank-roi-layout-20261106/` finished with exit 0: `anof_061224_X5152moth`
has **308 extracted ROI** and **412 pre-existing unextracted ROI**, across 14
FOV/720 ROI. Every empty-path ROI also has an empty embedded image and no
im/results in the fully verified original/copy manifest. This is not an
unrecognized foreign path or a missing extraction introduced by the move.
Preserve blanks and the actual sidecar layout; do not invent files or paths.
See `blank-roi-layout-verification.json`, result SHA256
`9ec2c8956ce2be6ea3bb010d0be218476ef10a4c7c5b227c5c3a2b5b9db7d836`.

Its bounded `blank-roi-identity-continuation-20261106/` is also **finished,
exit 0, success true**, reusing the two already-completed fields without replay.
All 14 fields' saved names/sizes/constant timestamp offsets pass; all real ROI
references are verified, 252 raw pixel samples total (including reused fields),
six exact extracted-image crops and eight fields explicitly unextracted.
Original/copy main hashes unchanged. This brings full identity preparation to
**13 projects / 168 FOV / 44,567 ROI**, separate from the two historical-baseline
projects. Scientific save/reopen and final host gates remain.
See `blank-roi-identity-completion-verification.json`, result SHA256
`2ff4bcfe55a26ef04f879aeb066869781e91cdf248627e930904a177c987df01`.
No saved scientific metadata or catalog changes.

### Other readonly preparation continues

- **Persistent metadata batch2 is running**, nine new identity successes only:
  tmux `alex2024-persistent-paths-batch2-20261106`, private
  `persistent-path-audit-20261106-batch2/`, script SHA256
  `9368ceb1dfa95b76d26d429a7ef19006cfd0b426cd21915994a4b67711592b84`.
  At 10:50 UTC, 5,500 metadata files in the first project, zero unresolved paths.
  Read progress/result/receipt and controller exit; do not duplicate or edit.
- **Identity batch3 is running**, only newly verified project 16,
  `anof_210524_cre16` (30 FOV): tmux `alex2024-identity-audit-batch3-20261106`,
  private `identity-audit-20261106-batch3/`, script SHA256
  `e8612456dab6354be1158c86581fe64bdd1effee96964874b3366914939166d8`.
  Six FOV complete at 10:50. It does not replay projects 1–15.
  Loop exit 0 alone is not success: check each project receipt.

The earlier identity batch1/batch2, first-three persistent metadata/FIG scans,
legacy reviews and two exception-copy jobs are finished. Do not replay them.
Continue remaining metadata/sidecar audits, later verified copy identities,
format-aware special-layout tests and missing/unreadable main relevance review.
Before any relink obtain fresh admission/own leases, back up only planned MATs,
then prove every scientific value/type and annotation before/after/save/reopen.
Never save an incomplete GPU representation. All continuation/retirement gates
at the end of this report remain mandatory.

## Historical follow-up — 2026-11-06, 09:58 UTC

**This checkpoint supersedes the running/completed descriptions below.**
2024_1 is still copying normally: **3,024,771,071,009 bytes / 58.4179%**.
2024_2 has not started. Fifteen of 32 standard project copies have complete
hash/ownership/native-stability receipts; project 16, `anof_210524_cre16`, is
in verification. Both nonstandard copies were already verified. No raw full
SHA1 terminal success, project path rewrite, publication, preference switch
or source retirement has occurred. No 2024 space has been reclaimed.

### Identity audits: retain successful work and diagnose individual failures

The original `identity-audit-20261106-batch1/` ended with controller exit 1
at project 4. Its first **three projects / 35 FOV / 8,845 ROI / 525 raw pixel
samples** passed, with exact first-frame ROI crop equality in all 35 FOV.
Their main files stayed unchanged. Completed partial receipt SHA256:
`0b185958de375c30dab634a9981ea830362996f126cab5b935e5743901a5ad20`.
Do not rerun that audit or those successful FOVs.

The separate bounded `identity-audit-20261106-batch2/` examined projects 5–15
only, using per-project isolation. It is now **finished, controller exit 0**,
but overall success is **false**: **nine projects / 119 FOV / 35,002 ROI /
1,665 raw samples** pass, with exact ROI/raw crop equality in all 119 FOV.
Two rows require review. See `identity-batch2-completion-verification.json`.
Result SHA256: `a78f2f75a3b59db332bea3a6347ad7cbb6c03e68a571629ba887b4b5f1d144ff`.
Script SHA256: `e8612456dab6354be1158c86581fe64bdd1effee96964874b3366914939166d8`.
The two unresolved rows are:

- `anof_060224_cres_ARS1`: saved srclist size mismatch, no completed FOV.
  Diagnose independently; do not assume the counts from the project below.
- `anof_061224_X5152moth`: unreviewed ROI path prefix after two completed FOV.
  The discovery receipt's unrecognized value is an **empty string**. Inspect
  the actual blank-path/embedded/unextracted variant read-only; do not invent
  image or result sidecars, convert blanks to paths, or replay completed FOVs.

Thus twelve projects have full identity preparation, not all 34 candidates.
Their persistent path audit and scientific save/reopen gates remain separate.

### Historical 060224_cres source discrepancies, not a migration regression

A corrected read-only diagnosis of `anof_060224_cres` completed with exit 0
in `saved-size-diagnosis-v2-20261106/`. It established nine cached GFP names
are missing and 27 BF tail-image sizes/mtimes differ from the old srclist.
All nine absences and all 27 actual sizes were **already present in the pinned
native source-before manifest at transfer start**. Source/copy main SHA256
remained `56939d73ef23fb2be117636189b415592a1c9bc57ad3dea6219e91cfae81b58b`.
All three FOV first-frame ROI crops match current raw images exactly.
Diagnostic result SHA256:
`6a8a9a48ddaa77a615507940a2443dbb17994ea6e0696d373a8e61d155ceb1d1`.
See `size-mismatch-baseline-verification.json` for exact baseline evidence.

The first diagnostic attempt stopped before a FOV receipt when it tried to
sample a missing filename; its traces are preserved. Do not replay it. The v2
script SHA256 is `6edb720fc2ebce8dee0ec1c1c60659e8d0aa1b81fb93d109e53ec5e3a7383396`.
Scoped readonly backup lookups found no exact copies in the examined secondary
Alexander data/nobackup/collab roots or checked primary recycle locations.
This is not an exhaustive snapshot/backup search or proof of permanent loss.
See `historical-tail-backup-search.json` and `historical-tail-backup-nested-search.json`.
No snapshot, backup policy, mount or privilege was changed. Do not attribute
the differences to compression or establish an acquisition history without evidence.

The original strict saved-cache identity assertion remains failed; it has not
been silently waived. Preserve all scientific cached entries and original
projects. A future validator may explicitly prove **before/after availability
parity** for a reviewed immutable baseline, while requiring the full current
physical source SHA1/manifest and all normal relink/final-host/API gates. It
must reject any additional missing/changed reference and record the existing
limitation. Never fabricate nine images, drop cached entries, or describe a
pre-existing absent image as restored. Source retirement remains pending.

### First three projects: persistent metadata preparation complete

`persistent-path-audit-20261106-batch1/` finished with exit 0: **8,852 metadata
MAT/JSON files**, excluding 8,845 image MATs; no unresolved metadata path.
Only **four MATs** need future edits: three main files and the third project's
`combine_channels_1_processor.mat`, with **133,284** exact planned changes.
No file was saved. Receipt SHA256:
`d464f8e99dbc24b2211f2ea218ac98031bd6ee20aaab5e7cca8fde3ee5628b03`.
Script SHA256: `9368ceb1dfa95b76d26d429a7ef19006cfd0b426cd21915994a4b67711592b84`.
See `persistent-batch1-completion-verification.json`. Backups, fresh own leases
and canonical scientific/annotation comparisons before/after/save/reopen are
still required before changing even those four unpublished copies. GPU warning
representations must never be saved without full content/type equivalence.

Both FIG-only read-only audits also finished, exit 0, with the **same complete
receipt SHA256** `8cf73304815b01e3b261b97c8855aec028a795d1e273387cf644b194f3154e32`.
The second scan broadened callback literals and captured workspace strings;
all seven FIGs have unchanged hashes. Only two absolute values occur, both
stored FileName properties in `rlsplot_select.fig`. The installed MATLAB loader
sets the currently opened filename; `figure-filename-classification.json`
records its hash and classification. These are not raw/ROI input dependencies.
Preserve all FIG, PNG and .DS_Store bytes; no `openfig` callback was executed.
V2 script SHA256: `976e5af84ce6a2f91e25d0dd3cc74a52dd98e88d0ce225f5415bc36dcd01d81c`.
See `figure-batch1-v2-completion-verification.json`. The MAT/FIG scans are done,
not running: do not replay them.

Next useful work: persistent metadata audits for the nine newly identity-verified
projects, bounded format/baseline diagnosis of the three unresolved identity
cases, identity preparation for later fully copied projects and the two special
layouts, and relevance classification of missing/unreadable legacy mains. Never
repeat successful FOVs, duplicate active copies, or reinterpret a loop exit as
all-project success. All raw/final-host/API/retirement gates below still apply.

## Historical follow-up — 2026-11-06, starting 02:29 UTC

2024_1 is still copying normally: **1,955,819,451,234 bytes / 37.7731%** at
02:29 UTC, same live runner/rsync, no error. 2024_2 has not started. No complete
raw SHA1/stability gate, project relink, publication, catalog cutover or source
retirement has occurred. Older paragraphs below describe earlier checkpoints.

Nine standard project copies have complete native source-before/after equality
and full owned-source/copy SHA256 receipts (49,612 files / 659,755,111,576 bytes).
`standard-project-checkpoint-verification.json` and `identity-batch1-launch.json`
record this verified subset. The existing 32-project copy runner continues;
do not duplicate it or equate a partial count with all 32 finished.

### Read-only identity preparation now running for those nine

Tmux **`alex2024-identity-audit-batch1-20261106`**, private
**`identity-audit-20261106-batch1/`**, with `request.json`, `progress.json`,
per-project `project-NN.partial.json`, `result.partial.json`, `result.json`,
`controller.log/exit`. Script SHA256
`1811321391975bc7687e5b6ea218ccfad11234e2b27a5b0b27ed1717bf775f10`.
It inspects only the first nine verified project copies: recognized raw/ROI
aliases, all saved srclist filename/size matches and a constant timestamp offset
per channel, all ROI image/results references in the verified manifest,
representative real ROI loads, raw first/middle/last frame pixel fingerprints
and optional exact crop equality. It verifies both main MAT hashes unchanged
before/after reading and never saves files. One MATLAB thread, no GPU.
At 02:42 UTC it was in project 2 with no exit/error; the first project finished.
Do not duplicate it or edit its pinned script. A crop mismatch is explicitly
not established identity, not automatic scientific data corruption. Any failed
path/size/time assertion must be diagnosed from retained partial receipts;
prepare a bounded read-only continuation instead of replaying completed FOVs.
GPU warnings do not authorize saving incomplete GPU representations.

### Both nonstandard project copies verified, still unpublished

The distinct one-shot `exception-copies-20261106/` controller completed with
exit 0 in tmux `alex2024-exception-copies-20261106` (now finished). Runner SHA256
`65bbbd8d6160107c02f9c0753a0838767ef2e2c4ad5c570d0c2b759668b95ee9`.
Both projects have full native source/destination SHA256 equality and identical
native source-before/after metadata/hash inventories: **106 files /
5,293,672,634 bytes**, all originals unchanged. Native destination files are
owned by maliavko; Hub ownership remains Alexander for the qp project and
Gilles for `281124_Alex`. No catalog field was changed by copying. Stages:

- `/homes2/maliavko/DetecdivHub/projects/.incoming-qp_250724_dbud4stest-20261106`
- `/homes2/maliavko/DetecdivHub/projects/.incoming-281124_Alex-20261106`

Evidence: `exception-copy-completion-verification.json` and
`exception-existing-layout-verification.json`. Do not relaunch or overwrite.
These layouts differ from standard projects: qp has a pre-existing 128-byte
`im_` placeholder and **no results/data sidecars**. Gilles has 58 `im_` files
and **no results/data sidecars** (104 total payload files). Preserve exactly
those existing states; never invent missing sidecars, call the qp placeholder
an extracted image, or impose the standard three-sidecar assumption. Their
path-bearing metadata, scientific non-path comparisons and format-appropriate
Linux/Windows/raw tests are still required before publication or retirement.

### Legacy review completed; only explicitly classified issues remain

The initial 1,998-file legacy reader finished with exit 0: **1,812 inspected,
zero candidates**, 186 unresolved. Receipt SHA256
`13bb535dc5de28255ef4c99ec35675cdba73e1b411110b703550cf05761adacb`.
Its precise errors were 16 char-matrix concatenation errors, 137 graphics-Line
objects needing a reader, and 33 unreadable MAT variable directories.

The bounded v2 review in `legacy-path-review-retry-20261106/` finished, resolving
17 and leaving 136 already-deleted graphics handles; no candidate paths found.
It normalized multi-line character arrays and read only Line textual/UserData
properties. Receipt SHA256
`b11c9f9f58e6f21cd6e1599cf2097ff104a6422c5211b9f97054f35b3f89beec`.
The bounded v3 review of those **136 only** also finished with exit 0, no
unresolved/candidate paths. It counted and did not dereference 136 invalid GUI
Line handles, reading all other accessible text metadata; no scientific files
were saved. Receipt SHA256
`7f92f85546bf7af50ec504605def6e96c7d79d97efe8ccaa09d5976bae9434fb`.
The v3 script hash is
`a0cbb88717e894e154faaed3b1ab13670132e0fee651f62e13335865c164c898`.
All three legacy readers are **finished**, not running. Do not replay them.
Thirty-three unreadable MATs, 1,030 declared missing mains and the three tiny
uncataloged shallow/MCOS stubs still require relevance classification; do not
describe them as negative matches. No missing-record metadata contained a
positive 2024 acquisition lead, which is not proof of no reference.

The 28 JSON alternatives were rechecked with corrected **single** Windows
separator normalization (`chr(92)`), stable source metadata and full content
hashes; all parsed, no candidate/explicit 2024 reference. The earlier Python
checker normalized only double separators; its zero count was not sufficient.
The corrected receipt supersedes it, SHA256
`f2acf3175e674da79152be7735dd1fd9a68f39035480e0029e87064269210952`;
see `json-alternatives-normalization-verification.json`. No data was rewritten.

## Follow-up — 2026-11-05, starting 20:29 UTC

The original raw runner is healthy and still copying 2024_1: 960.20 GB of
5,177.81 GB (18.54%) at 20:29 UTC. No SHA1 gate or source retirement has passed.
All original project files, source raws and catalog locations remain intact.

The main read-only discovery **finished with exit 0**, 3,326 receipts, SHA256
`1e5eb048a7af595eff6be2dce01c765bb02379d1108935bea00d58088df9fdaa`.
It found **34 candidates**: 33 Alexander (426 FOV / 104,981 ROI) and Gilles's
`281124_Alex` (2 FOV / 58 ROI), which also references 2024_2. There are seventeen
candidate projects for each year. This is path evidence, not final identity.
Do not rerun the completed main discovery.

### Independent project copies running

Thirty-two Alexander candidates have the reviewed Sauvegarde/analysis sibling
MAT + same-name directory layout. After fresh read-only job/lease/indexing
admission, their unpublished owned copies were launched in tmux
**`alex2024-project-copies-20261105`**, private **`project-copies/`**.
Read `state.json`, `controller.log`, `controller.exit` and per-project receipts
there. Runner SHA256
`51a00d2ffd6f247141ff95594f496d3a19166c86b3cd19adff6bf7f6d3c71d6d`.
The first project reached full hash verification; do not infer terminal success
from a phase or file counter. Each project needs complete source/copy SHA256
receipts and native before/after inventory equality. CIFS-created placeholders
are owned by maliavko; native same-NAS reflinks create independent files with
distinct inodes, followed by complete content hashing. The runner never saves
MATs, relinks paths, publishes, changes the DB or removes originals. It refuses
existing stages and nonempty partial placeholders. Never directly rerun it or
modify its pinned executing script; inspect a failure and prepare bounded
recovery from preserved receipts. A conservative capacity reserve includes
both raw years plus 2 TB. No source projects were modified.

Two known candidates intentionally remain separate from that standard-layout
copy runner: `qp_250724_dbud4stest` lives under Sauvegarde/Alexander/nobackup/qp
and records ROI paths under `/data/Alexander/qp`; Gilles's `281124_Alex` lives
under `/data/Gilles/analysis/projects`. Resolve their actual layouts, copy and
relink with correct preserved ownership/access; do not squeeze them into the
analysis-root runner or overwrite their originals.

### Remaining discovery review, with new bounded tasks

The main discovery recorded 3,056 unresolved rows: 1,058 missing declared main
files and 1,998 unrecognized MAT layouts. Those are not negative matches.
Read-only format preflight found 28 existing JSON replacements for declared
MAT files; every JSON was parsed with stable source metadata and no 2024
acquisition match. Full private receipt:
`legacy-path-review/json-alternatives-receipt.json`. 1,030 declared main files
still have no accessible MAT/JSON replacement (one Alexander:
`anof_071026_X54445_LOGEB`, declared in `/data/Alexander/nobackup/analysis`).
Classify these catalog issues against actual metadata/backups/locations rather
than assuming that their names or absence prove irrelevance. Do not ask Gilles
to resolve issues that can be investigated independently.

A **new** read-only legacy variable/string review of only the 1,998 unknown
MAT layouts is now running in tmux **`alex2024-legacy-path-review-20261105`**,
private **`legacy-path-review/`**, with `request.json`, `progress.json`,
`receipt.jsonl`, `result.json`, `controller.log/exit`. Script SHA256:
`e9b678a67633095234740c9e216dcfca6fa0849839afdc78adb6c90c076b7e24`.
It uses fresh busy-project admission, one MATLAB thread, idle I/O and no GPU.
Only complete stable non-object layouts can finish as inspected; unreadable,
large, occupied or class-specific layouts remain explicitly unresolved. This
is additional format support, not a replay of the initial shallowObj audit.
Do not duplicate it or modify the executing script.

The three uncataloged main MATs received their separate read-only supplement
in `project-discovery-supplemental/`, exit 0, all three **still unresolved**.
They are tiny 246–248-byte MAT containers holding shallow/MCOS object metadata;
MATLAB reports unreadable variable directories. Bounded container inspection
and SHA256s are recorded in `supplemental-mat-container-verification.json`.
Preserve them and their folders. Do not infer a valid empty project, recreate
scientific data or claim absence of references from these unreadable stubs.
Investigate available fuller counterparts/backups or sidecars without altering
the originals before the affected raw year's retirement.

No project relink or preferred-location change has occurred. The standing
retirement gates below still apply to all relevant years and cross-user refs.

## Actual running work

Private compute work:
`/home/charvin-admin/storage-transfers/alexander-2024-20261105`.
Hub batch **`0382c23e-ad54-4ec9-9ad8-53cc79b64d97`** belongs to Alexander.
Six-hour autonomous follow-up is active with automation ID
`alexander-2024-migration-et-lib-ration-data`; remove it only after completion.
Raw tmux **`alex2024-copy-20261105`**, controller PID 187291 at launch.
Read `state.json`, `controller.exit`, and only bounded tails of logs before
acting; corroborate actual process identities. No duplicate transfer/audit or
blind retry of partial output. `status.py` only reads existing state and saves
public summaries plus private details under ignored `runtime/`.

Sources: `/data/Alexander/data/2024_1` and `/data/Alexander/data/2024_2`.
Target parent:
`/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2024-20261105`.
All final targets and staging work were absent before launch. The existing
personal CIFS mount uses maliavko; 34,431,047,131,136 bytes were available.
The runner reserves at least 2 TB free and limits copy bandwidth to 61,440
KiB/s. Existing unrelated Hub jobs and leases remain untouched.

| Year | Files | Measured logical bytes |
| --- | ---: | ---: |
| 2024_1 | 732,579 | 5,177,814,875,236 |
| 2024_2 | 974,112 | 5,856,516,249,162 |
| Total | 1,706,691 | 11,034,331,124,398 |

The initial native readonly measurement counted 11,036,278,657,024 allocated
bytes, excluding NAS system metadata. It is a planning measurement, not proof
of transfer or a guaranteed free-space recovery. Project copies are additional.

2024_1 native full inventory completed and copy began **14:27:53 UTC**.
Manifest SHA256:
`8fd64d094fe29707e0ddd0b1480e9b861786d3adde677087ca7a6e95e252f61e`.
It records every file's size, exact native mtime, inode, UID/GID, mode and nlink.
Runner SHA256:
`a0576fffa3c1d43d232f54567c6c9f7153aed40b436ace98f26079af72432b99`.
Full manifests and catalog snapshots are private; Git contains summaries/code.

The runner copies 2024_1, compares **all file contents with SHA1** in rsync
dry-run (including destination-extra detection), then requires an identical
native source inventory. Only after that does it start 2024_2 with the same
gates. This runner has **no source-deletion, publication, MAT-write or catalog
cutover operation**. A successful full checksum has exit 0 and an empty
terminal differences log, followed by successful source stability; a running
process with an empty log is not success. On failure preserve all state and
sources and design a bounded recovery after diagnosis. Never simply rerun.

The isolated copy guards proved equal-size/equal-mtime corruption detection,
extra-file detection, dry-run nonmutation and empty differences for identical
content. No deletion flags exist in the actual copy command. Rsync's own early
percentage uses its incremental file list: compute whole-year progress from
transferred bytes divided by the pinned total, not that displayed percentage.

## Project inventory already running

Tmux **`alex2024-project-discovery-20261105`** reads 3,326 catalog main files,
the 154 Alexander projects first, using the protected engine
`d83c4dbc0638bc0d3f6012b4f1df5744e0c017c7`, one MATLAB thread and nice/idle I/O.
No ROI image sidecars or project files are saved. Request/script, per-project
JSONL receipt, progress, result, exit and log live in the private work folder.
Script SHA256:
`5ca111b8efde03912b914d78c75ff0f2aa501991cf28705111a2822ba9994231`.
Do not edit an executing script or duplicate this inventory. The `success`
field means the discovery loop finished, not that all projects were resolved.
Every `requires_review` row still needs classification; busy projects were
deferred at admission and must be reconsidered after fresh job/lease checks.

At 14:32 UTC, 27 main files had been inspected and ten candidate projects found.
One inaccessible main file was explicitly marked for review rather than treated
as a negative match. Some main MAT loads warn about limited/unavailable gpuArray
data. This discovery reads paths only and never saves those objects. Before
any later relink, scientific equivalence must account for the original array
types/contents; never resave an incomplete GPU representation or drop arrays.
The actual raw copy had transferred about 9.69 GB (0.187% of the complete year)
at that checkpoint. See `latest-status.json` for a timestamped observation.

The catalog contains 115 Alexander raw datasets and **no existing project/raw
links for these years**. There are 31 acquisition directories across both years.
Matching acquisition name segments identifies candidates, not scientific
identity. An early candidate is `anof_011024_x423x426_EB_x423noebonly` (9 FOV),
referencing `2024_2/011024_x423x426_EB`. Full alias/identity proofs remain.
The catalog has 1,970 preview Artifact references and 111 nonempty settings
fields; audit their actual source files/paths before cutover (counts alone do
not prove existing content). The API can now read homes2 directly; no new
temporary `/data` API cache should be needed.

Filesystem discovery also found three **uncataloged main MAT files** in
Sauvegarde/Alexander/analysis. They must be inspected read-only as supplemental
candidates, without inventing catalog identities or publishing them blindly:

- `anof_130224_dfob_rep_dslog.mat`
- `anof_10112023_nh_dhapxs_1530.mat`
- `anof_14092023_leftRDN_nhadhhap4.mat`

The top-level scan found 113 main MAT/JSON files in that analysis root. Inspect
other actual/nested project roots referenced by the catalog, and investigate
missing/unsupported main formats rather than treating them as negative
matches. The complete existing projects inventory includes other owners so
cross-user references cannot be missed. Read-only access is allowed; preserve
all non-Alexander files and user content.

## Required continuation gates

Continue useful project preparation while the raw copy/checksum runs. Resolve
old drive/path aliases against actual raw acquisitions using complete srclist
names/sizes/times and representative pixel identity checks; metadata match is
not a content checksum. Preserve ambiguity and require evidence. Inventory
all persistent path-bearing sidecars/processors before relinking; do not
rewrite image/HDF5 arrays to change paths. Classify every discovery error that
could reference these years, including deferred busy/uncataloged projects.

Copy only the associated projects into new owned unpublished homes2 folders;
keep original Sauvegarde/other original files intact. Require full per-file
source/copy hash equality, native ownership and source before/after stability.
Back up path-bearing MAT/JSON metadata before rewriting copies. Compare every
non-path scientific value and annotation before/after/save/reopen, with exact
expected path-change counts. MATs retain canonical server paths; Windows UNC
translation occurs in memory. Do not invent missing sidecars/ROI extraction.

After each raw year's true checksum/stability success, acquire only own leases
under fresh jobs/indexing/lock admission checks; publish folders without
overwriting existing paths; validate **final** paths on Linux and the actual
limited GMGM\\Charvin-Admin/session1 Windows worker with the protected engine.
Test all ROI references and representative raw/ROI reads in every FOV. Preserve
catalog identities, owners, notes, ACLs/groups, positions and links. Switch the
same objects' preferred locations and exact API file references transactionally.
Stream and hash every affected real API handler against homes2 before removal.
Cross-year projects may delay retirement until their relevant gates pass.

Build a **new explicitly 2024-bound** retirement orchestrator/leaf. Test guards
on isolated fixtures; require true raw/project/final-host/catalog/API receipts
and fresh own leases. Revalidate the entire native source manifest, reject new
files, unexpected symlinks or changes. Before each unlink require native
type/size/exact mtime plus checked no-follow ancestors and a live own-lease
deadline. Emit complete per-file receipts; measure actual NAS free space
before/after; preserve excluded NAS metadata and original projects. No blind
year-name substitution of the dated 2023 deletion controllers is allowed.

After removal verify receipt/manifest equality, empty payload, unchanged
catalog/scientific metadata and Linux/actual Windows/API readers again. Remove
dead source locations only after complete receipts; release only own leases.
Report actual recovered bytes. Continue 2024_2 automatically; retire each year
as soon as its full gates permit. Commit/push explicit public proofs only after
secret scanning; never stage runtime, secrets or credential/private-key content.
Stop the follow-up only after both years and project/API paths are complete.
