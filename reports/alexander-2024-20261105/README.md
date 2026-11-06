# Alexander 2024 migration — started 2026-11-05

The user explicitly authorized copying **2024_1 then 2024_2** to homes2,
inventorying and relinking associated project copies, and retiring verified
`/data` duplicates after complete copy/access validation, **keeping original
projects**. This authorization persists: do not ask for it again. No other
year/user, original project, NAS snapshot, backup policy, quota, default engine
or SMB credentials may be changed. The completed 2023 migration is separate;
never replay any of its one-shot scripts.

## Latest follow-up — 2026-11-06, starting 02:29 UTC

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
