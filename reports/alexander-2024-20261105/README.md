# Alexander 2024 migration — started 2026-11-05

The user explicitly authorized copying **2024_1 then 2024_2** to homes2,
inventorying and relinking associated project copies, and retiring verified
`/data` duplicates after complete copy/access validation, **keeping original
projects**. This authorization persists: do not ask for it again. No other
year/user, original project, NAS snapshot, backup policy, quota, default engine
or SMB credentials may be changed. The completed 2023 migration is separate;
never replay any of its one-shot scripts.

## Latest follow-up — 2026-11-07, starting 08:40 UTC

**This checkpoint supersedes all earlier running states.** At 09:01 UTC,
raw 2024_1 remains **verifying_full_content**, runner187291/rsync503795 alive.
No terminal SHA1 or source-after stability receipt yet; an empty live log is
not success. Do not duplicate/restart it. 2024_2 starts automatically after
both gates. No preferred location/API reference changed; **zero 2024 bytes
reclaimed from /data**. Keep all original projects and all remaining gates.

### First relink corroborated; batch4 finished and batches2/3 running

The first 23-MAT/twelve-project relink is done, never replay. Its additional
readonly postflight finished and **all 4,258 GPU content hashes, underlying
types/sizes and every class count exactly match original readiness** across
23 files. See `relink-batch1-gpu-postflight-verification.json`; after-result
SHA256 `7037d92382c67ce5939d8bf1516cdf5e0c5af2e02cd6b25b1b9826356eabeb0b`.
Its child batch and twelve leases were already finalized/released last night;
no watcher restart or additional release is needed.

Fresh jobs/indexing/lease admission launched only these disjoint unpublished
metadata batches. Every native full original inventory was freshly identical,
source/copy/backup full hashes matched, backups are real maliavko-owned files
with distinct inodes, and all source originals remain untouched:

| Batch/work | Projects / MATs | Planned path changes | Verified backup bytes | Own lease expiry UTC |
| --- | ---: | ---: | ---: | --- |
| `relink-batch2-20261107` | 9 / 18 | 394123 | 1080274086 | Nov7 14:42:26 |
| `relink-batch3-20261107` | 5 / 9 | 268978 | 1083950122 | Nov7 14:55:52 |
| `relink-batch4-20261107` | 4 / 6 | 110444 | 175175795 | Nov7 15:04:45 |

Batches2/3 are **RUNNING**, not terminal success (checkpoint 12/18 and 4/9
receipts). Batch4 finished at the end of this follow-up, as detailed below.
Read current state/result first; lease expiries in the table are admission
deadlines, not a claim that a completed batch still holds a lease.
Each tmux is `alex2024-<work>` under the usual private compute root. Native
backup gate hashes and snapshot states are in `relink-batchN-preparation-checkpoint.json`.
Backups stay under homes2 `.migration-backups/alexander-2024-20261105/<work>`.
These are new scopes of the reviewed 2024-only writer, not replays or changes
to any executing script. They compare canonical scientific/type/annotation
values before/after/save/reopen and enforce exact path counts and lease expiry.
Images/HDF5/originals/catalog cutover/source retirement are outside permission.

Child batches/holders (do not mix their leases):

- Batch2 `90c03028-f0b9-4f86-86b6-a90912e3ef50`, holder
  `alexander-2024-unpublished-relink-batch2-20261107`, nine leases.
- Batch3 `e72ca211-c526-4855-99ad-d2839da288a6`, holder
  `alexander-2024-unpublished-relink-batch3-20261107`, five leases.
- Batch4 `98f46050-bfe3-409f-a807-a2fb30c104ef`, holder
  `alexander-2024-unpublished-relink-batch4-20261107`, four leases.

Batch2/3 hidden local watchers are alive and identity-verified (launch PIDs
83164 / 95116; reverify before any intervention). Batch4 watcher24208 completed
and must not be restarted. These watchers
only finalize their own child/lease IDs after true terminal receipt. Private
plans/admission/status/logs are `runtime/relink-batchN-*`; public launch,
admission-summary and eventual lease-finalization receipts use the same prefix.
Do not duplicate watchers, extend active requests/deadlines, or replay partial
writers. On failure count modified-NN receipts even if final result is absent;
preserve all backups/temporary MATs and design bounded recovery. After batch2
success, schedule the same after-hash GPU corroboration against its own
2,585-value readiness; no old before hashes may be used on modified copies.
Batch3/4 readiness had zero GPU/function/private values; full canonical
save/reopen checks still apply. Scientific type/FIG summaries are public.

**Batch4 TERMINAL UPDATE:** controller/matlab exit0 at 09:12:04 UTC, all six
MATs/four projects and 110444 exact path changes have science/type/annotation
save-reopen receipts. Result SHA256
`302d43985081831ff1e2b0906744463e08dee0f57dfec95fe51fc298b66d5b7b`;
see `relink-batch4-completion-verification.json`. Watcher released its four
leases and closed only its child at 09:12:56. Independent readonly DB digest
comparison confirms all four project identities/metadata/locations unchanged;
see `relink-batch4-catalog-preservation-verification.json`. Never replay the
batch4 writer/watcher. There are now **sixteen projects / 29 metadata MATs**
fully relinked in completed batches1+4, separate from live batches2+3.
No preferred location, API reference, original or raw source was changed.

### New unresolved loxP15 raw absence, proven pre-migration

The availability-variant v2 loop is done, exit0 but overall **success false**.
Four projects are availability-qualified without any invented sidecar:
RMng, anof_dbud4, movie_12224_w303MBsnabI_test and qp_22032024_DHY210_nts1ng.
See `availability-variants-v2-completion-review.json`; do not repeat them.

The two failures received bounded metadata diagnosis. V1 stopped readonly on
mixed char/numeric-empty ROI path cells (`uniqueCellstrHelper`), after partial
references only. V2 preserves those traces, verifies/reuses matching prior
rows, keeps actual path types (no fabricated empty paths) and does not replay
raw pixels. It finished with unchanged original/copy main hashes. See
`pending-availability-diagnosis-completion-review.json`, result SHA256
`580f1db0aefc0394b85c2157dfcc0f71b564c217bbd7a7e002e2aaa298c53305`.

**loxP15 has 53,280 raw references absent at the expected paths**, every even
position Pos2..Pos30 (15 of its 30 fields), in addition to its already missing
derived project folder. This absence is present in the original Nov5 native
manifest, not created by migration. Do NOT claim these fields accessible or
recovered. A candidate scan of the three related acquisition roots found only
60 parent/basename candidates and no candidate for 53,220 references; no
scientific identity was proved and no file substituted. Preserve this plan,
not a recovery receipt (`pending-availability-candidate-plan-summary.json`).

The new **native parity controller is TERMINATED exit0, success true**:
112,956 unique references over movie+lox; initial/current source/current
homes2 agree in availability and size; every existing source also agrees in
exact native mtime_ns/inode/UID/GID, targets have actual maliavko ownership.
It confirms 53,280 old absences and 6,396 old movie cache-size discrepancies.
No cleanup/strict-access gate is waived. See
`pending-reference-parity-completion-verification.json`, result SHA256
`5534c9800b12487e4a58b9cf29fc36999c87e4ff8df5651069594ef4489b59b5`.
Full per-reference/native receipts stay private in `pending-reference-parity-20261107`.
Do not rerun this controller on its existing receipts.

An optional text question is pending with Gilles about intentional deletion or
another archive of these even lox fields/derived folder. Continue independent
work; preserve all originals. Remaining lox fields need available-pixel reads
and a complete explicit treatment of missing fields before any retirement.
Do not silently accept a partial-access project as fully accessible. Its first
FOV was already read by availability v2 and must be reused rather than replayed.

### Movie cache case now corroborated with actual pixels

`movie-cache-identity-20261107` is **TERMINATED exit0/success**. Its bounded
reader requires the completed native parity gate and exact recorded metadata
for every saved/current reference; no blanket size/date assertion was waived.
All 6,396 historical cache sizes differ, timestamps have 533 different offsets
per channel. The original cached values remain unchanged. Four FOV / six ROI
entries, **36 raw samples and two exact representative saved ROI crops** pass;
two pre-existing numeric-empty-path ROI slots stay unextracted and empty.
See `movie-cache-identity-completion-verification.json`, result SHA256
`48dda4afb2bd1a4caa57e2825cb6c5730dd99b8cf4851280c14a243c1c2dedbf`.
This is qualified identity/readability, not a successful strict historical
cache assertion or raw full checksum. Do not replay these reads.

A readonly persistent metadata plan for only this movie is now running in
`persistent-path-audit-movie-20261107`, tmux
`alex2024-persistent-paths-movie-20261107`. Existing audit script SHA256
`9368ceb1dfa95b76d26d429a7ef19006cfd0b426cd21915994a4b67711592b84`.
Read result/receipt and handle FIG/types before any writer for this movie.

Metadata batch5 is done: 17,581 files / nine future MATs / 268,978 path changes,
zero unknown; its type3 and FIG4 checks finished, used by running writer3.
Metadata batch6 is done: 533 files / six future MATs / 110,444 path changes,
zero unknown; type4 and FIG5 finished, used by running writer4. See public
completion receipts. Do not replay completed identity/metadata/FIG/type tasks.

### Gilles field policy reviewed against the protected engine

The HDF5 attribute/reference audit was already done (58 files, no unknowns);
do not repeat. New `gilles-typed-path-policy-review-20261107.json` records
actual consumer hashes from the protected d83c4db release. Its path-localizer
explicitly rebases reusable top-level run refs while preserving execution
logs/context. Proposed typed handling separates those active fields from
ctx/outputs, run_params and text/event provenance, existing external models/
pipelines and embedded Hub client mappings. parsedData.folder is an active
io.path-derived project container field. No Gilles file was rewritten.

An exact field-level MAT/JSON plan, readiness/backups, canonical non-path
comparisons and actual Linux/limited-Windows reusable-run readers remain.
Historical-run resumption has not been validated; frozen ctx is not a claim
that an old analysis can be resumed. The unresolved legacy main/MAT/stub
relevance review still remains; absence/old names are not negative references.
All raw/full-final-host/catalog/API/new-tested-2024-retirement gates still apply.

## Historical follow-up — 2026-11-07, starting 02:36 UTC

**This checkpoint supersedes earlier running states.** The complete raw copy
of **2024_1 finished at 02:22:00 UTC**: 732,579 files / 5,177,814,875,236
bytes. Its runner is now **verifying_full_content**, child rsync 503795 at
02:40 UTC (check current process identity before any intervention). No terminal
full SHA1 or subsequent native stability receipt yet. An empty differences
log while the process runs is not success. Do not restart the copy/checksum;
2024_2 starts automatically only after both gates. No preferred location or API
reference changed and **zero 2024 bytes have been reclaimed from /data**.

### First metadata relink completed, own leases released

`relink-batch1-20261106/` finished **2026-11-06 23:04:57 UTC**, controller
and MATLAB exit 0, all **23/23 metadata MATs**, exactly **636,618 path changes**
in twelve unpublished copies. Every file has canonical scientific/type/
annotation equivalence before/after/save/reopen and before/after hashes.
All 23 modified-NN receipts match the terminal result, SHA256
`e2f0b82b394bb7f7355a7e0ec503bbe0ee45b5c1cae49d60db1f4892993c4cea`.
The 23 backups (3,715,167,522 bytes), originals and image files are preserved.
See `relink-batch1-completion-verification.json`. **Never replay this writer.**

The hidden watcher finalized only child batch
`bf1a270b-1e18-419a-b11b-b2241871f13b` and released its twelve leases at
23:05:51 UTC, before expiry. It is done; do not restart it or release again.
See `relink-batch1-lease-finalization.json`. A fresh readonly DB comparison
verified identity/owner/ACL/groups/notes/links, metadata and all locations still
match admission for all twelve projects; see
`relink-batch1-catalog-preservation-verification.json`.

Copies remain incoming; their MAT paths target future final containers. Use
**after** hashes for changed MATs and final-to-incoming translation **only in
memory** before publication. Raw full checksum/stability, actual final host
readers, API continuity and fresh publication/cutover admission remain required.

A new readonly corroboration of all saved scientific type/GPU descriptors is
running in **`relink-batch1-gpu-postflight-20261107/`**, tmux
`alex2024-relink-batch1-gpu-postflight-20261107`. It uses after hashes, not the
old copy hashes, with `audit_relink_value_types.m` SHA256
`4689cff8762f91c5aea03f187433250b86dd75b950945921de5ab61d2f3c5982`.
After controller.exit=0, run `verify_gpu_postflight_20261107.py` to compare
all 23 class/count/GPU descriptors against the immutable original readiness
result (4,258 GPU arrays). A completed loop alone does not prove descriptor
parity. Do not duplicate the scan; preserve any discrepancy for diagnosis.

### More identity and metadata work completed

Identity batch5 is **finished, exit 0, all five projects successful**:
66 FOV / 16,788 ROI / 954 raw samples, all representative crops exact,
main files unchanged. Result SHA256
`3c4b5944cc235b29e88aa3ccc23837f9142679fbf574cd0ccd20edca35f7beac`.
See `identity-batch5-completion-verification.json`; do not replay.

Persistent metadata batch4 is **finished, exit 0, zero unresolved paths**:
18,856 metadata files, seven future MAT edits / 225,834 planned changes.
Result SHA256 `16204a8dca332677a6ac46dd9ae22ad814924acee3d120f94b325256f0909169`;
receipt SHA256 `9ddec690c3db3e8685f757ef7ac143b9a9bd586faa4d0c038e0cbf69b1d20e4e`.
See `persistent-batch4-completion-verification.json`. These files have NOT been
relinked; they are separate from the completed 23-file writer.

FIG batch3 is also **finished, exit 0**: five FIGs across the nine projects
from metadata batches3/4, no path edits or unresolved values, byte hashes
unchanged. Result SHA256
`379e7214983876a6b8b1919b9791df5a39e25b8bbabaafd5438bb8a38a7a87ae`;
receipt SHA256 `231ddae2d05d681dfc679bd0bcd55d938be838e74a8d1bfe8a7299633d791d8f`.
See `figure-batch3-completion-verification.json`. Preserve FIG/DS_Store bytes.

### Bounded readonly continuations now running

Inspect real result/partial/progress, controller.exit and bounded log tails
before any action; do not duplicate or change their pinned scripts.

- **Availability variants v2**, private
  `availability-variants-identity-v2-20261107/`, tmux
  `alex2024-availability-variants-identity-v2-20261107`, six reviewed variants.
  The first attempt is preserved, **exit 1 before any project/FOV receipt**:
  heterogeneous JSON project records decode to a MATLAB cell array. V2 only
  fixes that container access and keeps all identity/availability guards;
  SHA256 `e4c7db11bff3ad137a21a28dcb1675b7d4445ffd6de3ae404f036324a3b283eb`.
  It had completed RMng and reached FOV5 of anof_dbud4 at the lightweight
  check; examine every project success and exact missing-sidecar baseline.
  No missing image or derived folder is fabricated or called restored.
- **Persistent metadata batch5**, private
  `persistent-path-audit-20261107-batch5/`, tmux
  `alex2024-persistent-paths-batch5-20261107`, only five new identity successes.
  Script SHA256 `9368ceb1dfa95b76d26d429a7ef19006cfd0b426cd21915994a4b67711592b84`.
- **Scientific readiness batch2**, private
  `relink-value-types-20261107-batch2/`, tmux
  `alex2024-relink-value-types-20261107-batch2`, only eighteen proposed MATs
  from metadata batches3/4 (nine projects, 394,123 planned path changes).
  Same readonly type script SHA above; 8/18 files were complete at the early
  check. No writer or leases for this next batch have been launched. Require
  full types/contents, fresh admission, backups and scientific save/reopen
  before any future relink; preserve historical raw/empty-ROI baselines.
- **Gilles HDF5 metadata**, private `gilles-hdf5-metadata-20261107/`, tmux
  `alex2024-gilles-hdf5-metadata-20261107`, all 58 fully hash-verified HDF5s.
  `audit_gilles_hdf5_metadata.m` SHA256
  `626edf64301a499ea4a12f7ae0a0ce9e3d2256e246dd02f44a7830d51f863c77`.
  Reads attributes, layout, links and bounded strings; numeric image datasets
  are never rewritten. External/virtual storage, object references or path
  strings remain review blockers. Require every file success, not just exit0.

**HDF5 completion at the end of this follow-up:** all 58 files finished with
exit 0 and every file successful, hashes unchanged. No external/virtual storage,
links, object references or path-like strings require review. See
`gilles-hdf5-metadata-completion-verification.json` for dataset/attribute counts
and the immutable result hash. Do not replay the now-completed HDF5 audit.
The earlier bullet records its launch, not its current state.

The Gilles main/run JSON typed policy and unresolved legacy relevance review
remain pending. Do not call run.json entirely archival: `pipelineRunLoad`
loads project refs/ctx/outputs and `shallowLoad` reloads pipeline runs. Inspect
these consumers in the protected migration release before choosing field-level
changes; run_params snapshots and Hub client mappings need separate policy.
No second writer/publication/catalog/API switch or source removal is launched.

## Historical follow-up — 2026-11-06, starting 20:32 UTC

**This checkpoint supersedes all earlier task states.** Raw 2024_1 is still
copying normally: **4,409,354,641,147 bytes / 85.1586%** at 21:01 UTC, same
runner/rsync. No terminal full raw SHA1 or source stability gate yet; 2024_2
has not started. No preferred location/API reference changed and **zero 2024
bytes retired from /data**. Source projects remain intact.

### Original copy runner stopped safely; bounded recovery completed

The first project-copy runner ended **exit 1 / failed_safe** at project 29,
`anof_290424_loxP15`, before emitting any inventory row or creating a stage.
Its MAT exists on Sauvegarde (647,439,730 bytes), but the same-name associated
derived folder is absent at the expected native location. This was an incorrect
standard-layout assumption, not a partially copied stage to overwrite. The
original 28 receipts and state are preserved; never rerun that one-shot runner.
The original failing folder contains only the empty source-before receipt,
its request and the 129-byte assertion log. No original/source file was deleted.

After fresh jobs/locks/indexing admission, the distinct controller
**`project-copies-continuation-20261106/`** copied only projects 30–32:
`movie_12224_w303MBsnabI_test`, `movie_250124_diploids`,
`qp_22032024_DHY210_nts1ng`. It is now **finished, exit 0**, with complete owned
native source/copy hashes and unchanged original inventories:
**1,730 files / 22,194,114,680 bytes**. Receipts for those three are deliberately
in the original `project-copies/<stem>/` folders; continuation state is separate.
The original copy.lock was checked and held by the bounded continuation.
See `copy-continuation-completion-verification.json`.
Runner SHA256: `56b466a7ab157de37464749c9973433fc12bd88628720bc612ddbcb6bd35da2c`.

There are now **31 complete standard copies**, plus the two prior special
copies. `status.py` merges the verified counts without rewriting the original
failed state. Do not interpret the old failed state as 28 missing copies, and
do not add the same project twice to subsequent audits.

### Missing-folder project preserved as the available MAT only

A new readonly directory search finished with exit 0 in
`missing-project-directory-search-20261106/`: only the matching **raw acquisition**
directories occur in primary Alexander; no derived analysis folder was found
in the stated primary/secondary payload roots. This is not an exhaustive
snapshot/archive search and raw folders are not substitutes for derived ROIs.
See `missing-project-directory-search-verification.json`.

A separately admitted, explicitly main-only copy is **finished, exit 0** in
`main-only-copy-20261106/`, stage
`/homes2/maliavko/DetecdivHub/projects/.incoming-anof_290424_loxP15-20261106`.
Only the available MAT was independently cloned, full source/target SHA256
matched, original metadata/content unchanged, real NAS owner maliavko; no
missing derived folder or sidecar was invented. Original MAT SHA256:
`c1a32a8cca1847fe72937423501a1ae8ddbb0c9d1b03460d33f081b5b6558898`.
See `main-only-copy-verification.json`. This is **not** a complete project-access
or identity gate. All 34 candidate main files now have protected copies, but
there is still a qualified main-only layout, not 34 fully validated projects.

The readonly `existing-layout-review-20261106/` also finished, exit 0, all
three MAT hashes unchanged, result SHA256
`8d762c9622431f62d8155b2204ab40c37403c491baa4da453cb6f62ccedf67ee`:

- `anof_230124_RMng`: 7 FOV / 3,122 ROI, no source-manifest image/results
  sidecars and no embedded images.
- `anof_250724_dbud4stest`: 32 FOV / 191 ROI, all 191 image MATs present,
  only 33 result MATs, no embedded images. Preserve that exact existing subset.
- `anof_290424_loxP15`: 30 FOV / 11,703 ROI, no associated folder currently
  found and no embedded images. Do not call these images recovered or claim
  they were never extracted merely because they are absent now.

See `existing-layout-review-verification.json`. These are observed baseline
limitations, not migration-created files to fabricate or scientific arrays
to drop. Any final acceptance must explicitly preserve existing availability
and raw access, reject new differences, and retain all original scientific
metadata. Do not silently turn a strict old missing-sidecar assertion into a
claim that every derived ROI is readable.

### First actual metadata relink is RUNNING, never replay it

All **23-file type-readiness** work is now finished, exit 0: exactly **4,258
GPU arrays** gathered/fingerprinted, zero function handles and zero nonpublic
persistent properties. No scientific file was saved by that audit. See
`scientific-type-readiness-verification.json`, result SHA256
`c521b807b7d8b3f3e8329ca4242eec1062e7f91a25e8439f8631f19399172229`.

After fresh admission and own leases, a new write-bounded controller started:

- Tmux **`alex2024-relink-batch1-20261106`**, private **`relink-batch1-20261106/`**.
- Child batch **`bf1a270b-1e18-419a-b11b-b2241871f13b`**; holder
  **`alexander-2024-unpublished-relink-batch1-20261106`**.
- Twelve own leases expire **2026-11-07 02:47:12 UTC**. No affected job was
  active and no other lease was acquired/released by this operation.
- Controller SHA256 **`81a9ad2dd68dfeee0e01878f4bf8b7f1dc797ea1519a3fcc94424eaffcbf4f65`**;
  MATLAB SHA256 **`0c744a3264431e6e195ba9da059efc0b9c750ecb28d9a525b2f7d35bb71daccd`**.
- Only **23 metadata MATs in twelve unpublished standard project copies**,
  exactly **636,618 planned path changes**. No image MAT/HDF5, original MAT,
  publication, source deletion or catalog cutover permission exists.

The controller has already revalidated all twelve complete native original
inventories, and native source/copy/backup hash and real-owner checks passed
for all 23 files. **3,715,167,522 bytes of backups** are retained under
`/homes2/maliavko/DetecdivHub/.migration-backups/alexander-2024-20261105/relink-batch1-20261106`.
At 21:01 UTC, **12/23 files** have per-file scientific/type/annotation and
save/reopen receipts; this is **not** terminal success of all 23. The next
large main includes GPU data. The scientific visitor now snapshots actual
`gather` contents with GPU class/underlying type and excludes GPU from the
simple-cell fast path, while leaving the saved GPU objects unchanged.
MATLAB runs without selecting a GPU. Every expected path count, canonical
non-path comparison, save/reopen and lease-deadline check precedes replacing
a target. Partial results, temporary MATs and backups must survive any failure.
See `relink-batch1-preparation-checkpoint.json`.

**Inspect state.json, controller.exit/log, matlab.exit/log, progress.json,
visitor-progress.json, result.json and modified-NN.json before any action.**
Do not edit its pinned scripts/request, duplicate it, replay it on partially
modified copies or infer success from one counter. Original copy hashes remain
before-relink evidence: use after hashes in modified/result receipts for the
changed copies. Copies stay incoming while MATs record future final container
paths; translate final-container prefixes to incoming **in memory** for any
prepublication inspection. Do not resave merely to inspect or try nonexistent
final filesystem paths as though they were already published.

A hidden local watcher is running (PID **95344 at launch**, identity verified;
check identity again before any intervention), polls once per minute and
releases **only these own leases** after a real terminal receipt. It finalizes
only this child batch. Public launch receipt `relink-batch1-watcher-launch.json`;
private admission/plan/status/log are `runtime/relink-batch1-*`, terminal public
receipt `relink-batch1-lease-finalization.json`. Do not duplicate it. If it is
not running, inspect real terminal status before manually finalizing only its
holder/lease IDs. A failure with result.json absent can still have modified
files: always count modified-NN.json, not just a final files_relinked=0 summary.
On expiry/failure, preserve partials and design a bounded recovery; never
change a running controller's deadline/request to skip its guard.

After success, compare the saved GPU type/content descriptors with the
original readiness evidence as a postflight corroboration, using after hashes
and a new readonly request. Then raw full checksum, both actual final hosts,
API continuity, fresh admission and publication/catalog gates are still
required. No source copy may be removed because this child relink finished.

### Other preparation finished or newly running

Identity batch4 is **finished, exit 0, overall success false**: five projects
fully pass (73 FOV / 14,357 ROI / 1,026 raw samples, all crops exact); RMng and
anof_dbud4 fail at zero completed FOV because of the actual sidecar layouts
above. See `identity-batch4-completion-verification.json`; do not replay its
five successful projects or conflate loop exit 0 with all-project success.

Persistent metadata batch3 is **finished, exit 0, zero unknown path**:
25,963 MAT/JSON metadata files, eleven future MAT edits / 168,289 planned
changes, across cres/cres_ARS1/X5152moth/cre16. This is separate from the
23-file relink now running. See `persistent-batch3-completion-verification.json`.
FIG/other sidecars and readiness of these future files still need review.

New readonly tasks (do not duplicate):

- **Identity batch5**, only original verified copies 24–28: tmux
  `alex2024-identity-audit-batch5-20261106`, private `identity-audit-20261106-batch5/`.
  Same per-project isolated reader SHA256
  `e8612456dab6354be1158c86581fe64bdd1effee96964874b3366914939166d8`.
- **Persistent metadata batch4**, five new successes from identity batch4 only:
  tmux `alex2024-persistent-paths-batch4-20261106`, private
  `persistent-path-audit-20261106-batch4/`, script SHA256
  `9368ceb1dfa95b76d26d429a7ef19006cfd0b426cd21915994a4b67711592b84`.
- **Availability-qualified identity** of six bounded variants (RMng, anof_dbud4,
  three final normal copies and the main-only loxP15): tmux
  `alex2024-availability-variants-20261106`, private
  `availability-variants-identity-20261106/`, script SHA256
  `9097856bd860e509c575444df40d86a818ad39b655776460b3046fcdda307cb1`.
  It records exact source-manifest presence/absence for every ROI, preserves
  image-only/partial-result/empty-placeholder states, reads raw samples and
  compares actual available extracted crops. Missing derived files are not
  called restored/readable. No successful prior FOV is replayed. Inspect each
  result and exact baseline; loop success is not a waiver of further gates.

Gilles HDF5 attributes/external references and typed run-snapshot path policy
remain pending as described below. Existing original/legacy missing-main
relevance review remains separate. Raw/source cleanup is still forbidden until
all applicable year-specific gates are truly satisfied, using a new tested
2024-only controller with native manifest/no-follow/per-file receipt checks.

## Historical follow-up — 2026-11-06, starting 14:32 UTC

**This checkpoint supersedes the earlier running states.** At 14:41 UTC,
2024_1 is healthy in copy: **70.0278%**, same runner PID 187291 and rsync 188902.
Twenty-three of the 32 standard project copies are fully verified; project 24
`anof_270624_HpaRDNtest` is in verification. No terminal raw SHA1, relink,
publication, preferred-location switch or 2024 source retirement has occurred.
No 2024 space reclaimed. An own-batch-only database checkpoint was recorded
without changing status or writing project/location tables; see
`batch-checkpoint-20261106-1432.json`.

### Nine-image payload search completed, no historical image recovered

`historical-name-search-20261106/` finished with exit 0, success true. It read
names of **7,419,315 primary files** and **2,488,842 secondary files** under
the two stated Alexander payload roots. Secondary has no filename matches.
Primary has three namesakes, all under unrelated **2025_2 acquisitions**;
none is a copy of the 060224 acquisition. No pixels were substituted and no
file was modified. This is not a search of inaccessible NAS snapshots or all
possible backups. The fifteen pre-existing absences and 45 old cache-size
exceptions remain exactly the already-proven native availability baseline.
See `historical-name-search-completion-verification.json`.

- Result SHA256: `7215e3c355e2c60b5bfaf5717b3f4a7f7ca24cd071b96d7d264edc9d158e8f87`
- Primary receipt: `68a66d9233a41cdfea26d41d238a81cc9da7b45b45b59a92ed5488171a2d1b42`
- Secondary receipt: `d43a70e9c8887d5961b34c7a7e7484333ed3ca2fcb12771caf6d0f0dca856b80`

Do not replay this completed search. Do not claim the fifteen images recovered
or require erasing their scientific cached references to pass migration gates.
Final readers must enforce exact historical availability parity and reject any
new difference, alongside all ordinary full-content and scientific gates.

### Second metadata/FIG batch and identity batch3 completed

`persistent-path-audit-20261106-batch2/` finished with exit 0, success true:
**46,784 MAT/JSON metadata files**, excluding 35,002 image MATs, zero unresolved
metadata paths. **Nineteen MATs / 503,334 exact planned path changes**. Together
with batch1 this is **55,636 metadata files / 23 MATs / 636,618 planned changes**
across twelve projects. Nothing was saved. See
`persistent-batch2-completion-verification.json`, result SHA256
`b50266cc462af82dc4e2fb09c35fe9c78955b9d12db2507c8e8dd02455a21471`.
The complete receipt hash is in that public proof.

The bounded **12 FIG** scan `figure-path-audit-20261106-batch2/` also finished,
exit 0. Only four absolute strings, the two stored FileName properties in
each of `rlsbydhy0325.fig` and `rlsbydhywt0325.fig`, same archival destination
classification as the prior FIG loader proof. Preserve all FIG/PNG/DS_Store
bytes; no callback or openfig executed. See
`figure-batch2-completion-verification.json`, receipt SHA256
`302cbabd24ec414fd16aeef53708a37a4a12706c1264f852bb0d429293db6c55`.
Do not treat the scanner's loop success as automatic acceptance of unknown
paths: the four values were explicitly classified using the loader evidence.

`identity-audit-20261106-batch3/` is finished, exit 0 and success true:
`anof_210524_cre16`, **30 FOV / 11,083 ROI / 360 raw samples**, all 30 crops exact.
See `identity-batch3-completion-verification.json`, result SHA256
`b15ee80f7dd9b86af722979e0ea6b27b789256e74f91af2d53b7d8304d7feb38`.
No replay of this project or any earlier completed FOV.

### Both special-layout identities now verified without changing formats

`exception-identity-20261106/` completed its loop (exit 0) with **qp success**
and a Gilles validator-format failure, not a data-copy failure. Qp's one FOV
passes saved-name/size/constant timestamp and raw pixel reads. Its 128-byte
empty MAT image placeholder remains byte-exact (no extracted ROI image was
invented); no results/data added. See `exception-identity-partial-verification.json`.

The Gilles failure was the reader expecting `im_*.mat`; all **58 actual verified
images are `im_*.h5`**. A bounded readonly layout inspection is finished in
`gilles-roi-layout-20261106/`; its original MAT-only match counts must not be
misinterpreted as missing HDF5 images. Then the HDF5-aware
`gilles-hdf5-identity-20261106/` finished with exit 0, success true:
**2 FOV / 58 ROI / 36 raw samples / 2 exact crops**. Protected engine ROI.load
read the real HDF5 data from the copied stage with paths translated only in
memory. Source/copy main hashes unchanged, all original layouts/Hub owners
preserved, no HDF5 image file rewritten. Qp was not replayed. See
`gilles-hdf5-identity-completion-verification.json`, result SHA256
`38fad3529aa509ea3598926c8737e7ac126743b9769c916e89ac9bf723ff2edf`.

This brings completed identity preparation to **16 projects / 201 FOV /
55,709 ROI**, separately from the two historical-baseline projects. Still
required for Gilles: review HDF5 attributes and any external/virtual dataset
references without loading or rewriting image arrays solely for path changes.
Never impose a MAT/results sidecar assumption on this layout.

### Newly launched bounded readonly preparation

- **Identity batch4**, projects 17–23 only: private
  `identity-audit-20261106-batch4/`, tmux `alex2024-identity-audit-batch4-20261106`.
  Seven projects, script SHA256
  `e8612456dab6354be1158c86581fe64bdd1effee96964874b3366914939166d8`.
  Check each success/error and partial fields; exit 0 is only loop completion.
- **Persistent metadata batch3**, four newly reviewed standard-layout copies:
  cres, cres_ARS1 (explicit historical parity, not strict cache success),
  X5152moth (empty-path variant) and cre16. Private
  `persistent-path-audit-20261106-batch3/`, tmux
  `alex2024-persistent-paths-batch3-20261106`. Script SHA256
  `9368ceb1dfa95b76d26d429a7ef19006cfd0b426cd21915994a4b67711592b84`.
- **Special-layout metadata audit is now finished**, private `exception-path-audit-20261106/`,
  tmux `alex2024-exception-paths-20261106`, same persistent-reader script hash.
  Uses actual qp/Gilles path aliases and exact verified manifests. HDF5
  attributes/external references require separate review; do not blindly call
  image HDF5 files irrelevant merely because this MAT/JSON reader skips them. It
  completed exit 0, but has **15 Gilles files with unclassified paths**: the
  main's `parsedData.folder` and 14 `run.json`/`run_params.json` execution records.
  See `exception-path-audit-completion-review.json`; no complete path gate yet.
  Qp's one main has no unknown path. Do not replay the scan. Inspect the actual
  consumers before choosing a typed path policy: historical execution snapshots
  and Hub `/data`/`X:` client mappings are not equivalent to active resource paths.
  Keep historical provenance intact unless a reviewed consumer requires adaptation.
  The additional `X:/Alexander/data/2024_2/281124_X5152bud4s` alias names the same
  validated raw acquisition; it needs an explicit canonical mapping for active
  use. Three external Gilles pipeline/model paths exist at their canonical
  `/data/Gilles` locations (size preflight in `gilles-external-dependency-preflight.json`);
  no external pipeline/model or other user file was moved or modified.
- **Scientific type readiness**, private `relink-value-types-20261106/`, tmux
  `alex2024-relink-value-types-20261106`, only the 23 path-bearing MATs from the
  first twelve projects. Script SHA256
  `4689cff8762f91c5aea03f187433250b86dd75b950945921de5ab61d2f3c5982`.
  Four files in the first checkpoint, **4,258 gpuArray values successfully
  gathered/fingerprinted**, no function handles or nonpublic persistent state
  in that completed subset. Progress reached 19/23 files at 14:50 UTC.
  The rest are still pending: do not equate the subset with all 23. The audit never saves a scientific file. Limited-GPU
  warnings exist; gathering content is a readiness check, not scientific
  save/reopen verification or permission to discard GPU class/type.

Read their progress/result/partials/log tails and controller.exit before any
new launch; never duplicate or edit these pinned running scripts. All earlier
discovery/legacy/identity1–3/metadata1–2/FIG/search/exception-copy jobs are done.
Next actual relink must have fresh jobs/indexing/own leases, verified backups,
exact expected changes, full canonical scientific/type/annotation comparisons
before/after/save/reopen, including actual GPU content. The old 2023 visitor's
numeric/simple-cell fast paths need explicit GPU content handling if reused;
never compare only opaque limited GPU handles, drop arrays or claim a roundtrip
from a readonly type inventory. No relink or retirement controller was launched.

## Historical follow-up — 2026-11-06, 10:50 UTC

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
