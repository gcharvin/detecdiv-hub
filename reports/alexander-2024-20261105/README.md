# Alexander 2024 migration — started 2026-11-05

The user explicitly authorized copying **2024_1 then 2024_2** to homes2,
inventorying and relinking associated project copies, and retiring verified
`/data` duplicates after complete copy/access validation, **keeping original
projects**. This authorization persists: do not ask for it again. No other
year/user, original project, NAS snapshot, backup policy, quota, default engine
or SMB credentials may be changed. The completed 2023 migration is separate;
never replay any of its one-shot scripts.

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
