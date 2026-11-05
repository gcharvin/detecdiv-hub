# Final Alexander 2023 API cache retirement

## Completed — 2026-11-05 12:39 UTC

The full authorized migration and retirement scope is complete. All Alexander
2023_1/2/3 scientific source payloads and their temporary API caches are retired
from `/data`. Original Sauvegarde projects, migration backups, NAS system
metadata, other users/years and the Gilles JSON project remain preserved.
Keep the homes2 raw staging namespaces used by saved project paths.
**Do not replay any transfer, relink, publication, test or retirement controller.**

The user installed the readonly personal homes2 mount on **webserver-labo**.
The API now persistently binds that view readonly alongside `/data`, using the
same image. PostgreSQL and all worker PIDs/fingerprints are unchanged; unrelated
calculations continued. The mount service is active/enabled and HTTP health is
200. No engine/default/provider/quota/backup policy or SMB credentials changed.

All **1,926 files / 28,981,723,684 logical bytes** passed complete cache/homes2
SHA1 comparisons. At 12:17 UTC their existing references were switched
transactionally to homes2. Complete actual API content responses passed before
retirement and again after deletion at 12:38 UTC, with identical hashes for all
1,926 files. Six representative media reads per host passed on Linux and in the
actual limited `GMGM\Charvin-Admin`, session 1 Windows worker session. Its
diagnostic task/scripts were cleaned up; do not query or recreate that task.

At 12:27 UTC the exact native cache manifest was retired, with a unique receipt
for every unlink. Every operation checked native type, size, exact nanosecond
mtime, inode, UID/mode, ancestor no-follow resolution and own lease deadline.
All ten isolated guard fixtures passed. The final native audit independently
confirmed zero remaining payload files in all six exact source/cache roots,
excluding only NAS system metadata. No scientific file changed in this phase.

| Retirement | Measured increase in available bytes |
| --- | ---: |
| Scientific 2023_1 | 737,304,760,320 |
| Scientific 2023_2 | 3,577,917,788,160 |
| Scientific 2023_3 | 5,967,222,644,736 |
| Final API caches | 28,880,097,280 |
| **Sum** | **10,311,325,290,496** |

The final cache window measured native available space from 10,979,228,876,800
to 11,008,108,974,080 bytes: **28.880 GB recovered**. The cumulative figure is
**10.311 TB of measured deletion-window gains**, not the net NAS usage change
across the whole multi-day migration: temporary caches were created during the
operation and unrelated users also wrote data. Logical file sizes differ from
available-space measurements. Older 2023_1 files already absent at the start
are not counted.

At 12:39 UTC batch `468070ce-1489-456e-9175-27f188156e41` became
`api_cache_retired_verified` and released all fourteen own project leases.
There are zero own active leases and zero live cache URI references. Complete
before/after catalog snapshots preserve the same 109 raw identities, 1,820
preview Artifacts and fourteen projects, including positions, owners, notes,
ACLs/groups, locations and relationships. All 1,926 affected references now
point to homes2. The six-hour follow-up was deleted after completion.

### Completion evidence

- `final-verification.json`: terminal batch, space, clients and released leases.
- `cache-retirement-receipt-verification.json`: full manifest/receipt equality.
- `post-retirement-completion-verification.json`: every actual API body streamed.
- `linux-client-verification.json`, `windows-client-verification.json` and
  `windows-client-cleanup.json`: actual client checks and bounded cleanup.
- `final-primary-payload-verification.json`: all six source/cache payloads empty.
- `final-catalog-health-verification.json`: homes2 references and catalog closure.
- `final-runtime-verification.json`: HTTP health, persistent readonly mounts,
  unchanged API image/PostgreSQL/workers.
- `api-receipts-archive.json`: complete pair/before/after receipts archived on
  the VM under the private work directory, with matching hashes.

Private work on compute and webserver-labo is under
`/home/charvin-admin/storage-transfers/alexander-api-cache-retirement-20261105`.
Full manifests, catalog snapshots, requests and receipts remain private there
and in ignored `runtime/`; Git contains only controllers and public summaries.
The native manifest SHA256 is
`d004542da2bd51f483bbd58c1760a24fef4e17be29eef7cc45c3c04563baf7a8`;
unlink receipt SHA256 is
`fe43825cfdbbb726a42392781cde096e7168775bf6aa7eb3b300872877f9cc83`.
Both actual API stream receipts have SHA256
`b97f1b4434975ad0f3eab3f6fad4b8b72fe935f21446edeb4d67c36c4cbcb508`.

## Historical preparation checkpoints — superseded by completion above

## Current checkpoint — 2026-11-05 12:17 UTC

The user ran the prepared sudo installer on **webserver-labo**, successfully
mounting only `/homes2/maliavko/DetecdivHub` readonly from compute. No NAS/SMB
identity or private key contents were read or changed. Root key reuse was by
existing pathname; the user kept the sudo password in their own terminal.

At 11:54 UTC the API was recreated with the **same image**
`sha256:2326824f70336590f16c2256da7e213a7322d7fa626a50760be38543f2a3516b`,
adding only the readonly homes2 bind to the persistent base Compose file.
All other parsed Compose values were preserved, and the old file is backed up
privately. PostgreSQL's container stayed unchanged. Worker PIDs/fingerprints
were identical before/after and the unrelated running pipeline continued with
a live heartbeat. No worker/engine/default/provider/quota change occurred.
The original 2023_3 API receipts were archived on the VM before replacing their
old container. Evidence: `api-bind-activation.json`, `api-bind-admission.json`,
`api-bind-workers-verification.json`.

The native cache inventory contains exactly **1,926 files / 28,981,723,684
bytes**, no extra/symlink/hardlink payload. It is archived on compute in
`/home/charvin-admin/storage-transfers/alexander-api-cache-retirement-20261105/source-before.jsonl`,
SHA256 `d004542da2bd51f483bbd58c1760a24fef4e17be29eef7cc45c3c04563baf7a8`.
Per-file native inode, UID, mode, size and exact nanosecond mtime are retained.
Full manifests/plans and catalog snapshots stay private, not in Git.

At **12:14 UTC**, complete cache/home SHA1 comparisons passed for all 1,926
files inside the actual API container. Receipt SHA256:
`ce435fea11f8370330e969eab7ddcc7f7e8013c0f42a54a2f31f7893c2c72276`.
The first local admission comparison stopped before any database mutation
because the VM's SSHFS/SFTP timestamps have whole-second precision. All 1,926
observations were proven to equal the exact native timestamp floored to seconds;
sizes and full content hashes match. Only that cross-view precision check was
corrected. The native inventory/unlink still require exact native nanoseconds,
inode, UID and mode. No timestamps or contents were changed.
Evidence: `pairs-completion-verification.json`, `timestamp-precision-verification.json`.

Batch **`468070ce-1489-456e-9175-27f188156e41`**, holder
**`alexander-2023-api-cache-retirement-20261105`**, acquired fourteen own project
leases until **16:16 UTC** after fresh jobs/indexing/lock checks. Unrelated
calculations were preserved. The batch holds complete before snapshots of
109 raws, 1,820 preview Artifacts and fourteen affected projects, including
positions, notes, ACLs, groups, locations and relationships.

At **12:17 UTC**, one transaction switched all **1,926 existing URI references**
to the fully verified, currently readable homes2 files. No identity, position,
note, owner, scientific file or project metadata was changed. Original caches
are still present at this checkpoint. Batch status is
`home_uris_switched_verification_pending`.

The complete actual content-handler stream is now running inside the API in
`/tmp/detecdiv-api-cache-retirement-20261105/handlers` (PID 99 at launch).
Inspect its `state.json`, `controller.log`, `receipt.jsonl` before any action.
Do not launch a duplicate. `media_operations.py status handlers` archives the
receipt locally when complete. The prior `pairs` task is completed; never
relaunch it. Both requests pin script SHA256
`55471e9d2d1f1a3f0f4f71f68f18c4673c51a8b41cb340d9c191eedf14bf607f`.

## Guarded next steps

After actual home handler success, `cache_operations.py verify` checks the
complete catalog snapshots and own leases. `prepare` then records fresh actual
serving/jobs/leases gates, and `launch` starts the exact native cache retirement
in a new compute `retirement/` subfolder. Do not replay any partially executed
operation. The leaf is bounded to only these three cache roots, not raw/project
data, and checks the entire current manifest before unlink and every file's
type/size/nanosecond mtime/inode/UID/mode plus own-lease deadline at each unlink.
It retains directories and NAS system metadata, emits a receipt per file and
measures native available space before/after.

The original native inventory script was SHA256
`557df99ab9f4efb66c03885edfe1d5e2e98fddb8d89bdd6179ca8ca66cd8bdba`.
The execution leaf now expresses the identical total as a derived constant for
isolated fixtures, SHA256
`8ca66af24b3ba7cdb728f5ef4df3c43ac0b55b07dae5a1f786103dc3dcd995dd`.
All ten isolated cases passed: inventory, extra/changed payload, directory/root
symlink, hardlink, wrong digest, expired deadline, deadline after one unlink and
exact valid unlink preserving NAS metadata. No production file was touched by
those fixtures. The inventory script and its initial receipt remain archived.

After complete retirement, `cache_operations.py receipt` must match every
unique receipt to the entire 1,926-file manifest and prove all three payloads
empty. Then `media_operations.py launch post-retirement` streams every real home
handler again. `client_operations.py stage/status` checks six representative
home media files on compute and the actual limited GMGM\Charvin-Admin/session1
Windows session, with a distinct diagnostic task. `cache_operations.py finish`
requires all post-gates, preserves every catalog snapshot and releases only its
own fourteen leases. Clean only that diagnostic via `client_operations.py cleanup`.
Keep its request/results. Archive full API receipts on the private VM before any
later container replacement. Update documentation and commit/push after scanning
only explicit public proof/controller files. On full completion, remove the
six-hour automation `migration-alexander-2023-3-contr-le-6-h` and report measured
additional and cumulative space.

The active destinations retain the established namespaces:

- 2023_1: `/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_1`
- 2023_2: `/homes2/maliavko/DetecdivHub/raw/2023_2`
- 2023_3: `/homes2/maliavko/DetecdivHub/raw/.incoming-alexander-2023-20261008/2023_3`

Keep the scientific originals, other users/years, snapshots and backup policies.
Never print credentials, private keys, full environment or private catalog notes.
