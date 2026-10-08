# Sauvegarde catalog ingestion — 2026-10-01

## Scope and separation

The user requested catalog ingestion first; raw associations are a separate
later phase. No project/data migration is authorized before Alexander agrees.
Scientific sources remain unchanged on secondary NAS `10.20.8.250`, through
the read-only compute-host mount `/mnt/detecdiv-secondary-sauvegarde`.

The inventory covers loose project candidates under `Alexander`,
`Alumni/Sandrine`, `Alumni/basile`, and `Fred`. The latter contains a Duplicity
backup, not loose projects; the backup was neither unpacked nor modified.
Synology metadata, image/ROI payload directories, project backup/temp MATs,
and non-project stores are excluded. This is not a claim that every MAT file
on the share is a project or that project backups were individually ingested.

Native read-only discovery completed without errors: 3,908 directories visited,
1,493 candidate MAT files. A second pass revisited initially unconfirmed paired
containers and found one additional legacy project inside a folder whose
sibling MAT contained an unrelated `activated` variable.

## Format validation

MATLAB reads headers only, without `load` or saving scientific files:

- Modern project: a nonempty scalar `shallow` object.
- Legacy project: a `*-project.mat` file with scalar struct `timeLapse`.
- Empty/unrecognized files: retain in the review report, not in the project catalog.

The initial native header reader exposed a row/column logical-expansion bug
for legacy headers with multiple variables. The helper was corrected; its
already-retained name/class/size evidence is normalized by
`finalize_sauvegarde_headers.py`. Real read failures are not promoted to valid
projects. No MAT is rewritten by that normalization.

## Catalog application

The existing direct-registration service is used through the administrator's
SSH channel, with explicit owner mapping to existing accounts only:

- `Alexander` → `alexander`.
- `Alumni/Sandrine` → `sandrine`.
- `Alumni/basile` → `Basile` (case-sensitive existing Hub key).

Registration is private, read-only, and `health_status=warning`; directory
footprints/dependencies are not yet fully inventoried. `total_bytes` at this
stage is MAT-only, not the complete project or raw footprint. Legacy projects
are marked as sharing storage with raw data to avoid assuming their parent
folder is exclusively derived output.

Existing canonical file locations are kept, including the 18-project 2023
pilot and its one previously confirmed raw link. This ingestion does not
automatically link, ingest, or preview raws. Each new record includes source
path, header format, source size/time, operator, batch, and pending-lineage
metadata. Application is checkpointed in batches of 100 and can be rerun
without recreating already-registered source locations.

First wave: 1,413 validated entries applied while the remaining large modern
headers were inspected. The complete final dry-run selects 1,486 projects,
preserves 1,431 already-registered locations, and proposes only 55 additions.
The final application is complete (`complete=true`, no pending headers).
It added the last 55 entries and preserved the other 1,431. Across both waves,
1,468 new records were created and the 18-project pilot was preserved.
The authoritative result is `sauvegarde-registration-20261001.json`.
Independent live database read-back verified all 1,486 expected IDs, owner
assignments, private visibility, warning state, and read-only source locations;
the one previously confirmed raw link is intact, with no new links created.

| Namespace / owner | Modern shallow | Legacy timeLapse | Total selected |
| --- | ---: | ---: | ---: |
| Alexander / `alexander` | 150 | 0 | 150 |
| Alumni/basile / `Basile` | 0 | 558 | 558 |
| Alumni/Sandrine / `sandrine` | 0 | 778 | 778 |
| Total | 150 | 1,336 | 1,486 |

Seven MAT candidates are excluded and preserved: four tiny/empty shallow
headers belonging to Alexander, one zero-byte Basile project, and two unrelated
MAT files (`activated` and `MeanFluo`). Unknown/empty project headers still
require review, not deletion. The four Alexander candidates include a copy in
`analysis_after_29092023`; they are not four necessarily distinct experiments.

The 1,486 count is catalog locations/resources, not a claim of 1,486 unique
scientific experiments after content-level duplicate reconciliation.

## Evidence and safety

- Discovery: `sauvegarde-project-discovery-20261001.json`.
- Header facts/classification: `sauvegarde-project-headers-20261001.json`.
- Initial dry-run: `sauvegarde-registration-dryrun-20261001.json`.
- First catalog wave: `sauvegarde-registration-first-wave-20261001.json`.
- Second-pass header: `sauve-extra-headers-20261001.json`.
- Final dry-run: `sauvegarde-registration-final-dryrun-20261001.json`.
- Final application: `sauvegarde-registration-20261001.json`.
- Independent read-back: `sauvegarde-catalog-verification-20261001.json`.

No scientific file was copied, moved, relinked, rewritten, or deleted.
No worker implementation, worker configuration, running job, or Hub service
was changed or restarted. Only this task's own Windows header-reader processes
were stopped when the same read-only audit was moved closer to the NAS.
The compute-host audit runs at CPU nice 19 / idle I/O priority. Secondary
home provider activation, DSM quotas, and backup policy are unchanged.
