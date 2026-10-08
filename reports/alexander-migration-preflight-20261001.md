# Alexander migration preflight — 2026-10-01

## Live state verified

- Active Hub API/DB: `webserver-labo`, Docker Compose containers running.
- Main NAS: `10.20.11.250`; `/data` maps to `//10.20.11.250/DATA`.
- Main free capacity: 3,905,916,420,096 bytes (3.91 decimal TB).
- Secondary NAS: `10.20.8.250`; free capacity 50,864,544,145,408 bytes
  (50.86 decimal TB / approximately 46.26 TiB).
- Secondary home parent: `/homes2` maps to `//10.20.8.250/homes`, read-only,
  SMB identity `Fred`. The successful interactive preparation added the nested
  read-write `//10.20.8.250/home` mount at `/homes2/maliavko`, authenticated as
  `maliavko`; a NAS-side probe owner check returned `maliavko`.
- Hub account: `alexander` ↔ DSM `maliavko`,
  `/homes2/maliavko/DetecdivHub`.
- Active VM database still contained a 10 TB desired quota after the user
  reported setting 60 TB. This was aligned to exactly 60,000,000,000,000 bytes
  and recorded through a `quota_target_aligned` provisioning event.
- NAS quota of 60 TB is user-reported. It is not independently verified: the
  secondary DSM client has no credentials configured. Hub quota status remains
  `desired`; provisioning is `planned` and the provider is inactive.
- Initial owner-filtered Hub catalog: 277 raw datasets, 38,909,885,111,170 catalog bytes
  (35.39 TiB), and zero DetecDiv projects owned by Alexander. Do not sum raw
  locations: several roots expose the same dataset.

## Native size inventory

The initial bounded scan measured complete child directories before stopping
without a parent total. A low-priority native NAS continuation excludes those
completed directories and adds their sizes back when reporting ancestors.
Its durable state is `alexander-size-live-20261001.json` (local Python PID
34588). The total must be read only once `complete=true` and `exit_code=0`.
The scan counts apparent payload bytes and excludes Synology `@eaDir`, recycle
bins and snapshots; it does not measure physical blocks reclaimed by deletion.

Both native scans completed with exit code 0 and no diagnostics:

| Source tree | Apparent payload bytes | Decimal TB |
| --- | ---: | ---: |
| `/volume1/DATA/Alexander` | 57,319,663,161,801 | 57.32 |
| `/volume1/homes/maliavko` | 4,900,768,630,692 | 4.90 |
| Combined, before duplicate reconciliation | 62,220,431,792,493 | 62.22 |

The secondary free space measured earlier is only 50.86 TB, and the desired
home quota is 60 TB. A wholesale unchanged copy cannot fit either boundary.
Select verified batches and preserve a free-space margin; do not infer that a
quota reserves capacity. These numbers exclude secondary `Sauvegarde` and
other copies already there, and do not deduplicate identical scientific data.
Detailed completed state is in `alexander-size-live-20261001.json` and
`alexander-primary-home-size-20261001.json`.

## Read-only 2023 project pilot

Source: `S:\Alexander\analysis` =
`//10.20.8.250/Sauvegarde/Alexander/analysis`.

There are 113 top-level MAT files (19,531,544,109 bytes), including 20
filename-dated 2023 candidates (5,817,109,998 bytes). MAT headers were inspected
locally with MATLAB R2026a. Two tiny files need review; the reader issued an
unknown MAT-variable warning for them. Do not automatically classify them as
corrupt or delete them.

Eight candidate names correlate with existing raw acquisition names after
normalizing dates and acquisition-part suffixes. These are candidate links,
not confirmed lineage. The manifest is
`alexander-2023-project-pilot-20261001.json`; MATLAB findings are in
`alexander-2023-mat-inspection-20261001.json`.

The smallest nonempty sample, `anof_03112023_dhy_fobs_ah4s.mat`, loads as
`shallow` and has 18 FOVs. Its project IO points at the old
`Z:\Alexander\analysis`, and its FOV paths point at the old
`X:\maliavko\data\03112023_dhy_fobs_ah4s\...`.
The corresponding raw acquisition exists today at
`/homes/maliavko/data/03112023_dhy_fobs_ah4s` on the main NAS, not under the
legacy `/data/Alexander/data/2023_3` root. Positions 0 and 17 were verified
as present. Native acquisition size: 636,887,215,927 apparent bytes.
It has no matching acquisition name among the 277 owned catalog raws.
This is concrete evidence that global drive-letter replacement is unsafe and
that the owner's primary NAS home contains relevant data outside this scan.

## Interactive mount setup completed

The syntax-checked helper is staged at
`/home/charvin-admin/prepare_alexander_homes2_20261001.py`.
Local and remote SHA-256 are
`13b8c4bd844848a5240addb5585de08fc8de26f92e621f8e768a67b0c8300a7f`.
The parent mount guard was exercised successfully against the real mount.
The user successfully ran the credentialed write/ownership pilot using their
own interactive password entry and local Linux sudo authentication:

```powershell
ssh -tt detecdiv-server "sudo python3 /home/charvin-admin/prepare_alexander_homes2_20261001.py"
```

This prepares the per-user write mount, validates NAS ownership, creates only
the Hub destination directories, mounts Sauvegarde read-only for cataloguing,
and saves the new mount entries with a backup of fstab. It does not change
worker services or activate the secondary provider.

The fstab backup is `/etc/fstab.detecdiv-homes2-20261001T083814474007Z.bak`.
The source mount and destination mount were independently checked after the
run. Generated systemd mount/automount definitions exist; no reboot test was
performed. The interactive helper leaves its manually mounted destinations
active; generated automount units need not be active until boot. Root-only SMB
credentials are not stored in the Hub. Provisioning audit event
`c790d02a-78cc-4b5f-b7d8-7b52f81aa56b` records the successful ownership test.

## Catalog and lineage progress

18 header-valid projects were registered privately under owner `alexander`,
on storage root `alexander-sauvegarde-analysis`, host `detecdiv-server`.
All 18 catalog locations were independently verified as `readonly`.
The existing direct-registration service was used without launching a scan;
health remains `warning`, because folder inventory and legacy raw aliases are
not fully reconciled. Reported size is MAT-only, not the entire project.
The two tiny ambiguous files were excluded, not deleted.

MATLAB subsequently loaded all 18 valid projects read-only and extracted their
FOV paths and counts. Directory candidates were found for all saved paths in
16 projects; two have no candidate directory among the examined roots.
This is path evidence, not content-level proof of relocated resource identity.
Only the exact saved Linux paths in `anof_26102023_yam76s_100ms` were linked to
catalog raw `d43dff09-d53f-4d27-9264-7eef08ebb444`. No name-only links were made.
Four November projects refer to one acquisition, and three others to another:
do not copy the same raw separately for each project.

Reports: `alexander-2023-registration-20261001.json`,
`alexander-2023-lineage-20261001.json`, and
`alexander-2023-raw-reconciliation-20261001.json`.

## Pilot copy simulation completed

The read-only `rsync --dry-run` for `anof_03112023_dhy_fobs_ah4s` includes both
the MAT and its paired project folder. It completed successfully: 4,637 regular
files, 19 directories, 64,013,022,845 bytes. Destination would be
`/homes2/maliavko/DetecdivHub/projects/anof_03112023_dhy_fobs_ah4s/`.
The destination directory was **not** created; no bytes were copied.
Its primary-home raw acquisition (636,887,215,927 bytes) is not included in
those 64 GB and remains at its current location.
See `alexander-project-copy-dryrun-20261001.json`.

Next: reconcile the other aliases and project dependencies, choose a batch
that fits actual free capacity, then copy and verify before switching locations
or rewriting copied MAT files. `Sauvegarde` stays preserved; no source deletion
is authorized by this preflight. Do not activate the provider as a general
authoritative write target until backup coverage and a restore test exist.

Remote changes are limited to the quota record/audit, the user-executed mount
setup, mount audit, and the 18 catalog registrations/inspection metadata plus
one exact raw link. No scientific file was moved or rewritten, no Hub or worker
was restarted, and no scientific job was queued. Worker implementation and
deployments remain the responsibility of the separate thread.
