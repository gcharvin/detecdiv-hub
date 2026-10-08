# Secondary NAS user homes: staged rollout

## Target policy

- On `detecdiv-server`, `/homes` is the primary NAS (`10.20.11.250`) and
  `/homes2` is the secondary NAS (`10.20.8.250`). They remain separate mount
  roots and storage-root identities.
- All Hub users use their personal home on the primary NAS except Alexander,
  whose home is on the secondary NAS because of his existing storage volume.
- A user's home is also their personal file space. The DSM quota applies to
  the full home; Hub writes should stay under `DetecdivHub/` and not manage
  files outside that subtree.
- This is a destination policy, not permission to migrate existing data.

Do not repoint `/homes` or move catalog locations as part of mount setup.

The secondary NAS has the DSM user home `/volume1/homes/maliavko`. The Hub user
is `alexander`; the different login names are intentionally linked by
`user_storage_accounts`. The planned `DetecdivHub` subdirectory was absent at
the 2026-09-28 worker-side check; do not create it until the write identity and
ownership behavior are settled.

## Current staged state

- Hub root `user-homes2` points to `/homes2` on `detecdiv-server`.
- Provider `synology-secondary` is inactive and points to `/homes2`, whose
  Synology share is `//10.20.8.250/homes`. Its configured quota scope is the
  DSM per-user quota on `homes`, not a dedicated-share quota.
- The Hub catalog now records the expected per-user SMB source for both
  providers (`//10.20.11.250/home` and `//10.20.8.250/home`). This is metadata
  for the worker guard; it does not mount either per-user share or enable a
  provider.
- Alexander has a planned account under `maliavko/DetecdivHub` on root
  `user-homes2` (`/homes2`). On 2026-10-01 the active VM database was aligned
  to the user's selected 60,000,000,000,000-byte (60 decimal TB) quota. It remains
  `desired`: the user reports having applied 60 TB in DSM, but the secondary
  DSM API client has no credentials configured and cannot verify that setting.
- The independent `Alexander` shared folder and its 10 TB shared-folder quota
  still exist on the secondary NAS, but are not the Hub mapping or the selected
  home-storage strategy. Do not delete or use that share as part of this rollout.
- Alexander's legacy `/data/Alexander` account and every existing dataset or
  project location remain unchanged.
- `/home` on `detecdiv-server` is local to that Linux machine, not a third NAS
  mount. Workers can already reach it locally. Do not expose all of `/home` as
  a Hub storage root without choosing a narrower intended data directory; it
  contains host/user state and credentials.
- The existing `/homes` mount points to the primary NAS and is read-write;
  `/homes2` points to the secondary NAS and is read-only. A read-only live
  check on 2026-09-28 confirmed both authenticate to SMB as `Fred`; Linux
  `uid=`/`gid=` options do not make writes NAS-owned by the individual user.
- The live `synology-main` provider is active. Its accounts and the current
  main mount have not yet been reconciled with the target per-user write
  identity. Do not assume DSM per-user quotas are correctly charged for Hub
  writes through that mount.
- One Hub job was `running` at the live read-only check on 2026-09-28. No
  workers or services were restarted during this review.
- DSM account `detecdiv-worker` exists on the secondary NAS, but currently has
  no access to the `homes` share; its Read/Write permission is on the separate
  `Alexander` shared folder only. It cannot currently write through `/homes2`.
- Synology's default `homes` ACL grants administrators full access and limits
  Everyone to directory traversal. Synology warns that changing this default
  can break home access, administrator SSH-key login, or packages
  ([documentation](https://kb.synology.com/fr-fr/DSM/tutorial/default_permissions_of_homes)); do not grant worker rights on the parent share as a shortcut.
- The API refuses to queue home preparation while the provider is inactive.
  The worker-side mount guard has been copied to `detecdiv-server`, but worker
  services must be restarted after currently running jobs finish for it to
  take effect in all processes.
- Until that restart, do not launch a manual Micro-Manager ingest for
  Alexander: an already-running worker may still select the newest planned
  account. Automatic ingest is disabled in the current worker configuration.

The DSM API configuration for each provider is separate. The secondary
provider reads `DETECDIV_HUB_SYNOLOGY_SECONDARY_DSM_ACCOUNT` and
`DETECDIV_HUB_SYNOLOGY_SECONDARY_DSM_PASSWORD` from the API environment;
credentials must not be stored in `storage_providers.config_json`. Secondary
SSH administration is disabled unless explicitly configured.

## Mount preflight on `detecdiv-server`

### Interactive per-user pilot completed on 2026-10-01

The current parent `/homes2` still authenticates as Fred and remains read-only.
The account and provider are still planned/inactive. The user has separately
verified a write/list/delete through `smbclient //10.20.8.250/home -U maliavko`.
That interactive SMB connection does not create a worker-visible CIFS mount.

The subsequent interactive helper run succeeded: `/homes2/maliavko` is now a
read-write CIFS mount of the individual `home` share, authenticated as
`maliavko`. The NAS probe owner was verified as `maliavko`, and the exact probe
was removed. `DetecdivHub/projects` and `DetecdivHub/raw` exist. The aggregate
`/homes2` mount remains read-only. `Sauvegarde` is mounted read-only at the
path below. Persistent fstab definitions were verified; no reboot was done.
The fstab backup is `/etc/fstab.detecdiv-homes2-20261001T083814474007Z.bak`.
The Hub records a passed `per_user_mount_verified_20261001` audit event, but
the provider remains inactive and the account planned: the mount test does not
verify the DSM API quota or establish backup coverage.

`scripts/ops/prepare_alexander_homes2.py` is staged, with verified SHA-256, at
`/home/charvin-admin/prepare_alexander_homes2_20261001.py`. Run it from the
user's own interactive terminal so Linux sudo and DSM password prompts are
visible; do not send the password through the chat or store it in the Hub:

```powershell
ssh -tt detecdiv-server "sudo python3 /home/charvin-admin/prepare_alexander_homes2_20261001.py"
```

The helper mounts `//10.20.8.250/home` at `/homes2/maliavko`, authenticated
as `maliavko`; verifies a worker-user write and its NAS-side owner; removes
that probe; and prepares `DetecdivHub/projects` and `DetecdivHub/raw`.
It also mounts `//10.20.8.250/Sauvegarde` read-only at
`/mnt/detecdiv-secondary-sauvegarde` with the existing Fred read credentials.
It backs up `/etc/fstab`, adds only the two new mount entries, and reloads the
systemd unit definitions. It does not restart workers, activate the Hub
provider, copy datasets, edit MAT files, or delete source data.

The secondary NAS has 50,864,544,145,408 available bytes at the 2026-10-01
check; the main NAS has 3,905,916,420,096 available bytes. A 60 TB quota does
not reserve capacity. The main NAS native size scan runs at low CPU priority
with durable progress in `reports/alexander-size-live-20261001.json`.
Completed native inventories measure 57,319,663,161,801 bytes in
`/volume1/DATA/Alexander` and 4,900,768,630,692 bytes in the primary
`/volume1/homes/maliavko`. Their 62.22 TB combined apparent footprint, before
duplicate reconciliation, exceeds both the observed secondary free capacity
and the desired 60 TB quota. Plan migrations in verified batches.

The initial 2023 project pilot is documented in
`reports/alexander-2023-project-pilot-20261001.json`: 20 top-level MAT candidates,
5,817,109,998 bytes, and two tiny files requiring explicit metadata review.
Do not treat filename similarity as sufficient evidence for raw lineage.
18 valid projects are now privately registered to Alexander with read-only
source locations and a warning/pending-inventory status. MATLAB read their
internal paths; one exact server-path raw relationship was registered, while
legacy drive aliases remain candidates for review. No source was rewritten.
A bounded copy dry-run of the smallest MAT plus its project folder completed
(64,013,022,845 bytes); the destination was not created and nothing was copied.
See `reports/alexander-migration-preflight-20261001.md` for the current audit.

The subsequent all-years loose-project ingestion of `Sauvegarde` is complete:
150 Alexander shallow projects, 558 Basile legacy timeLapse projects, and
778 Sandrine legacy timeLapse projects are privately catalogued with read-only
source locations (1,486 total). Seven empty/unrecognized/result MAT candidates
remain unregistered and preserved; Fred's Duplicity archive was not unpacked.
No new raw associations, data transfer, home-provider activation, or worker
change accompanied this ingestion. See
`reports/sauvegarde-ingestion-20261001.md` and the independently verified final
catalog manifest. Alexander's agreement was reported on 2026-10-08. The initial
physical phase copies/verifies only `/data/Alexander/data/2023_{1,2,3}`, retaining
all original paths and catalog locations; see `docs/alexander_2023_transfer.md`.

The following older aggregate-mount procedure is retained as historical setup.

The following requires an administrator with local `sudo` access. First
confirm the existing credentials file is still the one used for the secondary
NAS's `/DataVault` mount. Do not print its contents.

Start with a read-only mount, restricted on the Linux host to the worker user
`charvin-admin` (UID/GID 1002). Check that `/homes2` is not already in use,
then create the mountpoint and add this entry to `/etc/fstab`:

```bash
findmnt /homes2
sudo install -d -m 0700 -o charvin-admin -g charvin-admin /homes2
sudoedit /etc/fstab
```

```fstab
//10.20.8.250/homes /homes2 cifs ro,vers=3.0,credentials=/home/fred/.smbcredentials_sv,uid=1002,gid=1002,file_mode=0600,dir_mode=0700,nobrl,nosuid,nodev,_netdev,nofail,x-systemd.automount 0 0
```

Then run:

```bash
sudo systemctl daemon-reload
sudo systemctl start homes2.automount
ls -ld /homes2/maliavko
findmnt -T /homes2/maliavko -o SOURCE,TARGET,FSTYPE,OPTIONS
```

The last command must show `//10.20.8.250/homes`, not the local filesystem or
the primary NAS. If it fails, leave the provider inactive and inspect
`journalctl -u homes2.mount -u homes2.automount` before changing anything.

## Write ownership check (2026-09-25)

A controlled SMB upload through the same `Fred` credentials as the worker
mount created a 14-byte file in `maliavko`. On the NAS, both the POSIX owner
and the DSM ACL owner were `Fred`, not `maliavko`. The exact test file was
removed after inspection. A read-write aggregate mount authenticated as Fred
would therefore not create Alexander-owned files and must not be treated as
enforcing Alexander's DSM user quota. Keep `/homes2` read-only and the
secondary provider inactive until a least-privilege write identity and quota
boundary are chosen and tested. Synology's Storage Analyzer defines its
per-shared-folder User Quota report by the files owned by each user
([Synology documentation](https://kb.synology.com/index.php/da-dk/DSM/help/StorageAnalyzer/view_reports?version=7)); a shared worker SMB identity must not be assumed to charge writes to `maliavko`.

The selected layout is two distinct NAS roots (`/homes` and `/homes2`) with
each Hub user's account mapped to their own DSM home. All users except
Alexander route to the primary; Alexander routes to the secondary. The Hub
admin UI should use DSM per-user quotas for these accounts. The separate
`Alexander` shared folder remains outside this mapping.

The next technical step is a small per-user write pilot, not a broad ACL
change: access Synology's per-user `home` share with the DSM identity linked to
the Hub account, and mount it under that user's existing namespace (for
example `/homes/<DSM username>` or `/homes2/maliavko`). Validate that the NAS
records the file as that DSM user, denies access to other users' homes, and
charges the expected per-user quota. CIFS credentials must be provisioned and
rotated securely on the worker without storing DSM passwords in the Hub. Do
not grant `detecdiv-worker` or an administrator broad write access to the
`homes` parent as a shortcut. The existing aggregate mounts should not be
treated as the production write path until this passes.
The last live Hub database check showed `backup_enabled=false` and no backup
runs. Do not assume the personal homes or their human-generated files are
protected. Define and verify a backup scope that includes home content, and
test a restore, before using these homes as the authoritative Hub write target.
Do not use the broad `Fred` administrator mount as the production write path.
User-facing SMB access should continue to use each person's own DSM login.
No existing raw data, DetecDiv project, or `Sauvegarde` content is to be moved
until the selected design and path mapping tests pass.

## Dedicated-share preflight (2026-09-25)

- `/volume1` on `10.20.8.250` is Btrfs and had 50,864,544,288,768 bytes
  available (`df -B1`) at inspection time. A 10,000,000,000,000-byte quota
  is a limit, not reserved free space.
- No Alexander-specific top-level share existed under `/volume1` at the initial
  inspection.
- The `charvin-admin` SSH login works, but `sudo -n` requires a password, so
  no share or ACL was created through SSH. DSM administrator sign-in is needed.
- DSM on `https://10.20.8.250:5001` presented a browser certificate warning:
  the certificate identifies `synology` and is issued by `Synology Inc. CA`.
  Do not submit DSM credentials over the available HTTP login or bypass the
  HTTPS warning. Use the lab's trusted DSM HTTPS address or have IT establish
  a trusted certificate before browser-side share provisioning.
- The live Hub `backup_settings` row has `backup_enabled=false`, with raw and
  project inclusion flags both true. There were no rows in `backup_runs` at
  inspection. Do not treat the new share or its uncatalogued files as backed up.

## Dedicated-share creation (2026-09-28)

The DSM administrator created `//10.20.8.250/Alexander` on Volume 1 (Btrfs)
with a shared-folder quota of 10 TB. The DSM editor reports 10 TB and 0.00 MB
used. Btrfs data checksum is enabled. `maliavko` has Read/Write; guest has no
access; administrators retain their normal DSM access. Hyper Backup opens with
no backup tasks configured on this NAS.

The existing `/homes2` mount is active as a read-only mount of
`//10.20.8.250/homes`; `/homes` is active read-write from
`//10.20.11.250/homes`. All three worker services were active at the
2026-09-28 check. No mount changes, directory creation, data migration, or
worker restart were performed during that check. The current mount credentials
are Fred's; a least-privilege service identity and its protected credential
provisioning are still required before writes to either NAS home share.

DSM access is currently blocked for credentialed UI work: the available browser
tab is the HTTP sign-in at `http://10.20.8.250:5000`. The NAS HTTPS certificate
at port 5001 is issued to `synology` only, and neither `synology` nor
`SRV-DATA-PB` resolves from the worker or the Windows workstation. Do not enter
DSM credentials over HTTP or bypass the certificate warning. IT must provide a
trusted HTTPS hostname/certificate (or the correct existing trusted URL) before
DSM settings or per-user mount credentials can be administered safely.

Next: provision a per-user authenticated mount for a test account, then verify
NAS ownership, isolation, and quota charging. Keep the secondary provider
inactive, do not enable Hub writes through the current Fred mount, and do not
migrate files until both mount paths are tested and a real backup destination
and restore test cover the homes data. After the pilot succeeds, apply the
explicit routing rule (all users primary; Alexander secondary), prepare only
the Hub-owned subdirectory, and activate providers in a controlled rollout.
The local `/home` remains a separate worker-local path, not a Hub storage root.
