# Secondary NAS user homes: staged rollout

The existing `/homes` mount on `detecdiv-server` remains the primary NAS
(`10.20.11.250`). `/homes2` is reserved for the secondary NAS
(`10.20.8.250`). Do not repoint `/homes` or move catalog locations as part of
mount setup.

The secondary NAS already has a DSM user home at
`/volume1/homes/maliavko`. The Hub user is `alexander`; the different login
names are intentionally linked by `user_storage_accounts`.

## Current staged state

- Hub root `user-homes2` points to `/homes2` on `detecdiv-server`.
- Provider `synology-secondary` is inactive until the mount, credentials, and
  quota attribution have been tested.
- Alexander has a planned account under `maliavko/DetecdivHub` on that provider.
- Alexander's legacy `/data/Alexander` account and every existing dataset or
  project location remain unchanged.
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

Before switching the mount to read-write or activating the provider, test a
small write in a dedicated test directory and verify which DSM user's quota
is charged. The worker's aggregate SMB mount currently authenticates as
`Fred`; do not assume writes through that mount count against `maliavko`'s
quota. User-facing SMB access should use each person's own DSM login. No
existing raw data, DetecDiv project, or `Sauvegarde` content is to be moved
until that check and path mapping tests pass.
