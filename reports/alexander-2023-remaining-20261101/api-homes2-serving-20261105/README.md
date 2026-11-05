# Read-only API homes2 mount preparation

The API currently binds only `/data:ro`. Its live preview/settings caches for
Alexander 2023_1/2/3 occupy 28,981,723,684 bytes there. They must remain until
their homes2 equivalents pass real content handlers through the API container.

Fresh VM checks on 2026-11-05 confirmed no homes2 mount and `sudo -n true`
returned `sudo: a password is required`. The existing root SSHFS data bridge
uses `/root/.ssh/detecdiv_hub_data` to compute; only its pathname was inspected.
No credential, private key content or SMB identity was read or changed.

The installer reuses that existing transport and adds a separate readonly mount
of the **single personal DetecdivHub directory**, not other users' homes. It
also orders the existing API boot helper after the homes2 mount. It does not
restart the API, alter current `/data`, change URIs, or remove any cache.
The boot dependency and enablement are installed only after the readonly mount
passes its live probes. A failed new mount is stopped and leaves the API boot
dependencies unchanged.

Files are intended for the private VM directory
`/home/charvin-admin/storage-transfers/alexander-api-homes2-serving-20261105`.
The root step is then:

```sh
sudo bash /home/charvin-admin/storage-transfers/alexander-api-homes2-serving-20261105/install_homes2_readonly_mount.sh
```

After the mount succeeds, the remaining agent work is to add its readonly bind
to the existing API Compose service, activate only the API under fresh idle
checks, fully hash and stream all live homes2 media through actual handlers,
transactionally switch matching URI references, verify the clients, then retire
only the three exact cache manifests with audit and measured space. Preserve
the old cache URIs until the serving change works. Root Docker workarounds are
not an alternative to the explicit mount privilege.

To undo only this preparation after confirming no API dependency, remove the
specific `homes2.conf` drop-in, disable/stop `detecdiv-homes2-share.service`,
remove its unit and reload systemd. Never unmount `/data` or remove NAS data.
