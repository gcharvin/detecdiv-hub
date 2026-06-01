# Webserver-Labo Boot Mount

These helpers keep the API VM aligned with the live deployment expectation:
`/data` is mounted from `detecdiv-server` before the FastAPI container serves
storage-backed files such as raw preview MP4s.

Install on `webserver-labo`:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm
sudo bash scripts/ops/install_webserver_labo_boot_mount.sh --start
```

Installed files:

- `/etc/systemd/system/detecdiv-data-share.service`
- `/etc/systemd/system/detecdiv-hub-after-data.service`
- `/usr/local/sbin/detecdiv-webvm-after-data.sh`

The data-share service runs `sshfs` in the foreground and retries on failure.
The after-data service waits until `/data` is mounted, starts the Compose stack,
then restarts the API container so Docker's bind mount is attached to the real
mounted `/data`, not the empty local fallback directory.

Verify after a reboot:

```bash
findmnt /data
systemctl status detecdiv-data-share.service --no-pager
systemctl status detecdiv-hub-after-data.service --no-pager
docker exec detecdiv-hub-api test -e /data/Alexander && echo "API sees /data"
curl -fsS http://127.0.0.1:8000/health
```
