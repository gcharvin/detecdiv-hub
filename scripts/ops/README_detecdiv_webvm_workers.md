# DetecDiv Hub Web VM Worker Scripts

These scripts manage compute workers on `detecdiv-server` for the primary
`webserver-labo` deployment.

## Current Topology

- API and PostgreSQL run on `webserver-labo`.
- Compute workers run on `detecdiv-server`.
- The worker target key is `detecdiv-server`.
- PostgreSQL is reached from the compute host through the libvirt address
  `192.168.122.185`.

Do not commit real database passwords. Pass the database URL through the
environment when installing or updating the worker override.

## Install Or Update The Worker Override

Run on `detecdiv-server`:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm
sudo DETECDIV_HUB_DATABASE_URL='postgresql+psycopg://USER:PASSWORD@192.168.122.185:5432/detecdiv_hub' \
  bash scripts/ops/install_webvm_worker_override.sh
```

This writes:

- `/etc/detecdiv-hub/worker-webvm.env`
- `/etc/systemd/system/detecdiv-worker@.service.d/10-webvm-db.conf`

It also runs `systemctl daemon-reload`.

## Manage Worker Count

Run on `detecdiv-server`:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm
sudo bash scripts/ops/manage_webvm_workers.sh status
sudo bash scripts/ops/manage_webvm_workers.sh one
sudo bash scripts/ops/manage_webvm_workers.sh three
sudo bash scripts/ops/manage_webvm_workers.sh stop
sudo bash scripts/ops/manage_webvm_workers.sh restart
```

The current target is three workers:

```bash
sudo bash scripts/ops/manage_webvm_workers.sh three
```

## Boot Orchestration

The host-level boot sequence for `webserver-labo` lives in the adjacent
`Webserver` repository, not here.

See:

- `../Webserver/OPERATIONS.md`
- `../Webserver/deploy/systemd/detecdiv-webvm-bootstrap.service`

## Verify From The API

```bash
curl -sS 'http://detecdiv-hub.detecdiv.internal/execution-targets?user_key=localdev'
```

Expected stable state for three workers:

- `worker_instances`: `@1`, `@2`, `@3`
- `registered_workers`: `3`
- `online_workers`: `3`
- `stale_workers`: `0`
- `max_concurrent_jobs`: `3`

The authoritative per-process state is stored in the `worker_instances` database
table. `execution_targets.metadata_json.worker_healths` is kept as a compatibility
view for the existing UI.
