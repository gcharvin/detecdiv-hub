# Current Deployment

This file records the current live deployment target for agents and developers
working on this repository.

## Stable Deployment Target

As of 2026-04-26, the primary DetecDiv Hub deployment is the VM:

- VM: `webserver-labo`
- Internal URL: `http://detecdiv-hub.detecdiv.internal/`
- Web UI: `http://detecdiv-hub.detecdiv.internal/web/`
- Health check: `http://detecdiv-hub.detecdiv.internal/health`
- VM address on the libvirt network: `192.168.122.185`

For installation details and remote worker configuration, see
`docs/deployment_installation.md`.

The old API process on `detecdiv-server` may still exist for rollback/history,
but it is no longer the deployment target to assume by default.

## Runtime Split

The production-like split is:

- `webserver-labo`
  - PostgreSQL, in Docker Compose
  - FastAPI app, in Docker Compose
  - static web UI served by the FastAPI app
- `detecdiv-server` / `GC-CALCUL-306`
  - compute worker only
  - MATLAB access
  - direct visibility of server project/dataset storage

The VM should not be assumed to have direct access to project or dataset file
storage. Any server-side filesystem work, including project-root indexing, must
run through the worker on `detecdiv-server`.

## Current Services

On `webserver-labo`, the Compose deployment lives at:

```bash
/home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
```

Useful checks:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
docker-compose ps
curl -sS http://127.0.0.1:8000/health
```

On `detecdiv-server`, the workers currently run from:

```bash
/home/charvin-admin/repos/detecdiv-hub-webvm
```

The active worker services are:

```bash
systemctl list-units 'detecdiv-worker@*.service' --all --no-pager
systemctl status detecdiv-worker@1.service detecdiv-worker@2.service detecdiv-worker@3.service --no-pager
```

The systemd override that points the worker to the VM database is:

```bash
/etc/systemd/system/detecdiv-worker@.service.d/10-webvm-db.conf
```

The boot-time VM orchestration service now lives in the adjacent `Webserver`
repository because it belongs to the VM host layer, not the hub control plane.

## Data State

The PostgreSQL database from the former local `detecdiv-server` deployment was
restored into the VM PostgreSQL container on 2026-04-26.

The VM is now the main database to use for ongoing DetecDiv Hub work unless an
explicit rollback is requested.

## Agent Rules

Future agents should assume:

- The VM deployment is the primary runtime.
- New deployment work should target `webserver-labo`.
- Compute and storage-visible worker work should target `detecdiv-server`.
- Do not reintroduce API-side filesystem scans for server paths.
- Do not assume the VM can see project storage.
- The current stable worker state uses `detecdiv-worker@1`, `@2`, and `@3`.
  Per-worker heartbeat state is stored in the `worker_instances` table.
- VM autostart and host-level reboot orchestration are documented in
  `../Webserver`, not here.

## Rollback Note

Rollback is possible by removing the worker systemd override and restarting the
old local services on `detecdiv-server`, but that should be treated as an
explicit incident/rollback action, not as the default development target.
