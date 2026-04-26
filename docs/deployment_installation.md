# Deployment And Worker Installation

This document explains the intended deployment model for `detecdiv-hub` and how
to configure compute workers on a remote server. It uses the current lab
deployment as the concrete reference.

## Current Reference Deployment

The live lab deployment is split across two machines:

- `webserver-labo`: web VM hosting PostgreSQL, FastAPI, and the static web UI.
- `detecdiv-server` / `GC-CALCUL-306`: compute and storage-visible host running
  the worker service.

The internal URL is:

```text
http://detecdiv-hub.detecdiv.internal/web/
```

Health check:

```bash
curl http://detecdiv-hub.detecdiv.internal/health
```

The VM is reachable from `detecdiv-server` on the libvirt network as:

```text
192.168.122.185
```

## Architectural Principle

The API is a control plane. It should expose state, accept requests, and enqueue
work. It should not scan server project folders or run MATLAB jobs itself.

The worker is the execution plane. It must run on a machine that can see:

- DetecDiv project storage
- raw dataset storage
- the MATLAB installation
- the DetecDiv MATLAB repository

For the lab setup, that machine is `detecdiv-server`.

This separation matters because `webserver-labo` does not have direct filesystem
visibility of project and dataset storage. Server-side indexing and pipeline
execution must therefore be worker-backed.

## VM Installation Pattern

The current VM installation uses Docker Compose for the API and PostgreSQL.

Live path on `webserver-labo`:

```bash
/home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
```

Expected services:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
docker-compose ps
```

Expected result:

- `detecdiv-hub-api` is `Up`
- `detecdiv-hub-postgres` is `Up (healthy)`

API local health check on the VM:

```bash
curl -sS http://127.0.0.1:8000/health
```

Public internal health check through `detecdiv-server` Nginx:

```bash
curl -sS http://detecdiv-hub.detecdiv.internal/health
```

## Reverse Proxy

`detecdiv-server` remains the internal Nginx frontend, as for GitLab and Wiki.js.

The DetecDiv Hub vhost is:

```text
detecdiv-hub.detecdiv.internal
hub.detecdiv.internal
```

It proxies to:

```text
http://192.168.122.185:8000
```

Clients that do not have lab DNS for these names need a hosts entry pointing to
`detecdiv-server`:

```text
10.20.11.100 detecdiv-hub.detecdiv.internal hub.detecdiv.internal
```

## Database Migration Pattern

For migration from an existing PostgreSQL instance:

1. Stop or pause workers that write to the target database.
2. Create a custom-format dump from the source DB.
3. Backup the target VM database.
4. Drop and recreate the target DB.
5. Restore the dump into the VM PostgreSQL container.
6. Restart the API.
7. Recreate or verify operational users and execution targets.
8. Restart the compute worker.

Example source dump from `detecdiv-server`:

```bash
PGPASSWORD=... pg_dump -h 127.0.0.1 -U detecdiv -d detecdiv_hub -Fc \
  -f /home/charvin-admin/detecdiv_hub_local_YYYYMMDD_HHMMSS.dump
```

Do not store real database passwords in repository files.

## Remote Worker Installation Pattern

A compute worker is a normal Python process managed by systemd. It connects to
the VM PostgreSQL database and claims jobs from the central `jobs` table.

The current lab worker checkout is:

```bash
/home/charvin-admin/repos/detecdiv-hub-webvm
```

The current stable services are:

```bash
detecdiv-worker@1.service
detecdiv-worker@2.service
detecdiv-worker@3.service
```

Check it on `detecdiv-server`:

```bash
systemctl list-units 'detecdiv-worker@*.service' --all --no-pager
systemctl status detecdiv-worker@1.service detecdiv-worker@2.service detecdiv-worker@3.service --no-pager
```

The worker systemd override is:

```bash
/etc/systemd/system/detecdiv-worker@.service.d/10-webvm-db.conf
```

It changes the worker to:

- run from `/home/charvin-admin/repos/detecdiv-hub-webvm`
- use a worker-specific env file
- connect to the PostgreSQL service on `webserver-labo`

The worker env file is:

```bash
/etc/detecdiv-hub/worker-webvm.env
```

Required variables:

```bash
DETECDIV_HUB_ENVIRONMENT=prod
DETECDIV_HUB_DATABASE_URL=postgresql+psycopg://USER:PASSWORD@192.168.122.185:5432/detecdiv_hub
DETECDIV_HUB_LOG_LEVEL=INFO
DETECDIV_HUB_DEFAULT_USER_KEY=localdev
DETECDIV_HUB_AUTO_PROVISION_USERS=true
DETECDIV_HUB_MATLAB_COMMAND=/usr/local/bin/matlab
DETECDIV_HUB_MATLAB_REPO_ROOT=/home/charvin-admin/repos/DetecDiv
DETECDIV_HUB_WORKER_TARGET_KEY=detecdiv-server
DETECDIV_HUB_WORKER_POLL_INTERVAL_SEC=5
```

Important details:

- From the worker host, the database hostname must be `192.168.122.185`, not the
  Docker-internal hostname `postgres`.
- `DETECDIV_HUB_WORKER_TARGET_KEY` must match an existing row in
  `execution_targets`.
- The current target key is `detecdiv-server`.
- The target metadata currently uses `max_concurrent_jobs=3` and
  `worker_instances_desired=3`.

The worker management scripts are versioned in:

```bash
scripts/ops/install_webvm_worker_override.sh
scripts/ops/manage_webvm_workers.sh
scripts/ops/README_detecdiv_webvm_workers.md
```

## Execution Target Contract

The execution target is the routing contract between the API and workers.

For the current lab worker:

- `target_key`: `detecdiv-server`
- `host_name`: `GC-CALCUL-306`
- `supports_matlab`: `true`
- `supports_python`: `true`
- `supports_gpu`: `true`
- `metadata_json.storage_visible`: should be treated as true

Indexing jobs that need server storage should be routed to this target, either
explicitly or through:

```bash
DETECDIV_HUB_INDEXING_TARGET_KEY=detecdiv-server
```

## Multi-Worker State

The current stable deployment uses `detecdiv-worker@1`, `@2`, and `@3`.

Per-process heartbeat state is stored in the `worker_instances` table. Each
worker updates only its own row, keyed by `(execution_target_id,
worker_instance)`.

`execution_targets.metadata_json.worker_healths` and
`execution_targets.metadata_json.worker_health_summary` are still populated as a
compatibility view for the current UI.

## Operational Checks

From Windows or a client machine:

```powershell
curl.exe http://detecdiv-hub.detecdiv.internal/health
```

From `webserver-labo`:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
docker-compose ps
docker-compose logs --tail=100 api
```

From `detecdiv-server`:

```bash
systemctl status detecdiv-worker@1.service detecdiv-worker@2.service detecdiv-worker@3.service --no-pager
journalctl -u detecdiv-worker@1.service -u detecdiv-worker@2.service -u detecdiv-worker@3.service -n 100 --no-pager
```

Through the API:

```bash
curl -sS 'http://detecdiv-hub.detecdiv.internal/execution-targets?user_key=localdev'
curl -sS 'http://detecdiv-hub.detecdiv.internal/dashboard/summary?user_key=localdev'
```

Expected worker state:

- target key `detecdiv-server`
- `status=online`
- three registered worker instances, `@1`, `@2`, and `@3`
- `stale_workers=0`

## Rollback Outline

Rollback to the former local deployment should be explicit, not automatic.

High-level rollback steps:

1. Stop the VM-connected worker.
2. Remove or disable the systemd override
   `/etc/systemd/system/detecdiv-worker@.service.d/10-webvm-db.conf`.
3. Run `systemctl daemon-reload`.
4. Restart the old local worker services.
5. Point any client or proxy traffic back to the former local API if needed.

Do not perform rollback without confirming which database is considered the
source of truth at that moment.
