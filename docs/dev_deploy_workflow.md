# Development And Deployment Workflow

This document defines the safe workflow for changing `detecdiv-hub` without
breaking active indexing or leaving the live API and workers on different code.

## Core Rule

Treat these as three separate states unless you have explicitly synchronized
them:

- local checkout on the Windows development machine
- deployed API copy on `webserver-labo`
- deployed worker copy on `detecdiv-server`

Do not assume that a local edit is live. Do not assume that restarting one side
updates the other. The live incident on 2026-05-04 confirmed that this
assumption is unsafe.

## Current Lab Reality

- `webserver-labo` runs PostgreSQL and FastAPI in Docker Compose
- `detecdiv-server` runs `detecdiv-worker@1`, `@2`, and `@3` under `systemd`
- the worker checkout on `detecdiv-server` is an operational copy, not a
  guaranteed mirror of the current local repository state

This means API code and worker code can drift if deployment is done only on one
host or only from one local checkout.

## Change Classification

Before deploying, classify the change.

### Local-only change

Examples:

- exploratory debugging
- local UI iteration against a local API
- prototype scripts not used by the live VM or worker

Action:

- do not restart remote services
- do not infer anything about production behavior from the local process alone

### API-only change

Examples:

- FastAPI routes
- API-side service logic used only in the container
- static files under `api/static/`
- Dockerfile, compose files, Alembic, schema bootstrap used by the VM

Action:

- deploy on `webserver-labo`
- verify health locally on the VM and through the front proxy
- do not restart workers unless the change also affects worker-imported code or
  the DB contract they rely on

### Worker-only change

Examples:

- `worker/`
- job execution helpers
- service modules imported only from the worker process

Action:

- deploy on `detecdiv-server`
- verify no critical job is running before restart
- restart only the worker services

### Cross-layer change

Examples:

- shared models
- shared services imported by both API and worker
- schema changes used by both sides
- indexing and raw ingest changes

Action:

- deploy both `webserver-labo` and `detecdiv-server`
- treat the deployment as incomplete until both sides are updated

## Pre-Restart Safety Checks

Before restarting the API or workers on the live lab deployment:

1. Check whether active jobs are running.
2. Check whether indexing is running.
3. If worker-backed indexing or long jobs are active, do not restart casually.
4. If a restart is necessary, record that it is an operational interruption.

Reason:

- restarting workers during an active indexing job can orphan the worker job
- the hub then marks the indexing job as `stale`
- this is expected behavior from the current code, not a mystery failure

## Minimum Live Deployment Protocol

For any non-trivial live change:

1. Identify whether the change is `API-only`, `worker-only`, or `cross-layer`.
2. Confirm which remote copies must be updated.
3. Verify the remote copy actually contains the intended code before restart.
4. Restart only the affected services.
5. Run post-deploy verification.

If step 3 is skipped, you are not deploying intentionally. You are guessing.

## Required Verification

### After API deployment

Run on `webserver-labo`:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
docker-compose ps
curl -sS http://127.0.0.1:8000/health
docker-compose logs --tail=100 api
```

Then verify through the proxy:

```bash
curl -sS http://detecdiv-hub.detecdiv.internal/health
```

### After worker deployment

Run on `detecdiv-server`:

```bash
systemctl status detecdiv-worker@1.service detecdiv-worker@2.service detecdiv-worker@3.service --no-pager
journalctl -u detecdiv-worker@1.service -u detecdiv-worker@2.service -u detecdiv-worker@3.service -n 100 --no-pager
```

Then verify through the API:

```bash
curl -sS 'http://detecdiv-hub.detecdiv.internal/execution-targets?user_key=localdev'
curl -sS 'http://detecdiv-hub.detecdiv.internal/dashboard/summary?user_key=localdev'
```

Expected stable state:

- target key `detecdiv-server`
- three worker instances: `@1`, `@2`, `@3`
- no unexpected stale workers

## Deployment Discipline For Agents

When an agent is asked to "debug production" or "deploy a fix", it must not:

- assume the remote worker tree matches the local repo
- assume a restart is harmless during active indexing
- conclude that a local fix is live without remote verification

The agent should explicitly report:

- what code was inspected locally
- what code was inspected remotely
- which host was updated
- which host was not updated

## Recommended Next Hardening

The current deployment is operable but too implicit. The next improvements
should be:

1. add a deployed version marker for both API and worker copies
2. make the worker checkout a real tracked deployment artifact rather than an
   opaque copy
3. add a pre-restart check for active jobs before worker restarts
4. add an explicit deploy runbook for `API-only`, `worker-only`, and
   `cross-layer` changes
