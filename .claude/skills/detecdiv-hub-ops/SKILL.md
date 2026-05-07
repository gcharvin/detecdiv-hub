---
name: detecdiv-hub-ops
description: Deploy, restart, and verify DetecDiv Hub. Use when working on the split deployment where webserver-labo runs the API and PostgreSQL in Docker, detecdiv-server runs workers under systemd, or when deciding which restart and validation command applies after hub changes.
---

# Detecdiv Hub

## Use this skill

Use this skill for DetecDiv Hub deployment, restart, and live verification
work.

Before making any host, restart, or topology assumption, read
`CURRENT_DEPLOYMENT.md`. That file is the source of truth for what is live.
This skill is the operational playbook for acting on that state.

## State model

Treat these as separate states unless you have explicitly synchronized them:

- local checkout on the Windows development machine
- deployed API copy on `webserver-labo`
- deployed worker copy on `detecdiv-server`

Do not assume that a local edit is live.
Do not assume that restarting one side updates the other.
Do not claim a production fix unless the relevant remote copy was inspected or updated.

## SSH and auth preflight

Before relying on this skill for a live deploy:

1. Verify SSH access with Windows OpenSSH, not the Anaconda client.
2. Verify you can reach `detecdiv-server`.
3. Verify you can reach `webserver-labo`, which currently depends on `ProxyJump detecdiv-server`.
4. Verify whether the endpoint you plan to use is public (`/health`) or requires a logged-in session.

Useful checks on this machine:

```powershell
git config --get core.sshCommand
where.exe ssh
& 'C:\Windows\System32\OpenSSH\ssh.exe' -o BatchMode=yes detecdiv-server exit
& 'C:\Windows\System32\OpenSSH\ssh.exe' -o BatchMode=yes webserver-labo exit
```

Do not describe the deployment workflow as validated if SSH access itself is failing.

## Change classification

Classify the change before deploying.

### Local-only

Examples:

- exploratory debugging
- local-only repros
- local UI iteration against a local API

Action:

- do not restart remote services
- do not infer live behavior from the local process alone

### API-only

Examples:

- FastAPI routes
- container-only API services
- static files under `api/static/`
- Dockerfile, compose files, Alembic, schema bootstrap used only by the VM

Action:

- deploy on `webserver-labo`
- rebuild or restart the API container as appropriate
- do not restart workers unless the DB or shared code contract also changed

### Worker-only

Examples:

- `worker/`
- job executors
- worker-only helpers

Action:

- deploy on `detecdiv-server`
- verify active jobs before restart
- restart only the worker services

### Cross-layer

Examples:

- shared models
- shared services imported by both API and worker
- schema changes used by both sides
- indexing and raw ingest logic

Action:

- deploy both `webserver-labo` and `detecdiv-server`
- treat the deployment as incomplete until both sides are updated

## Pre-restart checks

Before restarting API or workers on the live deployment:

1. Check whether active jobs are running.
2. Check whether indexing is running.
3. If long-running worker jobs are active, do not restart casually.
4. If a restart is necessary, state that it is an operational interruption.

Reason:

- restarting workers during active indexing can orphan the worker job
- the hub will then mark the indexing record as `stale`
- this is expected behavior from the current implementation

## Deployment rules

1. Read `CURRENT_DEPLOYMENT.md` before making host or restart assumptions.
2. Compare the local checkout against the remote checkout that is actually live. A rebuild or restart alone does not deploy local edits.
3. Synchronize the affected local changes into the affected remote checkout before any restart. Acceptable paths include a normal git push/pull workflow or an explicit file copy such as `scp`, but the remote files must be updated first and then re-checked.
4. If the change touches API, schema, routes, static UI, Dockerfile, Alembic, or compose files, deploy on `webserver-labo`.
5. If the change is code or static assets used by the API container, rebuild the container with:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
docker-compose up -d --build
```

6. If only the running container needs a restart and no code changed, use:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
docker-compose restart api
```

7. If the change affects workers, update the worker checkout on `detecdiv-server` first, then restart `detecdiv-worker@*.service` with `systemctl`.
8. Do not describe a deployment as complete until the affected remote copy has been verified.

Remote inspection should include a repository check when the remote tree is a
git checkout:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm
if test -d .git; then
  git rev-parse HEAD
  git status --short
  git diff --stat
else
  echo "Remote deployment tree is not a git checkout; verify files by checksum."
fi
```

If the relevant remote files still differ from the local intended state, the deployment is not ready for rebuild/restart.

## Practical deploy paths

The current live deployment copies are operational trees. They may not be clean
git checkouts, and on `detecdiv-server` the worker tree may not be a git
repository at all. A naive agent must therefore deploy by copying the intended
files and verifying those files, not by assuming `git pull` is available.

Always start from the local repo root:

```powershell
cd C:\Users\charvin\Documents\MATLAB\detecdiv-hub
$SSH = 'C:\Windows\System32\OpenSSH\ssh.exe'
$SCP = 'C:\Windows\System32\OpenSSH\scp.exe'
New-Item -ItemType Directory -Force C:\tmp | Out-Null
git status --short
git diff --name-only
```

Classify changed files by path:

- `api/routes_*.py`, `api/app.py`, `api/config.py`, `api/db.py`: API container, deploy to `webserver-labo`, rebuild API container.
- `api/static/*`: static web UI, deploy to `webserver-labo`, rebuild API container.
- `ops/compose/webserver-labo/*`, `Dockerfile`, `pyproject.toml`, `constraints.txt`: API container image/runtime, deploy to `webserver-labo`, rebuild API container.
- `db/schema.sql`, `alembic/*`, schema/model changes in `api/models.py`: database/API contract, deploy to `webserver-labo`; if workers import the changed model/schema contract, also deploy to `detecdiv-server`.
- `worker/*`: worker runtime, deploy to `detecdiv-server`, restart workers after checking active jobs.
- `api/services/*`: classify by imports and use. If imported by API only, deploy to `webserver-labo`. If imported by workers or job execution, deploy to `detecdiv-server`. If shared, deploy both.
- `scripts/ops/*`: operational scripts, deploy only to the host where the script is run.
- docs-only files: no live restart.

### API-only deploy

Use this for FastAPI routes, API services, static UI, schema/bootstrap files, or
container/runtime files used only by the API.

From Windows:

```powershell
tar --exclude='__pycache__' --exclude='*.pyc' --exclude='.git' -czf C:\tmp\detecdiv-api-deploy.tar.gz api db alembic ops pyproject.toml constraints.txt README.md alembic.ini
& $SCP C:\tmp\detecdiv-api-deploy.tar.gz webserver-labo:/tmp/
& $SSH webserver-labo "cd /home/charvin-admin/repos/detecdiv-hub-webvm && tar -xzf /tmp/detecdiv-api-deploy.tar.gz"
```

Then rebuild the API container on `webserver-labo`:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
docker-compose up -d --build
docker-compose ps
curl -sS http://127.0.0.1:8000/health
```

Use `docker-compose restart api` only when no code, dependency, static asset, or
container input changed.

### Worker-only deploy

Use this for `worker/*`, worker-only helpers, MATLAB/Python job execution logic,
or service files used only on `detecdiv-server`.

Before restarting workers, check running work:

```bash
systemctl status detecdiv-worker@1.service detecdiv-worker@2.service detecdiv-worker@3.service --no-pager
journalctl -u detecdiv-worker@1.service -u detecdiv-worker@2.service -u detecdiv-worker@3.service -n 80 --no-pager
```

From Windows:

```powershell
tar --exclude='__pycache__' --exclude='*.pyc' --exclude='.git' -czf C:\tmp\detecdiv-worker-deploy.tar.gz worker api db pyproject.toml constraints.txt README.md
& $SCP C:\tmp\detecdiv-worker-deploy.tar.gz detecdiv-server:/tmp/
& $SSH detecdiv-server "cd /home/charvin-admin/repos/detecdiv-hub-webvm && tar -xzf /tmp/detecdiv-worker-deploy.tar.gz"
```

Then restart only the worker services:

```bash
sudo systemctl restart detecdiv-worker@1.service detecdiv-worker@2.service detecdiv-worker@3.service
systemctl status detecdiv-worker@1.service detecdiv-worker@2.service detecdiv-worker@3.service --no-pager
journalctl -u detecdiv-worker@1.service -u detecdiv-worker@2.service -u detecdiv-worker@3.service -n 100 --no-pager
```

Restarting workers is an operational interruption when jobs are active. State
that explicitly in the report.

### Cross-layer deploy

Use this when a change touches shared models, shared services, job contracts,
schema, indexing, raw dataset lifecycle, preview generation, pipeline execution,
or anything imported by both API and worker code.

Order:

1. Check active jobs and indexing before touching workers.
2. Deploy and rebuild `webserver-labo`.
3. Deploy `detecdiv-server`.
4. Restart workers only after the API is healthy.
5. Verify API health, worker service health, and API-side worker health.

This order keeps the API/database contract available before fresh workers start
claiming jobs.

## Remote file verification without git

When the remote deployment tree is not a git checkout, verify by checksums for
the files that matter.

From Windows, for a file that should be live on `webserver-labo`:

```powershell
Get-FileHash api\static\app.js -Algorithm SHA256
& $SSH webserver-labo "sha256sum /home/charvin-admin/repos/detecdiv-hub-webvm/api/static/app.js"
```

For a worker file on `detecdiv-server`:

```powershell
Get-FileHash worker\run_worker.py -Algorithm SHA256
& $SSH detecdiv-server "sha256sum /home/charvin-admin/repos/detecdiv-hub-webvm/worker/run_worker.py"
```

If checksums differ for intended files after deployment, do not rebuild or
restart yet; fix the copy step first.

## Verification order

After a deploy or restart:

1. Check `docker-compose ps` on `webserver-labo`.
2. Check `curl http://127.0.0.1:8000/health` on `webserver-labo`.
3. Check `curl http://detecdiv-hub.detecdiv.internal/health` through the front proxy.
4. If the change touches raw data UI, verify `raw-dataset.html` for the positions/MP4 viewer and `raw-ops.html` for ops-only screens.
5. If the change touches workers, check `execution-targets`, worker journals, and worker instance health.

Worker verification commands on `detecdiv-server`:

```bash
systemctl status detecdiv-worker@1.service detecdiv-worker@2.service detecdiv-worker@3.service --no-pager
journalctl -u detecdiv-worker@1.service -u detecdiv-worker@2.service -u detecdiv-worker@3.service -n 100 --no-pager
```

API-side worker verification:

```bash
TOKEN="$(curl -sS -X POST 'http://detecdiv-hub.detecdiv.internal/auth/login' \
  -H 'Content-Type: application/json' \
  -d '{"user_key":"<user>","password":"<password>","client_label":"ops-check"}' \
  | python -c "import json,sys; print(json.load(sys.stdin)['session_token'])")"
curl -sS 'http://detecdiv-hub.detecdiv.internal/execution-targets' -H "Authorization: Bearer $TOKEN"
curl -sS 'http://detecdiv-hub.detecdiv.internal/dashboard/summary' -H "Authorization: Bearer $TOKEN"
```

Expected stable state:

- whatever `CURRENT_DEPLOYMENT.md` says is current
- the worker instances listed there
- `stale_workers=0`

## Required reporting

When using this skill, report explicitly:

- what was inspected locally
- what was inspected remotely
- which host was updated
- which host was not updated
- whether active jobs were present before restart

## Failure handling

- Prefer logs over guesswork. On the VM, inspect container logs before assuming the build succeeded.
- If the remote checkout was never updated, treat the incident as a failed deployment procedure, not as an application runtime failure.
- If the API container fails to start, look for import errors, syntax errors, or schema mismatches in the logs.
- If a worker restart orphaned a running job, say so explicitly instead of describing it as a generic indexing crash.
- Do not ask the user to restart the API or worker when you have SSH access and can perform the restart directly.
