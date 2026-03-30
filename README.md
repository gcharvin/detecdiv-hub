# detecdiv-hub

Central catalog, API, and job orchestration layer for DetecDiv.

This repository is intended to host the server-side control plane around the
MATLAB DetecDiv engine:

- central project and raw-data catalog
- indexing services
- remote job submission
- worker orchestration on GPU-capable hosts
- future near-real-time microscope ingestion

The DetecDiv MATLAB repository remains the compute engine. This repository is
the control plane around it.

## Initial scope

- PostgreSQL schema for projects, raw datasets, pipelines, jobs, and artifacts
- FastAPI service for listing projects and submitting jobs
- browser-based web UI served directly by FastAPI at `/web/`
- worker loop that polls queued jobs and dispatches execution
- scripts for local development from a Windows client using SSH tunnels
- deployment stubs for Linux services

## Quick start

Create a virtual environment, install dependencies, then run the API:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
uvicorn api.app:app --reload --host 127.0.0.1 --port 8000
```

On Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .
uvicorn api.app:app --reload --host 127.0.0.1 --port 8000
```

Shortcuts:

```bash
./scripts/run_api.sh
./scripts/run_worker.sh
```

```powershell
.\scripts\run_api.ps1
.\scripts\run_worker.ps1
```

See [docs/architecture.md](docs/architecture.md), [docs/install_server.md](docs/install_server.md), [docs/install_client.md](docs/install_client.md), and [AGENTS.md](AGENTS.md).

## Bootstrap a local development database

This repository expects PostgreSQL. Create a database first, then:

```bash
python scripts/bootstrap_db.py
python scripts/seed_demo.py
uvicorn api.app:app --reload --host 127.0.0.1 --port 8000
```

The demo seed adds:

- two storage roots
- one execution target
- two sample projects with both Linux and Windows-facing locations

## Governance model

The hub now includes a first governance layer:

- `users`
- password-backed login sessions
- project ownership
- private-by-default visibility
- per-project ACL entries
- project groups
- project notes
- project size accounting fields

The project catalog now also exposes richer direct indexing metrics:

- `fov_count`, `roi_count` when imported from the MATLAB SQLite catalog
- `classifier_count`, `processor_count`
- `pipeline_run_count`
- total `run_json_count` across pipeline/classification/processor runs
- `h5_count` and `h5_bytes`
- latest observed run status and timestamp
- compact inventory metadata in `metadata_json.inventory`

In local development, the API resolves the current user from:

- a bearer session token or HTTP-only session cookie
- `?user_key=...`
- `X-DetecDiv-User`
- or `DETECDIV_HUB_DEFAULT_USER_KEY` as fallback

For a real deployment, create at least one password-backed admin user:

```bash
python scripts/set_user_password.py localdev localdev --role admin --display-name localdev
```

## Import real projects from the local SQLite catalog

If you already indexed real projects with the MATLAB catalog browser, you can
import them into PostgreSQL:

```powershell
python scripts\import_catalog_sqlite.py "C:\Users\charvin\Documents\MATLAB\DetecDiv-catalog\catalog\detecdiv_catalog.sqlite"
```

This creates:

- one `storage_root` per imported catalog root
- one `detecdiv_project` per SQLite catalog project
- one `project_location` pointing to the original local `.mat` location

This is the shortest path to test the hub against real projects before adding a
server-side indexer.

## Index a project root directly from the hub

The hub can now scan a DetecDiv project root directly, without importing a
local SQLite catalog first:

```powershell
python scripts\index_project_root.py "C:\Users\charvin\SynologyDrive\Data\DetecDivProjects" --host-scope client --owner-user-key localdev
```

Or through the API:

```powershell
curl -Method POST -ContentType "application/json" -Body '{"source_kind":"project_root","source_path":"C:\\Users\\charvin\\SynologyDrive\\Data\\DetecDivProjects","host_scope":"client","visibility":"private"}' "http://127.0.0.1:8000/indexing?user_key=localdev"
```

For a deployed server, the indexed `source_path` should be the server-side
canonical root. Clients then remap that root locally using their own settings.

## Audit a storage root over SSH

For space-pressure work on a NAS or Linux server, you can generate a read-only
JSON audit over SSH:

```powershell
python scripts\audit_storage_ssh.py Gilles@10.20.11.250 /data --output .\storage-audit.json
```

If your SSH key is not picked up automatically, pass it explicitly:

```powershell
python scripts\audit_storage_ssh.py Gilles@10.20.11.250 /volume1/DATA --identity-file $HOME\.ssh\id_ed25519_nas --output .\storage-audit.json
```

The audit currently includes:

- filesystem usage for the target path
- largest directories under the target path
- largest `.tif`, `.h5`, and `.mat` files
- per-type counts and byte totals
- Synology-oriented directories such as `#recycle`, `#snapshot`, and `@eaDir`

This is intended as a reusable first step before wiring richer storage
accounting into the hub database and cleanup workflows.

For a targeted cleanup pass inside a user folder such as `Pierre`, you can add
an inventory row per immediate child directory:

```powershell
python scripts\audit_storage_ssh.py Gilles@10.20.11.250 /volume1/DATA/Pierre --identity-file $HOME\.ssh\id_ed25519_nas --per-child-inventory --output .\pierre-audit.json
```

Each child inventory row currently includes:

- `total_bytes`
- `.tif`, `.h5`, and `.mat` counts and byte totals
- `latest_mtime`
- a coarse profile such as `raw_only`, `derived_only`, or `mixed`

## Basic governance endpoints

```powershell
curl "http://127.0.0.1:8000/users/me?user_key=localdev"
curl "http://127.0.0.1:8000/projects?user_key=localdev"
curl "http://127.0.0.1:8000/project-groups?user_key=localdev"
curl "http://127.0.0.1:8000/dashboard/summary?user_key=localdev"
curl "http://127.0.0.1:8000/storage-roots?user_key=localdev"
curl "http://127.0.0.1:8000/projects?user_key=localdev&search=viterbi&owner_key=localdev&storage_root_name=projects_aizea"
```

## Web UI

The hub now serves a minimal browser UI at:

- [http://127.0.0.1:8000/web/](http://127.0.0.1:8000/web/)

Current web UI capabilities:

- login with `user_key` + password, with legacy fallback still available
- browse projects with name/owner/group/storage-root filters
- limit the number of visible projects in the table
- inspect per-project storage and inventory metrics
- review notes and ACL entries
- add notes
- share a project with another user
- update project owner and visibility
- create groups and add a project to a group
- preview and execute deletion
- queue hub-side indexing on a project root
- follow indexing progress and recent indexing history
- search and filter projects by name, owner, and storage root
- manage a first pipeline registry
- review active sessions and revoke stale ones
- review pipelines observed from indexed project runs and import them into the registry

The UI is now split across two pages:

- [http://127.0.0.1:8000/web/](http://127.0.0.1:8000/web/) for catalog/admin work
- [http://127.0.0.1:8000/web/indexing.html](http://127.0.0.1:8000/web/indexing.html) for indexing

Async indexing endpoints:

```powershell
curl -Method POST -ContentType "application/json" -Body '{"source_kind":"project_root","source_path":"C:\\Users\\charvin\\SynologyDrive\\Data\\DetecDivProjects","host_scope":"client","visibility":"private"}' "http://127.0.0.1:8000/indexing/jobs?user_key=localdev"
curl "http://127.0.0.1:8000/indexing/jobs?user_key=localdev"
```

## Safe project deletion

Project deletion now follows a preview-first workflow:

1. ask for a deletion preview
2. inspect reclaimable bytes and affected paths
3. confirm deletion explicitly

Preview:

```powershell
curl -Method POST -ContentType "application/json" -Body '{"delete_project_files":true,"delete_linked_raw_data":false,"confirm":false}' "http://127.0.0.1:8000/projects/<PROJECT_ID>/deletion-preview?user_key=localdev"
```

Execute:

```powershell
curl -Method DELETE "http://127.0.0.1:8000/projects/<PROJECT_ID>?user_key=localdev&delete_project_files=true&delete_linked_raw_data=false&confirm=true"
```

Current behavior:

- the project is hidden from normal project listing after deletion
- physical project files are deleted only if `delete_project_files=true`
- linked raw data is deleted only if explicitly requested and not shared with other projects
- deletion is logged in `project_deletion_events`

## Pipeline registry

The hub now exposes:

- a minimal independent `pipelines` registry
- a dynamic `pipelines/observed` view built from project-linked run history

This keeps pipelines independent from projects while still surfacing what has
been observed in indexed project folders.

Examples:

```powershell
curl "http://127.0.0.1:8000/pipelines?user_key=localdev"
curl "http://127.0.0.1:8000/pipelines/observed?user_key=localdev"
curl -Method POST -ContentType "application/json" -Body '{"pipeline_key":"segmentation_v2","display_name":"Segmentation V2","version":"2.0","runtime_kind":"matlab","metadata_json":{"entrypoint":"pipelineRun"}}' "http://127.0.0.1:8000/pipelines?user_key=localdev"
curl -Method POST "http://127.0.0.1:8000/pipelines/import-observed?user_key=localdev"
```

## Install systemd units on Linux

The repository now includes a helper script that renders and installs concrete
systemd units from the actual repo path and service user:

```bash
sudo bash ./scripts/install_systemd.sh \
  --repo-root /srv/detecdiv/detecdiv-hub \
  --service-user detecdiv \
  --env-file /etc/detecdiv-hub/detecdiv-hub.env \
  --api-host 127.0.0.1 \
  --api-port 8000
```
