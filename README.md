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

See [docs/architecture.md](docs/architecture.md) and [AGENTS.md](AGENTS.md).

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
python scripts\index_project_root.py "C:\Users\charvin\SynologyDrive\Data\DetecDivProjects" --host-scope client
```

Or through the API:

```powershell
curl -Method POST -ContentType "application/json" -Body '{"source_kind":"project_root","source_path":"C:\\Users\\charvin\\SynologyDrive\\Data\\DetecDivProjects","host_scope":"client"}' http://127.0.0.1:8000/indexing
```

For a deployed server, the indexed `source_path` should be the server-side
canonical root. Clients then remap that root locally using their own settings.
