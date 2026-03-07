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
