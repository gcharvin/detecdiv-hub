# Development

## Local development on a Windows PC

Recommended setup:

- local clone of this repository
- SSH access to the Linux server
- Samba access to server data if needed
- Python 3.11+

## Suggested workflow

1. edit code locally
2. run API locally if needed
3. open an SSH tunnel to the remote server for API/DB testing
4. push changes
5. pull and restart services on the server

## Useful commands

Start local API:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .[dev]
uvicorn api.app:app --reload --host 127.0.0.1 --port 8000
```

Start worker locally:

```powershell
python worker\run_worker.py
```

Optional storage lifecycle settings:

```powershell
$env:DETECDIV_HUB_DEFAULT_ARCHIVE_ROOT="C:\detecdiv-archives"
$env:DETECDIV_HUB_DEFAULT_ARCHIVE_COMPRESSION="zip"
```

Optional automatic archive policy settings:

```powershell
$env:DETECDIV_HUB_ARCHIVE_POLICY_ENABLED="true"
$env:DETECDIV_HUB_ARCHIVE_POLICY_INTERVAL_MINUTES="1440"
$env:DETECDIV_HUB_ARCHIVE_POLICY_RUN_AS_USER_KEY="archive-bot"
$env:DETECDIV_HUB_ARCHIVE_POLICY_OLDER_THAN_DAYS="60"
$env:DETECDIV_HUB_ARCHIVE_POLICY_MIN_TOTAL_BYTES="10737418240"
$env:DETECDIV_HUB_ARCHIVE_POLICY_LIMIT="50"
$env:DETECDIV_HUB_ARCHIVE_POLICY_LIFECYCLE_TIERS="hot"
$env:DETECDIV_HUB_ARCHIVE_POLICY_ARCHIVE_STATUSES="none,restored,archive_failed,restore_failed"
$env:DETECDIV_HUB_ARCHIVE_POLICY_ARCHIVE_URI="C:\detecdiv-archives"
$env:DETECDIV_HUB_ARCHIVE_POLICY_ARCHIVE_COMPRESSION="zip"
$env:DETECDIV_HUB_ARCHIVE_POLICY_DELETE_HOT_SOURCE="false"
```

In plain terms this means: every 24 hours, queue archive jobs for raw datasets older than 60 days and larger than 10 GB.

Optional Micro-Manager ingestion settings:

```powershell
$env:DETECDIV_HUB_MICROMANAGER_INGEST_ENABLED="true"
$env:DETECDIV_HUB_MICROMANAGER_INGEST_INTERVAL_MINUTES="15"
$env:DETECDIV_HUB_MICROMANAGER_INGEST_RUN_AS_USER_KEY="micromanager-bot"
$env:DETECDIV_HUB_MICROMANAGER_INGEST_ROOT="C:\micromanager-landing"
$env:DETECDIV_HUB_MICROMANAGER_INGEST_STORAGE_ROOT_NAME="microscope-hot"
$env:DETECDIV_HUB_MICROMANAGER_INGEST_HOST_SCOPE="server"
$env:DETECDIV_HUB_MICROMANAGER_INGEST_VISIBILITY="private"
$env:DETECDIV_HUB_MICROMANAGER_INGEST_SETTLE_SECONDS="300"
$env:DETECDIV_HUB_MICROMANAGER_INGEST_MAX_DATASETS="25"
$env:DETECDIV_HUB_MICROMANAGER_INGEST_GROUPING_WINDOW_HOURS="12"
$env:DETECDIV_HUB_MICROMANAGER_POST_INGEST_PIPELINE_KEY="detectdiv_default_raw"
$env:DETECDIV_HUB_MICROMANAGER_POST_INGEST_REQUESTED_MODE="server"
$env:DETECDIV_HUB_MICROMANAGER_POST_INGEST_PRIORITY="90"
```

In plain terms this means: every 15 minutes, scan the Micro-Manager landing zone and ingest datasets that have stopped changing for at least 5 minutes.
Acquisitions that look like the same session within a 12-hour window can be grouped into one auto-created experiment, and an optional raw-dataset pipeline can be queued after ingest.

Open SSH tunnel:

```powershell
scripts\dev_tunnel.ps1
```

## Bootstrap PostgreSQL schema and demo data

After creating a PostgreSQL database and setting `DETECDIV_HUB_DATABASE_URL`:

```powershell
python scripts\bootstrap_db.py
python scripts\seed_demo.py
```

Useful API checks once the server is running:

```powershell
curl http://127.0.0.1:8000/health
curl "http://127.0.0.1:8000/users/me?user_key=localdev"
curl "http://127.0.0.1:8000/experiments?user_key=localdev"
curl "http://127.0.0.1:8000/raw-datasets?user_key=localdev"
curl "http://127.0.0.1:8000/projects?user_key=localdev"
curl "http://127.0.0.1:8000/dashboard/summary?user_key=localdev"
curl -Method POST -ContentType "application/json" -Body '{"batch_name":"pilot-project-migration","source_kind":"legacy_project_root","source_path":"C:\\Users\\charvin\\SynologyDrive\\Data\\DetecDivProjects","strategy":"pilot","max_items":20}' "http://127.0.0.1:8000/migrations/plans?user_key=localdev"
```

Queue a raw dataset archive:

```powershell
curl -Method POST -ContentType "application/json" -Body '{"archive_uri":"C:\\detecdiv-archives","archive_compression":"zip","mark_archived":true}' "http://127.0.0.1:8000/raw-datasets/<RAW_DATASET_ID>/archive?user_key=localdev"
```

Queue a raw dataset restore:

```powershell
curl -Method POST "http://127.0.0.1:8000/raw-datasets/<RAW_DATASET_ID>/restore?user_key=localdev"
```

Preview an archive policy batch:

```powershell
curl -Method POST -ContentType "application/json" -Body '{"older_than_days":30,"min_total_bytes":1073741824,"limit":20,"lifecycle_tiers":["hot"],"archive_statuses":["none","restored"],"archive_uri":"C:\\detecdiv-archives","archive_compression":"zip","mark_archived":true}' "http://127.0.0.1:8000/raw-datasets/archive-policy/preview?user_key=localdev"
```

Queue an archive policy batch:

```powershell
curl -Method POST -ContentType "application/json" -Body '{"older_than_days":30,"min_total_bytes":1073741824,"limit":20,"lifecycle_tiers":["hot"],"archive_statuses":["none","restored"],"archive_uri":"C:\\detecdiv-archives","archive_compression":"zip","mark_archived":true}' "http://127.0.0.1:8000/raw-datasets/archive-policy/queue?user_key=localdev"
```

Inspect the automatic archive policy:

```powershell
curl "http://127.0.0.1:8000/raw-datasets/archive-policy/automatic?user_key=admin"
```

Run the automatic archive policy manually in report-only mode:

```powershell
curl -Method POST -ContentType "application/json" -Body '{"report_only":true}' "http://127.0.0.1:8000/raw-datasets/archive-policy/automatic/run?user_key=admin"
```

Inspect Micro-Manager ingestion status:

```powershell
curl "http://127.0.0.1:8000/micromanager-ingest/status?user_key=admin"
```

Run Micro-Manager ingestion manually in report-only mode:

```powershell
curl -Method POST -ContentType "application/json" -Body '{"report_only":true}' "http://127.0.0.1:8000/micromanager-ingest/run?user_key=admin"
```

Open the browser UI:

```powershell
start http://127.0.0.1:8000/web/
```

## Import a real SQLite catalog

If you already have a local catalog generated by the MATLAB browser, import it
into PostgreSQL:

```powershell
python scripts\import_catalog_sqlite.py "C:\Users\charvin\Documents\MATLAB\DetecDiv-catalog\catalog\detecdiv_catalog.sqlite"
```

Then restart the API if needed and query the imported projects:

```powershell
curl http://127.0.0.1:8000/projects
```

## Direct hub-side indexing

You can also index a DetecDiv root directly into PostgreSQL:

```powershell
python scripts\index_project_root.py "C:\Users\charvin\SynologyDrive\Data\DetecDivProjects" --host-scope client --owner-user-key localdev
```

Or by calling the API:

```powershell
curl -Method POST -ContentType "application/json" -Body '{"source_kind":"project_root","source_path":"C:\\Users\\charvin\\SynologyDrive\\Data\\DetecDivProjects","host_scope":"client","visibility":"private"}' "http://127.0.0.1:8000/indexing?user_key=localdev"
```

On the real Linux server, the hub should index the canonical server path.
MATLAB clients should not reuse that path directly; they should map it to their
own Samba mount using the hub client settings.

Direct hub indexing currently enriches each project with:

- classifier and processor counts
- `run.json` discovery across pipeline/classification/processor folders
- H5 artifact counts and byte footprint
- latest observed run status/timestamp
- compact top-level inventory metadata

## Governance smoke test

With one imported private catalog under `localdev`, you should see:

- `localdev` sees all owned projects
- another user sees nothing until a project ACL is added
- project groups are scoped to the owner
- project notes are attached through project-level API routes

## Deletion smoke test

Recommended flow on a temporary project:

1. index a disposable project root
2. call `/projects/{id}/deletion-preview`
3. verify `reclaimable_bytes`
4. call `DELETE /projects/{id}` with `confirm=true`
5. verify the files are gone and the project no longer appears in `/projects`
