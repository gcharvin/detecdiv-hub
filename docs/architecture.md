# Architecture

## Goal

`detecdiv-hub` is the central catalog and orchestration layer around the
DetecDiv MATLAB engine.

It is designed to support:

- central indexing of raw microscope datasets
- central indexing of experiment-level scientific records
- central indexing of DetecDiv projects
- remote browsing from client machines
- remote execution on a central Linux GPU server
- optional local execution from a client machine when data is reachable there
- future asynchronous or near-real-time processing as data appears on storage

## Core principles

1. Files remain the source of truth.
2. The database is the central index and job state store.
3. Execution is decoupled from indexing.
4. Paths are machine-dependent views of stable resources.
5. Jobs are executed by workers, not by the database.

## Main entities

- `raw_datasets`
  - microscope acquisitions
  - now also carry lifecycle state such as `hot`, `warm`, `cold`, and archive status
- `experiment_projects`
  - scientific experiments that group acquisitions and downstream analysis
- `raw_dataset_locations`
  - where a dataset is visible on a given host/root
- `detecdiv_projects`
  - optional DetecDiv analysis workspaces derived from experiments
- `project_locations`
  - where a project is visible on a given host/root
- `project_raw_links`
  - project-to-raw lineage
- `experiment_raw_links`
  - experiment-to-raw lineage
- `pipelines`
  - executable pipeline templates or references
- `execution_targets`
  - server GPU, server CPU, local workstation, etc.
- `jobs`
  - queued/running/completed requests
- `artifacts`
  - files or URIs produced by jobs
- `indexing_jobs`
  - auditable asynchronous scans with progress, errors, and final counts
- `users`
  - authenticated or provisioned hub users
- `project_acl`
  - explicit per-project sharing entries
- `project_groups`
  - user-defined project collections
- `project_notes`
  - collaborative or private notes attached to projects

Future entity extensions that should be planned now:

- `users`
  - authenticated users of the hub
- `project_acl` or equivalent sharing tables
  - who can see or modify which project
- `project_groups`
  - user-defined named collections of projects
- `project_group_members`
  - membership of projects inside groups
- `project_notes`
  - free-text or structured notes on projects
- `fovs` and `fov_annotations`
  - future exposed position-level metadata such as mutant or condition
- `storage_usage_snapshots`
  - measured project/raw/derived storage footprints
- `storage_lifecycle_events`
  - auditable archive and restore transitions for raw datasets
- `deletion_requests` and `deletion_artifacts`
  - auditable cleanup workflows

## Distributed path model

Do not assume one absolute path works everywhere.

Instead:

- keep a stable entity ID
- record one or more storage roots
- store relative paths under those roots

Examples:

- server project root: `/srv/detecdiv/projects`
- Windows Samba root: `Z:\detecdiv\projects`
- server raw root: `/srv/microscope/raw`
- Windows raw root: `Z:\microscope\raw`

The same resource can therefore be resolved differently depending on host.

This matters for permissions and destructive actions:

- the server should decide what physical data can actually be deleted
- clients may only request deletion or cleanup through the API
- clients should not be trusted as the source of canonical paths

## Execution model

Jobs can be requested in one of three modes:

- `server`
- `local`
- `auto`

The worker or dispatcher resolves the request against available execution
targets and storage visibility.

Typical flow:

1. Client lists projects through the API.
2. Client requests a job for a project and pipeline.
3. API inserts a queued job.
4. Worker claims the job and executes it.
5. Worker updates status and artifacts.

Execution must remain secondary to governance:

- ownership and access checks should happen before listing or executing on a
  project
- destructive actions must be authorized separately from read access
- storage accounting should be available before offering deletion workflows

## Client/server development workflow

Recommended setup:

- Windows PC:
  - local git clone for editing with Codex
  - Samba mount for browsing server files
  - SSH access to server
  - SSH tunnels for API and PostgreSQL
- Linux server:
  - PostgreSQL
  - FastAPI service
  - worker service
  - MATLAB and/or Python compute runtime

## Near-real-time future path

The architecture is intended to support ingestion daemons that monitor
microscope output folders and create catalog entries or jobs as new data lands.

Key requirement:

- detect when a dataset is stable enough to process
- publish experiment metadata to external systems such as Labguru or eLabFTW
  immediately after successful indexing when configured

That logic belongs in ingestion workers, not in the database itself.

## Storage lifecycle execution

Raw dataset archive and restore requests are now modeled as ordinary worker jobs:

1. API validates access and records the request on the raw dataset.
2. API inserts a queued `jobs` row with `params_json.job_kind` set to either:
   - `archive_raw_dataset`
   - `restore_raw_dataset`
3. The worker claims the job and performs the physical file operation.
4. The worker updates the raw dataset lifecycle state and appends a
   `storage_lifecycle_events` audit row.

Current physical behavior:

- archive writes a compressed bundle (`zip` or `tar.gz`)
- archive can optionally delete the hot source after success
- restore extracts the archive back to the preferred raw location when missing
- failures are reflected both on the job and on the raw dataset lifecycle audit

The first archive policy layer is intentionally computed, not persisted:

- the admin UI proposes candidates based on age, size, tier, and archive status
- preview and queue operations run against the current catalog state
- batch queueing reuses the same per-dataset lifecycle job machinery as manual archive requests

Archive destination resolution:

- first use the request-level `archive_uri` if provided
- otherwise use `DETECDIV_HUB_DEFAULT_ARCHIVE_ROOT`
- compression defaults to `DETECDIV_HUB_DEFAULT_ARCHIVE_COMPRESSION`

## Additional product constraints

### Multi-user visibility

The default assumption should be private-by-default visibility:

- a user sees their own projects
- shared visibility is explicit
- admin/service accounts may have broader access

### Notes and annotations

Two layers of metadata will coexist:

- hub-managed metadata used for indexing, filtering, and collaboration
- DetecDiv project metadata that may ultimately belong in the `shallow` object

If a field must travel with the project outside the hub, prefer eventually
storing it in the DetecDiv project format and syncing it with the hub.

### Storage footprint

Disk pressure is a first-class problem, not a secondary metric. The hub should
be able to report:

- project folder size
- project `.mat` size
- raw-data size
- derived artifact size
- rolled-up per-user and per-group usage

The current schema already carries first project-level size fields:

- `project_mat_bytes`
- `project_dir_bytes`
- `estimated_raw_bytes`
- `total_bytes`

The current implementation also carries a first inventory layer directly on
projects:

- `classifier_count`
- `processor_count`
- `pipeline_run_count`
- `run_json_count`
- `h5_count`
- `h5_bytes`
- `latest_run_status`
- `latest_run_at`

These metrics are intentionally lightweight and server-computable without
MATLAB. Richer FOV/ROI/raw linkage can still be imported from the MATLAB-side
catalog when available.

### Deletion workflow

Project deletion should not begin as a bare `DELETE FROM` operation. The target
model is:

1. preview what would be deleted
2. report size to reclaim
3. require explicit confirmation and authorization
4. execute server-side cleanup
5. log what happened

The current implementation already supports:

- preview endpoint with reclaimable bytes
- explicit confirmation for execution
- optional deletion of project files
- optional deletion of linked raw data when not shared elsewhere
- deletion audit rows in `project_deletion_events`

The current implementation performs a logical delete in the catalog and a
physical delete of files only when requested. This preserves auditability while
removing the project from normal listings.

### Web UI

The API should be designed so that a browser-based frontend can browse and
manage the catalog without MATLAB. The MATLAB client should be one client among
others, not the sole interaction surface.

The repository now includes a minimal web UI served by FastAPI under `/web/`.
It is intentionally framework-free for easy deployment on the Linux server and
currently targets:

- project browsing and filtering
- size and health inspection
- notes and ACL review
- project grouping
- preview-first deletion
- direct hub-side indexing
- live indexing progress and recent indexing history
- search by project name, owner, and storage root
- project admin updates such as owner/visibility changes
- a first pipeline registry UI/API

### Pipeline registry direction

Pipelines will become central once the hub starts dispatching real processing.
The `pipelines` table should evolve from a placeholder into a registry that
captures:

- a stable `pipeline_key`
- a human-readable name and semantic version
- runtime kind (`matlab`, `python`, or hybrid)
- parameter schema and defaults
- expected input type (`project`, `raw_dataset`, batch)
- execution-target compatibility
- provenance links so each launched job records the exact pipeline definition used

This registry should stay separate from historical `pipeline runs` discovered in
project folders. One defines what can be launched; the other records what did
happen.
