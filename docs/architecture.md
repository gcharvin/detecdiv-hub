# Architecture

## Goal

`detecdiv-hub` is the central catalog and orchestration layer around the
DetecDiv MATLAB engine.

It is designed to support:

- central indexing of raw microscope datasets
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
- `raw_dataset_locations`
  - where a dataset is visible on a given host/root
- `detecdiv_projects`
  - DetecDiv projects and their metadata
- `project_locations`
  - where a project is visible on a given host/root
- `project_raw_links`
  - project-to-raw lineage
- `pipelines`
  - executable pipeline templates or references
- `execution_targets`
  - server GPU, server CPU, local workstation, etc.
- `jobs`
  - queued/running/completed requests
- `artifacts`
  - files or URIs produced by jobs
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

That logic belongs in ingestion workers, not in the database itself.

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

### Deletion workflow

Project deletion should not begin as a bare `DELETE FROM` operation. The target
model is:

1. preview what would be deleted
2. report size to reclaim
3. require explicit confirmation and authorization
4. execute server-side cleanup
5. log what happened

### Web UI

The API should be designed so that a browser-based frontend can browse and
manage the catalog without MATLAB. The MATLAB client should be one client among
others, not the sole interaction surface.
