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

