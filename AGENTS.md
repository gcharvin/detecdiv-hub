# AGENTS.md

## Purpose

This repository is the server-side control plane around DetecDiv. It is not the
MATLAB compute engine itself. Its job is to index, expose, orchestrate, and
dispatch work across storage and compute hosts.

Primary responsibilities:

- index raw microscope datasets and DetecDiv projects
- maintain a central catalog database
- expose a remote API to clients
- queue and dispatch processing jobs
- run workers on central compute hosts, including Linux GPU machines
- support both remote execution and local execution against server-hosted data

## Architectural boundaries

Keep the separation explicit:

- `DetecDiv` MATLAB repo:
  - project format
  - GUI and MATLAB processing engine
  - pipeline execution details
- `detecdiv-hub` repo:
  - database schema
  - API
  - job queue and execution orchestration
  - host/path resolution
  - ingestion of raw datasets

Do not move MATLAB project internals into this repo unless there is a clear
cross-host orchestration need.

## Current target architecture

Main subsystems:

- `api/`: FastAPI application exposing catalog and job endpoints
- `db/`: PostgreSQL schema and future migrations
- `worker/`: polling worker that executes queued jobs
- `scripts/`: local dev helpers and SSH helpers
- `ops/`: deployment assets such as systemd unit files
- `docs/`: architecture, deployment, and operating assumptions

Core entities:

- `raw_datasets`: microscope acquisitions and their indexing state
- `detecdiv_projects`: DetecDiv projects derived from raw datasets
- `project_raw_links`: many-to-many mapping between projects and raw datasets
- `pipelines`: pipeline templates or references
- `jobs`: queued/running/completed processing requests
- `artifacts`: outputs produced by jobs
- `execution_targets`: local workstation, server CPU, server GPU, etc.
- `storage_roots` and `locations`: path resolution across Windows/Linux hosts

## Near-term goals

1. Stabilize the schema around projects, raw datasets, and jobs.
2. Expose a minimal API for listing projects and submitting jobs.
3. Implement a worker that can dispatch MATLAB or Python jobs.
4. Support a Windows client connecting through SSH tunnels to server API/DB.
5. Prepare for asynchronous ingestion of microscope data as it lands on server storage.

## Coding guidelines

- Prefer small, explicit modules over clever abstractions.
- Favor PostgreSQL-compatible SQL and keep SQLite assumptions out of the server path.
- Treat paths as machine-dependent views of a stable resource identity.
- Keep job state transitions explicit and auditable.
- Assume multiple hosts and multiple clients from the start.
- Avoid embedding environment-specific secrets or hostnames in code.

## Path and execution model

Always distinguish:

- resource identity
- storage location
- execution target

The catalog should not assume one canonical Windows path or one canonical Linux
path for clients. Server-side services may have canonical Linux storage roots,
but clients can see the same data through Samba or other mapped paths.

Jobs should carry:

- requested execution mode: `auto`, `server`, or `local`
- resolved execution target
- enough metadata to reconstruct what happened later

## Agent workflow expectations

When opening a new agent thread on this repo, the agent should:

1. read this `AGENTS.md`
2. read `docs/architecture.md`
3. inspect `db/schema.sql`
4. inspect the API entrypoint in `api/app.py`
5. inspect the worker entrypoint in `worker/run_worker.py`

For new features, agents should first identify which layer is affected:

- schema only
- API only
- worker only
- path resolution only
- deployment only
- cross-layer change

## High-priority future work

- add migrations instead of a single static schema file
- add authn/authz for remote clients
- define a durable indexing pipeline for raw microscope data
- support streaming or scheduled ingestion from microscope storage
- define server-side execution wrappers for `matlab -batch`
- define structured logs and artifact retention rules

## Non-goals for the early phase

- rebuilding DetecDiv processing logic here
- replacing the MATLAB engine
- implementing every possible microscope format before the schema is stable
- overengineering distributed scheduling before a single-host worker is stable
