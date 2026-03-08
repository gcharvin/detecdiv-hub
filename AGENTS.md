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
- `indexing_jobs`: auditable asynchronous scans of project or raw-data roots
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

## Product priorities to keep in mind

The repository is not just a technical queue runner. It must evolve into a
usable multi-user catalog for real data stewardship. Keep these product needs
explicit in future threads:

1. access control and authorship
2. reliable project deletion and storage reclamation
3. storage footprint accounting
4. project grouping and filtering
5. project- and FOV-level annotations
6. a web UI that does not depend on MATLAB

These items are not optional polish. They affect the schema and ownership model
early, so they should be considered before the job system gets too rigid.

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

Server paths and client paths must remain separate concerns:

- the hub stores canonical server-visible locations
- each client maps those locations locally through Samba or other mounts
- the hub must not depend on one user's Windows drive letters
- client settings may include path-prefix mappings, but the database should
  keep stable server-side roots as the primary reference

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
- define a first-class pipeline registry with:
  - stable pipeline keys and versions
  - runtime kind (`matlab`, `python`, hybrid)
  - parameter schema/defaults
  - compatibility constraints for execution targets
  - provenance links between a launched job and the exact pipeline definition used

## Required future capabilities

The following capabilities should shape upcoming schema and API changes:

- Project access control:
  - each project should have an owner or author
  - users should normally see only projects they own or are granted access to
  - groups and shared access must be possible
- Project deletion:
  - deleting a project should support a safe dry-run mode
  - deletion may include derived artifacts, project folders, and optionally
    linked raw data if explicitly requested and safe
  - deletion must be auditable and reversible where possible
- Storage accounting:
  - the hub should measure project footprint, raw-data footprint, and total
    owned storage
  - these metrics should be queryable for cleanup and quota decisions
- User grouping:
  - users should be able to organize projects into named groups or collections
  - filtering by group should be first-class in API and UI
- Notes and annotations:
  - projects need user-editable notes
  - FOVs will eventually need annotations such as mutant, condition, or other
    biological context
  - if a note semantically belongs to the DetecDiv project object itself, the
    long-term source of truth may need to be pushed back into the `shallow`
    project format rather than living only in the hub DB
- Web interface:
  - plan for a browser-based UI that can browse the catalog without MATLAB
  - the API should therefore avoid MATLAB-specific assumptions in its contract

## Recommended implementation order

Unless a thread has a narrower goal, the default order should be:

1. authentication, users, and project ownership
2. server/client path mapping and stable storage-root semantics
3. richer project/raw indexing including size accounting
4. notes, project groups, and annotation primitives
5. safe deletion workflows with dry-run and audit trail
6. web UI
7. remote execution and job orchestration hardening

## Non-goals for the early phase

- rebuilding DetecDiv processing logic here
- replacing the MATLAB engine
- implementing every possible microscope format before the schema is stable
- overengineering distributed scheduling before a single-host worker is stable
