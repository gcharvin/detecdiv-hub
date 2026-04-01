# detecdiv-hub Project Status

This note is intended for agents and contributors who need a fast, accurate
summary of what `detecdiv-hub` is trying to become and what is already
implemented in the repository.

## Mission

`detecdiv-hub` is the server-side control plane around the DetecDiv MATLAB
engine. It is not the compute engine itself.

Its job is to:

- index raw microscope datasets and DetecDiv projects
- keep a central PostgreSQL catalog
- expose a remote API for clients and a lightweight web UI
- manage ownership, visibility, sharing, notes, groups, and deletion workflows
- queue and dispatch processing jobs to workers and execution targets
- support multi-host path resolution between server-visible and client-visible
  storage
- prepare for near-real-time ingestion of microscope acquisitions

The MATLAB `DetecDiv` repository remains responsible for project internals,
pipeline execution logic, and the desktop GUI.

## Current Architectural Shape

Main implemented layers:

- `api/`
  - FastAPI application and route modules
  - auth, project catalog, indexing, jobs, pipelines, raw datasets, dashboard,
    migrations, experiments, and Micro-Manager ingest controls
- `db/schema.sql`
  - PostgreSQL-first schema with explicit catalog, governance, raw-data, job,
    and audit entities
- `worker/`
  - polling worker loop
  - automatic archive-policy scheduler
  - automatic Micro-Manager landing-zone ingest scheduler
  - raw dataset archive/restore execution
- `api/static/`
  - minimal browser UI served by FastAPI under `/web/`
- `scripts/`
  - local development helpers, bootstrap/import scripts, SSH-based storage
    audit tooling, and deployment helpers

## What Is Already Working

The repo is past the pure scaffold stage. It already contains a first usable
hub with the following implemented capabilities.

### Catalog and governance

- `users` table and password-backed login sessions
- project ownership and private-by-default visibility
- per-project ACL sharing
- project groups and project notes
- project-level storage and inventory metrics
- experiment-level catalog objects separate from DetecDiv projects
- raw dataset catalog with lifecycle and archive-related fields

### Project ingestion and browsing

- direct indexing of DetecDiv project roots from the hub
- import from the legacy/local MATLAB SQLite catalog
- project search by name, owner, and storage root
- web UI for browsing projects and reviewing health/size metadata
- live indexing progress and recent indexing history

### Raw dataset and storage lifecycle

- raw dataset records and server-visible storage locations
- archive/restore requests represented as worker jobs
- archive-policy preview and scheduled automatic policy runs
- storage lifecycle audit trail
- SSH storage audit script for remote NAS inspection

### Pipeline and execution surfaces

- minimal independent pipeline registry
- observed-pipeline discovery from indexed project runs
- queued jobs table and worker claim/execute loop
- execution targets model for server CPU/GPU or local execution modes

### Micro-Manager ingestion

- worker-driven polling of a configured landing-zone root
- detection of Micro-Manager-like datasets after a settle period
- creation/update of `raw_datasets` and `experiment_projects`
- optional post-ingest job queueing after dataset ingestion
- audit history for ingest runs

## Current Maturity Assessment

The project is currently a functional early control plane, not yet a hardened
production platform.

Broadly:

- strong progress on schema and product direction
- usable API and web UI for catalog/governance workflows
- real implementation for indexing, deletion preview, archive policy, and
  Micro-Manager landing-zone ingestion
- worker execution is still intentionally partial and conservative
- migrations are still schema-file based rather than handled by a proper
  migration framework

## What Is Still Incomplete

The following areas are partially implemented or still open:

- durable migrations
  - schema is still centered on `db/schema.sql`
- authentication and authorization hardening
  - current auth is enough for local/dev and first admin workflows, but not yet
    a complete remote multi-user security story
- execution wrappers
  - the worker loop exists, but generic job execution is still mostly a
    placeholder outside storage lifecycle operations
- path mapping ergonomics
  - the data model supports multi-host storage roots, but client-side path
    translation and UX still need hardening
- richer ingest provenance
  - current Micro-Manager ingest is landing-zone based, not session/event based
- safe deletion breadth
  - preview-first deletion exists, but reversible cleanup and broader storage
    reclamation policy remain open
- browser UI depth
  - the UI is useful, but still intentionally lightweight and framework-free

## Important Current Constraint

Micro-Manager ingest is currently based on scanning a stable landing-zone
directory after files have finished changing for a configured settle period.

That means:

- it is robust for post-copy ingestion
- it is suitable for near-real-time ingestion after transfer
- it is not true real-time streaming ingestion
- it does not yet capture acquisition intent or microscope state at launch time

This is important when discussing future integration with `pycro-manager` or
`pymmcore-plus`: event-driven acquisition metadata capture would be a new layer
on top of the current stable-dataset ingest model, not a description of the
current implementation.

## Recommended Reading Order For Agents

When picking up work in this repository, read in this order:

1. `AGENTS.md`
2. `PROJECT_STATUS.md`
3. `docs/architecture.md`
4. `db/schema.sql`
5. `api/app.py`
6. `worker/run_worker.py`

Then inspect the route or service layer matching the feature you are about to
change.

## Recommended Near-Term Priorities

If there is no narrower user request, the most leverage is currently in:

1. add a real migrations layer
2. harden authn/authz and multi-user access rules
3. improve server/client path mapping semantics and user-facing workflows
4. enrich raw-data indexing and storage accounting
5. add session-aware microscope acquisition ingest alongside landing-zone ingest
6. harden real job execution wrappers for MATLAB/Python pipelines

## Practical Summary

`detecdiv-hub` is already more than a schema sketch: it is a working catalog,
governance, indexing, and early orchestration service.

The repository is strongest today on:

- project/raw catalog modeling
- governance surfaces
- deletion and archive policy workflows
- direct indexing and landing-zone ingestion
- lightweight admin UI

It is weakest today on:

- durable migrations
- hardened remote security model
- full pipeline execution integration
- event-driven microscope acquisition integration

Agents should treat the repo as an active early product with real functionality,
not as a blank architecture prototype.
