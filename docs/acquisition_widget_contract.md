# Acquisition Widget Contract

## Purpose

This document defines the boundary between `detecdiv-hub` and a future
`pymmcore-plus` acquisition widget.

The widget is a client-side acquisition tool. The hub remains the server-side
catalog, storage, ingestion, preview, job, and external-publication control
plane.

## Repository Boundary

Use two repositories:

- `detecdiv-hub`
  - owns API contracts, authentication, database state, ingestion workers,
    preview generation, storage lifecycle, and Labguru/eLabFTW publication
    state;
  - must not import `pymmcore-plus`, Qt, napari, camera drivers, or microscope
    device adapters.
- `detecdiv-acquisition-widget` or equivalent
  - owns the `pymmcore-plus` UI and microscope-facing runtime;
  - connects to the hub as an API client;
  - writes or transfers acquired data into a hub-visible landing zone;
  - sends metadata, progress, and completion events to the hub.

The shared surface is an HTTP API plus storage-root conventions. Do not share
database access or server internals with the widget.

## Target Workflow

1. The widget authenticates to the hub.
2. The widget creates an acquisition session in the hub before acquisition.
3. The hub returns a stable `acquisition_session.id`, a `session_key`, and a
   recommended landing-zone relative path when enough information is available.
4. The widget records user parameters and microscope metadata against the
   acquisition session.
5. The widget writes files to a server-visible landing zone, or writes locally
   first and transfers the final dataset to that landing zone.
6. The widget sends heartbeats and progress updates while acquisition is active.
7. The widget marks the acquisition session complete or failed.
8. A server-side worker indexes the landing-zone dataset once it is stable.
9. The hub creates or updates `raw_datasets`, `experiment_projects`, dataset
   positions, preview jobs, and external-publication placeholders.
10. Labguru/eLabFTW publication remains attached to the `experiment_project`
    through `external_publication_records`.

## Data Movement Rule

Large image data must not be uploaded through the FastAPI API.

Use the API for:

- authentication;
- acquisition session state;
- metadata and parameters;
- progress and completion events;
- links to storage roots and relative paths.

Use storage mechanisms for image data:

- Samba/NFS mounted landing zones;
- `robocopy`, `rsync`, or equivalent transfer tools;
- a future dedicated transfer service if needed.

## Hub-Side Concepts

The hub should distinguish:

- acquisition session: live or recently completed acquisition coordination
  state;
- raw dataset: indexed physical acquisition;
- experiment project: scientific container that can later be published to an
  external ELN/LIMS;
- DetecDiv project: optional analysis workspace derived from one or more raw
  datasets.

The acquisition session may exist before a `raw_dataset` exists. The raw dataset
should only become canonical after worker-side indexing verifies the files.

## Acquisition Session API Shape

Initial endpoints:

- `POST /acquisition-sessions`
  - creates a session before acquisition starts;
  - accepts acquisition label, microscope name, optional landing storage root,
    optional landing relative path, metadata, and acquisition parameters.
- `GET /acquisition-sessions/{id}`
  - returns the current state.
- `PATCH /acquisition-sessions/{id}`
  - updates draft metadata or path information before or during acquisition.
- `POST /acquisition-sessions/{id}/heartbeat`
  - records progress and live metadata from the widget.
- `POST /acquisition-sessions/{id}/complete`
  - marks the acquisition complete and records final metadata.

Status values:

- `draft`
- `acquiring`
- `transferring`
- `completed`
- `failed`
- `cancelled`
- `indexed`

The API should be idempotent where practical. The widget may retry heartbeat and
completion calls after a network interruption.

## Metadata Convention

Store hub-centric metadata under explicit namespaces in JSON fields:

- `source_system`: `pymmcore-plus`
- `acquisition`
- `instrument`
- `sample`
- `operator`
- `timestamps`
- `positions`
- `channels`
- `objective`
- `exposure`
- `pixel_size_um`
- `storage`
- `widget`

Metadata that belongs to the physical acquisition should later be copied or
normalized into `raw_datasets.metadata_json`.

Metadata that describes the scientific experiment should later be copied or
normalized into `experiment_projects.metadata_json`.

Do not store Labguru-specific state in these metadata blobs. Labguru links and
publication state belong in `external_publication_records`.

## First Implementation Slice

The first hub implementation should only provide coordination state:

1. add an `acquisition_sessions` table;
2. expose the API endpoints above;
3. keep paths as `storage_root_id` plus `landing_relative_path`;
4. avoid creating physical directories from the API;
5. avoid moving image data through the API;
6. let the existing Micro-Manager landing-zone worker perform indexing and
   preview generation after files are stable.

Later slices can link completed sessions to worker ingestion runs, raw datasets,
experiment projects, and Labguru publication jobs.

## Open Questions

- Whether the acquisition machine writes directly into server storage or first
  into a local spool directory.
- Which storage root should be the default landing zone for each microscope.
- Whether session creation should reserve a directory on the storage host via a
  worker job instead of returning only a recommended path.
- Which metadata fields are mandatory before acquisition can start.
- How much of the Labguru entry can be created before image indexing succeeds.
