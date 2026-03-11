# Space Administration Synthesis

## Why this note exists

This note captures the current target direction for storage governance around
microscopy data and aligns it with `detecdiv-hub`.

The main conclusion is that the hub should evolve into the central web-based
control plane for:

- indexing acquisitions and projects
- organizing storage and ownership
- driving hot/warm/cold lifecycle actions
- exposing backup and restore visibility
- launching and auditing batch processing
- publishing indexed experiments to external systems such as Labguru or eLabFTW

It should not replace the underlying storage, backup, or compute engines.

## Current infrastructure assumptions

- one compute server with 128 GB RAM and RTX 3090
- one Synology NAS used as primary storage
- one Synology NAS used as backup target
- `restic` already used for backup and restore
- Micro-Manager used for acquisition
- OMERO evaluated and rejected as too constraining for the desired workflow

## Core design decision

The primary business object should no longer be the DetecDiv analysis project.

The first-class object should be the scientific experiment. Raw acquisitions can
exist, be indexed, be published to Labguru, be retained, archived, restored,
and later analyzed without requiring immediate creation of a DetecDiv project.

The model therefore becomes:

- `raw_dataset`
  - physical acquisition coming from the microscope
- `experiment_project`
  - scientific container for an experiment and its acquisitions
- `detecdiv_project`
  - optional analysis workspace derived from an experiment
- `job`
  - processing execution on raw data or on an analysis project
- `artifact`
  - outputs, reports, exports, or archive packages

## Target workflow

1. Micro-Manager writes a new acquisition into a landing area.
2. The hub detects the dataset and waits until it is stable.
3. The dataset is indexed as a `raw_dataset`.
4. The dataset is linked to an `experiment_project`.
5. The experiment is published to Labguru or eLabFTW immediately after indexing.
6. Optional automatic analysis is launched.
7. A `detecdiv_project` may be created and linked when analysis needs a
   dedicated workspace.
8. Lifecycle rules decide when data stays hot, becomes warm, or is archived to
   cold storage.

## Storage lifecycle model

Use four data classes instead of treating all storage the same way:

- `hot`
  - recent acquisitions, active projects, direct batch access
- `warm`
  - less active but still directly browsable
- `cold`
  - archived and verified packages, potentially with strong compression
- `backup`
  - independent recovery copy, not the same thing as cold archive

Cold storage should be managed by the hub as a stateful process, not by manual
compression alone. The hub must know:

- what has been archived
- where the archive package lives
- whether archive verification passed
- how many bytes can be reclaimed from hot storage
- whether restore is required before processing

## Role of external systems

- `Synology`
  - physical storage, snapshots, replication, restore primitives
- `restic`
  - backup engine and versioned restore capability
- `detecdiv-hub`
  - orchestration, inventory, UI, audit trail, lifecycle policy
- `Labguru` / `eLabFTW`
  - ELN/LIMS context and external publication targets

The hub should integrate with these systems rather than reimplement them.

## Software guidance

- Keep `detecdiv-hub` as the central control plane.
- Use Synology features for data protection, but do not make DSM the primary UI.
- Keep `restic`, but expose restore operations through a friendlier hub workflow.
- Treat OMERO as optional and scoped to imaging-centric use cases, not as the
  global backbone for storage and orchestration.
- Keep Labguru and eLabFTW as external systems of record for experiment context,
  with links and sync metadata stored in the hub.

## Immediate development consequences

The schema and API should evolve in this order:

1. add first-class `experiment_projects`
2. link `raw_datasets` to experiments
3. link optional `detecdiv_projects` to experiments
4. allow jobs to target raw datasets directly
5. add external publication state for Labguru / eLabFTW
6. add storage lifecycle jobs for archive and restore
7. add admin UI screens for these actions

## Progressive migration from existing folders

The existing storage cannot be flipped in one pass. Migration must be staged and
auditable.

Recommended operating mode:

1. register a legacy root as a migration plan
2. scan and discover candidate raw datasets or DetecDiv projects
3. review the generated plan items
4. decide item-by-item whether to:
   - skip
   - create a placeholder experiment
   - attach to an existing experiment
   - migrate as an analysis project
5. execute pilot migrations on a very small subset first
6. only then widen to complete folders

This allows the hub to support:

- a `discover_only` mode for reconnaissance
- a `placeholder_experiments` mode when raw data exists without mature metadata
- a `pilot` mode when only a few projects should be migrated initially

The current codebase now includes first migration-plan primitives to support
this gradual approach.

## External publication staging

Publication to Labguru or eLabFTW should follow indexing, but it should remain
auditable and retryable. The hub therefore needs persistent publication records
instead of opaque fire-and-forget API calls.

The current implementation direction is:

- create `pending` publication records as soon as an experiment object is
  created or materialized from migration
- keep the exact payload and publication status in the hub database
- let a later connector worker perform the real API push and update the record

## Open implementation questions

- how to detect acquisition completeness robustly for Micro-Manager outputs
- which compression formats are acceptable for cold archive without harming
  future recoverability
- whether archive should target a second NAS share, object storage, or both
- what metadata must be published immediately to Labguru versus linked later
- how much of the restore flow can be safely self-served by non-admin users
