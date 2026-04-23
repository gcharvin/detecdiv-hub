# Pipeline Dependency Hub UI Spec

## Purpose

This document defines how `detecdiv-hub` should surface pipeline dependency
status before execution.

It complements:

- [pipeline_path_policy.md](C:/Users/charvin/Documents/MATLAB/detecdiv-hub/docs/pipeline_path_policy.md)
- [pipeline_dependency_matlab_spec.md](C:/Users/charvin/Documents/MATLAB/detecdiv-hub/docs/pipeline_dependency_matlab_spec.md)
- [pipeline_run_contract.md](C:/Users/charvin/Documents/MATLAB/detecdiv-hub/docs/pipeline_run_contract.md)

The hub should not become the primary owner of MATLAB-internal dependency
rewriting. Its job is to:

- validate execution readiness per target host
- expose normalized pipeline references and dependency status
- fail early when required linked resources are unresolved

## Product Goal

Before a user launches a pipeline run, the hub UI should answer:

- is this pipeline portable
- if not portable, is it still executable on the selected host
- which dependencies are embedded
- which dependencies are linked externally
- which dependencies are missing or stale

The user should not need to infer this from a late MATLAB crash on
`classifier_6`.

## Status Model

The hub should display one pipeline dependency status:

- `portable`
- `linked_resolvable`
- `linked_unresolvable`
- `broken`

Operational meaning:

- `portable`
  - all required run dependencies are embedded or relative to project or
    pipeline anchors
- `linked_resolvable`
  - the pipeline depends on external resources, but all required ones resolve
    on the selected execution target
- `linked_unresolvable`
  - the pipeline depends on external resources and at least one required
    dependency does not resolve on the selected execution target
- `broken`
  - the dependency model is malformed or required assets are missing even before
    host-specific resolution

## Validation Timing

Validation should happen in three places.

### Pipeline Detail View

When a pipeline is viewed in the UI:

- show the last known dependency status
- show whether this status is stale or current
- show a compact summary of dependency counts by mode

### Run Builder

When a project, pipeline, and execution target are selected:

- compute or fetch target-specific dependency validation
- show the effective execution readiness before submit
- block or warn depending on status

### Worker Execution

Just before execution:

- persist the final normalized dependency validation in the job payload or
  result
- fail before MATLAB starts if required linked resources are unresolved

This protects against races and stale UI state.

## Suggested API Shape

The exact route may evolve, but the hub should expose a target-aware validation
surface returning structured data.

Example:

```json
{
  "pipeline_id": "uuid",
  "project_id": "uuid",
  "execution_target_id": "uuid",
  "status": "linked_resolvable",
  "summary": {
    "embedded_count": 4,
    "linked_count": 1,
    "derived_count": 2,
    "ephemeral_count": 3,
    "required_missing_count": 0
  },
  "dependencies": [
    {
      "module_id": "classifier_6",
      "resource_type": "classifier_bundle",
      "dependency_mode": "linked",
      "required_for": ["run"],
      "locator_kind": "external_path",
      "configured_reference": "C:/Users/charvin/.../cnnlstm_2",
      "resolved_path": "/data/Gilles/models/cnnlstm_2",
      "is_resolved": true,
      "is_legacy": true,
      "message": "Legacy external path resolved through host mapping."
    }
  ],
  "normalized_pipeline_ref": {
    "pipeline_id": "uuid",
    "pipeline_json_path": "/data/Gilles/analysis/projects/projectpipeline/projectpipeline_pipeline"
  }
}
```

The UI does not need to show all of this by default, but the API should return
enough structure for diagnostics and later automation.

## UI Presentation

### Pipeline Table

Add lightweight columns or badges:

- portability status
- linked dependency count
- unresolved required dependency count

Example labels:

- `Portable`
- `Linked OK`
- `Linked Missing`
- `Broken`

### Pipeline Detail Panel

Show:

- normalized pipeline reference used by the hub
- dependency summary by mode
- required linked dependencies
- unresolved items with module names
- whether the status came from:
  - last audit
  - selected target validation
  - actual run-time normalization

### Run Builder

Show a compact readiness box:

- selected project
- selected pipeline
- selected target
- resulting dependency status

Behavior:

- `portable`: submit enabled
- `linked_resolvable`: submit enabled with visible note
- `linked_unresolvable`: submit disabled by default
- `broken`: submit disabled

The user should see the reason in plain language, for example:

- `classifier_6 requires linked classifier bundle not available on GC-CALCUL-306`
- `module classifier_6 still references legacy absolute path with no target resolution`

## Job Payload And Result Visibility

The hub should persist or expose:

- the normalized `pipeline_ref` actually used for execution
- dependency validation status at submit time
- dependency validation status at worker execution time
- differences between configured and normalized references

This is especially important because the current worker already normalizes the
top-level pipeline reference before MATLAB launch. That normalized reference
should be visible in the UI, not hidden in worker-only behavior.

## Failure Policy

The hub should fail early before launching MATLAB when:

- a required linked dependency is unresolved on the chosen host
- a required dependency record is malformed
- the pipeline reference itself cannot be normalized

The hub may allow submission with warnings when:

- only non-required dependencies are unresolved
- only `derived` or `ephemeral` dependencies are missing

## Transitional Support For Legacy Pipelines

During transition, many pipelines will still contain raw absolute paths.

The hub should therefore:

- mark them clearly as legacy
- distinguish "legacy but resolved" from "legacy and unresolved"
- encourage normalization rather than masking the issue

The hub should not silently claim portability for a pipeline that merely
happens to work on one host because a legacy path was guessed successfully.

## Suggested First Implementation

1. Add a dependency-status block to the pipeline run detail and builder views.
2. Persist normalized top-level `pipeline_ref` in job result or params for
   inspection.
3. Introduce a server-side validation function returning `portable`,
   `linked_resolvable`, `linked_unresolvable`, or `broken`.
4. Block run submission on `linked_unresolvable` and `broken`.
5. Extend the UI later with dependency drill-down once the validation payload is
   stable.
