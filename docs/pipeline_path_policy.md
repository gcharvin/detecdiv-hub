# Pipeline Path Policy

## Purpose

This document defines how DetecDiv pipelines and pipeline modules should
reference files, models, and other runtime assets so that:

- projects remain movable across hosts
- pipelines can be portable when appropriate
- heavy external classifiers do not need to be copied by default
- runs fail early with explicit dependency diagnostics

This policy is intended to align pipeline behavior with the existing project
loading philosophy already used by `shallowLoad`: the current project location
is the anchor, and old machine-specific paths must not be treated as the
primary source of truth.

## Problem Statement

Today, some pipeline objects are effectively portable while others carry hidden
absolute paths inside module metadata.

This creates several failure modes:

- the project itself rebases correctly on load, but an attached pipeline still
  points to an old Windows path
- a module such as a classifier may depend on large external assets that were
  intentionally not copied
- the current run may succeed in resolving the top-level pipeline path while
  still failing later on a nested module dependency
- old absolute paths survive as implicit references even when the actual intent
  was "linked external resource", not "embedded local file"

The core issue is therefore not only path syntax. It is that the mobility and
dependency status of module assets are not modeled explicitly enough.

## Core Principles

1. Absolute paths must never be the only canonical persisted reference.
2. Every module dependency must declare its mobility mode.
3. A pipeline may be executable without being fully portable.
4. Large classifier bundles must not be copied by default.
5. Migration and execution must validate dependencies explicitly instead of
   relying on best-effort path guessing.

## Anchors

Persisted path-like references must be interpreted relative to a known anchor.

Supported anchors:

- `project_root`
- `pipeline_root`
- `module_root`
- `external_resource`

Examples:

- a portable model shipped with the pipeline should usually be stored relative
  to `pipeline_root` or `module_root`
- a resource intentionally shared across several pipelines should be treated as
  `external_resource`

## Dependency Modes

Each pipeline module dependency must declare one of the following modes.

### `embedded`

The asset is stored with the pipeline or module and is expected to travel when
the pipeline is copied or migrated.

Typical examples:

- lightweight config files
- small model weights intentionally bundled with the pipeline
- static assets required for execution on any host

### `linked`

The asset is external and is not copied by default during project or pipeline
migration.

Typical examples:

- a shared classifier bundle already curated elsewhere
- a very large training dataset or ROI corpus
- a central server-side resource intended to be reused by many pipelines

This is the correct mode for heavy classifiers that should remain shared rather
than duplicated.

### `derived`

The asset is not stored as primary data and can be rebuilt from other canonical
sources.

Typical examples:

- recomputable caches
- exported intermediate representations

### `ephemeral`

The asset is runtime-local or temporary and should not be considered part of
the portable pipeline definition.

Typical examples:

- temporary run caches
- transient exports
- scratch files created during execution

## Canonical Dependency Record

The implementation does not need to adopt this exact JSON immediately, but the
logical structure should converge toward something equivalent.

```json
{
  "id": "classifier_model",
  "resource_type": "classifier_bundle",
  "dependency_mode": "linked",
  "required_for": ["run"],
  "locator": {
    "kind": "registry_key",
    "key": "cnnlstm_2_v2025_09_12"
  },
  "resolution_policy": {
    "must_exist": true,
    "copy_on_migrate": false
  },
  "observed": {
    "last_resolved_path": "C:/Users/charvin/.../cnnlstm_2"
  }
}
```

Accepted locator kinds:

- `anchored_path`
- `registry_key`
- `content_hash`
- `external_path`

Preferred usage:

- use `anchored_path` for portable bundled assets
- use `registry_key` for shared external resources
- use `external_path` only as a compatibility bridge for legacy content

## Classifier Bundles

Classifiers are special because the full historical bundle may be much larger
than the minimum set needed to run inference.

For path policy purposes, a classifier should be treated as a logical bundle
with separable parts such as:

- inference assets
- training assets
- metadata
- provenance

Recommended profiles:

- `embed_minimal`
  - copy only the assets required for inference
- `embed_full`
  - copy the full classifier bundle
- `link_full_bundle`
  - keep a shared external classifier bundle without copying it

Default recommendation:

- for routine pipeline execution, depend on inference assets only
- do not copy training ROIs or large training corpora unless explicitly needed

## Pipeline Status

The system should compute a portability or resolution status for each pipeline.

Suggested statuses:

- `portable`
- `linked_resolvable`
- `linked_unresolvable`
- `broken`

Meaning:

- `portable`
  - all required run dependencies are embedded or anchored within the pipeline
    or project
- `linked_resolvable`
  - external dependencies exist and can be resolved on the selected execution
    target
- `linked_unresolvable`
  - external dependencies are declared but cannot currently be resolved on the
    selected execution target
- `broken`
  - required dependencies are missing, malformed, or internally inconsistent

This distinction is important: a linked classifier is not automatically an
error. It only becomes an error when the target host cannot resolve it.

## Rules By Workflow

### Adding a Module to a Pipeline

When a module is added:

- small required assets should default to `embedded`
- heavy shared classifiers should default to `linked`
- the user should choose whether the dependency is intended to travel with the
  pipeline or remain external

The system must not silently persist a bare absolute path without also
recording that the dependency is linked and non-portable by default.

### Saving a Pipeline

When a pipeline is saved:

- all dependencies should be normalized
- internal paths under the pipeline should be rewritten as anchored relative
  references
- project-local references should be marked relative to `project_root`
- linked external dependencies should remain external but explicit
- a portability report should be produced

### Migrating a Project or Pipeline

During migration:

- `embedded` dependencies should be copied or preserved with their anchor
- `linked` dependencies should not be copied by default
- `derived` dependencies may be omitted and later rebuilt
- `ephemeral` dependencies should be ignored

After migration, the system should recompute which linked dependencies are
resolvable in the destination environment.

### Running a Pipeline

Before execution:

- every required dependency should be resolved against the chosen host
- the run preparation step should emit structured diagnostics
- execution should fail early if a required dependency is missing

The current best-effort behavior of repairing only the top-level pipeline path
is not sufficient for nested module dependencies.

## Legacy Compatibility

Existing pipelines may still store historical absolute paths.

Transitional behavior should therefore be:

- accept legacy absolute paths during load or migration
- import them as `linked` with locator kind `external_path`
- mark them as legacy or unstructured
- encourage normalization during save, migration, or explicit repair actions

This avoids breaking old content immediately while still moving the format
toward explicit dependency modeling.

## Responsibilities

### DetecDiv MATLAB Side

DetecDiv should own:

- dependency discovery inside `pipeline.json` and `module.json`
- path normalization during save or export
- migration-time conversion of legacy absolute paths
- module-level dependency audits

This belongs primarily in the pipeline and project format layer rather than in
the hub.

### detecdiv-hub Side

The hub should own:

- host-aware validation of whether linked dependencies are resolvable
- execution-target-specific diagnostics
- display of the normalized run payload and dependency status
- early refusal of runs whose required linked dependencies cannot be resolved

The hub may assist with path validation, but it should not become the place
where arbitrary MATLAB-internal module metadata is heuristically rewritten.

## Recommended Next Steps

1. Add a DetecDiv dependency audit function that scans `pipeline.json` and all
   `module.json` files and classifies every path-like dependency.
2. Add a DetecDiv normalization function that rewrites portable dependencies as
   anchored relative references.
3. Define a first structured representation for linked classifier bundles.
4. Extend the hub UI and run diagnostics to surface portability and dependency
   resolution clearly before execution.
5. Migrate legacy pipelines progressively instead of trying to rewrite them only
   at run time.
