# Pipeline Dependency MATLAB Spec

## Purpose

This document defines the MATLAB-side responsibilities for auditing,
normalizing, and migrating pipeline dependencies.

It complements [pipeline_path_policy.md](C:/Users/charvin/Documents/MATLAB/detecdiv-hub/docs/pipeline_path_policy.md)
by translating the policy into concrete DetecDiv-side functions and expected
behavior.

The goal is to make path and dependency handling explicit when:

- building a pipeline
- attaching a module
- saving a pipeline
- migrating a project or pipeline

The hub may validate execution readiness, but dependency discovery and
normalization belong primarily to the DetecDiv format layer.

## Scope

This spec applies to:

- `pipeline.json`
- `module.json`
- module-adjacent assets
- classifier bundles and references
- project-attached and external pipelines

This spec does not require immediate format freeze. It defines the target
behavior and the first implementation steps needed to converge there.

## Design Principles

1. Dependency discovery must be deterministic and repeatable.
2. Normalization must preserve intent, not just rewrite strings.
3. Legacy absolute paths must be accepted temporarily but classified
   explicitly.
4. The save path must not silently destroy a legitimate external link.
5. Migration must distinguish between assets to copy and assets to leave
   external.

## Canonical MATLAB Functions

### `pipelineAuditDependencies`

Recommended signature:

```matlab
report = pipelineAuditDependencies(pipelineRoot, varargin)
```

Suggested optional inputs:

- `ProjectRoot`
- `Mode` with values such as `save`, `migrate`, `run`, `repair`
- `TargetHost`
- `Strict`

Responsibilities:

- load `pipeline.json`
- enumerate modules
- inspect each `module.json`
- discover all path-like dependency fields known by the runtime
- classify each dependency according to the path policy
- report missing assets, legacy absolute paths, and portability status

The output should be machine-readable and suitable for both CLI output and GUI
or hub integration.

Suggested report fields:

```matlab
report.status
report.pipelineStatus
report.summary
report.dependencies
report.errors
report.warnings
report.suggestedFixes
```

Each dependency entry should include at least:

- module id or module path
- field name
- dependency mode
- locator kind
- resolved path if available
- whether it is required for run
- whether it is portable
- whether it is legacy or unstructured

### `pipelineNormalizeDependencies`

Recommended signature:

```matlab
[changed, report] = pipelineNormalizeDependencies(pipelineRoot, varargin)
```

Suggested optional inputs:

- `ProjectRoot`
- `WriteChanges`
- `CreateBackup`
- `CopyEmbeddedAssets`
- `RepairLegacyPaths`

Responsibilities:

- call the audit logic first
- rewrite portable dependencies as anchored relative references
- preserve linked external dependencies explicitly
- optionally copy embedded assets if requested by the chosen workflow
- emit a normalization report

This function must not blindly rewrite every absolute path into a relative one.
It must first determine the dependency intent.

### `pipelineMigrateDependencies`

Recommended signature:

```matlab
[changed, report] = pipelineMigrateDependencies(srcPipelineRoot, dstPipelineRoot, varargin)
```

Suggested optional inputs:

- `SourceProjectRoot`
- `DestinationProjectRoot`
- `CopyMode`
- `ClassifierPolicy`

Responsibilities:

- migrate the pipeline definition and its explicit dependency records
- copy only dependencies that should travel
- leave linked resources external
- recompute dependency status in the destination environment

This may eventually be a wrapper around the same underlying audit and
normalization primitives.

## Discovery Rules

Dependency discovery should not rely on generic string scraping alone.

Preferred order:

1. known structured fields in `pipeline.json`
2. known structured fields in `module.json`
3. module-type-specific extractors
4. optional fallback detection for legacy content

This matters because the meaning of a path depends on the field and module
semantics.

Examples:

- a classifier model path is not equivalent to a temporary export directory
- a training ROI bundle may be optional for run but required for retraining

## Dependency Classification

Each discovered dependency should be classified along these axes:

- `dependency_mode`
  - `embedded`
  - `linked`
  - `derived`
  - `ephemeral`
- `locator_kind`
  - `anchored_path`
  - `registry_key`
  - `content_hash`
  - `external_path`
- `required_for`
  - `run`
  - `edit`
  - `retrain`
  - `export`
- `legacy_state`
  - `structured`
  - `legacy_unstructured`

The classifier should prefer preserving semantics over maximizing portability.

## Normalization Rules

### Portable Internal Assets

If an asset lives under the pipeline or module tree and is intended to travel
with the pipeline:

- classify it as `embedded`
- store it as an anchored relative reference
- prefer `module_root` when the asset belongs to one module
- prefer `pipeline_root` when the asset is shared by several modules

### Project-Local Assets

If an asset is intentionally project-local:

- classify it relative to `project_root`
- do not silently upgrade it into a pipeline asset

This is especially important when the pipeline is attached to a project and is
expected to move with that project rather than as a standalone export.

### External Shared Assets

If an asset is intentionally shared and should not be copied:

- classify it as `linked`
- store a structured external locator
- prefer `registry_key` if available
- use `external_path` only as transitional compatibility

### Temporary Or Recomputable Assets

If an asset is a cache or derivative:

- classify it as `derived` or `ephemeral`
- do not treat it as a portability blocker unless required by the current mode

## Classifier Policy

Classifier dependencies need explicit handling because they often mix:

- inference weights
- metadata
- training ROIs
- historical artifacts

Recommended policy options:

- `embed_minimal`
  - embed only assets required for inference
- `embed_full`
  - embed the complete classifier bundle
- `link_full_bundle`
  - keep the classifier external and shared

Default recommendation:

- use `embed_minimal` for portable exports
- use `link_full_bundle` for shared heavy classifiers in day-to-day work

The audit report should make this choice visible instead of hiding it in raw
absolute paths.

## Save Workflow

When a pipeline is saved:

1. audit dependencies
2. classify each dependency
3. rewrite structured portable references
4. preserve explicit linked references
5. warn or block on unresolved required dependencies
6. persist a portability summary

The save operation should not silently produce a pipeline that looks valid but
contains hidden stale external paths.

## Migration Workflow

When a project or pipeline is migrated:

1. audit source dependencies
2. decide copy behavior from dependency mode and selected migration policy
3. copy `embedded` assets when required
4. preserve `linked` assets as linked
5. drop or rebuild `ephemeral` and `derived` assets as appropriate
6. recompute status in the destination
7. emit a migration report

The migration report should make clear:

- what was copied
- what remained external
- what became unresolved
- what requires user action

## Run Preparation Workflow

Before starting execution from MATLAB:

1. audit dependencies in `run` mode
2. verify every dependency required for run
3. fail early with structured errors if a required dependency is unresolved

This is the last safety net, not the primary repair path.

## Backward Compatibility

Legacy pipelines may still contain fields with raw absolute paths.

Transitional behavior:

- detect them
- mark them as `legacy_unstructured`
- map them to `linked` plus `external_path` if no better structure exists
- offer normalization on save or migration

The implementation should prefer surfacing this state explicitly rather than
pretending the pipeline is portable.

## Suggested First Implementation

1. Implement a read-only `pipelineAuditDependencies`.
2. Add module-type-specific extractors for classifiers first.
3. Define a first report schema stable enough for UI and tests.
4. Implement `pipelineNormalizeDependencies` in write-disabled dry-run mode.
5. Enable write mode only after audit output looks correct on real legacy
   projects.
