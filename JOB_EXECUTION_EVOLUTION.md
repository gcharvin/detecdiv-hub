# Job execution evolution notes

This note summarizes a design discussion about how DetecDiv Hub should evolve
its internal and external job execution model.

## Current behavior

Pipeline runs submitted through the app are stored in the `jobs` table and
claimed by `worker/run_worker.py`.

With the current deployment model, a single `detecdiv-worker` process runs:

- one queued pipeline job is claimed at a time
- the active job is marked `running`
- other jobs remain `queued` in PostgreSQL
- jobs are ordered by priority, then creation time

This means pipeline runs are effectively sequential across users as long as only
one worker process is running. Queued jobs themselves are not a memory pressure
risk, because they mostly exist as database rows. The main memory risk comes
from the currently running job, for example a MATLAB process loading a large
project or raw dataset.

The database claim logic uses `FOR UPDATE SKIP LOCKED`. This prevents duplicate
claiming, but it also means multiple worker processes would be able to run
multiple jobs in parallel. That is useful later, but concurrency limits must then
be explicit.

Indexing jobs currently behave differently. They are submitted through the
`indexing_jobs` path and launched by a Python `ThreadPoolExecutor` in the API
process with `max_workers=2`. If three users launch three indexing jobs, two may
run in parallel and the third waits in the executor queue. This is acceptable for
an early implementation, but it is less durable than the `jobs` worker model
because the queue is in API process memory.

## Recommended near-term direction

Keep the pipeline worker sequential until the resource model is explicit.

For indexing, consider moving indexing work into the same durable worker model
used for pipeline jobs, or introduce an equivalent persisted claim loop. The
goal is to avoid relying on API-process memory for queued background work.

Useful next steps:

- represent indexing as durable queued work rather than only an API thread-pool task
- make worker concurrency an explicit deployment/configuration decision
- record which execution target claimed a job
- preserve auditable state transitions for queued, running, done, failed, stale,
  and cancelled work

The admin UI can now store first resource-control hints on an execution target
through `metadata_json`:

- `max_concurrent_jobs`: maximum jobs that workers for this target should claim
- `matlab_max_threads`: MATLAB thread cap injected as `maxNumCompThreads(N)`

These are intentionally target-level knobs rather than global assumptions,
because a future server CPU target, server GPU target, Slurm partition, or
external compute target may need different values.

## Internal scaling

Running more than one hub worker is possible in principle because the current
claim logic uses row-level locking with `SKIP LOCKED`. However, that should only
be enabled once the system can express resource constraints.

Concurrency policy should eventually account for:

- maximum simultaneous jobs per execution target
- CPU versus GPU jobs
- MATLAB versus Python runtime requirements
- memory-heavy pipelines
- user or group priority
- exclusive jobs that should never overlap
- cancellation and cleanup behavior

Until these rules exist, a single worker is the safest operational default.

## Slurm integration

Slurm would be useful for heavy compute jobs, especially MATLAB batch runs,
Python batch runs, GPU jobs, and long-running processing workflows.

The hub should remain the control plane:

- validate access and ownership
- store job parameters
- store pipeline provenance
- decide the execution target
- track job status and artifacts

Slurm should be introduced as one execution strategy behind `execution_targets`,
not as a global dependency of the hub.

A future Slurm executor would likely:

- generate an `sbatch` script from a pipeline job
- submit it to Slurm
- store the returned `slurm_job_id`
- map Slurm states to hub job states
- collect stdout and stderr paths as artifacts or metadata
- support cancellation through Slurm

Likely fields can live initially in `jobs.result_json` or `jobs.params_json`, but
a later schema can promote them to first-class columns if needed:

- `slurm_job_id`
- `slurm_partition`
- `slurm_account`
- `requested_cpus`
- `requested_memory`
- `requested_gpus`
- `time_limit`
- stdout/stderr log paths

Slurm is most valuable once the hub needs controlled parallelism. If the system
only runs one compute job at a time, the current queue is simpler.

## External compute targets

The hub can eventually delegate jobs to an external compute server, but the hard
part is data access, not submitting the command.

Possible data-access models:

- shared storage: the external server sees the same data through NFS, Samba,
  Ceph, GPFS, or another shared mount
- temporary staging: the hub copies required data to remote scratch space,
  launches the job, then retrieves artifacts
- execution bundle: the hub creates a reproducible bundle containing project
  files, parameters, and minimal required data
- compute near storage: the preferred imaging model when datasets are large

Shared storage is the best first target. It keeps the hub aligned with the
existing storage-root model: a stable resource is resolved to a target-specific
path rather than copied.

Temporary staging is more flexible but adds substantial complexity:

- deciding exactly which files must be copied
- handling hundreds of GB or TB-scale datasets
- retries and resumable transfers
- scratch quotas and cleanup
- data integrity checks
- artifact collection
- security and access control

Do not treat external compute as just "run SSH remotely". The execution target
must describe whether it has direct visibility of the data or needs staging.

## Architectural rule

Keep these concerns separate:

- resource identity: which project, raw dataset, pipeline, and parameters
- storage location: where the data lives on server-managed storage
- execution target: which machine or scheduler will run the job
- data-access strategy: direct path visibility, shared mount, staging, or bundle
- audit trail: what was launched, where, by whom, with which parameters, and what
  artifacts were produced

This separation should guide future schema, API, worker, and deployment changes.
