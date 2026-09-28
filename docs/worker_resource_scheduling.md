# Worker Resource Scheduling

## Behavior

Workers admit queued jobs against shared resource capacities for their execution
target. The current settings cover CPU cores, GPU VRAM, and relative disk I/O
weights. `max_concurrent_jobs` and the configured worker process pool remain
additional concurrency ceilings.

The admin Execution Targets page exposes per-job-kind priority and resource
profiles. Priorities and resource profiles are saved in separate
`SystemSetting` values (`job_priority_settings` and `job_resource_settings`).
Defaults are defined in `api/services/job_priority_settings.py` and apply when
no administrator override exists.

The default CPU capacity is 36 cores and the API rejects values above 36. Each
job reserves at least one CPU core. CPU usage is summed for active jobs on a
target before a queued job is claimed. The worker passes a pipeline's allocation
to MATLAB through `maxNumCompThreads`; Python TIFF decode and encode are limited
to one worker in the metadata-preserving compression path.

The default disk capacity is 8 relative units. These are admission weights, not
MB/s: indexing and compression reserve 2 units, while larger archive and
restore operations reserve 3. Administrators can edit both job weights and the
capacity. The initial values are conservative placeholders until storage
throughput is measured.

GPU allocation is disabled by default for non-pipeline jobs. For `pipeline_run`,
the profile allows GPU use, then the worker inspects the pipeline JSON, selected
nodes, node overrides, and run-level device mode. Only selected deep-learning
nodes configured for GPU execution reserve VRAM. A VRAM request of 0 means an
exclusive GPU reservation. The default total VRAM capacity is also 0, so GPU
jobs run one at a time until an administrator enters a measured capacity. If
the worker cannot inspect a pipeline, it conservatively reserves the GPU when
the target supports one and GPU use is allowed. The resolved node classification
and allocation are stored with the job for audit; GPU arbitration uses that same
allocation when pausing or resuming Qwen.

The resource-profile list includes every current queued job kind. TIFF storage
optimization, project indexing, Micro-Manager landing ingestion, raw-data
archive and restore, backup, deletion, inventory, preview, ELN sync, and
user-storage preparation each have an editable default. Ingestion and indexing
work starts with a one-core profile. The periodic Micro-Manager ingestion is
queued as `micromanager_ingest`, so it participates in normal CPU and disk
admission. Raw dataset ingestion requested inside a `pipeline_run` is included
in that pipeline's allocation.

## Admission and runtime notes

The worker locks the execution-target row while evaluating the queue, so worker
instances sharing a target serialize their claims. It scans queued jobs in
priority order and claims the first job that fits the remaining CPU, disk, and
GPU budgets. When a higher-priority job cannot fit yet but can fit once current
jobs release resources, lower-priority jobs may start only if they leave enough
CPU and disk capacity for that waiting job. The scheduler also keeps one worker
slot free for it. This avoids letting small jobs repeatedly consume capacity
needed by a larger pipeline; some capacity can remain idle while the higher
priority job waits. Jobs already running are not preempted.

An allocation is saved in the job parameters at claim time and copied into the
result on completion or failure. Requeued chunk jobs release their previous
allocation before returning to the queue. Orphan recovery frees capacity by
moving stale jobs out of the running state.

The scheduler reserves CPU capacity; it does not pin every Python process to a
CPU set or impose systemd/cgroup quotas. The current compression and ingestion
paths are sequential, and TIFF decode/encode worker counts are explicitly
limited. MATLAB pipeline thread count is capped by its allocation, even when a
job or target asks for more threads. Strict CPU isolation for arbitrary native
libraries would require OS-level limits.

Workers report the host's logical CPU count and the CPUs available through
their process affinity. While a job runs, the worker samples CPU time for its
own process tree and publishes an approximate live core-equivalent usage.
Completed, failed, cancelled, and requeued jobs retain aggregate CPU seconds,
wall time, average cores, and sampled peak cores in `result_json.cpu_usage`.
These are measurements, not hard CPU limits; short-lived subprocesses and work
outside the worker process tree can be undercounted.

The worker CPU display requires the `20260926_worker_cpu_usage.sql` migration
and the worker dependency `psutil`.

Manual Micro-Manager ingestion still runs synchronously through its admin API
and uses the database advisory lock. It does not create a queued job, so it
does not reserve CPU or disk units in the worker budget while it runs.

System RAM is now part of admission. The compute host has one budget shared by
its workers, with RAM kept outside that budget for the Hub VM and host services.
The VM's configured RAM is backed by physical host RAM, not a separate resource.
The worker uses its host's memory measurements, never the API VM's measurements.
Claims remain serialized by the execution-target lock. Memory reservations are
stored alongside CPU/GPU/disk allocations; priority backfill also preserves RAM
for waiting jobs. New claims pause when available host RAM drops below a safety
headroom, including pressure caused by processes outside the Hub.

Worker count is a configurable concurrency ceiling, not a CPU/RAM divisor.
Idle Linux workers have one CPU and 512 MiB RAM. The scheduler reserves 512 MiB
per configured worker from the shared RAM capacity for idle process overhead.
On admission, the shared target lock checks the complete job resource demand.
The worker applies that job's CPUQuota, MemoryMax, MemoryHigh (90%), and swap
share to its own systemd service in place, before starting execution. No other
worker is restarted or resized. Completion/failure returns the worker to its
idle limits. The shared slice enforces the total host budget throughout.

Per-kind CPU and RAM profiles are editable in Execution Targets. Default MATLAB
jobs request 24 GiB RAM; other jobs request 1 GiB. These are conservative defaults,
not measured peaks. Explicit `params_json.resources.cpu_cores` and `memory_mb`
requests override the profile. Memory requests must include the worker process,
MATLAB pools, and Python descendants. Swap is proportional to the job's RAM
share of the pool and remains bounded by the shared swap cap. Requests wait for
available shared capacity instead of being truncated to a fixed worker share.

The UI displays current job CPU/RAM reservations alongside the service limits.
An idle worker reserves no job resources. VRAM is shared and reserved per GPU
job; an exclusive reservation is marked explicitly, not presented as a private
hardware partition. ROI extraction reads the current service's cgroup limits,
so its batching follows the job-specific RAM budget.

The helper's `--job-worker-instance`, `--job-cpu-cores`, `--job-memory-mb`, and
`--job-swap-mb` mode validates the target service's owner and compute slice and
changes only runtime properties. A missing helper or failed quota change stops
admission/execution; it never silently runs a job without its limits. The
persistent unit definitions return to idle limits after a restart.

The temporary global max_concurrent_jobs=1 has been removed. Worker slots and
CPU/RAM/GPU/disk admission determine effective concurrency. Linux systemd
limits include all descendants. Non-systemd workers retain resource admission
but need equivalent platform controls for strict isolation.

The admin worker-scale API now records a desired count instead of invoking
systemd in the API container. `detecdiv-worker-manager.service` on the compute
host applies requests under the same target lock used by job claims. It drains
new jobs, waits for running/cancelling jobs to finish, reconfigures the pool,
and restores the preceding drain setting. Failed scale requests remain drained
and are not retried repeatedly; submit a corrected request. A fresh manager
heartbeat is required by the API. The manager is outside the compute slice and
uses a small separate budget; changing worker count does not restart it.

`--verify-memory-isolation` runs a disposable systemd service that requests more
than its 64 MiB RAM plus 32 MiB swap budget. It verifies an `oom-kill` confined to
that service without changing the worker pool or creating a scientific job.

## Calibration

The disk capacity and per-kind weights should be tuned from observed storage
throughput. GPU VRAM can move from exclusive reservations to concurrent MB
reservations after measuring peak VRAM for actual image sizes and model
settings. The worker pool and `max_concurrent_jobs` should stay bounded while
those measurements are collected.
