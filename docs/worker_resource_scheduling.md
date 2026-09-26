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

System memory is not currently part of admission. The initial scheduler tracks
CPU, GPU VRAM, and disk pressure as requested; memory measurements can inform a
later capacity field.

## Calibration

The disk capacity and per-kind weights should be tuned from observed storage
throughput. GPU VRAM can move from exclusive reservations to concurrent MB
reservations after measuring peak VRAM for actual image sizes and model
settings. The worker pool and `max_concurrent_jobs` should stay bounded while
those measurements are collected.
