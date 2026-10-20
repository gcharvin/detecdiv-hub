# Forced stop of a pipeline run

The selected run in the project's **Run Monitor** has two actions. **Cancel run**
requests cooperative cancellation. **Force stop** asks the executing worker to
terminate that run's entire process tree, independently of MATLAB checkpoints.
The confirmation explains that unsaved work is lost and interrupted writes can
leave incomplete files. Existing files are retained for inspection.

`POST /pipeline-runs/{job_id}/force-stop` requires the same project edit access
as cooperative cancellation (admin/service access for projectless runs). It
accepts only `running` and `cancelling` pipeline runs whose worker has advertised
an isolated process scope. The API never signals processes or accepts client
PIDs. Duplicate requests retain the first actor and timestamp. Requests for a
finished job return 409.

The request and authenticated actor are stored in dedicated nullable columns on
`jobs`, so concurrent progress updates cannot overwrite them. The worker polls
the control column every second. It keeps the job `cancelling`, its project
locks, and its resource reservations until all processes have exited. The normal
worker cancellation finalizer then marks it `cancelled` and releases resources.
`force_stopped_at` records confirmed process exit; stdout/stderr remain attached.
If execution finishes before the worker applies the request, the UI distinguishes
the unexecuted request from a confirmed forced stop.

On Windows, the worker creates a private non-breakaway Job Object. The initial
process starts suspended, is assigned to the object, and is then resumed. This
contains even children outliving the original MATLAB process. On Linux, a small
private supervisor becomes a child subreaper before launching MATLAB. Orphaned,
double-forked, and detached descendants stay under this supervisor. Forced stop
freezes the descendants to prevent concurrent forks, kills them, and waits for
the supervisor to reap them. The Hub worker and other job scopes are outside this
tree. Cleanup failure or uninterruptible kernel I/O keeps resources reserved
until the OS confirms exit.

## Rollout

1. Apply `db/migrations/20261020_job_force_stop.sql` before updating API/workers.
   The idempotent schema bootstrap also includes these columns.
2. Deploy the API/static assets and updated worker files, including both new
   modules in `worker/executors/`.
3. Activate updated workers only when idle, following `CURRENT_DEPLOYMENT.md`.
   Do not restart busy instances to enable this feature.
4. New executions advertise `worker_runtime.force_stop_supported=true` only
   after containment is established. Executions already running on older worker
   code cannot acquire containment retroactively; their button is disabled.

No MATLAB engine or plugin changes are required. No scientific job needs to be
submitted to verify the stop mechanism.

## Verification

Run `python -m pytest tests/test_job_force_stop.py tests/test_pipeline_terminal_status.py`
on Windows and Linux, plus `node --test tests/test_force_stop_ui.cjs`.
The process tests use synthetic Python trees, including parents exiting before
detached children, preserve an unrelated process, and execute a subsequent job in
the same worker process. An integration test checks cancellation finalization,
log artifacts, actor audit, and release of project locks after confirmed exit.
