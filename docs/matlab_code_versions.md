# DetecDiv code versions per MATLAB job

New pipeline and legacy MATLAB jobs carry `execution.code_commit`, a full
40-character SHA. The API copies the default release into the job at submission.
An explicit SHA selects a different version for a debug run. A retry retaining
the same job payload retains its version; submit a new job to use a new release.

Workers use detached Git worktrees in a sibling `DetecDiv-jobs` directory,
named `<full-sha>-<job-uuid>`. Each job gets its own directory. The source
checkout is never pulled, reset or switched by the job launcher. Missing
commits are fetched from the configured remote/branch under an OS file lock.
Git preparation is serialized on each host. An unavailable commit fails the job
without silently running a different version.

Before MATLAB starts, the worker persists the selected SHA and code directory
in `result_json.worker_runtime`, including for jobs that subsequently fail.
For jobs queued before this feature, the worker pins the existing source HEAD
on first execution. It does not silently upgrade those old jobs to the release.
MATLAB's `DETECDIV_ROOT` points at the selected job checkout.

## Publish a fix

1. Commit and push DetecDiv to the worker's configured remote and branch.
2. Prepare the SHA on both hosts using the command below. This only fetches
   objects and creates a separate worktree; existing job directories remain
   untouched. Check out the same SHA on Linux and Windows.
3. As a Hub admin, `PUT /matlab-code/release` with
   `{"code_commit":"<full-sha>"}`. `GET /matlab-code/release` reports the default.
   No worker restart is required for subsequent release changes.
4. Submit the next job. It records that SHA at submission, even if it waits
   in the queue while newer releases are published.

```bash
python -m worker.matlab_code_checkout --repo-root /path/to/DetecDiv --commit <full-sha>
```

The command prints the directory and verified SHA. Its worktree is locked against
accidental Git cleanup. It is a preparation directory, not a running job.

Optional worker environment settings:

- `DETECDIV_HUB_MATLAB_JOB_CHECKOUT_ROOT`: directory outside the source checkout.
- `DETECDIV_HUB_MATLAB_GIT_REMOTE`: defaults to `origin`.
- `DETECDIV_HUB_MATLAB_GIT_BRANCH`: defaults to `unstable`.

The API refuses new MATLAB jobs without an explicit SHA or published default.
No database migration is required; the default uses `system_settings` and each
job stores its pin in the existing JSON fields.

## First activation

Install the API additions and publish the initial SHA before admitting new
submissions. New versioned jobs are explicitly bound to a target whose metadata
contains `matlab_code_isolation_ready: true`. Set this capability only after
every polling worker on that target has adopted the new launcher. Unprepared
targets are rejected for explicit requests and excluded from automatic routing.
This permits activating Windows independently while old Linux jobs continue;
old Linux pollers cannot claim versioned jobs targeted to Windows.
Install the worker additions and restart each polling worker only
when it is idle. Drain job admission during the restart to avoid a claim race.
Workers already executing jobs keep their process and code directory; they need
the new launcher when they next become idle. Coordinate the Windows restart
with its current operator. The new policy replaces the requirement to pull a
shared DetecDiv checkout after future processing pushes; release publication
still requires the SHA to be available on both compute hosts.

For the remaining old Linux pollers, the one-shot administration helper
`scripts/finish_matlab_code_activation.py` waits until each specified poller
is idle, reserves target admission, and signals only its original PID. The
existing systemd restart policy reloads it. The helper restores admission and
marks the target ready once all six baseline pollers report the expected new
fingerprint. Run it with the administration workstation's existing SSH aliases:

```powershell
python -u scripts/finish_matlab_code_activation.py --ssh C:\Windows\System32\OpenSSH\ssh.exe --fingerprint <new-worker-fingerprint> --old-poller @1=<original-pid> --old-poller @3=<original-pid>
```

Keep the administration workstation running until the helper reports readiness.
If another deployment changes worker fingerprints, inspect the workers before
setting readiness; this helper deliberately requires the expected fingerprint.
It does not copy database credentials or signal MATLAB processes.

## Retention and limits

No automatic deletion is enabled. Keep job checkouts for debugging. To retire
one, first verify the job is terminal and no process or worker still uses it,
then use `git worktree unlock <directory>` and `git worktree remove <directory>`
from the source repo. Do not use `--force`: modified files must be preserved.
Never prune directories belonging to queued, running, cancelling or resumable
jobs. Preparation directories can be retired once their inspection is complete.

This freezes tracked DetecDiv code, not Python environments, external model
bundles, mutable pipeline definitions, or external legacy routine files.
Do not install/change shared Python environments while another job uses them.
Modules must keep persistent outputs in project or run storage. If a module
modifies tracked code inside its checkout, a subsequent resume is rejected
with an actionable error instead of reusing altered code.
