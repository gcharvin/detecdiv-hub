# DetecDiv code versions per MATLAB job

New pipeline and legacy MATLAB jobs carry `execution.code_commit`, a full
40-character SHA. The API copies the default release into the job at submission.
An explicit SHA selects a different version for a debug run. A retry retaining
the same job payload retains its version; submit a new job to use a new release.

Workers reuse one detached Git worktree per SHA and machine in the sibling
`DetecDiv-jobs/releases/<full-sha>` directory. Release files are read-only:
POSIX write permissions are removed on Linux; an inherited NTFS deny-write/delete
ACL applies on Windows. This protects against accidental module writes, not
administrators deliberately changing permissions. Each job attempt has its own
persistent `DetecDiv-jobs/work/<job-uuid>/<attempt-uuid>` directory for payloads,
logs, results and temporary files. MATLAB's working directory and temporary
environment point there; `DETECDIV_ROOT` and MATLAB paths point at the release.
On Linux, MATLAB receives a short `/tmp/dd-matlab/<attempt-id>` alias for the
attempt's `tmp` directory. R2024b can exit before running `-batch` when
`TMPDIR` contains the full work-directory path. The alias is removed after the
job, while its temporary files remain in the attempt directory. The source
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

From the Windows administration workstation, after committing DetecDiv on
`unstable`, run one command from the Hub checkout:

```powershell
python scripts/deploy_detecdiv_release.py <full-40-character-sha>
```

The script uses `ops/detecdiv_release_targets.json` for the local source repo,
Git remotes, Linux and Windows worker locations, API container, and Hub publisher.
It checks that the SHA is on the local `unstable` branch, pushes it to `origin`
and `gitlab` when needed without force, verifies both targets have MATLAB code
isolation enabled, and prepares the same protected release worktree on both.
Only after both preparations succeed does it publish the new default through
the Hub's existing release service. It verifies the default afterwards. A
failure leaves the previous default in place; a successful worktree preparation
is safe to reuse on retry. An already published SHA is safe to request again.
Use `--config` for another deployment topology or `--publisher` to override the
recorded Hub admin/service identity.

The source checkouts' HEADs do not move. No worker restart is needed. Jobs that
were already submitted keep their pinned SHA; the next new job receives the
published SHA at submission, even if it waits in the queue.

For manual administration, prepare each target with:

```bash
python -m worker.matlab_code_checkout --repo-root /path/to/DetecDiv --commit <full-sha>
```

The command prints the directory and verified SHA. Its worktree is locked against
accidental Git cleanup. It is a preparation directory, not a running job.
After both preparations, an authenticated Hub admin can publish with
`PUT /matlab-code/release` and `{"code_commit":"<full-sha>"}`;
`GET /matlab-code/release` reports the default.

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

Automatic code-cache cleanup runs when a MATLAB job is prepared, under the
target admission and Git preparation locks. It considers only launcher-managed
worktrees unused for at least 30 days. It preserves the published default,
versions referenced by queued/running/cancelling jobs, all failed/cancelled jobs
(conservatively treated as resumable), recent completed jobs, and any modified
or extra files. An unfinished MATLAB job without a SHA blocks cleanup entirely.
Historical per-job worktrees are eligible under the same rules; none is moved
while its job is running. Job work directories and logs remain available.

For an operator preview, with the worker's database configuration available:

```bash
python -m worker.matlab_code_checkout --repo-root /path/to/DetecDiv --cleanup
```

Add `--apply` to remove only eligible copies. Permissions are restored solely
for removal, Git worktree locks are released, and removal never uses `--force`.
A future job can recreate an expired release from its pinned commit.

This freezes tracked DetecDiv code, not Python environments, external model
bundles, mutable pipeline definitions, or external legacy routine files.
Do not install/change shared Python environments while another job uses them.
Modules must keep persistent outputs in project or run storage. If a module
modifies tracked code inside its checkout, a subsequent resume is rejected
with an actionable error instead of reusing altered code.
