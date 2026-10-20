# Windows worker recovery — 2026-10-20

The poller and its batch MATLAB job were already absent before this intervention.
The latest job heartbeat was 13:21:16 UTC. Three remaining MATLAB processes were
owned by `guest` in interactive session 3, not by the managed Hub job. All three
retained their PIDs and creation times, and their CPU counters advanced across
the intervention. No existing process was stopped.

Task Scheduler recorded a 72-hour execution timeout on October 19 at 09:48 Paris
time (event 329). This is a confirmed configuration problem, now corrected to
unlimited duration in both the installed task and its registration helper.
Since the Python poller continued heartbeating after that event, the precise
cause of its final disappearance on October 20 remains unproven.

The old job was reconciled to failed only after checking process identity,
stale heartbeat and unchanged stdout. Its results, parameters, logs and existing
outputs were preserved, with an explicit recovery audit and no automatic retry.

The verified pending update was applied under admission drain. New PID 45124
advertises fingerprint a1fffd69452e, with fresh advancing heartbeats. The existing
interactive scheduled task is Running with PT0S duration. Its completed activation
watcher removed itself. The queued HOCT job was dispatched to MATLAB, failed its
classifier dependency validation, and the worker returned to idle.

The Windows database tunnel and Linux poller running the other scientific job
were preserved. No API or Linux code/service was updated. Admission was restored.
Backups are retained on Windows under
`.deploy-backups/windows-worker-recovery-20261020/`.

Evidence: `before.json`, `diagnosis.json`, `events-and-logs.json`,
`final-preflight.json`, `deployment.json`, `orphan-reconciliation.json`,
`fresh-worker.json`, `admission-restored.json`, `after.json`,
`host-verification.json`.
