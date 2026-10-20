# UNC and mapped-drive resolution fix — 2026-09-29

## Cause and changes

The client's Hub preflight only recognized literal configured prefixes.
A project containing an accessible UNC source therefore stayed in that view
on import and failed submission when the configured Local root was a drive.
Windows workers can persist UNC source paths when they save a shared project;
the available records do not establish the first historical writer of each
UNC path.

DetecDiv now obtains drive/share equivalences from the current Windows session,
resolves both aliases to the same canonical server root, and prefers the
configured Local root when opening sources and reusable pipeline references.
Loading does not rewrite shared project files or historical execution context.
Mappings recorded by another client do not acquire aliases from this machine.

The Hub executor separately supplies its trusted execution-host mappings.
MATLAB uses those to translate all raw pointers together, including
`omeZarrPath`, which previously could remain inconsistent with `srcpath`.
No database schema or API deployment is required.

Implementation and client instructions:
`../DetecDiv/docs/shared_path_views.md`.

## Validation

- 15 MATLAB alias regression tests passed, plus 7 existing project/job tests
  and the existing pipeline path mapping smoke test.
- 12 Hub executor, dependency preflight and pipeline-run tests passed.
- Read-only validation of `Hyper7_CEN_BY_22092037`: 40 FOVs, 1772 ROIs,
  40 parsed positions; raw and pipeline sources resolve to this client's
  mapped drive and to canonical `/data` for submission.
- The project manifest, first FOV sidecar and run_2 record were compared before
  and after the real-project validation and were unchanged.

This development session uses `X:` for the DATA share; the affected client's
configured `Z:` is resolved using that client's own session. Drive letters
must not be copied between clients.

## Initial rollout state (before deployment request)

Source changes are local in the DetecDiv and detecdiv-hub repositories;
no push, remote file update or service restart was performed.

Read-only SSH checks of both compute hosts found clean DetecDiv checkouts
at `36bfde6c9bcbc413ceead6cf723f5f6aa10d3b27`.
The Windows checkout is on `unstable`, with no MATLAB process at the check.
The database and Linux process list confirmed two active Linux jobs:

- `5eebb08a-d825-4180-9242-0deebad7ab61`
- `a4bdf595-28be-4e2b-b138-91b243a7bac2`

The detecdiv-hub-ops skill and DetecDiv AGENTS.md require preserving a busy
target. Worker rollout is therefore deferred until the target is idle.
Deploy the same pushed DetecDiv commit to both compute hosts, synchronize the
Hub executor on both workers, and restart affected Hub workers only when idle.
Preserve unrelated current Hub checkout changes during packaging.

The affected MATLAB client needs the updated DetecDiv sources and must reopen
the project. Existing raw data and ROIs need no reparse. The failed historical
run remains failed; submit a new run after updating.

## Deployment requested and prepared

The intended changes were committed and pushed to their existing upstreams:

- DetecDiv `unstable`: `45398914393762ae8f47a9e840c67bb0050be289`.
- detecdiv-hub `master`: `d7eaa6b` (trusted worker mapping snapshot).

Unrelated local changes were preserved, including subsequent independent edits
to `helpers/detecdiv_hub_submit_pipeline_run.m`.

Windows `10.20.11.56` is deployed. New claims were temporarily drained;
the database confirmed no running/cancelling job, and no MATLAB process was
present. The MATLAB checkout was advanced with a fast-forward to `45398914`.
Only the intended Hub executor was copied, with a backup of its prior version.
Its SHA256 matches the local intended file:
`42882ee07f180f2c0027d405cef21e5f64cbde22ab90d2b665d6dc383a1309ce`.
The scheduled worker task was restarted and a fresh online heartbeat from
PID 35464 confirmed its return. Its temporary drain was removed. The database
tunnel task was left running.

Linux has separate, verified staged releases:

- `/home/charvin-admin/repos/DetecDiv-releases/45398914`
- `/home/charvin-admin/repos/detecdiv-hub-releases/pathfix-d7eaa6b`

The MATLAB revision and executor checksum match Windows. The original Linux
checkout and its `e9a41fc0` release symlink were preserved because the two
existing jobs still use them. The API on webserver-labo was inspected through
its database; its code and service were not changed.

Linux activation needs privileged systemd access. Passwordless sudo does not
permit those operations; the user was asked for the appropriate sudo access.
No Linux drain or service change has been applied yet. Consequently, Linux
workers still launch their existing version until activation is completed.

A one-time activation script is staged at
`/home/charvin-admin/repos/detecdiv-hub-releases/pathfix-d7eaa6b/activate_pathfix.py`.
It verifies the releases, drains Linux claims, waits for active jobs to finish,
switches and restarts workers, fast-forwards the now-unused original checkout,
then resumes claims. It never stops a running job. It can be started from an
authorized sudo session with:

```bash
sudo systemd-run --unit=detecdiv-pathfix-rollout --collect \
  --property=WorkingDirectory=/home/charvin-admin/repos/detecdiv-hub-releases/pathfix-d7eaa6b \
  --property=EnvironmentFile=/etc/detecdiv-hub/worker-webvm.env \
  /home/charvin-admin/repos/detecdiv-hub-webvm/.venv/bin/python \
  /home/charvin-admin/repos/detecdiv-hub-releases/pathfix-d7eaa6b/activate_pathfix.py
```
