# Current Deployment

This file is the source of truth for the current live deployment target and
runtime topology for this repository.

Use the `detecdiv-hub-ops` skill for the operational sequence:
restart commands, verification steps, and deployment classification.
This file should describe what is live; the skill should describe how to act on
it.

## Stable Deployment Target

As of 2026-04-26, the primary DetecDiv Hub deployment is the VM:

- VM: `webserver-labo`
- Internal URL: `http://detecdiv-hub.detecdiv.internal/`
- Web UI: `http://detecdiv-hub.detecdiv.internal/web/`
- Health check: `http://detecdiv-hub.detecdiv.internal/health`
- VM address on the libvirt network: `192.168.122.185`

For installation details and remote worker configuration, see
`docs/deployment_installation.md`.

The old API process on `detecdiv-server` may still exist for rollback/history,
but it is no longer the deployment target to assume by default.

## Runtime Split

The production-like split is:

- `webserver-labo`
  - PostgreSQL, in Docker Compose
  - FastAPI app, in Docker Compose
  - static web UI served by the FastAPI app
  - read-only `/data` bridge mounted at boot from `detecdiv-server` for
    project ops browsing and preview serving
- `detecdiv-server` / `GC-CALCUL-306`
  - compute worker only
  - MATLAB access
  - direct visibility of server project/dataset storage
- `CG-PCDELL01-306` / `10.20.11.56`
  - supplemental native Windows worker, one job at a time
  - local MATLAB R2025b and Python environments
  - accesses shared jobs only when they are unassigned; jobs explicitly
    targeted at `detecdiv-server` stay there

The VM should not be assumed to have direct access to project or dataset file
storage. Any server-side filesystem work, including project-root indexing, must
run through the worker on `detecdiv-server`.

## Current Services

On `webserver-labo`, the Compose deployment lives at:

```bash
/home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
```

Useful checks:

```bash
cd /home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo
docker-compose ps
curl -sS http://127.0.0.1:8000/health
```

On `detecdiv-server`, the workers currently run from:

```bash
/home/charvin-admin/repos/detecdiv-hub-webvm
```

The active worker services are:

```bash
systemctl list-units 'detecdiv-worker@*.service' --all --no-pager
systemctl status detecdiv-worker@1.service detecdiv-worker@2.service detecdiv-worker@3.service --no-pager
```

The systemd override that points the worker to the VM database is:

```bash
/etc/systemd/system/detecdiv-worker@.service.d/10-webvm-db.conf
```

Since 2026-09-28 the compute pool uses `detecdiv-workers.slice` with a shared
96 GiB RAM, 6 GiB swap, and 36 CPU-core budget. The remaining approximately
29 GiB physical RAM and 2 GiB swap stay outside the pool for the 16 GiB Hub VM
and host services. Three workers receive 32 GiB RAM, 2 GiB swap, and 12 CPU cores
each was the initial static arrangement, now superseded by per-job sizing.
Idle workers have 1 CPU / 512 MiB RAM. Their quotas grow to the admitted job's
CPU/RAM request and proportional swap share, then return to idle limits.
The manager keeps six workers ready by default and grows the pool automatically
from admissible queued resource demand, up to 36 instances. It shrinks after
60 seconds of stable lower demand without stopping active jobs. Changing the
baseline changes ready slots, not job sizes. Shared budgets
remain persisted in `/etc/systemd/system/detecdiv-worker-resources.conf`.

`detecdiv-worker-manager.service` runs on the compute host and automatically
adds/removes idle instances under the same admission lock as resource claims.
The admin worker-count control sets the ready-worker baseline (default six).
Explicit manual mode retains the drain-before-reconfiguration workflow. Its database
drop-in inherits only environment directives from the worker override, never
the worker's ExecStart. It must remain outside the worker pool slice so scaling
does not stop the manager itself. The former global `max_concurrent_jobs=1`
emergency setting has been cleared; worker slots and resource budgets now bound
concurrency. See `docs/worker_resource_scheduling.md` for limits and verification.

The boot-time VM orchestration service now lives in the adjacent `Webserver`
repository because it belongs to the VM host layer, not the hub control plane.

## Windows MATLAB Worker Deployment Policy

### MATLAB code isolation rollout (2026-09-29, API and Windows active)

Hub commits `6afec5b` and `eee5902` add per-job detached Git worktrees,
submission-time SHA pins, and queued-job routing hardening. Both are pushed to
GitHub and GitLab. API image `3ca3a9abcf8c` is now running on `webserver-labo`;
health and authenticated release retrieval succeed. API fingerprint:
`0d93ee7f38ba`. Original files are backed up under
`.deploy-backups/codeversions-6afec5b` in the operational tree.

The published default release is
`e33264d90f671fed4d27fe5d7c77171d95a5e9c4`. Preparation worktrees at that SHA
were verified on both hosts. Linux shared source HEAD remains
`36bfde6c9bcbc413ceead6cf723f5f6aa10d3b27`; Windows source HEAD equals the
published SHA. Their HEADs need not match for isolated jobs.

Windows launcher files are installed, preserving the trusted path-mapping
patch. Its poller was restarted while admission was reserved, with no active
Windows job or MATLAB process. Fingerprint: `d99cba4190bf`. Windows now has
`matlab_code_isolation_ready=true`, and admission has been restored. The active
Cellpose training debug task was informed of the rollout and publication API.

Linux launcher files are installed. Pollers `@2`, `@4`, `@5`, and `@6` were
reloaded idle at 16:05 CEST; `@1` reloaded after its job finished. Fresh pollers
report `b005cc07895f`. Old `@3` (PID 150213) continues job
`a4bdf595-28be-4e2b-b138-91b243a7bac2` unchanged. Linux readiness remains unset
until this final poller reloads. Versioned jobs can already run on Windows;
explicit Linux requests are refused until Linux is ready.

The one-shot helper `scripts/finish_matlab_code_activation.py` runs on the
administration workstation (initial PID 51188). It waits for old `@3` to become
idle, reserves admission during its reload, then marks Linux ready only after
all six baseline pollers report the expected fingerprint. Logs:
`C:\Users\Gilles\AppData\Local\Temp\detecdiv-codeversions-finish.log` and the
adjacent `.err` file. Keep the workstation running until completion. Inspect
live heartbeat state before changing readiness manually.

Future processing releases are prepared in separate worktrees on both hosts
and published through `PUT /matlab-code/release`. No shared-checkout pull or
worker restart is needed for a release change. Legacy targets without isolation
retain the idle-only policy. See `docs/matlab_code_versions.md` for procedures.

### Shared MATLAB release cache (2026-09-29)

Worker changes `b574d69`, `f7126ca` (executor file only), and `a1295ad` are
installed on both compute hosts. Each SHA now has one protected checkout at
`DetecDiv-jobs/releases/<SHA>`. Job attempts use persistent directories at
`DetecDiv-jobs/work/<job-id>/<attempt-id>`, including logs and private temporary
files. Only the four launcher files were deployed; unrelated storage changes
that landed in the same Git commit were not part of this rollout.

Windows is active with fingerprint `463c764ab270` and admission restored.
Repeated preparation returned the same release path successfully. NTFS uses
a deny-write/delete ACL created through .NET without the SYNCHRONIZE denial
introduced by icacls; Linux files have mode 444 and directories 555. Linux
cache reuse took approximately 1.7 seconds including Python startup and Git
validation. The default release remains `e33264d90f671fed4d27fe5d7c77171d95a5e9c4`.

Five idle Linux pollers now report `7389e458c980`. Old `@3` still owns its
original running job and was not signalled. The updated one-shot activation
helper (initial local PID 19028) waits for it, retries interrupted SSH access,
and records its admission reservation for recovery. Logs are now
`C:\Users\Gilles\AppData\Local\Temp\detecdiv-shared-release-final.log`
and the adjacent `.err` file. The old helpers have stopped. Keep the
administration workstation running until the final readiness message.

Code-cache GC runs during job preparation with a 30-day retention window.
Published/default versions, queued/active jobs, failed/cancelled resumable jobs,
recent completed jobs, and locally modified code are retained. Historical
per-job worktrees are preserved until eligible; job logs are not automatically
deleted. No scientific job was launched for this deployment.

The Windows worker is an additional queue consumer, not a replacement for the
Linux storage-visible workers. Keep its Hub checkout at
`C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub` and its MATLAB
processing checkout at
`C:\Users\Charvin-Admin\Documents\GitHub\DetecDiv`. The Linux worker's
DetecDiv checkout is configured through `DETECDIV_HUB_MATLAB_REPO_ROOT` (the
current deployment default is `/home/charvin-admin/repos/DetecDiv`). These are
separate repositories with separate purposes; do not copy MATLAB project
internals into the Hub repository.

For a processing release, prepare and verify the same SHA in the release cache
on both hosts, then publish it through `PUT /matlab-code/release`. Source HEADs
may differ, and running jobs retain their pinned release. Do not modify/remove
code directories used by a job. Legacy targets without isolation still require
idle, clean, fast-forward-only synchronization of their source checkouts.
The Hub worker code itself follows its own deployment revision and procedure;
reload polling workers only when idle.

The Windows MATLAB license was reported renewed on 2026-09-28; the unattended
`matlab.exe -batch "disp(version)"` check succeeded under
`GMGM\Charvin-Admin`. The Windows worker `.env` was updated on 2026-09-29 to
claim unassigned jobs of all kinds, including `pipeline_run`, `legacy_matlab`,
and `archive_raw_dataset`; `restore_raw_dataset` remains excluded. Its readiness
check reports three path mappings: `/data`, legacy `X:\`, and `/archive`.
Raw-data ingestion remains on the Linux storage-visible worker.

The Windows archive share is `\\10.20.11.251\archive`, mounted as `Y:` at its
share root (`Y:\`). Read and write/delete probes succeeded in the RDP
interactive session through both `Y:\` and the UNC path. SSH and RDP have
different Windows logon IDs; the SSH key session had no `Y:` mapping and could
not use that SMB connection. The worker task runs in the signed-in interactive
session. The `.env` update does not alter a worker that is already running.
At the 2026-09-29 check, one Abhilasha `pipeline_run` was active on Windows with
a fresh heartbeat, so the worker was left running until the job finishes; no
archive jobs were queued at that snapshot. Check the queue and active jobs
before restarting. Once enabled, an unassigned archive job with
`mark_archived=true` can delete its source after a successful copy. Keep restore
excluded until a separate restore test.

Incoming SSH on the Windows PC currently accepts the dedicated public key,
but command and SFTP sessions fail before shell startup. Server DEBUG3 logs show
`LsaLogonUser()` failing to create an S4U token for `GMGM\Charvin-Admin`
(`0xC00000BB`). Build `26200.9457` maps to Windows 11 25H2 KB5129195; an open
Win32-OpenSSH report describes the same S4U failure after KB5074109. Treat the
update link as a strong lead, not a confirmed root cause. Keep RDP as the
administration path. The same key opened an interactive SSH shell as local
account `detecdiv-ops`; `whoami /groups` confirmed enabled local Administrators
membership and high integrity, so this is a working elevated SSH path. The
domain secure channel also tests healthy. Focus remaining diagnosis on S4U and
AD group-read prerequisites. Do not rotate the SSH key, edit its ACL, or remove
Windows updates to address this server-side token failure. See
[`docs/windows_worker_ssh_strategy.md`](docs/windows_worker_ssh_strategy.md).

Follow-up inspection over the working local-admin SSH account confirmed that
`sshd` runs automatically as `LocalSystem`, using OpenSSH 9.5p2. Domain
controller discovery and clock synchronization to
`srv-data-com01.gmgm.lab` (`10.20.1.150`) succeed. The ActiveDirectory
PowerShell module is not installed, so `Charvin-Admin` account flags and AD
group memberships remain unverified. A read-only
`net user Charvin-Admin /domain` query from the local-admin SSH account returned system error 5
(Access denied), so a domain-authorized identity is needed for that inspection.
`DEBUG3`/`LOCAL0` logging is still active in `sshd_config`; no remote
configuration or service changes were made during this inspection.

## Data State

The PostgreSQL database from the former local `detecdiv-server` deployment was
restored into the VM PostgreSQL container on 2026-04-26.

The VM is now the main database to use for ongoing DetecDiv Hub work unless an
explicit rollback is requested.

The VM guest also mounts `/data` read-only through `detecdiv-server` so the API
container can serve raw-dataset preview MP4s and the project ops browser can
inspect storage-backed folders without logging into the storage host directly.

## Pending Live Schema Migrations

Before deploying synchronized physical locations for Labguru yeast stocks, apply:

```bash
psql "$DETECDIV_HUB_DATABASE_URL" -f db/migrations/20260911_labguru_yeast_storage.sql
```

This adds the synchronized Labguru storage tree, boxes, and physical stocks.
The same API-side synchronization that refreshes YeastStrains also refreshes
these records; compute workers remain uninvolved.

Before deploying incremental Labguru Yeast strains synchronization and
creation-date ordering, apply:

```bash
psql "$DETECDIV_HUB_DATABASE_URL" -f db/migrations/20260911_labguru_yeast_created_external_at.sql
```

This adds the Labguru-side creation timestamp and backfills it from the already
imported payloads without repeating local rows.

Before deploying the Labguru Yeast strains catalog and instant-search page,
apply the migration:

```bash
psql "$DETECDIV_HUB_DATABASE_URL" -f db/migrations/20260911_labguru_yeast_strains.sql
```

It creates the reconciled local Labguru yeast inventory used by the dedicated
`External accounts > Yeast strains` page. Synchronization runs as a short
FastAPI background task because it only needs the Labguru API and PostgreSQL;
compute workers are not involved.

Before deploying the Micro-Manager acquisition-widget position-description
changes from commit `a05b313`, apply the migration:

```bash
psql "$DETECDIV_HUB_DATABASE_URL" -f db/migrations/20260514_raw_dataset_position_description.sql
```

It adds `raw_dataset_positions.description`, used to persist per-position
annotations entered in the acquisition widget and exposed on raw dataset
details.

Before deploying worker CPU reporting, apply:

```bash
psql "$DETECDIV_HUB_DATABASE_URL" -f db/migrations/20260926_worker_cpu_usage.sql
```

This adds the per-worker CPU topology and live job-usage fields to
`worker_instances`.

## Agent Rules

Future agents should assume:

- The VM deployment is the primary runtime.
- New deployment work should target `webserver-labo`.
- Compute and storage-visible worker work should target `detecdiv-server`.
- The local checkout, the deployed API copy, and the deployed worker copy are
  separate states; never assume a local edit is live.
- Do not assume the remote worker copy is a clean git checkout. Verify the
  remote state before claiming a deploy.
- If a change touches code imported by both API and worker, treat it as a
  cross-layer deploy and update both hosts.
- Do not reintroduce API-side filesystem scans for server paths.
- Do not assume the VM can see project storage.
- The current ready pool uses `detecdiv-worker@1` through `@6`; additional
  numbered instances start automatically when resource demand permits.
  Per-worker heartbeat state is stored in the `worker_instances` table.
- VM autostart and host-level reboot orchestration are documented in
  `../Webserver`, not here.

## Rollback Note

Rollback is possible by removing the worker systemd override and restarting the
old local services on `detecdiv-server`, but that should be treated as an
explicit incident/rollback action, not as the default development target.
