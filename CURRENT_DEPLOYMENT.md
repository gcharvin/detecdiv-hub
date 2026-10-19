# Current Deployment

## Automatic ROI extraction memory rollout, 2026-10-19

The Linux worker tree now includes automatic admission for runs selecting only
the standard streaming `roiExtract.process`: preferred 49152 MiB, conservative
minimum 24576 MiB (or a higher configured pipeline profile), smaller admitted
budgets when shared capacity is occupied, and queuing below the minimum.
The existing MATLAB block/FOV planners consume the job's applied service limits.
Explicit requests and mixed/legacy/non-dynamic runs keep fixed budgets.

Five idle Linux pollers (`@1,@2,@3,@5,@6`) and the independent worker manager
were reloaded. Fresh poller fingerprint is `c367c1f32f1e`. Worker `@4` continues
its existing analysis with PID 2571587 and fingerprint `c0f13002d996`; it must
not be interrupted. A user systemd timer
`detecdiv-roi-memory-activation-20261019.timer` checks every 30 seconds and
reloads it only after a fresh idle heartbeat and an empty child-process tree.
It stops itself when all poller fingerprints match.

Windows remains on active fingerprint `957aa209126d` while its analysis runs
(PID 31820). The merged update is prepared and verified under
`.deploy-staging/roi-auto-memory-20261019` in its Hub checkout; intended
fingerprint is `89081645c071`. Host-specific claim compatibility, diagnostics,
target assignment and GPU/path adaptations are preserved. Automatic memory
remains disabled on Windows, retaining its current 24576-MiB pipeline default.
The SYSTEM task `DetecDiv Hub ROI Memory Activation 20261019` checks each
minute, activates only after the target has no active jobs and the managed
Python tree has no children, verifies fresh fingerprints on subsequent checks,
and unregisters itself after completion. The database tunnel and unrelated
MATLAB sessions are preserved.

Both activation paths briefly reserve admission under the target-row lock and
restore its preceding drain state. Original worker files are retained under
`.deploy-backups/roi-auto-memory-20261019`. No API rebuild, database migration,
MATLAB release change or scientific job submission was needed.

Verification: 65 targeted tests passed in isolated runtime copies on each
compute host, and 83 local Python tests passed. A read-only production-policy
probe confirmed Linux allocations of 48 and 30 GiB, and queuing with less than
24 GiB remaining. The Windows staged policy retained fixed 24-GiB requests.
Both active jobs kept their PIDs, parameter hashes, allocations and start times,
with advancing heartbeats. Evidence and activation helpers:
`reports/roi-auto-memory-deployment-20261019/`. Busy-poller activation remains
pending until those analyses finish; prepared files alone are not activation.

## Native Windows GPU activation, 2026-10-16

`windows-10-20-11-56` now declares `supports_gpu=true` for its RTX 2000 Ada
(16380 MiB). Its target-specific admission budget is 14336 MiB, preserving
display headroom; one concurrent job remains the limit. Windows poller
fingerprint `957aa209126d` includes that budget and external classifier package
path mapping, preserving its existing local adaptations.

The isolated phase CellposeSAM runtime is
`C:/ProgramData/DetecDivHub/cellpose-gpu-v1/Scripts/python.exe`, selected by
`DETECDIV_PHASE_CELLPOSE_PYTHON`. Validation passed in `GMGM\Charvin-Admin`'s
actual worker session: MATLAB GPU detection, Python CUDA forward/backward,
CellposeSAM inference with pinned weights, 100 input fingerprints and shared
output access. Temporary installation/validation tasks are removed after use.

The existing pilot job `43637ab4-4044-443d-b8f9-3372935f8356` was reserved on
Windows while drained; no duplicate job was created. Its first real attempt
stopped before training because client path specificity overrode trusted worker
mapping. DetecDiv `594217dd37bfa9b2b529f7f2204253d9901340bf` corrects that
priority, is prepared on both hosts, and is the published default. Running jobs
keep their earlier pins. The pilot uses a versioned Windows launcher (HOCT
`45f68ffc19e6faee2d727506a6bd4fa3b41f3a82`) with unchanged scientific protocol
and Python pilot hashes. See `reports/windows-gpu-activation-20261016/` and
`docs/windows_worker.md` for evidence and environment details.

After the complete dry run passed in the worker session, the same job was
requeued at 07:57 UTC with its first failure retained in `attempt_history`.
The actual GPU training started at 07:58 UTC and completed its first epoch
without error. At final verification the RTX reported 87% utilization and
9847 MiB total VRAM use; exactly one pilot launch was present. The original
scientific run request and frozen protocol hash are unchanged. Summary:
`reports/windows-gpu-activation-20261016.json`.

## Alex fresh-ROI extraction validation, 2026-10-15

DetecDiv `dce3ca768bcae597224424620abc46ede0788a12` is the published
MATLAB default, prepared and verified on both compute hosts. DetecDiv-plugins
`2b13ecb7f43085f2b0e211e95dd1d109ec5c0e0e` updates the extraction adapter
to 0.1.8. Its explicit `imagesToRoi` contract lets semantic validation and typed
resource binding project the raw channels through a processor extraction node.
The standard ROI extractor retains its existing-project inventory policy.

Alex's new `anof_071026_X54445_LOGEB_2` project exposes six raw source channels
but newly identified ROIs still advertise only `Channel 1`. Previously the
combined extraction/classification dry run rejected this provisional inventory.
Earlier first-project runs already exposed six ROI channels at validation time,
masking this missing transfer declaration. The analysis still requires six
channels; this fix changes no image processing, pool sizing, block size or saves.

Both of Alex's clean core/plugin checkouts under
`X:/Alexander/code/DetecDiv_2` and the shared extraction package are updated;
an immutable plugin release is retained under pipeline `assets/releases/<SHA>`.
All six Linux workers and native Windows use the prepared default/shared package
for new jobs. Existing jobs and pipeline definitions were preserved. Three new
core tests, four Alex pipeline tests and 29 existing contract tests passed on
local Windows R2026a, Linux R2024b and native Windows R2025b. The exact new
project passed dry-run validation. Windows account `GMGM\Charvin-Admin`
verified the shared package version and changed file hashes through a temporary
verification task, subsequently removed. Evidence:
`reports/alex-channel-contract-deployment-20261015.json` and
`reports/alex-channel-contract-validation-20261015.json`.

## Alex extraction read pool, 2026-10-15

DetecDiv-plugins `700c95ac2bbabf4c0e04e4afd2ff81bcb5cdbc29`
(`saveCroppedImagesXConservative` 0.1.7) is published on GitHub and GitLab.
The extraction adapter explicitly opens a process pool before the historical
frame-reading `parfor`, independently of `Pool.AutoCreate`. CPU admission uses
the job environment, matching Hub attempt allocation and MATLAB thread ceiling;
RAM admission reserves the raw
block/crop/drift buffers and each process's baseline and serialized image cache.
Workers use one compute thread each. An owned pool closes on success or failure;
an incompatible caller-owned pool is preserved and reported. The historical
script, fixed `cut=88`, scientific calculations and universal `roi.save` are
unchanged. For Alex's actual 2048-by-2048 six-channel inputs, the 24-GiB budget
admits two read workers within the eight-CPU ceiling.

The exact plugin SHA is prepared in detached worktrees on both compute hosts:
Linux `/home/charvin-admin/repos/DetecDiv-plugins-releases/<SHA>` and Windows
`C:/Users/Charvin-Admin/Documents/GitHub/DetecDiv-plugins-releases/<SHA>`.
The shared extraction package under
`/data/matlab/pipelines/Alex_Xtra_FullChannels_Conservative/assets/plugins/processor`
is updated for all six Linux workers and native Windows; an immutable copy is
under that pipeline's `assets/releases/<SHA>/plugins/processor`. Alex's clean
`X:/Alexander/code/DetecDiv_2/DetecDiv-plugins` checkout was fast-forwarded to
the same SHA. The live classification package, pipeline definition and active
job payload were verified unchanged. No global core release or worker service
was changed, and no analysis was restarted.

Six read-pool tests passed on Linux R2024b, local Windows R2026a and native
Windows R2025b, including real process workers, exact multi-block TIFF crops,
CPU/RAM limits (including a job smaller than the Windows worker capacity),
existing-pool preservation and failure cleanup. Eleven existing
ROI persistence tests passed on Linux and local Windows. Six actual frames
(1, 88, 89, 176, 440, 517), across all six channels, were read on two processes
and compared pixel-for-pixel with sequential reads. In the actual Windows
worker account `GMGM\Charvin-Admin`, a temporary verification task loaded the
shared UNC package, verified all 18 file hashes and started/closed two workers;
the task was removed afterwards. Evidence:
`reports/alex-readpool-deployment-20261015.json`.

## Cellpose failed-inference metadata rollback, 2026-10-15

The published MATLAB default is
`6c86c83cda16a7accaa2e4a5d20cd74507e52286`, prepared and verified in protected
release directories on Linux and native Windows. This extends `07422ee8` by
restoring the ROI image cache, logical channel mapping, and display metadata
when Cellpose inference raises an error or cancellation. The Hub can then save
the project after a node failure without exporting newly prepared but
uncommitted Cellpose output channels. Existing masks and prior completed ROIs
are preserved.

The source was reproduced with a first inference that creates a blank output
plane before failing: without rollback, the normal light-project exporter
records its name although the HDF5 store still contains only raw input. This
matches Windows attempt `f9d102cc-0011-44ac-925d-2235c82078ed`, submitted on
October 13 and failed on October 15 at 05:51 CEST after Python could not open
its configuration path. Both the blank-output creation and the subsequent
project metadata save are visible in the execution path/logs. Output
preallocation dates to February; September's output-cache reload change later
made the metadata-only result fatal on resume.

Eleven related tests passed on Windows R2026a, including two new regressions
that failed before the fix. Both new regressions also passed on Linux R2024b.
No workers were restarted or API images rebuilt. Already submitted/running
jobs keep their existing code pins. Evidence:
`reports/abhilasha-ghost-source-20261015.json` and
`reports/abhilasha-ghost-origin-job-20261015.json`.

## Cellpose metadata-only output fix, 2026-10-15

MATLAB release `07422ee84108dbb26f670520adcb5888348ce04d` was published earlier
today (previously `bd1d64ec03c6e91d972dd609e45780964e0d6203`). Its protected release was
prepared and verified on Linux and native Windows with
`scripts/deploy_detecdiv_release.ps1`. Shared checkouts and active job pins were
preserved; no API rebuild or worker restart was needed.

CellposeSAM now materializes an output plane when project/display metadata
advertises it but the readable HDF5 store has no corresponding dataset. It
reuses the logical channel identity, preserves existing on-disk masks for
partial reruns, and still propagates unreadable/corrupt-store errors. Four
output-channel regression tests passed on Linux R2024b; nine related tests
passed locally on Windows R2026a. Hub pilot
`2ec1cd2d-1b51-49a5-bb66-3d3dbe26f0f1` completed successfully on the actual
first Hyper7 ROI from failed job `e9dd1130-6815-4c75-bbba-55344c2301d9`:
50/50 frames contain masks; the HDF5 store retains its six original datasets
alongside the new segmentation dataset. Full Hyper7 recovery is
`2f53acb2-370c-4683-bc20-721ddcf115f1`, pinned to the corrected SHA.

The separate Diamide failure `00c9a5ef-2cdb-4bb9-ace3-6710a7288878` coincided
with local disk exhaustion (another MATLAB job logged ENOSPC at the same
minute). Subsequent disk cleanup restored approximately 79 GiB free (later
cleanup increased this to approximately 207 GiB). Its 40
completed FOVs remain present; FOVs 45–46 are partial and 47–50 are pending.
After Abhilasha closed the project and its client edit lease cleared, recovery
`bd3c852e-8728-4d65-a646-d5472cd1cf48` was submitted through the normal Hub
service and started. It skips existing extraction outputs, reuses the saved
ROI detection, and continues downstream processing. No client locks were
overridden. Evidence: `reports/abhilasha-recovery-live-20261015.json` and
`reports/abhilasha-failures-20261015.json`.

## Alex worker fixes: fleet activation completed, 2026-10-15

Hub fixes `538aa238` (MATLAB cancellation even with exit code zero, cancellation
actor audit) and `4c714c64` (explicit GPU inference by external processors) are
published. The API cancellation route is active on `webserver-labo`, with healthy
API/database and fingerprint `c4a3fc462f1f`. All six Linux pollers `@1` through
`@6` now report runtime fingerprint `c0f13002d996`. `@2` activated at the
2026-10-14 21:28 UTC check and `@6` at the 2026-10-15 03:58 UTC check, after their
analyses finished. Native Windows is also active with verified fingerprint
`8c31b3db2d99` (interpreter PID 53756, venv launcher PID 44188). Its previous
job finished before activation. Its raw-ingestion adaptation and other local
changes were preserved; staging passed 11 checks in its actual Python runtime.
The final administration regression suite passed 19 tests, including real
PowerShell guards for orphan pollers and active MATLAB child processes.

`scripts/finish_alex_worker_fix_rollout.py` reported `complete: true` after Windows
activation. It checks every baseline worker and additional
registered instances, reserves admission only during an idle reload, checks
actual child processes, validates source hashes, reloads the specific poller or
Windows scheduled worker task, confirms the new heartbeat fingerprint, and
restores admission. Admission lock waits are bounded at one second (SQL operations
at ten seconds), avoiding both indefinite waits and repeated NOWAIT contention.
On Windows the task launcher can leave Python children alive; activation checks
all managed Python process trees for active children, stops only verified idle
pollers, and starts one scheduled worker. The actual deployed hashes are:

- Linux executor: `0464da6627d90d99a8cf1ccec89da9243254e0a08d32be205d13e5514c75d146`
- Windows executor: `ba6fa9beb28c80ff48d9d431d5654aa74299c9f8666ddf7df903c7906e227e95`
- Resource module on both hosts: `6450a97cf1ae178e6a1a36b665807ddee10773b9fbd97ae98aec51d4f4862ef5`

The worker task and database tunnel are both running; exactly one Windows poller
remains (its venv launcher plus interpreter). Admission was restored on both
targets and no rollout marker remains. No active analysis or MATLAB release was
interrupted or changed. Evidence: `reports/alex-worker-fix-rollout-20261015.json`.
Windows staging and backups live under its Hub checkout in
`.deploy-staging/alex-terminal-gpu-20261014` and
`.deploy-backups/alex-terminal-gpu-20261014`. The Alex heartbeat is disabled after
this verified fleet completion; the 517-frame scientific run remains complete.

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

### MATLAB temporary-path correction (2026-09-30)

The Linux rerun `9b85ac9a-39a3-4197-a284-44344f435799` used DetecDiv release
`6c6e414a` but exited before writing any MATLAB output. An isolated
`matlab -batch "disp(version)"` reproduced exit 1 with the long attempt-local
`TMPDIR`; the same command succeeded with a short symlink to that directory.
Hub commit `37fe12c` makes Linux MATLAB jobs use a private
`/tmp/dd-matlab/<attempt-id>` alias while retaining temporary files in the
persistent attempt directory. The alias is removed when the job ends.

The changed Hub launcher file was copied to both worker hosts. All six Linux
pollers were restarted while idle and report fingerprint `68dc9b448e76`;
the manager stayed active. The Windows copy was synchronized, but its running
poller was not restarted because the new branch applies only on Linux. The
published DetecDiv release is `6c6e414a4a3b52a905f3aab7639cd0d7a935f7d5`.
The dose pipeline was resubmitted as job
`1535b10c-cc50-4301-a2d2-cbeff504f2f6`; by 07:34 CEST it had passed
MATLAB startup and entered the data loader. Its final result remains pending.

### ROI extraction memory follow-up (2026-09-30)

The dose job above and pulse job `ed821a11-0b68-42c1-b58a-16f647c5a638`
subsequently failed during `roiextract_3` with an apparent 0.8 GiB headroom
below `memory.high`. Both were pinned to the older `6c6e414a` DetecDiv release.
The raw frames are 2048 x 2048, five uint16 channels (40 MiB per frame), and
the extractor caps ordinary raw blocks at 512 MiB. Live cgroup measurements
showed that clean file cache from raw reads and HDF5 writes occupied many GiB.

DetecDiv `4ab5613d` excludes clean active and inactive file cache from cgroup
usage, while retaining shmem, dirty, and writeback pages. DetecDiv
`cc6bce4f64bac7be343ceda23bdde961bf27a5f6` also permits a single frame
above the 512 MiB ordinary block cap when the real memory budget allows it.
MATLAB memory-planner tests passed. The latter SHA was prepared on both Linux
and Windows workers and published as the default release; no polling worker
restart was needed. It also includes `fe2a1535`, which preserves submitted FOV
selections and deduplicates lightweight project imports.

At 14:26 CEST, the replacement Linux jobs were running with 24 GiB each:
`fd8c8501-1480-44c7-949e-aca993263580` for dose (FOV 41-50, entered ROI
extraction) and `3ae55b05-88df-4e4c-864e-6ef90022a3f4` for pulse (raw ingest
and dependency preflight complete). Their final results remain pending. The
thread heartbeat `surveiller-reprises-abhilasha-roi` checks them every 10 minutes.

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

From the administration workstation, run
`.\scripts\deploy_detecdiv_release.ps1 <full-sha>` for the DetecDiv release
sequence on the current Linux and Windows workers. Its host paths and remotes are in
`ops/detecdiv_release_targets.json`; see `docs/matlab_code_versions.md`.

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

### Alex legacy ROI results persistence (2026-10-14)

DetecDiv `da89797e0df58971f59dea1fedd746f7b8ddc867` is published as the
default for new MATLAB jobs. The protected release directory at that full SHA
was verified on both Linux and Windows. `roi.load` restores explicitly saved
`results_<roi-id>.mat` sidecars; the universal `roi.save` is unchanged.

DetecDiv-plugins `528bde9ceb6ab4df556692ea76489653e0210881` is pushed to
GitHub and GitLab. Both conservative Alex wrappers are version 0.1.1. Their
26 files were deployed and checked in
`/data/matlab/pipelines/Alex_Xtra_FullChannels_Conservative/assets/plugins/processor`;
the shared pipeline JSON is version 1.1. Previous packages and pipeline JSON
are retained in `.deploy-backups/20261014-results-528bde9`.

Alex's source checkout at `/data/Alexander/code/DetecDiv_2/DetecDiv`
(`X:\Alexander\code\DetecDiv_2\DetecDiv`) was updated fast-forward to the
same DetecDiv SHA. Its apparent dirty state consisted only of CRLF formatting;
`core.autocrlf=true` and `core.filemode=false` preserve that Windows checkout
format on the Linux administration host. The tracked checkout is clean.

Three jobs were active before deployment, in isolated code directories. No
worker was restarted and no active job's code directory was modified.
Regression checks passed: 11 Windows integration/save tests, nine Linux
integration tests, and three additional legacy-results loader tests.

After Alex closed his client edit lease on `GC-DELL01-308`, job
`8fe920f8-c614-4df3-b5a1-b0ef5a986979` ran on Linux with the pinned release
above. Run `alex_results_fix_30frames_20261014` processed FOV 1 / ROI 1 /
frames 1:30 and finished successfully at 11:11 CEST. Its two nodes completed
without errors. A fresh MATLAB process confirmed identical persisted
`roi.results` through both `roi.load('data')` and full `roi.load`: fields
`div_1`, `RLS`, and `signal`; probability size 6 x 30; fluorescence size 3 x 30;
image size 171 x 171 x 7 x 30; channel mapping 1:7.
The report is in the run folder as `results_reload_verification.json`.
Pre-test artifacts are retained under the project's
`diagnostic_backups/20261014-results-30frames-after-close`. The temporary chat
heartbeat is paused after successful verification. No client lock was forced.

A later adapter-only update, DetecDiv-plugins
`153403b87f1f43a797069b213b8125def737ba0e`, is pushed to GitHub and GitLab.
The extraction wrapper is version 0.1.2; Xtra remains 0.1.1. The shared
pipeline is version 1.2 and passes the existing historical `cut=256` option.
The added adapter code is marked with `ADAPTATION PIPELINE 2026-10-14`.
`saveCroppedImages_X.m` and `computeDrift_X.m` were verified byte-identical to
the previously deployed persistence fix. An intermediate adaptive-memory
change was reverted before deployment. Ten integration tests pass on both
Windows and Linux. Backups are in `.deploy-backups/20261014-cut256-153403b`.

Job `75a7de1f-6630-42c2-bb5e-0e597c32565f`, run
`alex_xtra_full_all_frames_20261014`, is submitted on Linux for Alex's latest
selection: FOV 1 / ROI 1 / all 517 frames, blocks 256 + 256 + 5. It pins the
same DetecDiv release and requests 32768 MiB through the supported per-job
resource override, included atomically before submission commit. A synthetic
32-frame allocation probe on the server measured roughly two resident raw
buffers during drift-style copy-on-write; extrapolation for 256 frames gives
24 GiB for raw buffers before MATLAB and pool overhead. At verification time
the job is queued because the resource manager reports `memory_capacity`;
the two existing Linux jobs continue undisturbed. No allocation or capacity
limits were lowered to force admission. Pre-run ROI artifacts and the project
header are backed up under `diagnostic_backups/20261014-before-all-frames`.

The queued 32-GiB job above was subsequently cancelled at the user's request.
DetecDiv-plugins `a0143f09a28947692a0c30021b15f7b109a5d964` is pushed to
GitHub and GitLab and deployed into the shared Alex pipeline. Extraction
version 0.1.3 / pipeline version 1.3 fix the block size at 176 frames by default
(`floor(2*265/3)`) in the pipeline params, `setparam`, and legacy script. The
only new legacy-script behavior change is its block-size default, marked
`ADAPTATION MEMOIRE 2026-10-14`; scientific calculations and universal
`roi.save` are unchanged. Ten integration tests pass on Windows and Linux.
The prior version is retained in `.deploy-backups/20261014-cut176-a0143f0`.

The first 24-GiB replacement, `99b96d64-3812-4ee0-a2ea-1a58ed49bd5d`, run
`alex_xtra_full_all_frames_cut176_20261014`, started on Linux with
`requested_by=alexander`, as explicitly requested by Gilles. Submission is
still authenticated as Gilles, and the client context records the authorized
resubmission. The original FOV 1 / ROI 1 selection and all 517 frames are
retained, in blocks 176 + 176 + 165. The worker's actual allocation was
verified as 24576 MiB; the core release remains
`da89797e0df58971f59dea1fedd746f7b8ddc867`. Existing jobs and worker services
were left running. This attempt then failed before raw extraction because the legacy routine
counted `srclist{1}` before lazy source metadata had been hydrated.

DetecDiv-plugins `7eaf0ade17b86d743b6045b99f93ff4dbb9cf945` is pushed to
GitHub and GitLab and deployed as extraction version 0.1.4 / pipeline 1.4.
The adapter now calls `fov.readImage(1,1)` for selected FOVs with an empty raw
file list before invoking the legacy all-frame routine. The new regression
reproduces lazy Micro-Manager source discovery; eleven tests pass on Windows
and Linux. The historical default remains 176, with no further legacy-script
change. Backup: `.deploy-backups/20261014-cut176-lazy-7eaf0ad`.

Job `ad0d3610-9c71-4693-99c7-f50af0c80bf1`, run
`alex_xtra_full_all_frames_cut176_retry_20261014`, is running on Linux as
`alexander`, with an actual 24576-MiB allocation, FOV 1 / ROI 1, all frames,
and `cut=176`. The same core SHA remains pinned. Full-run completion and
all-frame results reload remain unverified while the job executes.

Alex's local installation under `X:\Alexander\code\DetecDiv_2` (Linux
`/data/Alexander/code/DetecDiv_2`) now includes the conventional sibling
`DetecDiv-plugins` checkout at
`7eaf0ade17b86d743b6045b99f93ff4dbb9cf945`, branch main. Both core and
plugin Git checkouts are clean; core remains
`da89797e0df58971f59dea1fedd746f7b8ddc867`. The core ROI results loader was
compared with the committed development copy after LF/CRLF normalization.
Fresh MATLAB sessions on Windows using the actual X: installation and on
Linux verified automatic sibling discovery, both conservative processor
entrypoints resolving inside Alex's new checkout, and `setparam.cut=176`.
No personal plugin preferences or tracked core startup files were modified.
The running server job continues to use its existing code directories.

After the 176-frame run stalled with 23.6 GiB anonymous RAM and a full
1.5-GiB swap allowance, Gilles explicitly requested cancellation and a fixed
88-frame replacement. Job `ad0d3610-9c71-4693-99c7-f50af0c80bf1` is confirmed
cancelled. The cooperative token could not interrupt the blocked legacy code;
only its verified MATLAB PID 2452179 (matching job work directory and worker
cgroup) was terminated with SIGTERM. No worker service or other job was stopped.

DetecDiv-plugins `18ce411149c9fba85c5ab6d19540dc5d535241ac` is pushed to
GitHub and GitLab, with extraction version 0.1.5. The shared pipeline version
1.5, legacy default, and setparam all use `cut=88`. The only historical-script
change since the preceding release is that fixed block size. Eleven tests
pass on Windows and Linux. Pipeline backups are retained in
`.deploy-backups/20261014-cut88-18ce411`. Alex's clean sibling plugin checkout
at `X:\Alexander\code\DetecDiv_2\DetecDiv-plugins` was updated fast-forward to
the exact SHA; fresh Windows MATLAB resolved the local module and confirmed
`cut=88`. The universal ROI save implementation is unchanged.

Job `4fa7df14-9e80-4c89-b2b0-7e1e4393186b`, run
`alex_xtra_full_all_frames_cut88_20261014`, is running on Linux under
`requested_by=alexander`, with 24576 MiB actually allocated, FOV 1 / ROI 1,
and all 517 frames (five blocks of 88 plus 77). It retains the explicit
DetecDiv pin `da89797e0df58971f59dea1fedd746f7b8ddc867`; no global core release
was republished or rolled back. Its extraction log shows active progress
through the first block. The existing heartbeat is active every 30 minutes
and now tracks this new job. Its fresh-session `verify_results_517.m` and
final report will cover all six block boundaries, classification, HDF5/MAT
raw-channel equality, and exact roi.results reload. Completion remains to
be verified by the monitor.

Final monitoring on 2026-10-14 found a status inconsistency for the 88-frame
job: Hub Job.status is `done`, but both MATLAB result and run.json are
`cancelled`. The job-specific cancel token was created at 11:02:47 UTC
(13:02:47 CEST); API access logs corroborate a POST to this exact job's cancel
endpoint. The requester identity was not inferred from the proxy address.
MATLAB honoured the cancellation after legacy extraction and returned code
0; the worker therefore marked done despite result.status=cancelled. This
worker status defect is diagnosed, not changed during monitoring.

All six extraction blocks are logged. Fresh MATLAB inspection verified the
persisted/extracted image dimensions 171 x 171 x 6 x 517 and nonempty frames
at every block boundary. Classification did not start, and roi.results.div_1
still contains the preceding 30 predictions. The full cold-reload verifier
failed its 517-prediction assertion. The run's results_reload_verification.json
records `status=incomplete` and `completeVerificationPassed=false`; do not
report the full run as successful. The heartbeat is now paused after diagnosis
and notification. No job was resubmitted and no scientific data was modified
by this final monitoring pass.

### Alex monitored recovery and terminal-status fix (2026-10-14)

API access logs confirm seven cancellation requests for the first 88-frame
job between 11:02 and 11:06 UTC. The original 30 predictions were preview
results retained because classification never ran. The old API did not retain
requester identity, so no person was inferred from the proxy address.

Hub `538aa238a9d131949774bcd7b06d0e11a14d4de7` rejects MATLAB cancelled,
failed/error or incomplete results even when the process exits with code zero;
the API records the authenticated cancellation actor. The API route was
verified and rebuilt on webserver-labo; health and DB checks passed. The worker
executor was verified on Linux and loaded by restarting only idle instances
@1,@3,@4,@5 through their Restart=always service policy. Direct remote testing
confirmed a zero-exit cancelled result raises PipelineRunCancelled. Busy
Linux @2/@6 and the Windows worker were not restarted or updated during their
ongoing analyses; Windows remains pending for this Hub runtime fix.

Hub `4c714c64413e87cfb68681a528a83e84318f92ef` reserves GPU memory for custom
processors with explicit GPU execution settings. It is deployed on Linux;
idle @3,@4,@5 reloaded it. @1 started Alex's new job before this resource-rule
activation and was preserved. Future recovery submissions explicitly declare
GPU use. Existing jobs remain undisturbed.

Alex independently resubmitted `b38095c1-ec3a-403f-8787-5588e6e030ae` from
GC-DELL01-308 at 13:32 CEST, reusing run ID
`alex_xtra_full_all_frames_cut88_20261014`. Its FOV 1 / ROI 1 / all 517-frame
selection and cut 88 match the request, so no duplicate was submitted. It pins
core `bd1d64ec03c6e91d972dd609e45780964e0d6203`, already prepared, and is
currently running on @1. At inspection its six extraction blocks had been
read and the 517-frame HDF5 channels were being saved; classification and
final results validation are still pending.

Hub `8852501e63bd8431d96c434fd29afd775d5aea3e` adds the durable campaign
CLI and recovery runbook. Forty-four focused regression checks pass. The
campaign state lives at the project's
`pipeline/monitor_recovery_20261014/campaign.json`; actual Hub inspection
through the CLI was verified. The existing 30-minute heartbeat is active and
now authorized to diagnose, test, commit/deploy and retry technical failures,
with project-lock/preflight checks, fresh stop paths and no duplicate jobs.
It records attempts and permits at most three additional automatic submissions.
Human cancellation remains explicit. It can reuse proven extraction and retry
classification only. No scientific algorithm/model or universal roi.save was
changed. The job-specific fresh-session verifier is
`verify_results_517_b38095c1.m` in the run folder, with report
`results_reload_verification_b38095c1.json` identifying the actual job and SHA.

The adopted Alex job completed both nodes successfully: extraction 1118.075
seconds and classification 51.330 seconds. A fresh MATLAB session verified
job b38095c1-ec3a-403f-8787-5588e6e030ae: 517 predictions/labels,
probabilities 6 x 517, fluorescence 3 x 517, images 171 x 171 x 7 x 517,
unique channel mapping 1:7, raw channels identical to the legacy MAT, block
boundaries nonempty, and exact roi.results reload through data-only and full
load. The passed report results_reload_verification_b38095c1.json is also the
canonical results_reload_verification.json; the previous incomplete report is
archived as results_reload_verification_4fa7df14.json. The campaign state is
complete / verification_status=passed. Its heartbeat is paused after verified
success; the recovery CLI remains installed. No technical retry was needed
after adopting Alex's concurrent submission. Now-idle @1 reloaded the pending
GPU rule. Other busy Linux and Windows workers remain undisturbed.

Fresh Windows MATLAB using Alex's actual X:/Alexander/code/DetecDiv_2/DetecDiv
installation also passed exact data-only/full roi.results reload, probability
6 x 517, image 171 x 171 x 7 x 517, and channel mapping 1:7. This consumer
verification is recorded in both passed reports and in campaign.json.

## Data State

### ROI pattern library release (2026-10-06)

DetecDiv commit `9535f6e46babd15efc439895038a8bb2bfd07e15` is pushed to
GitHub and GitLab and published as the default for new MATLAB jobs, replacing
`501775949bf659930506d2e6386ab97087574822`. The exact SHA is verified in:

- Linux: `/home/charvin-admin/repos/DetecDiv-jobs/releases/9535f6e46babd15efc439895038a8bb2bfd07e15`
- Windows: `C:\Users\Charvin-Admin\Documents\GitHub\DetecDiv-jobs\releases\9535f6e46babd15efc439895038a8bb2bfd07e15`

The desktop pipeline editor now provides a thumbnail library popup, separate
pattern creation/testing in workflow2, and explicit per-run selection. Named
presets live beside the pipeline, replacements require confirmation and a
revision check, and runs embed their own motif pixels and preset provenance.
Structured runtime overrides are preserved when saving a run. Eight library
tests and fourteen existing pattern tests passed on temporary projects; the
popup was rendered and inspected. App Designer code is synchronized. Desktop
MATLAB applications must reload to use the new UI. No worker restart was needed;
previously submitted jobs retain their pinned code version.

### ROI pattern consistency release (2026-10-05)

DetecDiv commit `759de8853eb70ce2eeb099d6ad8ad5e8374b5381` is pushed to
GitHub and GitLab, verified in protected release directories on both Linux
and Windows, and published as the default for new MATLAB jobs. It replaces
default `5a71907dca8d51af0f52180fa81a90764c3b4be5` without restarting workers.
Existing submitted jobs retain their original SHA.

This release invalidates outdated ROI pattern previews, preserves the source
patch when browsing other FOVs/frames and closing either editor, pins pattern
settings in submitted node overrides, restricts downstream ROI lists to the
selected positions, and explicitly fails empty generation attempts. The local
App Designer applications are synchronized; existing desktop MATLAB sessions
must reload the applications to use the updated editor code. Four existing
regression tests and ten new consistency tests passed, including GUI callbacks
against temporary synthetic projects. The affected scientific project was not
modified or rerun during the fix.

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
