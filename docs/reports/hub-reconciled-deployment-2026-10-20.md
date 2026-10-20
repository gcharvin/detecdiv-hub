# Reconciled Hub deployment — 20 October 2026

Audited commit: `01204c96829b0b676384e7f182ae54074b56aa8d`.
Final checkpoint: 21:07 UTC, 23:07 Europe/Paris.
Evidence: [deployment summary](../../reports/hub-reconciled-deployment-20261020/deployment-summary.json).

## Installed and running states

| Component | Verified fingerprint | State |
| --- | --- | --- |
| Hub API on `webserver-labo` | `15fed3245ae8` | Updated, rebuilt and healthy through the public proxy. |
| Windows compute worker | `b35b0444d5f0` | Updated and running, PID 93632; fresh heartbeat confirmed. |
| Linux source and pollers `@1,@2,@6` | `a2649a68b316` | Files installed; these three idle pollers activated. |
| Linux pollers `@3,@4,@5` | Previous loaded fingerprints | Their existing analyses continue. Automatic idle activation is pending. |

The rollout does **not** claim all Linux runtimes are already current.
Files on disk and code loaded by each Python process were checked separately.
The expected fingerprints differ across hosts because their existing adapters
and operational trees differ; no shared checkout was reset or overwritten wholesale.

## What actually needed deployment

The initial full runtime inventory showed differences predating this cleanup.
The rollout reconciled only the published delta since `222dea1`, applying it
where each host executes that role. API-only UI files were not deployed as
worker changes, and the VM's unused compute executor was not treated as a compute
target. Existing path mappings and host compatibility patches were retained.

- **API:** one missing shared service update,
  `api/services/project_deletion.py`, preserving real backup path case.
  The force-stop schema columns, route, English UI and project resource-default
  implementation were already deployed. The schema was inspected; no migration
  was reapplied. Only the API container was rebuilt, using `--no-deps`.
- **Linux:** the same backup-path fix and validated project resource defaults
  in `api/services/pipeline_runs.py`. Compute-side force-stop containment,
  GPU admission and matching-driver selection were already installed.
- **Windows:** these two shared service updates, the compatible MATLAB child
  environment update, the generic NVIDIA helper (guarded off on Windows),
  and the registration helper's unlimited-duration comment/settings.
  The actual scheduled worker task already had `PT0S`; its principal, action
  and triggers were preserved. Existing Windows claim/path compatibility remains.
- Qwen experiment controllers remain administration-workstation scripts.
  Historical reports, tests and inactive SQL drafts are not production services
  to execute or replay. No model service or scientific pipeline was launched.

Production roots inspected:

- API operational tree on `webserver-labo`:
  `/home/charvin-admin/repos/detecdiv-hub-webvm`, and its running API container.
- Linux compute tree on `detecdiv-server`:
  `/home/charvin-admin/repos/detecdiv-hub-webvm`.
- Native Windows compute tree:
  `C:/Users/Charvin-Admin/Documents/MATLAB/detecdiv-hub`.

## Safe activation and continuing work

The three originally running analyses retained their start times, parameter
SHA256 fingerprints, poller PIDs and advancing heartbeats:

| Analysis | Poller | Preserved PID |
| --- | --- | ---: |
| `cd39f046-677f-4794-b934-e89f08cb5aaa` | `@3` | 382618 |
| `137eec41-3bfc-4c7a-bbbf-03d5a948a329` | `@4` | 2571587 |
| `e28d8d5e-3601-4c8d-8d69-ae67fe4c8974` | `@5` | 409307 |

The activation helper reserves admission using the same target-row lock as
job claim, verifies a fresh idle registry entry and confirms OS process identity
and an empty child tree before signalling a poller. Existing analyses are not
cancelled, retried, retargeted or resized. Brief admission reservations were
restored after each pass. The Linux changes affect control-plane shared services,
not model code or scientific settings; loaded processes retain their existing
module objects until a guarded idle reload.

`detecdiv-hub-reconciled-activation-20261020.timer` runs on Linux every 30 seconds.
It supersedes the previous force-stop maintenance timer and stops when all
pollers report the new fingerprint. `Linger=yes` and the timer's active state
were checked, so activation continues independently of this chat and SSH session.
The Windows maintenance task self-removed after confirming its new heartbeat.
The database tunnel remained running. Normal queue admission can start ordinary
jobs; the rollout itself submitted no analysis or force-stop request.

MATLAB default `6ed9c3e62c10a6bab3d1fa361adbfe54f9355c6b`, protected releases,
Python environments used by calculations, weights and source data were preserved.

## Tests and verification

Each host's exact merged files were copied into an isolated staging mirror.
The targeted synthetic suite passed **59 tests on Linux and 59 on Windows**,
including real isolated process trees, preserved unrelated processes, force-stop
authorization, project resource defaults, legacy GPU admission, staged driver
selection and backup path handling. Pytest dependencies were installed only in
the staging directory. Two additional synthetic backup-file tests passed inside
the API container environment before activation.

After rebuild, the actual API fingerprint matched the staged expectation;
database health, public proxy, force-stop OpenAPI route, English assets and
recent container logs passed verification. No schema errors were found.
All installed file hashes matched their staged variants. Existing NVIDIA
580.173.02 library fingerprints were verified without initializing CUDA or
allocating GPU memory. This is deployment verification, not biological validation.

Host-local backups and staged files are preserved under `.deploy-backups/` and
`.deploy-staging/hub-reconciled-20261020/`. Before/merged/source comparison copies
are preserved in verified ZIPs; local raw scratch copies are retained but excluded
from Git. The archives, SHA256 hashes and member counts are listed in
[reconciliation-archives.json](../../reports/hub-reconciled-deployment-20261020/reconciliation-archives.json).

The static checkpoint records the three pending Linux activations. Their live
completion is recorded remotely in
`.deploy-staging/hub-reconciled-20261020/activation-last.json` and in subsequent
Hub worker heartbeats; it is not inferred from file copying alone.
