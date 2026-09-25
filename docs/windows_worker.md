# Windows MATLAB worker pilot

This procedure prepares one native Windows worker, initially on `10.20.11.56`.
For the SSH server setup and an overview of the pilot strategy, see
[windows_worker_ssh_strategy.md](windows_worker_ssh_strategy.md).
The live API and the three Linux workers remain separate deployments. The
Windows worker is a Python process that polls the central PostgreSQL queue and
starts the MATLAB installation on its own machine with `-batch`.

## State of the first PC (2026-09-25)

On `CG-PCDELL01-306` (`10.20.11.56`), SSH key access through the local
`detecdiv-ops` administrator account works. The Windows hub checkout is at
`184d4dd`. MATLAB R2025b is installed, and DetecDiv `unstable` is at
`86c66b5`. Python 3.11 and the hub dependencies are installed in the hub
checkout's `.venv`. Python 3.13 could not resolve the pinned `nd2` and
`pydantic` versions.

The `windows-10-20-11-56` execution target (`a8eedb3f-85fc-47a8-b327-2aa766550f51`)
is now reported online. The readiness check passed under
`GMGM\Charvin-Admin`. A scheduled task named `DetecDiv Hub Database Tunnel`
runs as `SYSTEM` and forwards local port `15432` to PostgreSQL on
`webserver-labo` through `detecdiv-server`.

The first interactive SMB read test succeeded on `Y:` after authenticating to
`\\10.20.11.250\data` as `GMGM\Gilles`. The worker setup is being standardized
on `X:` because existing MATLAB pipeline dependencies use paths such as
`X:\matlab\ClassiRepository`. The `Y:` test proves access in that interactive
session only. Confirm that the worker's account and its launch session can read
and write the required paths before relying on automatic startup.
The worker was first launched in the foreground from PowerShell; closing that
window or ending its SSH session stops that process. A synthetic
`worker_smoke_test` was then claimed and completed on Windows (`CG-PCDELL01-306`)
with status `done`. This exercised the target-specific queue claim and result
writeback through the generic placeholder handler; it did not launch MATLAB.
No MATLAB pipeline run or scheduled-worker startup has yet been validated.

At the time of the smoke test, the live queue had 209 queued
`archive_raw_dataset` jobs with no execution target, while two `pipeline_run`
jobs and one archive job were already running on `detecdiv-server`. The Windows
worker's `.env` intentionally allows only `pipeline_run` and sets
`DETECDIV_HUB_WORKER_CLAIM_UNASSIGNED_JOBS=false`. The worker therefore ignores
both unassigned jobs and all archive jobs. The hub does not currently rebalance
queued work onto any idle worker automatically; new jobs must be assigned to
the Windows target when submitted. Keep the archive jobs on the Linux
storage-visible worker while Windows has `storage_visible=false`.

The server's `/data` mount comes from `//10.20.11.250/DATA`.

The live raw archive root is `/archive`, mounted on `detecdiv-server` from
`//10.20.11.251/archive`. TCP port 445 on `10.20.11.251` is reachable from
Windows, but SMB authentication and read/write access have not been tested
there. The configured archive policy has `delete_hot_source=true`; all 209
queued `archive_raw_dataset` jobs currently have `mark_archived=true`, so a
successful run deletes its source after writing the archive. Do not enable
these jobs on Windows until both shares are accessible to the worker account
and a copy-only archive test verifies destination write, archive integrity,
and path translation. The current worker path mapping is applied in the
pipeline executor; storage lifecycle jobs still resolve server paths such as
`/data` and `/archive` directly.

## Quick installation procedure for a future agent

Run Windows commands in **PowerShell**, not `cmd.exe`. The worker runs as
`GMGM\Charvin-Admin`; use the approved hub revision and keep `.env` private.
The commands below describe the first-PC pilot. Follow
[the SSH strategy](windows_worker_ssh_strategy.md) for the restricted database
tunnel key and task setup.

1. Install MATLAB, DetecDiv, and 64-bit Python 3.11. In the hub repository,
   create the Python environment and install dependencies:

   ```powershell
   Set-Location 'C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub'
   py -3.11 -m venv .venv
   .\.venv\Scripts\python.exe -m pip install -c constraints.txt -e .
   ```

2. Install and start the database tunnel task using the SSH strategy procedure.
   Confirm it is listening before configuring the worker:

   ```powershell
   Get-ScheduledTask -TaskName 'DetecDiv Hub Database Tunnel' | Select-Object TaskName, State
   Get-NetTCPConnection -LocalAddress 127.0.0.1 -LocalPort 15432 -State Listen
   ```

3. Map the data share as `X:` in the same Windows user session that will run
   the worker. This prompts for the `GMGM\Gilles` password without putting it
   in the command line:

   ```powershell
   $share = '\\10.20.11.250\data'
   net.exe use X: $share '*' '/USER:GMGM\Gilles' '/PERSISTENT:NO'
   Get-ChildItem -LiteralPath 'X:\' -ErrorAction Stop | Select-Object -First 1
   ```

   Once `X:` is verified, remove the old `Y:` mapping if it is still connected:
   `net.exe use Y: /delete`. Confirm write access to the specific output folder
   as well. Keep UNC targets in the worker path mappings because drive letters
   are session-specific. Map both canonical `/data` paths and legacy `X:\`
   paths to the same share:
   `[{"source":"/data","target":"//10.20.11.250/DATA"},{"source":"X:\\","target":"//10.20.11.250/DATA"}]`.

4. Create the protected machine-specific `.env`. Enter the PostgreSQL login
   and type its password only at the masked prompt:

   ```powershell
   .\scripts\configure_windows_worker_env.ps1
   ```

   Do not print, paste, or commit `.env` or its database URL. The helper sets
   the target key, MATLAB and DetecDiv paths, UNC mapping, and pilot-safe job
   filters (`pipeline_run` only, no unassigned jobs, schedulers disabled). The
   helper maps `/data` and legacy `X:\` pipeline paths to the share's UNC path.

5. Check configuration, then launch the worker manually for the first pilot:

   ```powershell
   .\scripts\run_worker.ps1 -EnvFile .env -Check
   .\scripts\run_worker.ps1 -EnvFile .env
   ```

   Keep this PowerShell window open. Confirm the target shows online. Send one
   small pipeline run explicitly to this execution target and inspect its job
   status, MATLAB log, Python calls, output paths, and `worker_host` before
   enabling automatic startup.

6. After the manual worker and data access are validated, register the logon
   task. Stop the foreground worker with `Ctrl+C` before starting the task so
   only one process sends the worker-instance heartbeat:

   ```powershell
   .\scripts\register_windows_worker_task.ps1 -EnvFile .env
   Start-ScheduledTask -TaskName 'DetecDiv Hub Worker'
   Start-Sleep -Seconds 5
   Get-ScheduledTask -TaskName 'DetecDiv Hub Worker' | Select-Object TaskName, State
   Get-Content .\logs\windows-worker.log -Tail 60
   ```

   The task uses the signed-in user's interactive session. It starts after
   `GMGM\Charvin-Admin` signs in following a reboot; it is not a boot-time
   Windows service. Keep the PC awake and the user signed in. Confirm the task
   can access the UNC share from its own run before relying on it.

7. For a quick operational check, look for the execution target online in the
   hub, review the worker log, and confirm the database tunnel is listening.
   Stop the task with
   `Stop-ScheduledTask -TaskName 'DetecDiv Hub Worker'`. To run interactively
   again, start `run_worker.ps1` from the repository root.

Common setup traps: `Get-Service`, `Get-Content`, and `Select-Object` are
PowerShell commands and will fail in `cmd.exe`; start PowerShell first. When
using SSH, `ssh -l 'GMGM\Charvin-Admin' 10.20.11.56` is the account form that
worked for this domain account. The Windows worker's incoming SSH access is
only for administration; the worker itself connects outward to the hub through
the restricted database tunnel.

## Pilot scope

- Use one execution target dedicated to this PC, for example
  `windows-10-20-11-56`, with `supports_matlab=true`, `supports_python=true`,
  and `max_concurrent_jobs=1`.
- Set `DETECDIV_HUB_WORKER_CLAIM_UNASSIGNED_JOBS=false` and
  `DETECDIV_HUB_WORKER_JOB_KINDS=pipeline_run`, and disable periodic schedulers.
  Submit a pipeline run with this
  target's `execution_target_id` explicitly. The worker cannot take general
  Linux maintenance jobs or jobs pinned to `detecdiv-server`.
- Pipeline runs requesting `ingest_raw_dataset` remain on the Linux storage
  worker. Their catalog ingest assumes server storage roots.
- Use one worker process initially. A process executes one job at a time; only
  raise concurrency after observing MATLAB CPU, RAM, GPU and license usage.

The hub administrator creates the target through the existing Execution Targets
admin page or `POST /execution-targets`. This does not install software on the
Windows PC. The API's `worker-scale` action runs a local systemd helper and
must not be used to install a remote Windows worker.

Suggested target values for the pilot:

```json
{
  "target_key": "windows-10-20-11-56",
  "display_name": "Windows MATLAB 10.20.11.56",
  "target_kind": "windows_matlab",
  "host_name": "10.20.11.56",
  "supports_matlab": true,
  "supports_python": true,
  "supports_gpu": false,
  "metadata_json": {"platform": "windows", "max_concurrent_jobs": 1, "storage_visible": false}
}
```

Set `supports_gpu` according to a MATLAB GPU check on the PC. Keep
`storage_visible=false` during the pilot so this target is not chosen for
server-root indexing.

## Prepare the PC

1. Install 64-bit Python 3.11, MATLAB, DetecDiv, and the Python
   environment used by the MATLAB pipelines. Verify that MATLAB can run the
   intended pipeline without its GUI, including the Python calls it makes.
2. Put a copy of this hub repository on the PC at the same worker code revision
   intended for the pilot. In PowerShell, from the repository root:

   ```powershell
   py -3.11 -m venv .venv
   .\.venv\Scripts\python.exe -m pip install -c constraints.txt -e .
   ```

3. Copy `ops/windows/worker.env.example` to `.env` in the repository root and
   fill in the database URL, target key, MATLAB executable, DetecDiv repository
   path, and storage mapping. `.env` is ignored by git and contains a database
   password; restrict it to the worker account.
4. The PC must reach PostgreSQL on `webserver-labo`. Direct access to
   `192.168.122.185:5432` was unavailable from `10.20.11.56`, so this pilot
   uses the managed SSH tunnel below. Set the database URL host and port to
   `127.0.0.1:15432`.
5. Give the worker account read access to input data and write access to the
   project/output locations required by the chosen pipeline. Use a UNC share
   path in the mapping, for example `//FILESERVER/SHARE`, because a scheduled
   task may not see interactive drive-letter mappings. Match `/data` and any
   legacy drive prefix such as `X:\` only to the share that contains the same
   files under the same relative paths. If MATLAB code opens `X:\...` paths
   directly instead of receiving them through the worker's mapped job config,
   ensure `X:` is mounted in the worker's own Windows logon session.

`DETECDIV_HUB_WORKER_PATH_MAPPINGS` is a JSON list of `source`/`target` path
prefixes. The worker applies the longest matching prefix to path fields in the
job and pipeline JSON. For example, a canonical `/data/projects/P1.mat` or a
legacy `X:\projects\P1.mat` can become
`\\FILESERVER\SHARE\projects\P1.mat`. Add more mappings if an external
classifier or another pipeline asset lives outside that root. The catalog
retains its canonical paths; these mappings belong only to this worker's env
file. A path mapping translates strings for the worker; it does not create a
Windows drive mapping. MATLAB code that directly opens `X:\...` needs `X:` to
be mounted in the worker's own logon session.

## Database tunnel on the first PC

The local SSH key used for the database tunnel has a restricted entry in
`charvin-admin`'s `authorized_keys` on `detecdiv-server`:

```text
from="10.20.11.56",restrict,port-forwarding,permitopen="192.168.122.185:5432" ssh-ed25519 <public-key> detecdiv-windows-db-tunnel
```

The private key is `C:\ProgramData\DetecDivHub\id_ed25519_db_tunnel`. Its
owner is `SYSTEM`, with access granted only to `SYSTEM` and local
Administrators. The server host key is pinned in
`C:\Users\detecdiv-ops\.ssh\known_hosts`; compare its fingerprint with the
server before installing it. The task runs
`C:\ProgramData\DetecDivHub\run_db_tunnel.ps1`, a copy of
[`scripts/run_windows_db_tunnel.ps1`](../scripts/run_windows_db_tunnel.ps1).
The registration helper is
[`scripts/register_windows_db_tunnel_task.ps1`](../scripts/register_windows_db_tunnel_task.ps1).

From a **PowerShell** session on the PC, inspect the task and tunnel with:

```powershell
Get-ScheduledTask -TaskName 'DetecDiv Hub Database Tunnel' | Select-Object TaskName, State
Get-NetTCPConnection -LocalAddress 127.0.0.1 -LocalPort 15432 -State Listen
Get-Content C:\ProgramData\DetecDivHub\db-tunnel.log -Tail 20
```

The scheduled task starts at system boot and the script reconnects if SSH
exits. Do not store the tunnel private key in the hub repository. Incoming SSH
to this PC uses a separate key and account.

## Finish the first PC configuration

In a **PowerShell** session as `GMGM\Charvin-Admin`, create `.env` from
`ops/windows/worker.env.example` and set these values:

The helper below prompts for the PostgreSQL login and masked password, writes
the machine-specific values, and locks down the `.env` ACL. Run it from the
hub checkout:

```powershell
.\scripts\configure_windows_worker_env.ps1
```

Enter the login from the database URL's `LOGIN:PASSWORD` portion. At the
password prompt, type the password and press Enter; PowerShell masks the
input. Do not paste the password or generated URL into chat.

```text
DETECDIV_HUB_DATABASE_URL=postgresql+psycopg://USER:PASSWORD@127.0.0.1:15432/detecdiv_hub
DETECDIV_HUB_WORKER_TARGET_KEY=windows-10-20-11-56
DETECDIV_HUB_WORKER_INSTANCE=windows-10-20-11-56-main
DETECDIV_HUB_MATLAB_COMMAND=C:\Program Files\MATLAB\R2025b\bin\matlab.exe
DETECDIV_HUB_MATLAB_REPO_ROOT=C:\Users\Charvin-Admin\Documents\MATLAB\DetecDiv
DETECDIV_HUB_WORKER_PATH_MAPPINGS=[{"source":"/data","target":"//10.20.11.250/DATA"},{"source":"X:\\","target":"//10.20.11.250/DATA"}]
```

Use the actual database username and password from the VM deployment. Keep
`DETECDIV_HUB_WORKER_CLAIM_UNASSIGNED_JOBS=false`,
`DETECDIV_HUB_WORKER_JOB_KINDS=pipeline_run`, and
`DETECDIV_HUB_WORKER_ENABLE_SCHEDULERS=false` from the example. Restrict `.env`
to the worker account, `SYSTEM`, and local Administrators. Do not paste the
database URL or password into a chat or commit it to git.

## Check and start manually

From the Windows repository root:

```powershell
.\scripts\run_worker.ps1 -EnvFile .env -Check
.\scripts\run_worker.ps1 -EnvFile .env
```

The check verifies that the MATLAB executable and DetecDiv repository exist,
that the path mapping is well formed, and that the PC can read its execution
target from PostgreSQL. The running worker then appears in the target's
`worker_instances` heartbeat. Submit one small pipeline run explicitly to this
target and inspect the job result, MATLAB log, and `worker_host` before enabling
automatic startup.

## Start after Windows sign-in

The supplied scheduled task runs as the signed-in user so MATLAB licensing and
network-share access use that account. It starts after that user signs in, and
is therefore a pilot setup for a PC kept awake and signed in.

```powershell
.\scripts\register_windows_worker_task.ps1 -EnvFile .env
Start-ScheduledTask -TaskName 'DetecDiv Hub Worker'
Get-Content .\logs\windows-worker.log -Tail 60
```

Stop the manually launched worker before starting the task so that only one
process writes the instance heartbeat.

For an unattended machine, have the Windows administrator replace the logon
task with a dedicated service account and verify its MATLAB license, network
share permissions, and startup behavior. An SSH server on the PC is useful for
remote installation and diagnostics but is not needed by the worker itself.
