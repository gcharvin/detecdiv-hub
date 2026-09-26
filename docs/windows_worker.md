# Windows MATLAB worker pilot

This procedure prepares one native Windows worker, initially on `10.20.11.56`.
For the SSH server setup and an overview of the pilot strategy, see
[windows_worker_ssh_strategy.md](windows_worker_ssh_strategy.md).
The live API and the three Linux workers remain separate deployments. The
Windows worker is a Python process that polls the central PostgreSQL queue and
starts the MATLAB installation on its own machine with `-batch`.

## State of the first PC (2026-09-26)

On `CG-PCDELL01-306` (`10.20.11.56`), MATLAB R2025b, DetecDiv, Python 3.11,
and the hub checkout's `.venv` are installed. Python 3.13 could not resolve
the pinned `nd2` and `pydantic` versions, so use Python 3.11 for this checkout.
Verify the exact hub revision with `git log -1` before an upgrade or
troubleshooting session.

The Windows OpenSSH operational log recorded `Accepted publickey` for
`GMGM\Charvin-Admin` from `192.168.190.2` at 2026-09-26 09:04, even though some
client sessions reset immediately after authentication. Treat that event as
proof that key authentication succeeded; a later reset is a shell/session
or transport issue after authentication. See the SSH troubleshooting section in
[windows_worker_ssh_strategy.md](windows_worker_ssh_strategy.md).

The `windows-10-20-11-56` execution target (`a8eedb3f-85fc-47a8-b327-2aa766550f51`)
is now reported online. The readiness check passed under
`GMGM\Charvin-Admin`. A scheduled task named `DetecDiv Hub Database Tunnel`
runs as `SYSTEM` and forwards local port `15432` to PostgreSQL on
`webserver-labo` through `detecdiv-server`.

The data share was verified on `X:` in an interactive PowerShell session. The
SMB connection was requested as `GMGM\Gilles`; that does not guarantee that a
scheduled worker session can see the drive or reuse those credentials. The
worker path mapping contains both `/data` and `X:\` to
`//10.20.11.250/DATA`. Verify read and write access from the worker's actual
launch session before relying on filesystem jobs.

A Windows `pipeline_run` was claimed and passed worker preflight. MATLAB then
exited before starting the pipeline with MathWorks Licensing Error 10 because
the installed license is expired. The job is failed and the Windows worker is
idle. MATLAB work must stay out of its queue until a license check succeeds.

The queue helper completed successfully on the Windows PC on 2026-09-26. It
set unassigned-job claiming to `true`, allowed all job kinds, and excluded
`archive_raw_dataset`, `restore_raw_dataset`, `pipeline_run`, and
`legacy_matlab`. The follow-up readiness command was mistyped as `-EnvFil`; run
the complete `-Check` command below to confirm the effective settings. Keep
MATLAB kinds excluded until the license is verified. Workers do not move jobs
already assigned to another execution target. The database's `SKIP LOCKED`
claim means the first eligible worker to poll an unassigned job gets it; this
is not a least-loaded-target scheduler.

At the last check, the scheduled task `DetecDiv Hub Worker` did not exist.
Register it only after confirming the manual worker and its share access. The
database-tunnel task is separate and was already reported installed.

The server's `/data` mount comes from `//10.20.11.250/DATA`.

The live raw archive root is `/archive`, mounted on `detecdiv-server` from
`//10.20.11.251/archive`. TCP port 445 on `10.20.11.251` is reachable from
Windows, but SMB authentication and read/write access have not been tested
there. The configured archive policy can delete hot sources after writing the
archive. Do not enable these jobs on Windows until both shares are accessible to the worker account
and a copy-only archive test verifies destination write, archive integrity,
and path translation. Archive and restore kinds remain excluded from Windows.
The worker's current path mapping is applied to pipeline execution; other
filesystem job handlers still need per-kind validation for Windows paths and
write access before being trusted with server-root operations.

## Quick installation procedure for a future agent

Run Windows commands in **PowerShell**, not `cmd.exe`. The worker runs as
`GMGM\Charvin-Admin`; use the approved hub revision and keep `.env` private.
The commands below describe the first-PC pilot. Follow
[the SSH strategy](windows_worker_ssh_strategy.md) for installing the incoming
SSH server, provisioning its separate key, setting up the restricted database
tunnel, and diagnosing Windows OpenSSH.

These scripts currently contain first-PC values: the Windows account,
execution-target key, MATLAB/DetecDiv paths, share mapping, and `.env` ACL.
Before reusing them on another Windows host, update those values and ACL
principals for that account. Do not copy the first PC's `.env`, tunnel private
key, or incoming SSH private key to another worker.

1. Install MATLAB, DetecDiv, and 64-bit Python 3.11. Confirm installed Python
   versions with `py -0p`; Python 3.13 failed to resolve the pinned `nd2` and
   `pydantic` packages. In an approved hub checkout, create the worker's
   separate Python environment and install dependencies:

   ```powershell
   Set-Location 'C:\Users\Charvin-Admin\Documents\MATLAB\detecdiv-hub'
   py -3.11 -m venv .venv
   .\.venv\Scripts\python.exe -m pip install -c constraints.txt -e .
   ```

   This `.venv` runs the Hub worker. It is not necessarily the Python
   environment called by MATLAB. Install/configure the pipeline's Python
   environment separately and verify that MATLAB can call it in `-batch` mode
   under the worker account. The pilot's MATLAB currently fails with
   MathWorks Licensing Error 10 because its license is expired.

2. For an existing clean checkout, inspect and update the Hub code before
   running setup scripts:

   ```powershell
   git status --short --branch
   git pull --ff-only origin master
   git log -1 --oneline
   ```

   Do not overwrite or discard `.env` or other uncommitted files. `.env` is
   machine-specific and ignored by Git.

3. Install and start the database tunnel task using the SSH strategy procedure.
   Confirm it is listening before configuring the worker:

   ```powershell
   Get-ScheduledTask -TaskName 'DetecDiv Hub Database Tunnel' | Select-Object TaskName, State
   Get-NetTCPConnection -LocalAddress 127.0.0.1 -LocalPort 15432 -State Listen
   ```

4. Map the data share as `X:` in the same Windows user session that will run
   the worker. This prompts for the `GMGM\Gilles` password without putting it
   in the command line:

   ```powershell
   $share = '\\10.20.11.250\data'
   net.exe use X: $share '*' '/USER:GMGM\Gilles' '/PERSISTENT:NO'
   Get-ChildItem -LiteralPath 'X:\' -ErrorAction Stop | Select-Object -First 1
   ```

   `Test-NetConnection 10.20.11.250 -Port 445` only tests whether SMB's TCP
   port is reachable; it does not prove that the share name, credentials, or
   permissions work. `Get-ChildItem X:\` is the actual read check. Use exactly
   two leading backslashes in `$share`; do not synthesize the UNC path from
   repeated escaped backslashes.

   Once `X:` is verified, remove the old `Y:` mapping if it is still connected:
   `net.exe use Y: /delete`. Confirm write access to the specific output folder
   as well. Keep UNC targets in the worker path mappings because drive letters
   are session-specific. Map both canonical `/data` paths and legacy `X:\`
   paths to the same share:
   `[{"source":"/data","target":"//10.20.11.250/DATA"},{"source":"X:\\","target":"//10.20.11.250/DATA"}]`.

5. Create the protected machine-specific `.env`. Enter the PostgreSQL login
   and type its password only at the masked prompt:

   ```powershell
   .\scripts\configure_windows_worker_env.ps1
   ```

   Do not print, paste, or commit `.env` or its database URL. The helper sets
   the target key, MATLAB and DetecDiv paths, UNC mapping, and queue settings.
   To update an existing `.env` without rewriting its database URL, use the
   queue helper below instead.

   For an existing Windows worker, update only its queue settings from
   PowerShell:

   ```powershell
   .\scripts\enable_windows_queue_worker.ps1 -EnvFile .env
   .\scripts\run_worker.ps1 -EnvFile .env -Check
   ```

   This enables unassigned jobs, excludes `archive_raw_dataset` and
   `restore_raw_dataset`, and also excludes `pipeline_run` and `legacy_matlab`
   until `-MatlabLicenseReady` is supplied after a successful license check.
   The helper preserves the database URL and secures the `.env` ACL. Restart
   the worker only while it is idle; it reads the `.env` at process startup.

6. Check configuration, then launch the worker manually for the first pilot:

   ```powershell
   .\scripts\run_worker.ps1 -EnvFile .env -Check
   .\scripts\run_worker.ps1 -EnvFile .env
   ```

   The readiness output should show the correct execution target, MATLAB path,
   DetecDiv path, and mappings. For the current queue policy, expect
   `Allowed job kinds: all`, `Claim unassigned jobs: True`, schedulers disabled,
   and exclusions for archive/restore plus MATLAB until the license is fixed.
   Keep this PowerShell window open during the manual pilot. Before sending
   MATLAB work, verify the license explicitly:

   ```powershell
   & 'C:\Program Files\MATLAB\R2025b\bin\matlab.exe' -batch "disp(version)"
   ```

   The pilot returned MathWorks Licensing Error 10 (expired license), so no
   `pipeline_run` should be enabled until this command succeeds. Then verify the
   pipeline-specific Python environment and inspect output paths and
   `worker_host` on a small run.

7. After the manual worker and data access are validated, register the logon
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

8. For a quick operational check, look for the execution target online in the
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

## Queue sharing scope

- Use one execution target dedicated to this PC, for example
  `windows-10-20-11-56`, with `supports_matlab=true`, `supports_python=true`,
  and `max_concurrent_jobs=1`.
- Set `DETECDIV_HUB_WORKER_CLAIM_UNASSIGNED_JOBS=true`, leave
  `DETECDIV_HUB_WORKER_JOB_KINDS` empty (all kinds). Keep archive and restore
  excluded by default. Archive jobs can be enabled after the SMB share is
  verified; keep restore excluded until a separate restore test is planned.
  Until MATLAB licensing is restored, also exclude `pipeline_run` and
  `legacy_matlab`. Disable periodic schedulers.
- This only shares jobs with `execution_target_id=NULL`. Jobs explicitly
  assigned to `detecdiv-server` remain there. For a shared job, the first
  eligible worker that claims it wins; priorities and resource limits still
  apply within each target.
- While archive jobs remain excluded, an unassigned archive-only backlog will
  leave Windows idle. Enabling archive jobs makes those queued jobs eligible
  after the worker is restarted.
- Queue eligibility alone does not guarantee that every filesystem job can run
  on Windows. Validate its path mapping, share permissions, and required tools
  before allowing storage-mutating jobs to use this target.
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

3. Create `.env` with `scripts/configure_windows_worker_env.ps1` as described in
   [Finish the first PC configuration](#finish-the-first-pc-configuration).
   It prompts for database credentials, encodes them for the URL, writes the
   pilot's machine-specific values, and restricts the file ACL. Do not edit or
   commit a credential-bearing `.env` by hand.
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

## Enable raw-dataset archiving on Windows

The archive storage root used by the Linux workers is `/archive`, backed by
the SMB share `\\10.20.11.251\archive`. The Windows lifecycle handler maps
canonical `/data/...` and `/archive/...` paths to this PC's configured UNC
paths for file operations, while it continues to store canonical `/data` and
`/archive` paths in the hub database. The SMB login must have read access to
the source data and write/delete access in the archive share.

Run these commands in **PowerShell as the same `GMGM\Charvin-Admin` account
that runs the worker**. The `net use` command prompts for the SMB password;
the password is not part of the command. This temporary `W:` mapping is for
the access check; the worker configuration below uses the UNC path directly.

```powershell
Test-NetConnection 10.20.11.251 -Port 445
$archiveShare = '\\10.20.11.251\archive'
net.exe use W: $archiveShare '*' '/USER:GMGM\Gilles' '/PERSISTENT:NO'
Get-ChildItem -LiteralPath 'W:\' -ErrorAction Stop | Select-Object -First 1
$probe = Join-Path 'W:\' ('.detecdiv-write-test-' + [guid]::NewGuid().ToString('N') + '.tmp')
New-Item -ItemType File -Path $probe -ErrorAction Stop | Out-Null
Remove-Item -LiteralPath $probe -Force -ErrorAction Stop
```

If the archive server authorizes a different SMB account, use that account in
`/USER:`. A successful TCP check alone does not prove share access; the
directory read and temporary-file create/delete must both succeed. If `W:` is
already assigned, choose another unused letter. Drive mappings are per logon
session, so do this from the worker's account.

After confirming the queued archive jobs and their `mark_archived` settings,
configure the worker with the UNC path. The helper verifies read access and
creates/deletes a uniquely named temporary file before it changes `.env`; it
preserves the database URL, updates the `/archive` path mapping, enables only
`archive_raw_dataset`, and leaves `restore_raw_dataset`, `pipeline_run`, and
`legacy_matlab` excluded by default. It also sets the worker's default archive
root to canonical `/archive`:

```powershell
.\scripts\enable_windows_queue_worker.ps1 `
    -EnvFile .env `
    -ArchiveSharePath '\\10.20.11.251\archive' `
    -EnableArchiveJobs
.\scripts\run_worker.ps1 -EnvFile .env -Check
```

The helper does not restart the worker. Apply the new environment on the next
intentional restart while the worker is idle. Once it restarts, any unassigned
archive jobs in the queue are eligible immediately. An archive job with
`mark_archived=true` removes the source data after a successful archive, so
review the queued jobs before restarting. `restore_raw_dataset` stays excluded;
enable it only after separately validating restore paths and permissions.

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
The key and the pinned `known_hosts` file must already have been provisioned
securely; do not generate/copy private key material from the repository. From
the Windows Hub checkout, install the reconnecting script and register the
SYSTEM startup task:

```powershell
$installDir = 'C:\ProgramData\DetecDivHub'
New-Item -ItemType Directory -Path $installDir -Force | Out-Null
Copy-Item .\scripts\run_windows_db_tunnel.ps1 (Join-Path $installDir 'run_db_tunnel.ps1') -Force
icacls.exe (Join-Path $installDir 'id_ed25519_db_tunnel') /inheritance:r /grant:r '*S-1-5-18:F' '*S-1-5-32-544:F'
.\scripts\register_windows_db_tunnel_task.ps1 `
    -SshHost 'detecdiv-server' `
    -SshUser 'charvin-admin' `
    -DatabaseHost '192.168.122.185' `
    -DatabasePort 5432 `
    -LocalPort 15432 `
    -IdentityFile (Join-Path $installDir 'id_ed25519_db_tunnel') `
    -KnownHostsFile 'C:\Users\detecdiv-ops\.ssh\known_hosts' `
    -TunnelScript (Join-Path $installDir 'run_db_tunnel.ps1')
Start-ScheduledTask -TaskName 'DetecDiv Hub Database Tunnel'
```

The `icacls` line assumes the private-key file was copied to the path above.
Confirm the tunnel task, listener, and log with the read-only checks below.

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

In a **PowerShell** session as `GMGM\Charvin-Admin`, create `.env` with the
helper below. It writes the first-PC values from
`ops/windows/worker.env.example` and sets the file ACL:

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

Use the actual database username and password from the VM deployment. For the
current Windows queue policy, use
`DETECDIV_HUB_WORKER_CLAIM_UNASSIGNED_JOBS=true`, leave
`DETECDIV_HUB_WORKER_JOB_KINDS` empty (all kinds), exclude
`archive_raw_dataset,restore_raw_dataset,pipeline_run,legacy_matlab` by default,
and keep
`DETECDIV_HUB_WORKER_ENABLE_SCHEDULERS=false`. The helper
`scripts/enable_windows_queue_worker.ps1` applies this policy to an existing
`.env` without rewriting its database URL. Once the archive share has passed
the read/write probe, use `-ArchiveSharePath ... -EnableArchiveJobs` to remove
only the archive exclusion and add its path mapping. After the MATLAB license
is successfully checked, run it with `-MatlabLicenseReady` to remove only the
two MATLAB exclusions. Restrict `.env` to the worker account, `SYSTEM`, and
local Administrators. Do not paste the database URL or password into a chat or
commit it to git.

## Check and start manually

From the Windows repository root:

```powershell
.\scripts\run_worker.ps1 -EnvFile .env -Check
.\scripts\run_worker.ps1 -EnvFile .env
```

The check verifies that the MATLAB executable and DetecDiv repository exist,
that the path mapping is well formed, and that the PC can read its execution
target from PostgreSQL. It does not test the MATLAB license or SMB access. The
running worker then appears in the target's `worker_instances` heartbeat.
Submit a small pipeline only after MATLAB license and the pipeline's Python
environment have been verified; inspect the job result, MATLAB log, output
paths, and `worker_host` before enabling automatic startup.

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

## Troubleshooting recorded during the first installation

### PowerShell versus `cmd.exe`

The prompt `C:\Users\...>` was `cmd.exe`, not PowerShell. Commands such as
`Get-Service`, `Get-WinEvent`, `Select-Object`, `Get-ChildItem`, and PowerShell's
call operator `&` then failed with “not recognized” or “unexpected”. Start
PowerShell first; do not paste prompt markers (`PS>` or `>`) with the commands.
If connected over SSH, the login shell may initially be `cmd.exe`; type
`powershell` before running PowerShell commands.

`Test-Connection` checks ICMP and has no `-Port` option. Use
`Test-NetConnection <host> -Port <port>` to check TCP, then test the actual
service/share separately.

### SSH username, key, and connection reset

For the domain account, this syntax worked:

```powershell
ssh -l 'GMGM\Charvin-Admin' 10.20.11.56
```

The form `ssh charvin-admin\@10.20.11.56` caused the server to parse `GMGM` as
the username (`Invalid user GMGM`). Do not infer key failure from
`Connection reset` alone. Run `ssh -vvv` with the intended `-i` key and check
the Windows `OpenSSH/Operational` event log. The log recorded
`Accepted publickey for GMGM\Charvin-Admin` during sessions that still reset;
once the client reports `Authenticated ... using "publickey"`, the key worked
and investigation should move to session/shell or transport behavior. Full commands are in
[windows_worker_ssh_strategy.md](windows_worker_ssh_strategy.md).

Windows OpenSSH normally uses the single file
`C:\ProgramData\ssh\administrators_authorized_keys` for administrators. Do not
confuse it with a folder named `administrators\authorized_keys`. The pilot's
`Match User` override instead selected the account's `.ssh\authorized_keys`.
Use `sshd -T -C ...` to discover the effective `authorizedkeysfile`; inspect
that file's key fingerprint and ACL. The key fingerprint used in this pilot was
`SHA256:J1A0r8jwW5LVY3clMjP3beXWJ+JjUPeg5Dxa9Ux4PLk`. A DEBUG3 setting was added
to `sshd_config` while diagnosing; remove/restore it after debugging, validate
with `sshd -t`, and restart `sshd`.

### SMB share and drive mappings

The `Y:` mapping was reported unavailable in `net use`; a malformed UNC path
also produced triple leading backslashes and `Test-Path` returned false. The
working PowerShell form used a literal UNC path and prompted for the SMB
password without putting it on the command line:

```powershell
$share = '\\10.20.11.250\data'
net.exe use X: $share '*' '/USER:GMGM\Gilles' '/PERSISTENT:NO'
Get-ChildItem -LiteralPath 'X:\' -ErrorAction Stop | Select-Object -First 1
Get-SmbConnection | Select-Object ServerName, ShareName, UserName
```

TCP 445 being reachable did not prove the share was accessible; the successful
`Get-ChildItem` read did. The `X:` mapping is session-scoped, so verify it under
the worker's actual sign-in/task. A JSON path mapping rewrites job paths; it does
not mount a drive or supply SMB credentials. Keep both `/data` and legacy
`X:\` mappings pointed to the corresponding UNC share.

### `.env`, readiness, queue, and scheduled task

- Use `scripts/configure_windows_worker_env.ps1` to create a new `.env`. It
  prompts separately for the PostgreSQL login and a masked password, URI-escapes
  both, and sets the ACL. Do not paste a large hand-written PowerShell block
  from chat into a prompt; one malformed paste previously put a PowerShell
  expression in the login field. Never display or paste `.env` or its database
  URL.
- For an existing `.env`, use
  `scripts/enable_windows_queue_worker.ps1 -EnvFile .env`; it preserves the DB
  URL. The first version of this helper used `$matches`, a reserved automatic
  PowerShell variable, and failed before writing the file. This was fixed in
  commit `c546012`. After pulling that commit, the helper completed successfully.
- Run the readiness check with the complete parameter spelling:
  `.\scripts\run_worker.ps1 -EnvFile .env -Check`. The truncated `-EnvFil`
  command failed with “Argument manquant”; it did not test readiness.
- A failed `Get/Stop/Start-ScheduledTask -TaskName 'DetecDiv Hub Worker'` means
  the task name was not registered. Check the exact tasks with
  `Get-ScheduledTask | Where-Object TaskName -like '*DetecDiv*'`. The worker
  script run in a console stays attached to that console and must be stopped
  there with `Ctrl+C` while idle. Register the logon task only after the manual
  run is validated; the task uses the signed-in user and is not a Windows
  service or boot-time task.
- The first MATLAB test failed with MathWorks Licensing Error 10. The queue
  helper therefore excludes `pipeline_run` and `legacy_matlab` as well as
  archive/restore. When MATLAB licensing is repaired, verify `matlab.exe
  -batch` first, then update `.env` using `-MatlabLicenseReady` and restart the
  worker while idle.

At the last queue inspection, all unassigned jobs were archives. With the
default exclusions in place, it is therefore expected for the Windows worker to
remain idle. If archive jobs are enabled, review pending jobs before restarting
because eligible jobs can be claimed immediately.
