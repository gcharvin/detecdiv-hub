# Windows MATLAB worker pilot

This procedure prepares one native Windows worker, initially on `10.20.11.56`.
For the SSH server setup and an overview of the pilot strategy, see
[windows_worker_ssh_strategy.md](windows_worker_ssh_strategy.md).
The live API and the three Linux workers remain separate deployments. The
Windows worker is a Python process that polls the central PostgreSQL queue and
starts the MATLAB installation on its own machine with `-batch`.

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

1. Install a supported 64-bit Python version, MATLAB, DetecDiv, and the Python
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
4. The PC must reach PostgreSQL on `webserver-labo`. The current address
   `192.168.122.185:5432` is on the VM's libvirt network and may not be
   routable from this PC. Confirm the actual route before setting
   `DETECDIV_HUB_DATABASE_URL`. A managed SSH tunnel through
   `detecdiv-server` is an alternative if direct access is unavailable.
5. Give the worker account read access to input data and write access to the
   project/output locations required by the chosen pipeline. Use a UNC share
   path in the mapping, for example `//FILESERVER/SHARE`, because a scheduled
   task may not see interactive drive-letter mappings. Match `/data` only to
   the share that contains the same files under the same relative paths.

`DETECDIV_HUB_WORKER_PATH_MAPPINGS` is a JSON list of `source`/`target` path
prefixes. The worker applies the longest matching prefix to path fields in the
job and pipeline JSON. For example, a canonical `/data/projects/P1.mat` can
become `\\FILESERVER\SHARE\projects\P1.mat`. Add more mappings if an external
classifier or another pipeline asset lives outside that root. The catalog
retains its canonical paths; these mappings belong only to this worker's env
file.

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
