# Local Qwen assistant

The Hub UI calls the authenticated API endpoints `/assistant/status` and
`/assistant/chat`. The API is deliberately the only public-facing component;
the inference server runs on `detecdiv-server`. Its port is restricted by the
compute host firewall to the `webserver-labo` VM only.

## Safety boundary for the first release

The first release supports conversation and translation only. The inference
server receives text, not Hub credentials, filesystem paths, database access,
shell access, or DetecDiv job controls. Storage search and job preparation
will be separate, audited tools added later.

## Administrator start/stop control

Administrators can start or stop Qwen from the Assistant page. The request is
stored as the desired state and queued as a Hub job for the configured GPU
worker target; the API VM never runs `systemctl` on the compute host. Starting
is refused while a Hub GPU pipeline job is active. When GPU arbitration is
enabled, pipeline completion only restarts Qwen if its desired state is still
`running`.

The same API can be controlled from a shell with `ops/scripts/qwen-control.sh`.
Set `DETECDIV_HUB_URL` to the Hub API base URL and `DETECDIV_HUB_TOKEN` to an
administrator session token, then run `bash ops/scripts/qwen-control.sh status`,
`start`, or `stop`. Start/stop print the queued job; the status command reports the
service's current and desired state. Treat the token as a password and do not
put it in shell history or a shared script.

The worker host still needs the narrow passwordless sudoers rule shown below.
Without it, the control job fails safely and reports an error in the Hub.

## GPU host installation

Do this on `detecdiv-server` during an agreed maintenance window. The service
is intentionally not enabled by this repository.

1. Create a dedicated Python environment outside the Hub worker environment.
2. Install a CUDA-compatible vLLM release in that environment.
3. Create `/home/charvin-admin/.cache/detecdiv-qwen/huggingface` owned by
   `charvin-admin`. Do not use the `/data` CIFS mount: Hugging Face needs
   symbolic links there, which that mount does not support.
4. Copy `ops/systemd/detecdiv-qwen.env.example` to
   `/etc/detecdiv-hub/qwen.env`, then select a tested quantized Qwen model.
5. Copy `ops/systemd/detecdiv-qwen.service` and
   `ops/scripts/run-qwen-server.sh` to the equivalent paths in the deployed
   worker checkout. Make the script executable.
6. Run `systemctl daemon-reload`, then start the service manually. Do not
   enable it at boot until it has passed GPU coexistence tests.

The Hub API configuration on `webserver-labo` then needs:

```ini
DETECDIV_HUB_ASSISTANT_ENABLED=true
DETECDIV_HUB_ASSISTANT_QWEN_BASE_URL=http://<internal-qwen-route>:8001/v1
DETECDIV_HUB_ASSISTANT_QWEN_MODEL=<same-model-name>
```

The internal route must be reachable from the VM but must not be exposed to
client browsers or the public network. When the service binds on all compute
host interfaces, configure the compute-host firewall to permit TCP 8001 only
from `webserver-labo` (`192.168.122.185`), before starting Qwen.

On the current compute host, keep `VLLM_USE_FLASHINFER_SAMPLER=0`: its CUDA
12.0 toolkit cannot compile the optional FlashInfer sampler shipped with the
tested vLLM release. vLLM's normal sampler remains available.

## Initial GPU policy

The RTX 3090 has 24 GiB VRAM. Start with one Qwen request at a time, an 8k
context limit, and a GPU utilization cap of 0.80. DetecDiv workers retain
priority. With `DETECDIV_HUB_ASSISTANT_GPU_ARBITRATION_ENABLED=true`, a
pipeline run targeting a GPU stops Qwen before MATLAB starts; Qwen restarts
only after no GPU pipeline job remains running. Install this narrow sudoers
rule first (adjust the systemctl path only if necessary):

```sudoers
charvin-admin ALL=(root) NOPASSWD: /usr/bin/systemctl start detecdiv-qwen.service, /usr/bin/systemctl stop detecdiv-qwen.service
```

The setting is off by default. Do not enable CUDA MPS until the baseline
stop/start policy has been measured with representative workloads.

## Read-only agent evaluation pilot

`scripts/evaluate_qwen_runs.py` evaluates diagnosis of recorded failures,
interpretation of successful runs, and progress of active jobs. It provides two
recorded tools, `get_job_evidence` and `get_progress_history`. These are evaluation
tools, not yet public assistant endpoints. Administrator captures stay in the
chosen local report directory; they are not part of the committed test fixtures.

From the administration workstation:

```powershell
& .venv/Scripts/python.exe scripts/evaluate_qwen_runs.py collect --output reports/qwen-pilot
# Collect again after a useful interval to establish an output trend.
& .venv/Scripts/python.exe scripts/evaluate_qwen_runs.py collect --output reports/qwen-pilot
& .venv/Scripts/python.exe scripts/evaluate_qwen_runs.py models --output reports/qwen-pilot
& .venv/Scripts/python.exe scripts/evaluate_qwen_runs.py benchmark --output reports/qwen-pilot
```

The benchmark never starts an inference service or changes scientific jobs. It
exits with code 2 if GPU jobs are active/queued, other compute processes own the
GPU, resource checks fail, or the served model is unavailable. GPU utilization
of zero does not override a job reservation. The served endpoint must support
automatic tool choice with the appropriate Qwen parser; the current basic chat
launcher does not enable it. Alternate test services must use the existing GPU
admission/arbitration and a controlled cleanup lifecycle.

Reuse the same frozen corpus for each checkpoint and set `--model`, `--label`
and, if needed, `--base-url`. Results include tool traces, token usage and request
latency; diagnosis quality still requires human review. Checkpoint sizes in
`model-plan.json` describe disk weights, not measured VRAM. A worker heartbeat
does not prove scientific progress, and local step fractions do not establish
whole-job completion or an ETA.

### Supplementary coexistence pilots

`scripts/run_qwen_coexistence_pilot.py --output reports/qwen-pilot-20261014`
tests the separately labelled Qwen3-4B-AWQ checkpoint on the frozen corpus with
an 8192-token context, one request at a time, eager execution and a 0.23 vLLM
memory-utilization setting. This is an experimental arm; it does not replace the
8B/14B quantization comparison.

The pilot temporarily uses the existing managed Qwen service and backs up its
launcher. A compute-host watchdog limits service residency to 5888 MiB, checks
free VRAM and unexpected compute processes, expires after 600 seconds, and stops
the service and restores the exact launcher on exit. Worker GPU arbitration
remains enabled. An API-container transaction holds the same execution-target
row lock used by worker admission; it changes no catalog rows and expires after
720 seconds. New queued pipeline requests ask the controller to preempt the
pilot. The controller releases admission only after verifying that Qwen is
stopped and the launcher is restored. The transaction also releases on process
exit. Admission may therefore wait briefly for an in-flight inference request
to finish; the controller must not hold the lock across unrelated work.

`--coexistence-owner` on the evaluator requires a matching, fresh watchdog and
admission owner; it is not a general GPU-guard bypass. Known scientific VRAM
reservations plus the pilot ceiling and 2048 MiB of headroom must fit the card.
Unknown reservations, new queued requests, stale leases and lost ownership
refuse inference. Controller state and measured residency are stored in the
local report directory. Inspect these records before attempting another pilot.

The controller also accepts `--model Qwen/Qwen3-8B-AWQ` with an explicit,
separate `--label`. A reduced-context experimental arm can use
`--context-tokens 4096 --kv-cache-mib 640 --max-new-tokens 512
--evidence-profile compact4096`. The fixed KV allocation is in MiB and keeps
the default cache dtype. The compact evidence view preserves error signatures,
summary counters and scoped progress while explicitly omitting long logs and
extra parameters. It adds the same 160-word response instruction for both
checkpoints. Do not silently combine this arm with the 8192-token comparison.

On the tested RTX 3090/vLLM runtime, reducing context and KV alone did not keep
8B startup peaks below the coexistence ceiling. The working experimental
configuration additionally uses `--cpu-offload-gib 2.5 --disable-uva
--allocator-gc 0.15`. Disabling UVA selects vLLM's copy-based CPU offloader;
this avoids the observed startup peak but makes inference considerably slower.
CPU offloading changes latency and host RAM use, so the comparison is not a pure
parameter-count or quantization-speed experiment. No runtime package is patched.
Measure startup peaks as well as steady residency; utilization caps are not
hard ceilings. The watchdog enforces the independent service ceiling.

Use the same compact view, context, KV cache and generation settings for a
paired 4B arm; CPU offloading is unnecessary for that smaller checkpoint.
Existing completed conversations are retained when a bounded lease expires or
a scientific request preempts the pilot. Resuming the same model/corpus/label
runs only missing cases. Keep those runtime settings unchanged when resuming,
and archive controller/VRAM records from each wave before reuse of the label.
Report unequal case counts and compare the common cases explicitly.

If systemd leaves a timed-out startup in `failed`, cleanup briefly starts a
temporary `/bin/true` launcher, stops it, and then restores the original launcher.
This clears the service failure without loading the production checkpoint.
Verify the launcher hash, inactive service and released admission after every
wave. Never count startup failures as model diagnostic answers.

### Main 8B/14B, AWQ/FP8 comparison

The completed 15 October 2026 campaign is documented in the
[anonymized benchmark report](reports/qwen-assistant-benchmark-2026-10-15.md),
with exact aggregate metrics and a qualitative review of all 44 outputs.
Raw scientific captures, responses and operational traces remain private.

The same controller accepts `--main-plan reports/qwen-pilot-20261014/model-plan.json`
and one of the four planned `--model` values. For example:

```powershell
& .venv/Scripts/python.exe scripts/run_qwen_coexistence_pilot.py --output reports/qwen-pilot-20261014 --main-plan reports/qwen-pilot-20261014/model-plan.json --model Qwen/Qwen3-14B-AWQ
```

This mode pins the plan's revision and experimental vLLM utilization setting.
It requires context 8192, standard evidence, output limit 1100, no explicit KV
override and no CPU offload. Concurrency remains one; batch tokens are fixed at
256 for all four main arms. The independent service ceiling is 22400 MiB and
the watchdog still requires at least 2048 MiB of physical free VRAM. Main arms
wait if any scientific GPU reservation or other compute process exists, before
acquiring admission, and repeat that check while holding the admission lock.
This ceiling is a safeguard, not a measured residency or promise of fit.

The official checkpoints must already be complete in the local Linux cache.
Check disk space before downloading, reuse Hugging Face blobs, and keep enough
disk headroom; do not acquire GPU admission for a download. An index referring
to a missing weight shard refuses startup. Validate FP8 using the installed
runtime and a controlled launch: lack of native FP8 on the RTX 3090 alone does
not establish incompatibility, since the runtime includes alternative kernels.
Record the selected kernel and any launch error; an unsupported format has no
diagnostic score.

A separate admission watcher requests the compute watchdog to stop this owner
when a new scientific request arrives, even during an in-flight model request.
It also stops on loss of admission ownership or freshness. The main-arm watchdog
rejects the appearance of any other compute process, including a worker's.
Admission releases only after service stop and launcher restoration. Neither
the watcher nor cleanup may request another owner's stop. API and worker GPU
arbitration must be enabled before launch.

Controller records preserve the corpus hash and inference settings. New records
archive the previous wave under its label, and completed conversations are
preserved across interruptions. Resuming with changed evidence, generation or
recorded inference settings is refused. Review all saved answers against the
frozen evidence before recommending a checkpoint for Hub; use measured peaks,
latency and actual tool traces, not successful parsing as a quality score.
