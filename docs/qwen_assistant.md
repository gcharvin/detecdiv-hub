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
