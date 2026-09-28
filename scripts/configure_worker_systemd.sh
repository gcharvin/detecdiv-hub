#!/usr/bin/env bash
set -euo pipefail

CONFIGURE_SCRIPT=$(realpath "$0")
REPO_ROOT=""
SERVICE_USER=""
ENV_FILE="/etc/detecdiv-hub/detecdiv-hub.env"
UNIT_DIR="/etc/systemd/system"
WORKER_SERVICE_NAME="detecdiv-worker.service"
WORKER_INSTANCE_COUNT="1"
MEMORY_BUDGET_MB=""
CPU_BUDGET=""
HOST_MEMORY_RESERVE_MB=""
SWAP_BUDGET_MB=""
PLAN_ONLY="false"
SKIP_MANAGER_RESTART="false"
VERIFY_MEMORY_ISOLATION="false"
JOB_WORKER_INSTANCE=""
JOB_CPU_CORES=""
JOB_MEMORY_MB=""
JOB_SWAP_MB=""

usage() {
  cat <<'EOF'
Usage:
  sudo ./scripts/configure_worker_systemd.sh --repo-root /path/to/detecdiv-hub --service-user USER [options]

Options:
  --repo-root PATH       Repository root containing .venv and worker/run_worker.py
  --service-user USER    Linux user that should run the worker service(s)
  --env-file PATH        Environment file path (default: /etc/detecdiv-hub/detecdiv-hub.env)
  --unit-dir PATH        systemd unit directory (default: /etc/systemd/system)
  --worker-name NAME     Worker service unit filename (default: detecdiv-worker.service)
  --worker-instances N   Number of worker service instances to run (default: 1)
  --memory-budget-mb N   Total RAM for ALL worker instances (persisted across scaling)
  --cpu-budget N         Total CPU cores for ALL workers (persisted across scaling)
  --swap-budget-mb N     Total swap for ALL workers (0 disables worker swap)
  --host-memory-reserve-mb N  RAM kept outside the workers for VMs and host services
  --plan-only            Print the resource plan without changing services
  --verify-memory-isolation  Run a disposable 64 MiB RAM / 32 MiB swap cgroup OOM check
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root)
      REPO_ROOT="$2"
      shift 2
      ;;
    --service-user)
      SERVICE_USER="$2"
      shift 2
      ;;
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --unit-dir)
      UNIT_DIR="$2"
      shift 2
      ;;
    --worker-name)
      WORKER_SERVICE_NAME="$2"
      shift 2
      ;;
    --worker-instances)
      WORKER_INSTANCE_COUNT="$2"
      shift 2
      ;;
    --job-worker-instance) JOB_WORKER_INSTANCE="$2"; shift 2 ;;
    --job-cpu-cores) JOB_CPU_CORES="$2"; shift 2 ;;
    --job-memory-mb) JOB_MEMORY_MB="$2"; shift 2 ;;
    --job-swap-mb) JOB_SWAP_MB="$2"; shift 2 ;;
    --memory-budget-mb) MEMORY_BUDGET_MB="$2"; shift 2 ;;
    --cpu-budget) CPU_BUDGET="$2"; shift 2 ;;
    --swap-budget-mb) SWAP_BUDGET_MB="$2"; shift 2 ;;
    --host-memory-reserve-mb) HOST_MEMORY_RESERVE_MB="$2"; shift 2 ;;
    --plan-only) PLAN_ONLY="true"; shift ;;
    --skip-manager-restart) SKIP_MANAGER_RESTART="true"; shift ;;
    --verify-memory-isolation) VERIFY_MEMORY_ISOLATION="true"; shift ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$REPO_ROOT" || -z "$SERVICE_USER" ]]; then
  usage
  exit 1
fi

if [[ $EUID -ne 0 && "$PLAN_ONLY" != "true" ]]; then
  echo "This script must run as root." >&2
  exit 1
fi

if [[ ! -x "$REPO_ROOT/.venv/bin/python" ]]; then
  echo "Missing virtualenv python at $REPO_ROOT/.venv/bin/python" >&2
  exit 1
fi

if ! [[ "$WORKER_INSTANCE_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "--worker-instances must be a positive integer." >&2
  exit 1
fi

WORKER_TEMPLATE_NAME="${WORKER_SERVICE_NAME%.service}@.service"
WORKER_TEMPLATE_BASENAME="${WORKER_SERVICE_NAME%.service}"
WORKER_BASENAME="${WORKER_SERVICE_NAME%.service}"

# One shared host budget. Scaling changes each slot's share, never the total.
RESOURCE_CONFIG="$UNIT_DIR/${WORKER_BASENAME}-resources.conf"
if [[ -f "$RESOURCE_CONFIG" ]]; then
  source "$RESOURCE_CONFIG"
fi
HOST_MEMORY_MB=$(awk '/MemTotal:/ {print int($2 / 1024)}' /proc/meminfo)
HOST_SWAP_MB=$(awk '/SwapTotal:/ {print int($2 / 1024)}' /proc/meminfo)
HOST_CPUS=$(nproc)
DEFAULT_RESERVE=$(( HOST_MEMORY_MB / 5 ))
(( DEFAULT_RESERVE >= 4096 )) || DEFAULT_RESERVE=4096
HOST_MEMORY_RESERVE_MB="${HOST_MEMORY_RESERVE_MB:-${SAVED_HOST_MEMORY_RESERVE_MB:-$DEFAULT_RESERVE}}"
MEMORY_BUDGET_MB="${MEMORY_BUDGET_MB:-${SAVED_MEMORY_BUDGET_MB:-$((HOST_MEMORY_MB - HOST_MEMORY_RESERVE_MB))}}"
DEFAULT_CPU_BUDGET=$(( HOST_CPUS * 4 / 5 ))
(( DEFAULT_CPU_BUDGET <= 36 )) || DEFAULT_CPU_BUDGET=36
CPU_BUDGET="${CPU_BUDGET:-${SAVED_CPU_BUDGET:-$DEFAULT_CPU_BUDGET}}"
DEFAULT_SWAP_BUDGET=$(( HOST_SWAP_MB * 3 / 4 ))
SWAP_BUDGET_MB="${SWAP_BUDGET_MB:-${SAVED_SWAP_BUDGET_MB:-$DEFAULT_SWAP_BUDGET}}"
for value in "$MEMORY_BUDGET_MB" "$CPU_BUDGET" "$HOST_MEMORY_RESERVE_MB"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo 'Resource budgets must be positive integers.' >&2; exit 1; }
done
[[ "$SWAP_BUDGET_MB" =~ ^(0|[1-9][0-9]*)$ ]] || { echo 'Swap budget must be a nonnegative integer.' >&2; exit 1; }
(( SWAP_BUDGET_MB <= HOST_SWAP_MB )) || { echo 'Worker swap budget exceeds host swap.' >&2; exit 1; }
if (( MEMORY_BUDGET_MB + HOST_MEMORY_RESERVE_MB > HOST_MEMORY_MB || CPU_BUDGET > HOST_CPUS )); then
  echo 'Worker budgets exceed physical host capacity after the host/VM reserve.' >&2
  exit 1
fi
# Job quota changes do not restart or modify any other service.
if [[ -n "$JOB_WORKER_INSTANCE" ]]; then
  [[ "$JOB_WORKER_INSTANCE" =~ ^[1-9][0-9]*$ && "$JOB_CPU_CORES" =~ ^[1-9][0-9]*$ && "$JOB_MEMORY_MB" =~ ^[1-9][0-9]*$ && "$JOB_SWAP_MB" =~ ^(0|[1-9][0-9]*)$ ]] || { echo 'Invalid job quota arguments.' >&2; exit 1; }
  (( JOB_CPU_CORES <= CPU_BUDGET && JOB_MEMORY_MB >= 512 && JOB_MEMORY_MB <= MEMORY_BUDGET_MB && JOB_SWAP_MB <= SWAP_BUDGET_MB )) || { echo 'Job quota exceeds compute budget.' >&2; exit 1; }
  JOB_UNIT="${WORKER_TEMPLATE_BASENAME}@${JOB_WORKER_INSTANCE}.service"
  [[ "$(systemctl show "$JOB_UNIT" -p User --value)" == "$SERVICE_USER" && "$(systemctl show "$JOB_UNIT" -p Slice --value)" == 'detecdiv-workers.slice' ]] || { echo 'Worker service ownership or slice mismatch.' >&2; exit 1; }
  systemctl set-property --runtime "$JOB_UNIT" "CPUQuota=$((JOB_CPU_CORES * 100))%" "MemoryMax=${JOB_MEMORY_MB}M" "MemoryHigh=$((JOB_MEMORY_MB * 9 / 10))M" "MemorySwapMax=${JOB_SWAP_MB}M"
  echo "worker=$JOB_WORKER_INSTANCE cpu=$JOB_CPU_CORES memory_mb=$JOB_MEMORY_MB swap_mb=$JOB_SWAP_MB"
  exit 0
fi
# Idle slots stay small. Their CPU/RAM limits grow only after job admission.
WORKER_MEMORY_MB=512
WORKER_CPU_CORES=1
WORKER_SWAP_MB=0
if (( WORKER_INSTANCE_COUNT > CPU_BUDGET || WORKER_INSTANCE_COUNT * WORKER_MEMORY_MB >= MEMORY_BUDGET_MB )); then
  echo 'Too many workers for the idle-process reserve and CPU budget.' >&2
  exit 1
fi
WORKER_MEMORY_HIGH_MB=$(( WORKER_MEMORY_MB * 9 / 10 ))
echo "Workers=$WORKER_INSTANCE_COUNT shared_memory_mb=$MEMORY_BUDGET_MB shared_swap_mb=$SWAP_BUDGET_MB shared_cpu_cores=$CPU_BUDGET host_reserve_mb=$HOST_MEMORY_RESERVE_MB per_worker_memory_mb=$WORKER_MEMORY_MB per_worker_swap_mb=$WORKER_SWAP_MB per_worker_cpu_cores=$WORKER_CPU_CORES"
[[ "$PLAN_ONLY" != "true" ]] || exit 0
if [[ "$VERIFY_MEMORY_ISOLATION" == "true" ]]; then
  CHECK_UNIT="detecdiv-memory-isolation-check-$(date +%s).service"
  if systemd-run --unit="$CHECK_UNIT" --wait -p MemoryMax=64M -p MemorySwapMax=32M -p OOMPolicy=kill "$REPO_ROOT/.venv/bin/python" -c 'allocation=bytearray(256*1024*1024)'; then
    echo 'Isolation check failed: allocation unexpectedly succeeded.' >&2
    exit 1
  fi
  CHECK_RESULT=$(systemctl show "$CHECK_UNIT" -p Result --value)
  systemctl reset-failed "$CHECK_UNIT"
  [[ "$CHECK_RESULT" == "oom-kill" ]] || { echo "Unexpected isolation result: $CHECK_RESULT" >&2; exit 1; }
  echo 'Verified: memory excess killed only the disposable cgroup.'
  exit 0
fi
mkdir -p "$UNIT_DIR"
cat >"$RESOURCE_CONFIG" <<EOF
SAVED_MEMORY_BUDGET_MB=$MEMORY_BUDGET_MB
SAVED_CPU_BUDGET=$CPU_BUDGET
SAVED_SWAP_BUDGET_MB=$SWAP_BUDGET_MB
SAVED_HOST_MEMORY_RESERVE_MB=$HOST_MEMORY_RESERVE_MB
EOF
cat >"$UNIT_DIR/detecdiv-workers.slice" <<EOF
[Unit]
Description=Shared DetecDiv compute budget (keeps RAM available for the Hub VM)
[Slice]
MemoryAccounting=true
MemoryMax=${MEMORY_BUDGET_MB}M
MemorySwapMax=${SWAP_BUDGET_MB}M
CPUAccounting=true
CPUQuota=$((CPU_BUDGET * 100))%
EOF

worker_limits() {
  cat <<EOF
Slice=detecdiv-workers.slice
MemoryAccounting=true
MemoryHigh=${WORKER_MEMORY_HIGH_MB}M
MemoryMax=${WORKER_MEMORY_MB}M
MemorySwapMax=${WORKER_SWAP_MB}M
CPUAccounting=true
CPUQuota=$((WORKER_CPU_CORES * 100))%
OOMPolicy=kill
KillMode=control-group
Environment=DETECDIV_HUB_WORKER_MEMORY_BUDGET_MB=$MEMORY_BUDGET_MB
Environment=DETECDIV_HUB_WORKER_SWAP_BUDGET_MB=$SWAP_BUDGET_MB
Environment=DETECDIV_HUB_WORKER_INSTANCES=$WORKER_INSTANCE_COUNT
Environment=DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB=$WORKER_MEMORY_MB
Environment=DETECDIV_HUB_WORKER_SWAP_LIMIT_MB=$WORKER_SWAP_MB
Environment=DETECDIV_HUB_HOST_MEMORY_RESERVE_MB=$HOST_MEMORY_RESERVE_MB
Environment=DETECDIV_HUB_WORKER_CPU_LIMIT=$WORKER_CPU_CORES
Environment=DETECDIV_HUB_WORKER_CPU_BUDGET=$CPU_BUDGET
Environment=DETECDIV_HUB_WORKER_DYNAMIC_RESOURCES=1
Environment=DETECDIV_HUB_WORKER_CONFIGURE_SCRIPT=$CONFIGURE_SCRIPT
EOF
}

cat >"$UNIT_DIR/$WORKER_SERVICE_NAME" <<EOF
[Unit]
Description=DetecDiv Hub Worker
After=network.target postgresql.service

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$REPO_ROOT
EnvironmentFile=$ENV_FILE
Environment=DETECDIV_HUB_WORKER_INSTANCE=main
$(worker_limits)
ExecStart=$REPO_ROOT/.venv/bin/python worker/run_worker.py
ExecStopPost=$REPO_ROOT/.venv/bin/python -m worker.record_worker_exit
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

cat >"$UNIT_DIR/$WORKER_TEMPLATE_NAME" <<EOF
[Unit]
Description=DetecDiv Hub Worker Instance %i
After=network.target postgresql.service

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$REPO_ROOT
EnvironmentFile=$ENV_FILE
Environment=DETECDIV_HUB_WORKER_INSTANCE=%i
$(worker_limits)
ExecStart=$REPO_ROOT/.venv/bin/python worker/run_worker.py
ExecStopPost=$REPO_ROOT/.venv/bin/python -m worker.record_worker_exit
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

CONFIGURE_SCRIPT=$(realpath "$0")
cat >"$UNIT_DIR/detecdiv-worker-manager.service" <<EOF
[Unit]
Description=DetecDiv worker pool resource manager
After=network.target
[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$REPO_ROOT
EnvironmentFile=$ENV_FILE
ExecStart=$REPO_ROOT/.venv/bin/python -m worker.manage_workers --configure-script $CONFIGURE_SCRIPT --repo-root $REPO_ROOT --service-user $SERVICE_USER --env-file $ENV_FILE --unit-dir $UNIT_DIR --initial-instances $WORKER_INSTANCE_COUNT
Restart=always
RestartSec=5
MemoryMax=512M
MemorySwapMax=256M
CPUQuota=100%
[Install]
WantedBy=multi-user.target
EOF
DB_OVERRIDE="$UNIT_DIR/$WORKER_TEMPLATE_NAME.d/10-webvm-db.conf"
if [[ -f "$DB_OVERRIDE" ]]; then
  mkdir -p "$UNIT_DIR/detecdiv-worker-manager.service.d"
  # The existing worker drop-in can also override ExecStart. Only inherit its
  # environment, otherwise the manager would accidentally become a worker.
  awk '/^\[Service\]$/ {print} /^(Environment|EnvironmentFile|PassEnvironment|UnsetEnvironment)=/ {print}' "$DB_OVERRIDE" >"$UNIT_DIR/detecdiv-worker-manager.service.d/10-webvm-db.conf"
fi

systemctl daemon-reload
systemctl set-property --runtime detecdiv-workers.slice MemoryMax="${MEMORY_BUDGET_MB}M" MemorySwapMax="${SWAP_BUDGET_MB}M" CPUQuota="$((CPU_BUDGET * 100))%"

mapfile -t EXISTING_WORKER_TEMPLATE_UNITS < <(
  # list-unit-files often contains only the template, not its enabled/running
  # instances. Include live units so scaling down really stops surplus slots.
  {
    systemctl list-unit-files --type=service --no-legend "${WORKER_TEMPLATE_BASENAME}@*.service" 2>/dev/null
    systemctl list-units --type=service --all --no-legend "${WORKER_TEMPLATE_BASENAME}@*.service" 2>/dev/null
  } | awk '{print $1}' | sort -u
)
mapfile -t LEGACY_DOUBLE_AT_UNITS < <(
  systemctl list-units 'detecdiv-worker@@*.service' --all --no-legend 2>/dev/null | awk '{print $1}'
)

if [[ "$WORKER_INSTANCE_COUNT" == "1" ]]; then
  for unit in "${EXISTING_WORKER_TEMPLATE_UNITS[@]}"; do
    [[ -n "$unit" ]] || continue
    [[ "$unit" != "$WORKER_TEMPLATE_NAME" ]] || continue
    systemctl stop "${unit%.service}" 2>/dev/null || true
    systemctl disable "${unit%.service}" 2>/dev/null || true
  done
  systemctl enable "$WORKER_BASENAME"
  systemctl restart "$WORKER_BASENAME"
else
  systemctl stop "$WORKER_BASENAME" 2>/dev/null || true
  systemctl disable "$WORKER_BASENAME" 2>/dev/null || true
  for unit in "${EXISTING_WORKER_TEMPLATE_UNITS[@]}"; do
    [[ -n "$unit" ]] || continue
    [[ "$unit" != "$WORKER_TEMPLATE_NAME" ]] || continue
    systemctl stop "${unit%.service}" 2>/dev/null || true
    systemctl disable "${unit%.service}" 2>/dev/null || true
  done
  for unit in "${LEGACY_DOUBLE_AT_UNITS[@]}"; do
    [[ -n "$unit" ]] || continue
    systemctl stop "${unit%.service}" 2>/dev/null || true
    systemctl disable "${unit%.service}" 2>/dev/null || true
  done
  for instance in $(seq 1 "$WORKER_INSTANCE_COUNT"); do
    systemctl enable "${WORKER_TEMPLATE_BASENAME}@${instance}"
    systemctl restart "${WORKER_TEMPLATE_BASENAME}@${instance}"
  done
fi

systemctl enable detecdiv-worker-manager.service
if [[ "$SKIP_MANAGER_RESTART" != "true" ]]; then
  systemctl restart detecdiv-worker-manager.service
fi

echo "Configured $WORKER_INSTANCE_COUNT worker instance(s)."
