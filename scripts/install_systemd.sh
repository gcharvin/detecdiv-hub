#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=""
SERVICE_USER=""
ENV_FILE="/etc/detecdiv-hub/detecdiv-hub.env"
UNIT_DIR="/etc/systemd/system"
API_SERVICE_NAME="detecdiv-api.service"
WORKER_SERVICE_NAME="detecdiv-worker.service"
WORKER_INSTANCE_COUNT="1"
API_HOST="127.0.0.1"
API_PORT="8000"

usage() {
  cat <<'EOF'
Usage:
  sudo ./scripts/install_systemd.sh --repo-root /path/to/detecdiv-hub --service-user USER [options]

Options:
  --repo-root PATH       Repository root containing .venv and api.app
  --service-user USER    Linux user that should run the services
  --env-file PATH        Environment file path (default: /etc/detecdiv-hub/detecdiv-hub.env)
  --unit-dir PATH        systemd unit directory (default: /etc/systemd/system)
  --api-host HOST        API bind host for uvicorn (default: 127.0.0.1)
  --api-port PORT        API bind port for uvicorn (default: 8000)
  --api-name NAME        API service unit filename (default: detecdiv-api.service)
  --worker-name NAME     Worker service unit filename (default: detecdiv-worker.service)
  --worker-instances N   Number of worker service instances to run (default: 1)
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
    --api-host)
      API_HOST="$2"
      shift 2
      ;;
    --api-port)
      API_PORT="$2"
      shift 2
      ;;
    --api-name)
      API_SERVICE_NAME="$2"
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

if [[ $EUID -ne 0 ]]; then
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

mkdir -p "$UNIT_DIR"

WORKER_TEMPLATE_NAME="${WORKER_SERVICE_NAME%.service}@.service"
WORKER_TEMPLATE_BASENAME="${WORKER_TEMPLATE_NAME%.service}"
WORKER_BASENAME="${WORKER_SERVICE_NAME%.service}"

cat >"$UNIT_DIR/$API_SERVICE_NAME" <<EOF
[Unit]
Description=DetecDiv Hub API
After=network.target postgresql.service

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$REPO_ROOT
EnvironmentFile=$ENV_FILE
ExecStart=$REPO_ROOT/.venv/bin/python -m uvicorn api.app:app --host $API_HOST --port $API_PORT
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

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
ExecStart=$REPO_ROOT/.venv/bin/python worker/run_worker.py
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
ExecStart=$REPO_ROOT/.venv/bin/python worker/run_worker.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${API_SERVICE_NAME%.service}"
systemctl restart "${API_SERVICE_NAME%.service}"

mapfile -t EXISTING_WORKER_TEMPLATE_UNITS < <(
  systemctl list-unit-files --type=service --no-legend "${WORKER_TEMPLATE_BASENAME}@*.service" 2>/dev/null | awk '{print $1}'
)

if [[ "$WORKER_INSTANCE_COUNT" == "1" ]]; then
  for unit in "${EXISTING_WORKER_TEMPLATE_UNITS[@]}"; do
    [[ -n "$unit" ]] || continue
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
    systemctl stop "${unit%.service}" 2>/dev/null || true
    systemctl disable "${unit%.service}" 2>/dev/null || true
  done
  for instance in $(seq 1 "$WORKER_INSTANCE_COUNT"); do
    systemctl enable "${WORKER_TEMPLATE_BASENAME}@${instance}"
    systemctl restart "${WORKER_TEMPLATE_BASENAME}@${instance}"
  done
fi

echo "Installed and restarted $API_SERVICE_NAME with $WORKER_INSTANCE_COUNT worker instance(s)."
