#!/usr/bin/env bash
set -Eeuo pipefail

REPO="${REPO:-/home/charvin-admin/repos/detecdiv-hub-webvm}"
ENV_DIR="${ENV_DIR:-/etc/detecdiv-hub}"
WORKER_ENV="${WORKER_ENV:-${ENV_DIR}/worker-webvm.env}"
OVERRIDE_DIR="${OVERRIDE_DIR:-/etc/systemd/system/detecdiv-worker@.service.d}"
OVERRIDE="${OVERRIDE:-${OVERRIDE_DIR}/10-webvm-db.conf}"
STAMP="$(date +%Y%m%d_%H%M%S)"

DATABASE_URL="${DETECDIV_HUB_DATABASE_URL:-}"
WORKER_TARGET_KEY="${DETECDIV_HUB_WORKER_TARGET_KEY:-detecdiv-server}"
MATLAB_COMMAND="${DETECDIV_HUB_MATLAB_COMMAND:-/usr/local/bin/matlab}"
MATLAB_REPO_ROOT="${DETECDIV_HUB_MATLAB_REPO_ROOT:-/home/charvin-admin/repos/DetecDiv}"
POLL_INTERVAL_SEC="${DETECDIV_HUB_WORKER_POLL_INTERVAL_SEC:-5}"

usage() {
  cat <<'EOF'
Usage:
  sudo DETECDIV_HUB_DATABASE_URL='postgresql+psycopg://USER:PASSWORD@192.168.122.185:5432/detecdiv_hub' \
    bash scripts/ops/install_webvm_worker_override.sh

Optional environment variables:
  REPO
  WORKER_ENV
  DETECDIV_HUB_WORKER_TARGET_KEY
  DETECDIV_HUB_MATLAB_COMMAND
  DETECDIV_HUB_MATLAB_REPO_ROOT
  DETECDIV_HUB_WORKER_POLL_INTERVAL_SEC

This script writes the worker env file and systemd override for compute workers
that connect to the webserver-labo PostgreSQL database. Do not commit real
database passwords to the repository.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run with sudo." >&2
  exit 1
fi

if [[ -z "${DATABASE_URL}" ]]; then
  echo "DETECDIV_HUB_DATABASE_URL is required." >&2
  usage >&2
  exit 1
fi

if [[ ! -x "${REPO}/.venv/bin/python" ]]; then
  echo "Missing worker Python: ${REPO}/.venv/bin/python" >&2
  exit 1
fi

mkdir -p "${ENV_DIR}" "${OVERRIDE_DIR}"
if [[ -f "${WORKER_ENV}" ]]; then
  cp -a "${WORKER_ENV}" "${WORKER_ENV}.bak.${STAMP}"
fi
if [[ -f "${OVERRIDE}" ]]; then
  cp -a "${OVERRIDE}" "${OVERRIDE}.bak.${STAMP}"
fi

cat > "${WORKER_ENV}" <<EOF
DETECDIV_HUB_ENVIRONMENT=prod
DETECDIV_HUB_DATABASE_URL=${DATABASE_URL}
DETECDIV_HUB_LOG_LEVEL=INFO
DETECDIV_HUB_DEFAULT_USER_KEY=localdev
DETECDIV_HUB_AUTO_PROVISION_USERS=true
DETECDIV_HUB_MATLAB_COMMAND=${MATLAB_COMMAND}
DETECDIV_HUB_MATLAB_REPO_ROOT=${MATLAB_REPO_ROOT}
DETECDIV_HUB_WORKER_TARGET_KEY=${WORKER_TARGET_KEY}
DETECDIV_HUB_WORKER_POLL_INTERVAL_SEC=${POLL_INTERVAL_SEC}
EOF
chmod 0640 "${WORKER_ENV}"

cat > "${OVERRIDE}" <<EOF
[Service]
WorkingDirectory=${REPO}
EnvironmentFile=
EnvironmentFile=${WORKER_ENV}
ExecStart=
ExecStart=${REPO}/.venv/bin/python worker/run_worker.py
EOF

systemctl daemon-reload
systemctl cat detecdiv-worker@.service --no-pager
