#!/usr/bin/env bash
set -Eeuo pipefail

REPO="${REPO:-/home/charvin-admin/repos/detecdiv-hub-webvm}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
SBIN_DIR="${SBIN_DIR:-/usr/local/sbin}"
STAMP="$(date +%Y%m%d_%H%M%S)"

usage() {
  cat <<'EOF'
Usage:
  sudo bash scripts/ops/install_webserver_labo_boot_mount.sh [--start]

Installs the webserver-labo boot helpers that:
  - mount /data from detecdiv-server with sshfs and retry on failure
  - restart the DetecDiv Hub API container after /data is mounted

Expected local repository path on webserver-labo:
  /home/charvin-admin/repos/detecdiv-hub-webvm

Optional environment variables:
  REPO
  SYSTEMD_DIR
  SBIN_DIR

After installation, verify with:
  findmnt /data
  systemctl status detecdiv-data-share.service --no-pager
  systemctl status detecdiv-hub-after-data.service --no-pager
EOF
}

START_NOW=false
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
elif [[ "${1:-}" == "--start" ]]; then
  START_NOW=true
elif [[ -n "${1:-}" ]]; then
  usage >&2
  exit 2
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run with sudo." >&2
  exit 1
fi

for path in \
  "${REPO}/ops/systemd/detecdiv-data-share.service" \
  "${REPO}/ops/systemd/detecdiv-hub-after-data.service" \
  "${REPO}/scripts/ops/detecdiv-webvm-after-data.sh"
do
  if [[ ! -f "${path}" ]]; then
    echo "Missing required file: ${path}" >&2
    exit 1
  fi
done

install_with_backup() {
  local source="$1"
  local target="$2"
  local mode="$3"

  if [[ -e "${target}" ]]; then
    cp -a "${target}" "${target}.bak.${STAMP}"
  fi
  install -D -m "${mode}" "${source}" "${target}"
}

install_with_backup \
  "${REPO}/ops/systemd/detecdiv-data-share.service" \
  "${SYSTEMD_DIR}/detecdiv-data-share.service" \
  0644

install_with_backup \
  "${REPO}/ops/systemd/detecdiv-hub-after-data.service" \
  "${SYSTEMD_DIR}/detecdiv-hub-after-data.service" \
  0644

install_with_backup \
  "${REPO}/scripts/ops/detecdiv-webvm-after-data.sh" \
  "${SBIN_DIR}/detecdiv-webvm-after-data.sh" \
  0755

systemctl daemon-reload
systemctl enable detecdiv-data-share.service detecdiv-hub-after-data.service

if [[ "${START_NOW}" == "true" ]]; then
  systemctl restart detecdiv-data-share.service
  systemctl start detecdiv-hub-after-data.service
fi

systemctl status detecdiv-data-share.service --no-pager || true
systemctl status detecdiv-hub-after-data.service --no-pager || true
