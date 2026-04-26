#!/usr/bin/env bash
set -Eeuo pipefail

ACTION="${1:-status}"
INSTANCES="${INSTANCES:-1 2 3}"
UNITS=()
for instance in ${INSTANCES}; do
  UNITS+=("detecdiv-worker@${instance}.service")
done

usage() {
  cat <<'EOF'
Usage:
  sudo bash scripts/ops/manage_webvm_workers.sh status
  sudo bash scripts/ops/manage_webvm_workers.sh stop
  sudo bash scripts/ops/manage_webvm_workers.sh one
  sudo bash scripts/ops/manage_webvm_workers.sh three
  sudo bash scripts/ops/manage_webvm_workers.sh restart

Actions:
  status   Show worker services and recent logs.
  stop     Stop all configured worker instances.
  one      Keep only detecdiv-worker@2 active.
  three    Start detecdiv-worker@1, @2, and @3.
  restart  Restart all configured worker instances.

The default INSTANCES list is "1 2 3". Override with:
  INSTANCES="1 2 3 4" sudo -E bash scripts/ops/manage_webvm_workers.sh restart
EOF
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "Run with sudo." >&2
    exit 1
  fi
}

show_status() {
  systemctl list-units 'detecdiv-worker@*.service' --all --no-pager || true
  systemctl is-active "${UNITS[@]}" || true
  journalctl -u detecdiv-worker@1.service -u detecdiv-worker@2.service -u detecdiv-worker@3.service \
    --since '5 minutes ago' --no-pager | tail -n 100 || true
}

case "${ACTION}" in
  -h|--help|help)
    usage
    ;;
  status)
    show_status
    ;;
  stop)
    require_root
    systemctl stop "${UNITS[@]}" || true
    systemctl reset-failed "${UNITS[@]}" || true
    show_status
    ;;
  one)
    require_root
    systemctl stop detecdiv-worker@1.service detecdiv-worker@3.service || true
    systemctl reset-failed detecdiv-worker@1.service detecdiv-worker@3.service || true
    systemctl restart detecdiv-worker@2.service
    sleep 6
    show_status
    ;;
  three)
    require_root
    systemctl stop "${UNITS[@]}" || true
    systemctl reset-failed "${UNITS[@]}" || true
    systemctl start "${UNITS[@]}"
    sleep 8
    show_status
    ;;
  restart)
    require_root
    systemctl restart "${UNITS[@]}"
    sleep 8
    show_status
    ;;
  *)
    echo "Unknown action: ${ACTION}" >&2
    usage >&2
    exit 2
    ;;
esac
