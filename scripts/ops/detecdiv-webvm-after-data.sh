#!/usr/bin/env bash
set -Eeuo pipefail

COMPOSE_DIR="${COMPOSE_DIR:-/home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo}"
DATA_MOUNT="${DATA_MOUNT:-/data}"
DATA_PROBE_PATH="${DATA_PROBE_PATH:-/data/Alexander}"
API_CONTAINER="${API_CONTAINER:-detecdiv-hub-api}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8000/health}"
WAIT_SECONDS="${WAIT_SECONDS:-180}"

log() {
  printf '%s %s\n' "$(date --iso-8601=seconds)" "$*"
}

compose() {
  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose "$@"
    return
  fi
  docker compose "$@"
}

data_ready() {
  findmnt -rn --target "${DATA_MOUNT}" >/dev/null 2>&1 || return 1
  if [[ -n "${DATA_PROBE_PATH}" ]]; then
    [[ -e "${DATA_PROBE_PATH}" ]] || return 1
  fi
}

container_data_ready() {
  docker exec "${API_CONTAINER}" sh -lc "findmnt -rn --target '${DATA_MOUNT}' >/dev/null 2>&1" >/dev/null 2>&1 || return 1
  if [[ -n "${DATA_PROBE_PATH}" ]]; then
    docker exec "${API_CONTAINER}" test -e "${DATA_PROBE_PATH}" >/dev/null 2>&1 || return 1
  fi
}

api_health_ready() {
  curl -fsS "${HEALTH_URL}" >/dev/null 2>&1
}

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root." >&2
  exit 1
fi

if [[ ! -d "${COMPOSE_DIR}" ]]; then
  echo "Compose directory not found: ${COMPOSE_DIR}" >&2
  exit 1
fi

if ! data_ready; then
  log "${DATA_MOUNT} is not ready; starting detecdiv-data-share.service"
  systemctl start detecdiv-data-share.service || true
fi

deadline=$((SECONDS + WAIT_SECONDS))
until data_ready; do
  if (( SECONDS >= deadline )); then
    echo "${DATA_MOUNT} did not become ready within ${WAIT_SECONDS}s." >&2
    findmnt "${DATA_MOUNT}" >&2 || true
    ls -ld "${DATA_MOUNT}" >&2 || true
    exit 1
  fi
  sleep 3
done

log "${DATA_MOUNT} is mounted and probe path is visible."
cd "${COMPOSE_DIR}"

log "Ensuring DetecDiv Hub compose services are up."
compose up -d

log "Restarting ${API_CONTAINER} so its bind mount sees the mounted ${DATA_MOUNT}."
compose restart api

deadline=$((SECONDS + 60))
until container_data_ready; do
  if (( SECONDS >= deadline )); then
    echo "${API_CONTAINER} does not see mounted ${DATA_MOUNT} after restart." >&2
    docker exec "${API_CONTAINER}" sh -lc "findmnt '${DATA_MOUNT}' || true; ls -ld '${DATA_MOUNT}' || true" >&2 || true
    exit 1
  fi
  sleep 2
done

log "${API_CONTAINER} sees ${DATA_MOUNT}."
deadline=$((SECONDS + 60))
until api_health_ready; do
  if (( SECONDS >= deadline )); then
    echo "DetecDiv Hub API did not pass health check within 60s." >&2
    curl -fsS "${HEALTH_URL}" >&2 || true
    exit 1
  fi
  sleep 2
done
log "DetecDiv Hub API health check passed."
