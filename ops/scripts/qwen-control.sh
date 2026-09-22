#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: DETECDIV_HUB_URL=https://hub.example DETECDIV_HUB_TOKEN=... $0 {status|start|stop}" >&2
}

action="${1:-}"
if [[ "$action" != "status" && "$action" != "start" && "$action" != "stop" ]]; then
  usage
  exit 2
fi

: "${DETECDIV_HUB_URL:?Set DETECDIV_HUB_URL to the Hub API base URL}"
: "${DETECDIV_HUB_TOKEN:?Set DETECDIV_HUB_TOKEN to an administrator session token}"
base_url="${DETECDIV_HUB_URL%/}"
headers=(-H "Authorization: Bearer ${DETECDIV_HUB_TOKEN}" -H "Accept: application/json")

if [[ "$action" == "status" ]]; then
  exec curl --fail-with-body --silent --show-error "${headers[@]}" "${base_url}/assistant/status"
fi

exec curl --fail-with-body --silent --show-error \
  -X POST "${headers[@]}" \
  -H "Content-Type: application/json" \
  --data "{\"action\":\"${action}\"}" \
  "${base_url}/assistant/control"
