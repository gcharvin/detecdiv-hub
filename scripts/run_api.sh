#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${DETECDIV_HUB_ENV_FILE:-$REPO_ROOT/.env}"

cd "$REPO_ROOT"
. .venv/bin/activate

if [[ -f "$ENV_FILE" ]]; then
  set -a
  . "$ENV_FILE"
  set +a
fi

HOST="${DETECDIV_HUB_API_HOST:-127.0.0.1}"
PORT="${DETECDIV_HUB_API_PORT:-8000}"

exec python -m uvicorn api.app:app --host "$HOST" --port "$PORT"
