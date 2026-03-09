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

exec python worker/run_worker.py
