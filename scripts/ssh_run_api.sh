#!/usr/bin/env bash
set -euo pipefail

cd "${HOME}/repos/detecdiv-hub"
git pull --ff-only
source .venv/bin/activate
exec uvicorn api.app:app --host 0.0.0.0 --port 8000

