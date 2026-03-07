#!/usr/bin/env bash
set -euo pipefail

cd "${HOME}/repos/detecdiv-hub"
git pull --ff-only
source .venv/bin/activate
exec python worker/run_worker.py

