#!/usr/bin/env bash
set -euo pipefail

exec journalctl -u detecdiv-api -u detecdiv-worker -f

