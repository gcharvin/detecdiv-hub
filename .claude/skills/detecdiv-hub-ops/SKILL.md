---
name: detecdiv-hub-ops
description: Deploy, restart, and verify DetecDiv Hub. Use when working on the split deployment where webserver-labo runs the API and PostgreSQL in Docker, detecdiv-server runs workers under systemd, or when deciding which restart and validation command applies after hub changes.
---

# DetecDiv Hub Ops

The full deployment playbook is in `docs/ops-deploy.md` in this repository.
Read that file before taking any deployment action.

For the live topology (hosts, services, URLs), read `CURRENT_DEPLOYMENT.md` first.
