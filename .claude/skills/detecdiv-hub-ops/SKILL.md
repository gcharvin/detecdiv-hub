# detecdiv-hub deployment

Automated deployment of detecdiv-hub API changes to webserver-labo. Handles static assets (JS, CSS, HTML) and API code changes.

## When to use

- After committing changes to `api/static/` or `api/` (non-worker code)
- When testing UI fixes, API route changes, schema updates
- Does NOT restart workers — use worker restart for `project_indexing.py` changes

## What it does

1. Validates that changes are committed locally
2. Pushes to GitLab (`gitlab/master`)
3. Copies changed files to webserver-labo via SCP
4. Rebuilds Docker containers (`docker-compose up -d --build`)
5. Verifies health check passes
6. Reports new container fingerprint

## Prerequisites

- All changes must be committed and pushed to `gitlab/master`
- SSH access to webserver-labo (via `gitlab-webserver`)
- No active indexing jobs (or they will be interrupted by container restart)

## Usage

Say **`/detecdiv-hub-ops`** in chat, or:

```
claude: Deploy detecdiv-hub changes
```

The skill will:
- Detect what's changed since last deployment
- Classify as API-only or worker-affecting
- Execute the appropriate deployment strategy
- Verify the new version is live

## Troubleshooting

**"Health check failed"**
- SSH into webserver-labo: `ssh gitlab-webserver`
- Check logs: `cd /home/charvin-admin/repos/detecdiv-hub-webvm/ops/compose/webserver-labo && docker-compose logs detecdiv-hub-api`
- Restart: `docker-compose down && docker-compose up -d --build`

**"Connection refused"**
- Verify SSH key is configured for `gitlab-webserver` in `~/.ssh/config`
- Check webserver-labo network: `ping detecdiv-hub.detecdiv.internal`

## Related

- **Worker restart**: For changes to `api/services/project_indexing.py` or other worker code
- **Database migration**: Run `alembic upgrade head` when schema changes are deployed
