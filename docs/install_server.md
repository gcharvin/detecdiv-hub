# Server Installation

## Target

This guide installs `detecdiv-hub` on the final Linux server that will host:

- PostgreSQL
- the FastAPI service
- the worker service
- the browser UI served at `/web/`
- direct project indexing on canonical server paths

Assumptions:

- Debian or Ubuntu-like Linux
- sudo access
- the final DetecDiv storage is mounted on the server
- the API will run behind either plain HTTP on the LAN or an Nginx reverse proxy

## 1. Create the service account and directories

```bash
sudo useradd --system --create-home --home-dir /srv/detecdiv --shell /bin/bash detecdiv
sudo mkdir -p /srv/detecdiv
sudo chown -R detecdiv:detecdiv /srv/detecdiv
sudo mkdir -p /etc/detecdiv-hub
sudo chown root:detecdiv /etc/detecdiv-hub
sudo chmod 750 /etc/detecdiv-hub
```

## 2. Install OS packages

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip postgresql postgresql-contrib nginx
```

If the worker will later launch MATLAB, install MATLAB separately and confirm the
binary is visible for the `detecdiv` user.

## 3. Clone the repository

```bash
sudo -u detecdiv -H bash -lc '
cd /srv/detecdiv
git clone https://github.com/gcharvin/detecdiv-hub.git
cd detecdiv-hub
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]
'
```

## 4. Create PostgreSQL database and role

```bash
sudo -u postgres psql
```

Inside `psql`:

```sql
CREATE USER detecdiv WITH PASSWORD 'change_me';
CREATE DATABASE detecdiv_hub OWNER detecdiv;
\q
```

## 5. Create the hub environment file

Copy [`.env.example`](C:/Users/charvin/Documents/MATLAB/detecdiv-hub/.env.example) to the server:

```bash
sudo cp /srv/detecdiv/detecdiv-hub/.env.example /etc/detecdiv-hub/detecdiv-hub.env
sudo chown root:detecdiv /etc/detecdiv-hub/detecdiv-hub.env
sudo chmod 640 /etc/detecdiv-hub/detecdiv-hub.env
sudo nano /etc/detecdiv-hub/detecdiv-hub.env
```

At minimum, set:

```text
DETECDIV_HUB_ENVIRONMENT=prod
DETECDIV_HUB_DATABASE_URL=postgresql+psycopg://detecdiv:change_me@localhost:5432/detecdiv_hub
DETECDIV_HUB_API_HOST=127.0.0.1
DETECDIV_HUB_API_PORT=8000
DETECDIV_HUB_LOG_LEVEL=INFO
DETECDIV_HUB_DEFAULT_USER_KEY=localdev
DETECDIV_HUB_AUTO_PROVISION_USERS=true
DETECDIV_HUB_MATLAB_COMMAND=/usr/local/MATLAB/R2025a/bin/matlab
DETECDIV_HUB_MATLAB_REPO_ROOT=/srv/detecdiv/DetecDiv
```

## 6. Bootstrap the schema

```bash
sudo -u detecdiv -H bash -lc '
cd /srv/detecdiv/detecdiv-hub
set -a
. /etc/detecdiv-hub/detecdiv-hub.env
set +a
. .venv/bin/activate
python scripts/bootstrap_db.py
'
```

Optional smoke seed:

```bash
sudo -u detecdiv -H bash -lc '
cd /srv/detecdiv/detecdiv-hub
set -a
. /etc/detecdiv-hub/detecdiv-hub.env
set +a
. .venv/bin/activate
python scripts/seed_demo.py
'
```

## 7. Install the systemd services

```bash
sudo bash /srv/detecdiv/detecdiv-hub/scripts/install_systemd.sh \
  --repo-root /srv/detecdiv/detecdiv-hub \
  --service-user detecdiv \
  --env-file /etc/detecdiv-hub/detecdiv-hub.env \
  --api-host 127.0.0.1 \
  --api-port 8000
```

Check status:

```bash
sudo systemctl status detecdiv-api --no-pager
sudo systemctl status detecdiv-worker --no-pager
```

Tail logs:

```bash
sudo journalctl -u detecdiv-api -u detecdiv-worker -f
```

If you are deploying under a different Linux user or a different repo path,
adjust `--service-user` and `--repo-root` accordingly. The service templates in
[`ops/systemd`](C:/Users/charvin/Documents/MATLAB/detecdiv-hub/ops/systemd) are
examples; the install script generates concrete units for the target machine.

## 8. Optional: put Nginx in front

Copy [detecdiv-hub.conf](C:/Users/charvin/Documents/MATLAB/detecdiv-hub/ops/nginx/detecdiv-hub.conf):

```bash
sudo cp /srv/detecdiv/detecdiv-hub/ops/nginx/detecdiv-hub.conf /etc/nginx/sites-available/detecdiv-hub.conf
sudo ln -s /etc/nginx/sites-available/detecdiv-hub.conf /etc/nginx/sites-enabled/detecdiv-hub.conf
sudo nginx -t
sudo systemctl reload nginx
```

Adjust `server_name` before enabling it.

## 9. First real index

Run indexing on the canonical server path that contains the DetecDiv projects:

```bash
sudo -u detecdiv -H bash -lc '
cd /srv/detecdiv/detecdiv-hub
set -a
. /etc/detecdiv-hub/detecdiv-hub.env
set +a
. .venv/bin/activate
python scripts/index_project_root.py /srv/detecdiv/projects --host-scope server --owner-user-key localdev --visibility private
'
```

Or via the API:

```bash
curl -X POST http://127.0.0.1:8000/indexing?user_key=localdev \
  -H "Content-Type: application/json" \
  -d '{"source_kind":"project_root","source_path":"/srv/detecdiv/projects","host_scope":"server","visibility":"private"}'
```

## 10. Sanity checks

```bash
curl http://127.0.0.1:8000/health
curl "http://127.0.0.1:8000/users/me?user_key=localdev"
curl "http://127.0.0.1:8000/dashboard/summary?user_key=localdev"
curl "http://127.0.0.1:8000/projects?user_key=localdev"
```

Open the browser UI:

```text
http://SERVER_OR_HOSTNAME:8000/web/
```

or behind Nginx:

```text
http://SERVER_OR_HOSTNAME/web/
```

## 11. Update procedure

```bash
sudo -u detecdiv -H bash -lc '
cd /srv/detecdiv/detecdiv-hub
git pull
. .venv/bin/activate
pip install -e .[dev]
'

sudo systemctl restart detecdiv-api detecdiv-worker
```
