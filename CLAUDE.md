# detecdiv-hub

Control plane FastAPI/PostgreSQL pour le moteur MATLAB DetecDiv.

## Quick Start

### Deployment

After committing changes, deploy to webserver-labo:

```
/detecdiv-hub-ops
```

This handles:
- **API-only changes** (routes, schemas, static assets): Rebuilds Docker container
- **Worker code changes** (project_indexing.py, etc.): ⚠️ Also requires manual worker restart on detecdiv-server

### Worker Restart

For changes to `api/services/` (non-HTTP code), restart workers manually on detecdiv-server:

```bash
ssh detecdiv-server
sudo systemctl restart detecdiv-worker@{1,2,3}
```

## Architecture

**webserver-labo** (Docker)
- FastAPI app + PostgreSQL
- Port 8000 (internal), proxied via `/web/` on detecdiv-hub.detecdiv.internal
- Static files served from `api/static/`

**detecdiv-server** (systemd)
- MATLAB workers (`detecdiv-worker@1,2,3`)
- Filesystem access to `/data/`
- Project/raw dataset indexing, preview generation

## Key Files

### API Routes
- `api/routes_projects.py` — Project catalog, filtering, permissions
- `api/routes_raw_datasets.py` — Raw dataset catalog, archival
- `api/routes_user_admin.py` — User account management

### Services
- `api/services/project_indexing.py` — Raw dataset discovery, MM parent detection
- `api/services/archival.py` — Archive/restore job orchestration
- `api/services/backup.py` — restic wrapper (backup, restore, repo init)
- `api/services/backup_settings.py` — Backup config read/write (`system_settings` table)

### Backup & Restore
- `api/routes_backup.py` — HTTP endpoints (backup-now, snapshots, restore, file browser)
- `worker/backup_executor.py` — Job executors (backup, restore, list_snapshot_dir)
- `worker/backup_scheduler.py` — Periodic backup trigger

### Frontend
- `api/static/app.js` — Main application logic (browser/UI)
- `api/static/styles.css` — Global styles
- `api/static/{index,raw-datasets,admin-users,...}.html` — Page templates

## Common Tasks

### Add a new project-level stat to the admin dashboard

1. Add column to database schema (`models.py`)
2. Update `Project` model and migration
3. Add compute logic to `routes_projects.py`
4. Expose in `GET /projects` response
5. Render in `app.js` `renderProjects()`
6. Deploy: `/detecdiv-hub-ops`

### Fix raw dataset indexing

Edit `api/services/project_indexing.py`:
- `looks_like_raw_dataset_dir()` — Heuristic for detecting acquisitions
- `_MICROMANAGER_MARKER_FILES` — Marker files (MM parent detection)
- `is_position_like_name()` — Position subfolder naming

Then:
1. Test locally if possible
2. Commit changes
3. Deploy API code first (no worker restart yet)
4. SSH to detecdiv-server and restart workers
5. Manually trigger re-indexing job in `/web/raw-ops.html`

### Add a UI feature to the projects page

1. Update `api/static/index.html` (add form field, table column, etc.)
2. Update `api/static/app.js` (add event listeners, state management, rendering)
3. Update `api/static/styles.css` if needed
4. Bump cache-buster version in HTML file
5. Deploy: `/detecdiv-hub-ops`

## Backup System

### Architecture

- **restic** est le moteur de backup (déduplication, snapshots).
- Le **repo** (`/archive/detecdiv-backup`) est sur un stockage accessible depuis `detecdiv-server`.
- Le **FUSE mount** (`/mnt/restic-mount`) expose les snapshots comme un filesystem navigable.
  Service systemd : `detecdiv-restic-mount.service` sur `detecdiv-server`.

### Configuration

Via **Admin → Backup** dans l'UI (stocké dans `system_settings` sur chaque DB) :
- `backup_repo` — chemin du repo restic (ex. `/archive/detecdiv-backup`)
- `backup_passphrase` — passphrase restic
- `backup_mount_path` — point de montage FUSE (ex. `/mnt/restic-mount`)
- `backup_enabled` — active le scheduler automatique

> ⚠️ Les deux bases PostgreSQL (webserver-labo et detecdiv-server) sont **indépendantes**.
> Toute modification via l'UI met à jour webserver-labo. Pour que les workers voient les
> changements, il faut également les appliquer sur detecdiv-server (INSERT/UPDATE dans
> `system_settings` en SQL ou via une session psql locale).

### Scope du backup par type de projet

| Type | Détection | Contenu sauvegardé |
|------|-----------|-------------------|
| **Modern** | `{stem}/` existe à côté du `.mat` | `.mat` + `{stem}/` uniquement (multi-path restic) |
| **Legacy** | `.mat` = `{name}-project.mat` dans le dir projet | tout le dir projet sauf `{name}-pos*` (raw datasets) |
| **Raw dataset** | — | dossier complet de l'acquisition |

### Snapshots en DB

Après chaque backup réussi, le worker insère dans `backup_snapshots` (avec FK vers le projet
ou raw dataset). L'API liste les snapshots depuis cette table sans appeler restic.

### File browser

Le bouton **Browse** sur un snapshot ouvre un file browser qui :
1. Soumet un job `list_snapshot_dir` au worker
2. Le worker fait `os.listdir()` sur `{mount_path}/ids/{snapshot_id[:8]}/{source_path}/`
3. L'UI poll le job et affiche l'arborescence avec navigation ↑ Up et restore sélectif

### Restauration

- **Full restore** → target `/` restaure en place (recrée les chemins d'origine).
- **Restore sélectif** → cocher des fichiers/dossiers dans le file browser, puis target `/`.
- restic recrée le chemin absolu complet sous le target dir.

### Opérations manuelles utiles

```bash
# Vérifier le service FUSE mount
systemctl status detecdiv-restic-mount.service
ls /mnt/restic-mount/ids/

# Lister les snapshots restic
restic -r /archive/detecdiv-backup snapshots

# Initialiser le repo manuellement (si pas fait via l'UI)
restic -r /archive/detecdiv-backup init
```

## Deployment Status

Last deployment: See `git log --oneline` for recent commits, or check `/web/health` for live version.

Active workers: Check `/web/raw-ops.html` → "Indexing Jobs" panel for live job count.

Database schema: Check `alembic_version` table or run `alembic current` on webserver-labo.

## Skills

- **detecdiv-hub-ops** — Deploy API changes to webserver-labo
- **gitlab-labo** — Push commits to internal GitLab (use when detecdiv-hub-ops fails to push)
