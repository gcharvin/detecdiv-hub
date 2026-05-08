#!/bin/bash
# Nightly PostgreSQL backup to /data/webserver-labo/db-backups/.
# Installed on webserver-labo as a daily cron job (2am):
#   0 2 * * * /home/charvin-admin/backup-db.sh >> /home/charvin-admin/backup-db.log 2>&1
BACKUP_DIR="/data/webserver-labo/db-backups"
DB_CONTAINER="detecdiv-hub-postgres"
DB_USER="detecdiv"
DB_NAME="detecdiv_hub"
RETAIN_DAYS=30

TIMESTAMP=$(date -u +%Y-%m-%dT%H%M%SZ)
OUTFILE="${BACKUP_DIR}/detecdiv_hub_${TIMESTAMP}.sql.gz"

echo "[${TIMESTAMP}] Starting backup → ${OUTFILE}"

docker exec "$DB_CONTAINER" pg_dump -U "$DB_USER" "$DB_NAME" | gzip > "$OUTFILE"

if [ $? -eq 0 ]; then
    SIZE=$(du -sh "$OUTFILE" | cut -f1)
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Backup OK (${SIZE}): ${OUTFILE}"
else
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] ERROR: pg_dump failed, removing partial file."
    rm -f "$OUTFILE"
    exit 1
fi

# Rotate: delete backups older than RETAIN_DAYS
DELETED=$(find "$BACKUP_DIR" -name "detecdiv_hub_*.sql.gz" -mtime "+${RETAIN_DAYS}" -print -delete | wc -l)
if [ "$DELETED" -gt 0 ]; then
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Rotated ${DELETED} backup(s) older than ${RETAIN_DAYS} days."
fi
