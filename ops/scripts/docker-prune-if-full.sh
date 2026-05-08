#!/bin/bash
# Prune unused Docker images if root disk usage exceeds threshold.
# Installed on webserver-labo as a daily cron job (3am):
#   0 3 * * * /home/charvin-admin/docker-prune-if-full.sh >> /home/charvin-admin/docker-prune.log 2>&1
THRESHOLD=70
USAGE=$(df / | awk "NR==2 {print \$5}" | tr -d '%')
if [ "$USAGE" -ge "$THRESHOLD" ]; then
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Disk at ${USAGE}% (>= ${THRESHOLD}%), pruning Docker images..."
    docker image prune -af
    USAGE_AFTER=$(df / | awk "NR==2 {print \$5}" | tr -d '%')
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Done. Disk now at ${USAGE_AFTER}%."
else
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Disk at ${USAGE}% (< ${THRESHOLD}%), nothing to do."
fi
