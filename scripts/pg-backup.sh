#!/usr/bin/env bash
# pg-backup.sh — nightly logical backup of the production Postgres.
#
# Runs on the HOST (cron), not in compose: the postgres container already
# mounts ./backups:/backups, so pg_dump writes inside the container and the
# file lands in $COMPOSE_DIR/backups on the host. Custom format (-Fc) so a
# restore can be selective and parallel.
#
# What this does NOT cover, deliberately: BRIDGE_KEK and the rest of .env
# live outside Postgres. A database backup without the KEK restores
# ciphertext nobody can open — DEPLOY.md "Backup and restore" says where the
# KEK copy must live. This script only refuses to let that be forgotten
# silently: it warns (to stderr and the log) if .env has no offsite marker.
#
# Cron example (02:17 daily, as the user that owns /opt/tidepool):
#   17 2 * * * /opt/tidepool/scripts/pg-backup.sh >> /opt/tidepool/backups/backup.log 2>&1

set -euo pipefail

COMPOSE_DIR="${COMPOSE_DIR:-/opt/tidepool}"
CONTAINER="${CONTAINER:-tidepool-prod-postgres}"
DB_USER="${POSTGRES_USER:-tidepool}"
DB_NAME="${POSTGRES_DB:-tidepool}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"

STAMP="$(date +%Y%m%d-%H%M%S)"
DUMP="tidepool-${STAMP}.dump"

echo "[$(date -u +%FT%TZ)] backup starting: ${DUMP}"

# Dump inside the container onto the shared mount. --no-owner/--no-acl so a
# drill restore into a scratch container needs no matching roles.
docker exec "${CONTAINER}" pg_dump -Fc --no-owner --no-acl \
    -U "${DB_USER}" -d "${DB_NAME}" -f "/backups/${DUMP}.partial"

# Verify the archive is readable BEFORE it gets the real name: a dump that
# pg_restore cannot list is not a backup, and finding that out during an
# incident is the failure mode this file exists to prevent.
docker exec "${CONTAINER}" pg_restore --list "/backups/${DUMP}.partial" > /dev/null
docker exec "${CONTAINER}" mv "/backups/${DUMP}.partial" "/backups/${DUMP}"

SIZE="$(du -h "${COMPOSE_DIR}/backups/${DUMP}" | cut -f1)"
echo "[$(date -u +%FT%TZ)] backup verified: ${DUMP} (${SIZE})"

# Retention: age out old dumps, never the log, never partials younger than a
# day (a partial that old is a failed run worth seeing, not rotating).
find "${COMPOSE_DIR}/backups" -name 'tidepool-*.dump' -mtime "+${RETENTION_DAYS}" -delete
find "${COMPOSE_DIR}/backups" -name 'tidepool-*.dump.partial' -mtime +1 -print | while read -r stale; do
    echo "[$(date -u +%FT%TZ)] WARNING: stale partial from a failed run: ${stale}" >&2
done

# The KEK reminder. touch backups/.env-backed-up after each offsite copy of
# .env; the warning fires when .env is newer than the marker.
ENV_FILE="${COMPOSE_DIR}/.env"
MARKER="${COMPOSE_DIR}/backups/.env-backed-up"
if [[ -f "${ENV_FILE}" && ( ! -f "${MARKER}" || "${ENV_FILE}" -nt "${MARKER}" ) ]]; then
    echo "[$(date -u +%FT%TZ)] WARNING: .env (BRIDGE_KEK) changed since its last recorded offsite copy — this database backup is ciphertext without it. See DEPLOY.md 'Backup and restore'." >&2
fi

echo "[$(date -u +%FT%TZ)] backup done"
