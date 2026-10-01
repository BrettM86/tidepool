#!/usr/bin/env bash
# pg-backup.sh — nightly logical backup of the production Postgres.
#
# Runs on the HOST (systemd timer), not in compose: the postgres container already
# mounts ./backups:/backups, so pg_dump writes inside the container and the
# file lands in $COMPOSE_DIR/backups on the host. Custom format (-Fc) so a
# restore can be selective and parallel.
#
# What this does NOT cover, deliberately: BRIDGE_KEK and the rest of .env
# live outside Postgres. A database backup without the KEK restores
# ciphertext nobody can open — DEPLOY.md "Backup and restore" says where the
# KEK copy must live. This script only refuses to let that be forgotten
# silently: it warns (to stderr, i.e. the journal) if .env has no offsite marker.
#
# That warning fires on ANY change to .env, not only a KEK change, and that is
# deliberate — do not "fix" it into comparing KEK values. The script cannot
# read the old KEK to compare against (it is not stored anywhere it can see),
# and a mtime is the only evidence available. A few false alarms after an
# unrelated env edit is the correct price for never missing the one edit that
# makes every dump on this host unopenable.
#
# Scheduled by scripts/systemd/tidepool-pg-backup.timer (02:17 UTC daily, as
# root); output goes to the journal. The unit runs a root-owned copy installed
# at /usr/local/sbin/tidepool-pg-backup, not this file, so re-install after
# editing it. Nothing here depends on the script's own location: paths come
# from COMPOSE_DIR. Install steps: DEPLOY.md "Backup and restore".

set -euo pipefail

COMPOSE_DIR="${COMPOSE_DIR:-/opt/tidepool}"
CONTAINER="${CONTAINER:-tidepool-prod-postgres}"
DB_USER="${POSTGRES_USER:-tidepool}"
DB_NAME="${POSTGRES_DB:-tidepool}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"

STAMP="$(date +%Y%m%d-%H%M%S)"
DUMP="tidepool-${STAMP}.dump"

echo "[$(date -u +%FT%TZ)] backup starting: ${DUMP}"

# A failed run's partial moves to one fixed name, so the timer's retries (up to
# 4 a night) overwrite a single ~2GB corpse instead of stacking timestamped
# ones. The host can mv it: the mount is the same files, and this runs as root.
PARTIAL="${COMPOSE_DIR}/backups/${DUMP}.partial"
LAST_FAILED="${COMPOSE_DIR}/backups/tidepool-last-failed.dump.partial"
COMPLETED=0
keep_last_failed_partial() {
    if [[ "${COMPLETED}" -ne 1 && -f "${PARTIAL}" ]]; then
        mv -f "${PARTIAL}" "${LAST_FAILED}"
        # Same secrets as a completed dump; pg_dump leaves it world-readable.
        chmod 600 "${LAST_FAILED}"
        echo "[$(date -u +%FT%TZ)] FAILED: partial kept as ${LAST_FAILED}" >&2
    fi
}
trap keep_last_failed_partial EXIT
# bash skips the EXIT trap on a signal it has no trap for; systemd stops and
# timeouts send SIGTERM, so turn it into an exit the EXIT trap sees.
trap 'exit 143' TERM
trap 'exit 130' INT

# Dump inside the container onto the shared mount. --no-owner/--no-acl so a
# drill restore into a scratch container needs no matching roles.
docker exec "${CONTAINER}" pg_dump -Fc --no-owner --no-acl \
    -U "${DB_USER}" -d "${DB_NAME}" -f "/backups/${DUMP}.partial"

# Verify the archive is readable BEFORE it gets the real name: a dump that
# pg_restore cannot read end to end is not a backup, and finding that out
# during an incident is the failure mode this file exists to prevent. A full
# read to /dev/null, not --list: --list reads only the TOC, so a dump
# truncated in its data blocks would pass.
docker exec "${CONTAINER}" pg_restore -f /dev/null "/backups/${DUMP}.partial"
docker exec "${CONTAINER}" mv "/backups/${DUMP}.partial" "/backups/${DUMP}"
# 600 inside the container, which is 600 on the host mount: this file holds
# the plaintext service-actor PEM plus every sealed blob in the database, and
# pg_dump's default leaves it world-readable. The directory wants 700 as a
# provisioning step (DEPLOY.md "Backup and restore"); this covers the file.
docker exec "${CONTAINER}" chmod 600 "/backups/${DUMP}"

SIZE="$(du -h "${COMPOSE_DIR}/backups/${DUMP}" | cut -f1)"
echo "[$(date -u +%FT%TZ)] backup verified: ${DUMP} (${SIZE})"
COMPLETED=1

# Retention: age out completed dumps only. Partials are never deleted by age —
# a failed run keeps only its most recent partial, as tidepool-last-failed
# (see the trap above), and the second find only warns about partials old
# enough (>24h) to be certainly dead. -mmin +1440 rather than -mtime +1, which rounds down to whole
# days and would not fire until the partial was nearly two days old.
find "${COMPOSE_DIR}/backups" -name 'tidepool-*.dump' -mtime "+${RETENTION_DAYS}" -delete
find "${COMPOSE_DIR}/backups" -name 'tidepool-*.dump.partial' -mmin +1440 -print | while read -r stale; do
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
