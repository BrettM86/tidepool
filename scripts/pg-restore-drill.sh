#!/usr/bin/env bash
# pg-restore-drill.sh — prove a dump restores, into a THROWAWAY container.
#
# An unverified backup is a claim, not a capability (DEPLOY.md). This drill
# restores the newest dump (or $1) into a scratch postgres:16 container on a
# random free port, checks the schema version and the irreplaceable tables,
# prints the inventory, and tears the container down. It never touches the
# production container, volume, or network.
#
# The drill validates DATA PRESENCE, not KEK correctness: sealed blobs are
# checked non-empty, not opened. Opening them requires BRIDGE_KEK, which a
# restore drill must not need to read. DEPLOY.md's runbook pairs this drill
# with the boot canary for the full proof.
#
# Usage: pg-restore-drill.sh [path-to-dump]   (defaults to newest in backups/)

set -euo pipefail

COMPOSE_DIR="${COMPOSE_DIR:-/opt/tidepool}"
BACKUPS_DIR="${BACKUPS_DIR:-${COMPOSE_DIR}/backups}"
DRILL_NAME="tidepool-restore-drill-$$"

DUMP="${1:-$(ls -1t "${BACKUPS_DIR}"/tidepool-*.dump 2>/dev/null | head -1 || true)}"
if [[ -z "${DUMP}" || ! -f "${DUMP}" ]]; then
    echo "no dump found in ${BACKUPS_DIR} and none given — run pg-backup.sh first" >&2
    exit 1
fi
echo "drilling restore of: ${DUMP}"

cleanup() { docker rm -f "${DRILL_NAME}" > /dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "${DRILL_NAME}" \
    -e POSTGRES_USER=drill -e POSTGRES_PASSWORD=drill -e POSTGRES_DB=drill \
    postgres:16 > /dev/null

for _ in $(seq 1 30); do
    docker exec "${DRILL_NAME}" pg_isready -U drill -d drill > /dev/null 2>&1 && break
    sleep 1
done
docker exec "${DRILL_NAME}" pg_isready -U drill -d drill > /dev/null

docker cp "${DUMP}" "${DRILL_NAME}:/tmp/drill.dump"
# --no-owner/--no-acl mirrors how the dump was taken; -e fails the drill on
# the first real error instead of burying it in a warning tally.
docker exec "${DRILL_NAME}" pg_restore -e --no-owner --no-acl \
    -U drill -d drill /tmp/drill.dump

psql_scalar() {
    docker exec "${DRILL_NAME}" psql -U drill -d drill -tA -c "$1"
}

VERSION="$(psql_scalar 'SELECT max(version_id) FROM goose_db_version')"
echo "schema at goose migration: ${VERSION}"
[[ -n "${VERSION}" && "${VERSION}" != "0" ]] || { echo "DRILL FAILED: no migration state" >&2; exit 1; }

echo "irreplaceable-table inventory (see DEPLOY.md for what each loses):"
FAILED=0
check() { # table, human name, min expected rows (0 allows empty-but-present)
    local count
    if ! count="$(psql_scalar "SELECT count(*) FROM $1")"; then
        echo "  $1: MISSING — ${2} not restored" >&2; FAILED=1; return
    fi
    printf '  %-18s %8s rows  (%s)\n' "$1" "${count}" "$2"
    if [[ "${count}" -lt "$3" ]]; then
        echo "DRILL FAILED: $1 restored empty but production requires rows (${2})" >&2
        FAILED=1
    fi
}
check bridged_actors  "bridged identities + escrowed keys" 0
check service_keys    "PLC escrow + service actor keys"    1
check ap_actors       "native users' AP signing keys"      0
check blocks          "repo blocks"                        0
check repo_state      "repo head commits"                  0
check blobs           "media blobs"                        0
check outbound_deliveries "in-flight federation state"     0

SEALED_NULL="$(psql_scalar "SELECT count(*) FROM service_keys WHERE name = 'plc-rotation' AND (key_material IS NULL OR length(key_material) = 0)")"
if [[ "${SEALED_NULL}" != "0" ]]; then
    echo "DRILL FAILED: plc-rotation row present but its sealed material is empty" >&2
    FAILED=1
fi

[[ "${FAILED}" == "0" ]] || exit 1
echo "DRILL PASSED: ${DUMP} restores and holds the irreplaceable tables"
