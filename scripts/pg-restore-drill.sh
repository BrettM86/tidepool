#!/usr/bin/env bash
# pg-restore-drill.sh — prove a dump restores, into a THROWAWAY container.
#
# An unverified backup is a claim, not a capability (DEPLOY.md). This drill
# restores the newest dump (or $1) into a scratch postgres:16 container,
# checks the schema version and the irreplaceable tables, prints the
# inventory, and tears the container down. It never touches the production
# container, volume, or network.
#
# The scratch container has NO published ports and runs on `--network none`:
# it is reached only via `docker exec`. That is deliberate — for the length of
# the drill this container holds the entire production dataset behind a
# one-word password, and it needs no network at all to be restored into.
#
# The drill validates DATA PRESENCE, not KEK correctness: it proves the sealed
# columns hold bytes (present, and not zero-length), never that those bytes
# open. Opening them requires BRIDGE_KEK, which a restore drill must not need
# to read. DEPLOY.md's runbook pairs this drill with the boot canary for the
# full proof.
#
# Usage: pg-restore-drill.sh [path-to-dump]   (defaults to newest in backups/)

set -euo pipefail

COMPOSE_DIR="${COMPOSE_DIR:-/opt/tidepool}"
BACKUPS_DIR="${BACKUPS_DIR:-${COMPOSE_DIR}/backups}"
DRILL_NAME="tidepool-restore-drill-$$"

# The script normally lives in the repo checkout on the prod host, so the
# migrations it should have been dumped at are a relative path away. When it
# has been copied somewhere else the gate downgrades to a warning (below).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-${SCRIPT_DIR}/../internal/db/migrations}"

DUMP="${1:-$(ls -1t "${BACKUPS_DIR}"/tidepool-*.dump 2>/dev/null | head -1 || true)}"
if [[ -z "${DUMP}" || ! -f "${DUMP}" ]]; then
    echo "no dump found in ${BACKUPS_DIR} and none given — run pg-backup.sh first" >&2
    exit 1
fi
echo "drilling restore of: ${DUMP}"

cleanup() {
    docker rm -f "${DRILL_NAME}" > /dev/null 2>&1 ||
        echo "WARNING: could not remove drill container ${DRILL_NAME} — it may still hold a copy of production data; remove it by hand" >&2
}
trap cleanup EXIT

# --network none: the drill needs no network, and this keeps the restored
# production dataset off the default bridge for the minutes it exists.
docker run -d --name "${DRILL_NAME}" --network none \
    -e POSTGRES_USER=drill -e POSTGRES_PASSWORD=drill -e POSTGRES_DB=drill \
    postgres:16 > /dev/null

# -h 127.0.0.1 on BOTH calls, deliberately: the postgres image's init runs a
# bootstrap server on the unix socket before the real one starts, so a
# socket-based pg_isready can report ready against a server that is about to
# go away and take the drill's first psql with it.
READY=0
for _ in $(seq 1 30); do
    if docker exec "${DRILL_NAME}" pg_isready -h 127.0.0.1 -U drill -d drill > /dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 1
done
if [[ "${READY}" != "1" ]] || ! docker exec "${DRILL_NAME}" pg_isready -h 127.0.0.1 -U drill -d drill > /dev/null 2>&1; then
    echo "DRILL FAILED: the scratch postgres never became ready (30s) — the drill proved nothing about ${DUMP}" >&2
    exit 1
fi

docker cp "${DUMP}" "${DRILL_NAME}:/tmp/drill.dump"
# --no-owner/--no-acl mirrors how the dump was taken; -e fails the drill on
# the first real error instead of burying it in a warning tally.
docker exec "${DRILL_NAME}" pg_restore -e --no-owner --no-acl \
    -U drill -d drill /tmp/drill.dump

psql_scalar() {
    docker exec "${DRILL_NAME}" psql -U drill -d drill -tA -c "$1"
}

if ! VERSION="$(psql_scalar 'SELECT max(version_id) FROM goose_db_version' 2>/dev/null)"; then
    echo "DRILL FAILED: no goose_db_version table in the restored database — this dump carries no migration state" >&2
    exit 1
fi
echo "schema at goose migration: ${VERSION:-<none>}"
[[ -n "${VERSION}" && "${VERSION}" != "0" ]] || { echo "DRILL FAILED: no migration state" >&2; exit 1; }

# Version gate: a dump can restore cleanly and still be USELESS if it predates
# the migrations this code needs. The expectation comes from the checkout the
# script sits in, so it moves with the code instead of being a pinned number.
EXPECTED_VERSION=0
if [[ -d "${MIGRATIONS_DIR}" ]]; then
    for migration in "${MIGRATIONS_DIR}"/*.sql; do
        [[ -e "${migration}" ]] || continue
        number="$(basename "${migration}")"
        number="${number%%_*}"
        [[ "${number}" =~ ^[0-9]+$ ]] || continue
        number=$((10#${number}))
        if (( number > EXPECTED_VERSION )); then
            EXPECTED_VERSION=${number}
        fi
    done
fi
if (( EXPECTED_VERSION > 0 )); then
    if (( VERSION < EXPECTED_VERSION )); then
        echo "DRILL FAILED: stale dump — restored schema is at goose ${VERSION} but ${MIGRATIONS_DIR} goes to ${EXPECTED_VERSION}; this dump predates migrations the running code requires" >&2
        exit 1
    fi
    echo "migration gate: ${VERSION} >= ${EXPECTED_VERSION} (from ${MIGRATIONS_DIR})"
else
    echo "WARNING: no migrations found at ${MIGRATIONS_DIR} — cannot tell a stale dump from a current one; falling back to the any-nonzero check" >&2
fi

echo "irreplaceable-table inventory (see DEPLOY.md for what each loses):"
FAILED=0
check() { # table, human name, min expected rows (0 allows empty-but-present)
    local count
    if ! count="$(psql_scalar "SELECT count(*) FROM $1")"; then
        echo "  $1: MISSING — ${2} not restored" >&2; FAILED=1; return
    fi
    printf '  %-20s %8s rows  (%s)\n' "$1" "${count}" "$2"
    if [[ "${count}" -lt "$3" ]]; then
        echo "DRILL FAILED: $1 restored empty but production requires rows (${2})" >&2
        FAILED=1
    fi
}
check bridged_actors       "bridged identities + escrowed keys" 0
check service_keys         "PLC escrow + service actor keys"    1
check ap_actors            "native users' AP signing keys"      0
check blocks               "repo blocks"                        0
check repo_state           "repo head commits"                  0
check blobs                "media blobs"                        0
check outbound_deliveries  "in-flight federation state"         0
check outbound_activities  "the delivery ledger"                0
check outbound_objects     "acceptance/publication state"       0
check outbound_votes       "outbound vote state"                0
check admissions           "admission decisions"                0

# THE escrow-key gate. A plc-rotation row that is MISSING is the dangerous
# case, not a present-but-empty one: LoadOrCreateRotationKey is
# create-on-absence, so a bridge booted onto a restore without this row mints
# a FRESH escrow key and comes up green while every already-minted did:plc
# document still names the old one (FOLLOWUPS.md). Absence is only legitimate
# when nothing has been bridged yet, so bridged_actors is the discriminator.
#
# scalar_or_die frames a read failure here as a drill failure rather than
# letting bare psql noise stand in for one: these five queries are the gates,
# and "the query errored" must never be mistaken for "the gate passed". The
# exit inside the substitution ends the subshell; set -e then ends the drill on
# the failed assignment, after the message has been printed.
scalar_or_die() { # query, what it reads
    local out
    if ! out="$(psql_scalar "$1" 2>/dev/null)"; then
        echo "DRILL FAILED: could not read $2 from the restored database" >&2
        exit 1
    fi
    printf '%s' "${out}"
}
BRIDGED_COUNT="$(scalar_or_die 'SELECT count(*) FROM bridged_actors' 'the bridged_actors count')"
PLC_ROWS="$(scalar_or_die "SELECT count(*) FROM service_keys WHERE name = 'plc-rotation'" 'the plc-rotation row count')"
PLC_SEALED="$(scalar_or_die "SELECT count(*) FROM service_keys WHERE name = 'plc-rotation' AND key_material IS NOT NULL AND length(key_material) > 0" 'the plc-rotation sealed-material count')"
if [[ "${PLC_ROWS}" != "${PLC_SEALED}" ]]; then
    echo "DRILL FAILED: plc-rotation row present but its sealed material is empty" >&2
    FAILED=1
elif [[ "${BRIDGED_COUNT}" != "0" ]]; then
    if [[ "${PLC_SEALED}" != "1" ]]; then
        echo "DRILL FAILED: ${BRIDGED_COUNT} bridged actors restored but ${PLC_SEALED} usable plc-rotation rows (expected exactly 1) — this restore would mint a fresh escrow key and orphan every bridged DID" >&2
        FAILED=1
    fi
elif [[ "${PLC_SEALED}" == "0" ]]; then
    echo "  note: no plc-rotation row, and no bridged actors either — consistent with a dump taken before anything was bridged, tolerated"
fi

# Zero-length sealed blobs are corruption, not emptiness: something wrote a
# column that can only ever hold ciphertext with nothing in it.
EMPTY_SIGNING="$(scalar_or_die 'SELECT count(*) FROM bridged_actors WHERE signing_key IS NOT NULL AND length(signing_key) = 0' 'zero-length bridged_actors.signing_key blobs')"
if [[ "${EMPTY_SIGNING}" != "0" ]]; then
    echo "DRILL FAILED: ${EMPTY_SIGNING} bridged_actors rows carry a zero-length signing_key — sealed material was truncated, not absent" >&2
    FAILED=1
fi
EMPTY_RSA="$(scalar_or_die 'SELECT count(*) FROM ap_actors WHERE rsa_key_sealed IS NOT NULL AND length(rsa_key_sealed) = 0' 'zero-length ap_actors.rsa_key_sealed blobs')"
if [[ "${EMPTY_RSA}" != "0" ]]; then
    echo "DRILL FAILED: ${EMPTY_RSA} ap_actors rows carry a zero-length rsa_key_sealed — sealed material was truncated, not absent" >&2
    FAILED=1
fi

[[ "${FAILED}" == "0" ]] || exit 1
echo "DRILL PASSED: ${DUMP} restores and holds the irreplaceable tables"
