#!/usr/bin/env bash
# purge-actor.sh — operator purge of a native (Coves-user) person actor's
# federated content, via POST /admin/actors/purge.
#
# Why this exists: Tidepool is the home instance of every Coves user's AP
# persona, so it is the only server that can send Delete{Person,
# removeData:true} for one. The automatic paths (the user's own deleteRemote
# opt-out, a confirmed account deletion) never fire for a spammer, so an
# operator needs this. DEPLOY.md §3 has the runbook.
#
# Runs ON the production host (ssh in first): it reads ADMIN_TOKEN from
# $COMPOSE_DIR/.env and talks to the loopback admin port. The token is never
# echoed and never placed in argv (curl reads the header from stdin), so it
# does not show up in ps or shell history.
#
# Usage: purge-actor.sh <did> [--execute]
#   Dry run by default: reports the actor and what the purge would reach.
#   --execute performs the purge. IRREVERSIBLE.
set -euo pipefail
# Never trace: the token is read below and sent in a header.
set +x

COMPOSE_DIR="${COMPOSE_DIR:-/opt/tidepool}"
ADMIN_URL="${ADMIN_URL:-http://127.0.0.1:8091}"

usage() {
  echo "usage: $0 <did> [--execute]" >&2
  exit 2
}

[ $# -ge 1 ] && [ $# -le 2 ] || usage
DID="$1"
DRY_RUN=true
if [ $# -eq 2 ]; then
  [ "$2" = "--execute" ] || usage
  DRY_RUN=false
fi

# The DID is interpolated into the JSON body, so it is held to DID syntax
# characters: no quotes or backslashes can reach the body.
if [[ ! "$DID" =~ ^did:[a-z]+:[A-Za-z0-9._:%-]+$ ]]; then
  echo "error: '$DID' is not a DID (expected did:<method>:<id>)" >&2
  exit 2
fi

ENV_FILE="$COMPOSE_DIR/.env"
if [ ! -r "$ENV_FILE" ]; then
  echo "error: cannot read $ENV_FILE (set COMPOSE_DIR, or run with sudo)" >&2
  exit 1
fi
# `|| true`: a missing line must reach the "not set" error below, not abort
# silently under pipefail.
ADMIN_TOKEN="$(grep -E '^ADMIN_TOKEN=' "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true)"
# Strip a CRLF line ending and surrounding whitespace before the quotes.
ADMIN_TOKEN="${ADMIN_TOKEN%$'\r'}"
ADMIN_TOKEN="${ADMIN_TOKEN#"${ADMIN_TOKEN%%[![:space:]]*}"}"
ADMIN_TOKEN="${ADMIN_TOKEN%"${ADMIN_TOKEN##*[![:space:]]}"}"
ADMIN_TOKEN="${ADMIN_TOKEN%\"}"
ADMIN_TOKEN="${ADMIN_TOKEN#\"}"
ADMIN_TOKEN="${ADMIN_TOKEN%\'}"
ADMIN_TOKEN="${ADMIN_TOKEN#\'}"
if [ -z "$ADMIN_TOKEN" ]; then
  echo "error: ADMIN_TOKEN is not set in $ENV_FILE" >&2
  exit 1
fi

if [ "$DRY_RUN" = false ]; then
  echo "WARNING: purging $DID is irreversible: Delete{Person} goes to every peer inbox and the actor is tombstoned forever." >&2
fi

BODY_FILE="$(mktemp)"
trap 'rm -f "$BODY_FILE"' EXIT

# -q must be curl's first option: it skips ~/.curlrc, which could otherwise
# turn on verbose or trace output that prints the Authorization header.
STATUS="$(curl -q -sS -X POST "$ADMIN_URL/admin/actors/purge" \
  -H @- \
  -H "Content-Type: application/json" \
  --data "{\"did\":\"$DID\",\"dryRun\":$DRY_RUN}" \
  -o "$BODY_FILE" -w '%{http_code}' \
  <<<"Authorization: Bearer $ADMIN_TOKEN")"

cat "$BODY_FILE"
echo
if [ "${STATUS:0:1}" != "2" ]; then
  echo "error: purge answered HTTP $STATUS" >&2
  exit 1
fi
