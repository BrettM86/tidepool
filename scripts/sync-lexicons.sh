#!/usr/bin/env bash
# Re-syncs the vendored lexicon JSON files from the Coves repository.
#
# Tidepool materializes social.coves.* records and validates them against
# the same lexicon files the Coves AppView ships (with indigo's
# atproto/lexicon validator — the same package Coves' validate-lexicon tool
# uses; see internal/lexicons). The files are vendored so builds and tests
# never depend on a sibling checkout. Run this script after Coves lexicon
# changes, then re-run the tests.
#
# Usage: scripts/sync-lexicons.sh [path-to-coves-checkout]
set -euo pipefail

COVES="${1:-$HOME/Code/coves}"
SRC="$COVES/internal/atproto/lexicon"
DST="$(cd "$(dirname "$0")/.." && pwd)/lexicons"

if [ ! -d "$SRC" ]; then
  echo "error: Coves lexicon directory not found at $SRC" >&2
  exit 1
fi

# Tidepool OWNS lexicons under social/coves/bridge/ (the vote-aggregate
# side channel, task 07) — they do not exist in Coves and must survive a
# re-sync.
BRIDGE_TMP="$(mktemp -d)"
trap 'rm -rf "$BRIDGE_TMP"' EXIT
if [ -d "$DST/social/coves/bridge" ]; then
  cp -R "$DST/social/coves/bridge" "$BRIDGE_TMP/bridge"
fi

rm -rf "$DST/social" "$DST/com"

# Everything under social/coves (records Tidepool emits plus every ref
# target they can reach), JSON only.
mkdir -p "$DST/social"
rsync -a --include='*/' --include='*.json' --exclude='*' \
  "$SRC/social/coves/" "$DST/social/coves/"

# Restore the bridge-owned lexicons. If upstream Coves ever ships its own
# social/coves/bridge/, the rsync above will have created the destination
# and a blind cp -R would nest bridge/bridge/ and silently shadow the
# Tidepool-owned lexicons — that namespace collision needs a human.
if [ -d "$BRIDGE_TMP/bridge" ]; then
  if [ -e "$DST/social/coves/bridge" ]; then
    echo "error: upstream Coves now ships social/coves/bridge/ — namespace collision" >&2
    echo "with the Tidepool-owned bridge lexicons. Resolve manually (rename one side" >&2
    echo "or reconcile the schemas); the pre-sync bridge files are preserved in" >&2
    echo "$BRIDGE_TMP (removed on exit)." >&2
    exit 1
  fi
  cp -R "$BRIDGE_TMP/bridge" "$DST/social/coves/bridge"
fi

# The com.atproto refs the social.coves records use.
mkdir -p "$DST/com/atproto/repo" "$DST/com/atproto/label"
cp "$SRC/com/atproto/repo/strongRef.json" "$DST/com/atproto/repo/strongRef.json"
cp "$SRC/com/atproto/label/defs.json" "$DST/com/atproto/label/defs.json"

# Refresh the manifest scripts/check-lexicons.sh verifies (CI drift guard).
(cd "$DST" && find social com -name '*.json' | sort | xargs shasum -a 256 > MANIFEST.sha256)

echo "synced lexicons from $SRC to $DST"
