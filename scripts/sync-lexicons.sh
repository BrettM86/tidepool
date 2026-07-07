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

rm -rf "$DST/social" "$DST/com"

# Everything under social/coves (records Tidepool emits plus every ref
# target they can reach), JSON only.
mkdir -p "$DST/social"
rsync -a --include='*/' --include='*.json' --exclude='*' \
  "$SRC/social/coves/" "$DST/social/coves/"

# The com.atproto refs the social.coves records use.
mkdir -p "$DST/com/atproto/repo" "$DST/com/atproto/label"
cp "$SRC/com/atproto/repo/strongRef.json" "$DST/com/atproto/repo/strongRef.json"
cp "$SRC/com/atproto/label/defs.json" "$DST/com/atproto/label/defs.json"

echo "synced lexicons from $SRC to $DST"
