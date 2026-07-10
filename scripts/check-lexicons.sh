#!/usr/bin/env bash
# Verifies the vendored lexicons are in sync.
#
# Two layers:
#   1. Manifest check (always): every vendored lexicon file must match
#      lexicons/MANIFEST.sha256, and no unlisted json files may exist. The
#      manifest is (re)written by scripts/sync-lexicons.sh, so this catches
#      hand-edited vendored files and half-done syncs — including in CI,
#      where no Coves checkout exists.
#   2. Upstream drift check (when a Coves checkout is available, default
#      ~/Code/coves or $1): the vendored tree must byte-match what
#      sync-lexicons.sh would copy today. Skipped gracefully when the
#      checkout is absent (CI).
#
# Exit non-zero on any drift.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DST="$ROOT/lexicons"
COVES="${1:-$HOME/Code/coves}"

# ── 1. Manifest check ──────────────────────────────────────────────────────
if [ ! -f "$DST/MANIFEST.sha256" ]; then
  echo "error: $DST/MANIFEST.sha256 missing — run scripts/sync-lexicons.sh" >&2
  exit 1
fi

cd "$DST"
if ! shasum -a 256 --check --quiet MANIFEST.sha256; then
  echo "error: vendored lexicons do not match MANIFEST.sha256 — run scripts/sync-lexicons.sh" >&2
  exit 1
fi
# Files present but not listed (a hash check alone can't see additions).
listed=$(awk '{print $2}' MANIFEST.sha256 | sort)
actual=$(find social com -name '*.json' | sort)
if [ "$listed" != "$actual" ]; then
  echo "error: vendored lexicon file set differs from MANIFEST.sha256:" >&2
  diff <(echo "$listed") <(echo "$actual") >&2 || true
  echo "run scripts/sync-lexicons.sh" >&2
  exit 1
fi
echo "ok: vendored lexicons match MANIFEST.sha256 ($(echo "$listed" | wc -l | tr -d ' ') files)"

# ── 2. Upstream drift check ────────────────────────────────────────────────
SRC="$COVES/internal/atproto/lexicon"
# Only a truly-absent checkout is a skip; a Coves checkout that exists but
# is unreadable or missing its lexicon dir is an error, not silence.
if [ ! -e "$COVES" ]; then
  echo "skip: Coves checkout not found at $COVES — upstream drift not checked (fine in CI)"
  exit 0
fi
if [ ! -d "$SRC" ] || [ ! -r "$SRC" ] || [ ! -x "$SRC" ]; then
  echo "error: Coves checkout exists at $COVES but $SRC is not a readable directory" >&2
  exit 1
fi
if [ ! -d "$SRC/social/coves" ]; then
  echo "error: $SRC/social/coves missing — $COVES does not look like a Coves checkout" >&2
  exit 1
fi

drift=0
# Everything sync-lexicons.sh copies must byte-match — including the
# vendored com/atproto files (strongRef, label defs), which are manually
# curated cherry-picks: divergence of a vendored com/ file is drift, but a
# NEW upstream com/ file is not (sync-lexicons.sh would not copy it).
# Lexicons under social/coves/bridge/ are Tidepool-owned (the vote-aggregate
# side channel) and intentionally absent from Coves.
while IFS= read -r rel; do
  case "$rel" in
    social/coves/bridge/*) continue ;;
    social/coves/*) src_file="$SRC/$rel" ;;
    com/*)          src_file="$SRC/$rel" ;;
    *)              continue ;;
  esac
  if [ ! -f "$src_file" ]; then
    echo "drift: $rel is vendored but no longer exists in Coves" >&2
    drift=1
  elif ! cmp -s "$src_file" "$rel"; then
    echo "drift: $rel differs from Coves" >&2
    drift=1
  fi
done <<<"$actual"

# New upstream files sync would pick up (social/coves only — new com/ files
# are deliberately not flagged, see above). The find output is captured up
# front so a find failure aborts the script (set -e) instead of silently
# feeding an empty loop via process substitution.
upstream=$(find "$SRC/social/coves" -name '*.json' | sort)
while IFS= read -r src_file; do
  [ -n "$src_file" ] || continue
  rel="${src_file#"$SRC/"}"
  if [ ! -f "$rel" ]; then
    echo "drift: Coves has new lexicon $rel not vendored yet" >&2
    drift=1
  fi
done <<<"$upstream"

if [ "$drift" -ne 0 ]; then
  echo "error: vendored lexicons drifted from $SRC — run scripts/sync-lexicons.sh" >&2
  exit 1
fi
echo "ok: vendored lexicons in sync with $SRC"
