package repo

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"tidepool/internal/errors"
)

// TID generation for the virtual repo layer. Two distinct kinds:
//
//   - Commit revs use clock TIDs that must increase monotonically per repo.
//     NextRev derives them from the previous stored rev, so monotonicity
//     survives process restarts and wall-clock regressions without any
//     in-memory clock state.
//
//   - Record keys use deterministic content TIDs (PLAN.md locked decision 4):
//     the timestamp half comes from the AP object's `published` time and the
//     10 clock-ID bits from a hash of the canonical AP id. Re-ingesting the
//     same AP object therefore always produces the same rkey (and at-uri),
//     across process restarts. Task 05 calls DeterministicTID for every
//     materialized record.

// NextRev returns the rev TID for a repo's next commit, strictly greater
// than prevRev. An empty prevRev (genesis commit) starts a fresh clock at
// the current time. A malformed stored prevRev is a bug in our own data, so
// it surfaces as an error rather than silently restarting the clock (which
// could emit a non-monotonic rev).
func NextRev(prevRev string) (syntax.TID, error) {
	if prevRev == "" {
		clk := syntax.NewTIDClock(0)
		return clk.Next(), nil
	}
	prev, err := syntax.ParseTID(prevRev)
	if err != nil {
		return "", err
	}
	clk := syntax.ClockFromTID(prev)
	return clk.Next(), nil
}

// DeterministicTID builds the content TID used as a record key for a
// materialized AP object. published supplies the timestamp bits (so records
// sort by original publication time); the clock-ID bits come from a SHA-256
// hash of the canonical AP id, so the same object always maps to the same
// rkey across process restarts.
//
// Collision resistance: 10 clock-ID bits alone hit birthday collisions at
// roughly ~40 objects sharing a timestamp, and AP `published` is usually
// second-precision. So when published has no sub-second component, the
// microsecond field is filled deterministically from further bytes of the
// sha256(ap_id) hash (range 0..999999, ~20 extra bits of entropy), keeping
// the TID idempotent, format-valid, and sorted within the correct second.
// Objects with genuine sub-second precision keep their real microseconds.
// Caveat: for second-precision inputs, ordering WITHIN a second is hash
// order, not publication order.
//
// It fails closed on invalid times (zero or pre-Unix-epoch published →
// errors.IsValidation): callers must gate on task 02's ap.Time.OK() so a
// present-but-malformed `published` is never turned into a zero-time TID
// (which would collide and mis-sort).
func DeterministicTID(published time.Time, canonicalAPID string) (syntax.TID, error) {
	if published.IsZero() {
		return "", errors.NewValidationError("published",
			"must not be the zero time (gate on ap.Time.OK() before deriving rkeys)")
	}
	if published.Unix() < 0 {
		return "", errors.NewValidationError("published",
			fmt.Sprintf("%s precedes the Unix epoch", published.UTC().Format(time.RFC3339)))
	}
	sum := sha256.Sum256([]byte(canonicalAPID))
	clockID := uint(binary.BigEndian.Uint16(sum[:2]) & 0x3FF)
	micros := published.UTC().UnixMicro()
	if published.Nanosecond() == 0 {
		micros += int64(binary.BigEndian.Uint32(sum[2:6]) % 1_000_000)
	}
	return syntax.NewTID(micros, clockID), nil
}
