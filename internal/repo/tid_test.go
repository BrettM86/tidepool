package repo

import (
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// mustDeterministicTID keeps the happy-path tests terse.
func mustDeterministicTID(t *testing.T, published time.Time, apID string) syntax.TID {
	t.Helper()
	tid, err := DeterministicTID(published, apID)
	require.NoError(t, err)
	return tid
}

func TestDeterministicTID_StableAcrossCalls(t *testing.T) {
	published := time.Date(2026, 5, 1, 12, 30, 45, 123456000, time.UTC)
	apID := "https://lemmy.world/post/12345"

	first := mustDeterministicTID(t, published, apID)
	second := mustDeterministicTID(t, published, apID)

	assert.Equal(t, first, second,
		"same AP id + published must always produce the same TID (idempotent re-ingestion)")

	_, err := syntax.ParseTID(first.String())
	require.NoError(t, err, "deterministic TID must be format-valid")
	assert.Equal(t, published, first.Time(),
		"genuine sub-second precision must round-trip the published time exactly")
}

func TestDeterministicTID_DifferentIDsDiffer(t *testing.T) {
	published := time.Date(2026, 5, 1, 12, 30, 45, 0, time.UTC)

	a := mustDeterministicTID(t, published, "https://lemmy.world/post/1")
	b := mustDeterministicTID(t, published, "https://lemmy.world/post/2")

	assert.NotEqual(t, a, b,
		"objects published in the same second must still get distinct rkeys via the id-hash bits")
}

func TestDeterministicTID_SecondPrecisionGetsHashMicros(t *testing.T) {
	// AP `published` is usually second-precision; the microsecond field is
	// then filled from the ap_id hash, so a same-second collision needs the
	// ~20 micro bits AND the 10 clock-ID bits to collide at once.
	published := time.Date(2026, 5, 1, 12, 30, 45, 0, time.UTC)

	a := mustDeterministicTID(t, published, "https://lemmy.world/post/1")
	assert.Equal(t, a, mustDeterministicTID(t, published, "https://lemmy.world/post/1"),
		"hash-filled micros must be deterministic")

	got := a.Time()
	assert.False(t, got.Before(published), "filled micros must stay within the published second")
	assert.True(t, got.Before(published.Add(time.Second)), "filled micros must stay within the published second")
	assert.NotEqual(t, published, got,
		"this fixture's hash micros are non-zero; equality means the fill was not applied")

	b := mustDeterministicTID(t, published, "https://lemmy.world/post/2")
	assert.NotEqual(t, a.Time(), b.Time(),
		"different ids land on different micros within the second (for these fixtures)")

	_, err := syntax.ParseTID(a.String())
	require.NoError(t, err, "hash-filled TID must stay format-valid")
}

func TestDeterministicTID_GoldenValues(t *testing.T) {
	// GOLDEN VALUES — these pin the persisted-rkey algorithm itself, not
	// just its properties. DeterministicTID output is stored in rkeys and
	// at-uris (PLAN.md locked decision 4): if any of these assertions ever
	// fails, the algorithm changed and EVERY at-uri already persisted by a
	// deployed bridge breaks. Do not "fix" a diff by updating the golden
	// strings unless you are knowingly migrating all stored rkeys.
	//
	// The values were computed by running the current implementation once
	// and hard-coding its output.

	// (a) Second-precision published time: AP `published` is usually
	// second-precision, so the microsecond field is filled from
	// sha256(ap_id) bytes (the hash-derived-micros path).
	secondPrecision := time.Date(2026, 5, 1, 12, 30, 45, 0, time.UTC)
	tid := mustDeterministicTID(t, secondPrecision, "https://lemmy.world/post/12345")
	assert.Equal(t, "3mks4zznhkard", tid.String(),
		"hash-derived-micros path changed: every persisted at-uri would break")

	// (b) Sub-second-precision published time: genuine microseconds are
	// kept verbatim (the real-micros path).
	subSecond := time.Date(2026, 5, 1, 12, 30, 45, 123456000, time.UTC)
	tid = mustDeterministicTID(t, subSecond, "https://lemmy.world/post/12345")
	assert.Equal(t, "3mks4zzqmg2rd", tid.String(),
		"real-micros path changed: every persisted at-uri would break")

	// (c) Two different ap_ids in the same second: distinct, pinned TIDs
	// (both hash micros and clock-ID bits derive from the ap_id).
	a := mustDeterministicTID(t, secondPrecision, "https://lemmy.world/post/1")
	b := mustDeterministicTID(t, secondPrecision, "https://lemmy.world/post/2")
	assert.Equal(t, "3mks522bgpxc5", a.String())
	assert.Equal(t, "3mks522eqiu4d", b.String())
	assert.NotEqual(t, a, b,
		"same-second objects must keep getting distinct rkeys")
}

func TestDeterministicTID_RejectsInvalidTimes(t *testing.T) {
	_, err := DeterministicTID(time.Time{}, "https://lemmy.world/post/1")
	require.Error(t, err, "zero published time must fail closed")
	assert.True(t, errors.IsValidation(err))

	_, err = DeterministicTID(time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), "https://lemmy.world/post/1")
	require.Error(t, err, "pre-epoch published time must fail closed")
	assert.True(t, errors.IsValidation(err))
}

func TestDeterministicTID_SortsByPublishedTime(t *testing.T) {
	early := mustDeterministicTID(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "https://lemmy.world/post/9")
	late := mustDeterministicTID(t, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "https://lemmy.world/post/1")

	assert.Less(t, early.String(), late.String(),
		"TIDs must sort by published time regardless of AP id")
}

func TestDeterministicTID_TimezoneNormalized(t *testing.T) {
	utc := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	offset := utc.In(time.FixedZone("CEST", 2*3600))

	assert.Equal(t, mustDeterministicTID(t, utc, "https://x/1"), mustDeterministicTID(t, offset, "https://x/1"),
		"the same instant in different zones must produce the same TID")
}

func TestNextRev_Genesis(t *testing.T) {
	rev, err := NextRev("")
	require.NoError(t, err)
	_, err = syntax.ParseTID(rev.String())
	require.NoError(t, err)
}

func TestNextRev_Monotonic(t *testing.T) {
	rev, err := NextRev("")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		next, err := NextRev(rev.String())
		require.NoError(t, err)
		assert.Greater(t, next.String(), rev.String(), "revs must strictly increase")
		rev = next
	}
}

func TestNextRev_MonotonicPastFutureRev(t *testing.T) {
	// A stored rev from the future (clock skew) must still yield a greater
	// rev, not a smaller one.
	future := syntax.NewTIDFromTime(time.Now().Add(time.Hour), 7)
	next, err := NextRev(future.String())
	require.NoError(t, err)
	assert.Greater(t, next.String(), future.String())
}

func TestNextRev_RejectsMalformedStoredRev(t *testing.T) {
	_, err := NextRev("not-a-tid!")
	require.Error(t, err, "corrupt stored rev must fail loudly, not restart the clock")
}
