package consume

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// Task 14 cycle C: the rev gate. This is the guard that makes the whole
// pipeline replay-safe, and the thing stable activity ids CANNOT substitute
// for: a peer has no way to reject a Create for an id it has never seen, so a
// cursor rewind past a delete would resurrect content the user removed.

func revGateTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "jetstream_record_revs")
	return database
}

const (
	gateURI      = "at://did:plc:7iza6de2dwap2sbkpav7c6c6/social.coves.community.comment/3lzcmnt3333bb"
	gateRevLow   = "3lzaaaaaaaaaa"
	gateRevMid   = "3lzbbbbbbbbbb"
	gateRevHigh  = "3lzcccccccccc"
	gateConsumer = ConsumerNative
	gateDID      = "did:plc:7iza6de2dwap2sbkpav7c6c6"
)

func gateCommit(operation, rev string) *CommitEvent {
	return &CommitEvent{
		Rev:        rev,
		Operation:  operation,
		Collection: CollectionComment,
		RKey:       "3lzcmnt3333bb",
	}
}

// storedRev reads the gate row directly. A missing row returns "".
func storedRev(t *testing.T, database *sql.DB, uri string) string {
	t.Helper()
	var rev string
	err := database.QueryRowContext(context.Background(),
		`SELECT rev FROM jetstream_record_revs WHERE record_uri = $1`, uri).Scan(&rev)
	if err == sql.ErrNoRows {
		return ""
	}
	require.NoError(t, err)
	return rev
}

// ---------------------------------------------------------------------------
// C1 — the gate decides who applies
// ---------------------------------------------------------------------------

func TestCommitRecordURI(t *testing.T) {
	assert.Equal(t, gateURI, commitRecordURI(gateDID, gateCommit("create", gateRevLow)),
		"the gate is keyed by the record's AT-URI, built from the repo DID plus the "+
			"commit's collection and rkey")
}

func TestApplyGated_FirstWriterWinsAndRecordsTheRev(t *testing.T) {
	database := revGateTestDB(t)
	gate := NewRevGate(database)
	ctx := context.Background()

	applied := 0
	err := applyGated(ctx, gate, gateConsumer, gateDID, gateCommit("create", gateRevMid),
		func() error { applied++; return nil })
	require.NoError(t, err)
	assert.Equal(t, 1, applied, "a record with no gate row yet always applies")
	assert.Equal(t, gateRevMid, storedRev(t, database, gateURI),
		"the applied rev is recorded so the next event can be compared against it")
}

func TestApplyGated_RevOrdering(t *testing.T) {
	tests := []struct {
		name        string
		incomingRev string
		wantApplied bool
		wantStored  string
		why         string
	}{
		{
			name:        "the same rev is a replay",
			incomingRev: gateRevMid,
			wantApplied: false,
			wantStored:  gateRevMid,
			why: "equal rev IS the same event arriving twice (reconnect rewind, redrive) — " +
				"applying it again would re-enqueue an activity the peer already has",
		},
		{
			name:        "a lower rev is a stale copy",
			incomingRev: gateRevLow,
			wantApplied: false,
			wantStored:  gateRevMid,
			why:         "an older revision must never overwrite a newer one, and must never lower the gate",
		},
		{
			name:        "a higher rev is genuinely newer",
			incomingRev: gateRevHigh,
			wantApplied: true,
			wantStored:  gateRevHigh,
			why:         "a strictly greater rev is the next real write and applies",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			database := revGateTestDB(t)
			gate := NewRevGate(database)
			ctx := context.Background()

			// Establish the gate at the middle rev.
			require.NoError(t, applyGated(ctx, gate, gateConsumer, gateDID,
				gateCommit("create", gateRevMid), func() error { return nil }))

			applied := 0
			err := applyGated(ctx, gate, gateConsumer, gateDID,
				gateCommit("update", tc.incomingRev),
				func() error { applied++; return nil })

			require.NoError(t, err,
				"a gate SKIP is a normal outcome, not an error: the event is fully "+
					"accounted for and the cursor must advance past it")
			if tc.wantApplied {
				assert.Equal(t, 1, applied, tc.why)
			} else {
				assert.Zero(t, applied, tc.why)
			}
			assert.Equal(t, tc.wantStored, storedRev(t, database, gateURI), tc.why)
		})
	}
}

func TestApplyGated_TombstoneRejectsAStaleCreate(t *testing.T) {
	database := revGateTestDB(t)
	gate := NewRevGate(database)
	ctx := context.Background()

	// A delete advances the gate...
	require.NoError(t, applyGated(ctx, gate, gateConsumer, gateDID,
		gateCommit("delete", gateRevHigh), func() error { return nil }))

	// ...and the row SURVIVES the record it describes. A cursor rewind now
	// re-delivers the original create.
	applied := 0
	err := applyGated(ctx, gate, gateConsumer, gateDID, gateCommit("create", gateRevLow),
		func() error { applied++; return nil })

	require.NoError(t, err)
	assert.Zero(t, applied,
		"the gate row is a TOMBSTONE: a replayed create with an older rev must not "+
			"resurrect content the user deleted — stable activity ids cannot prevent "+
			"this, because the peer has never seen the id it would be asked to accept")
	assert.Equal(t, gateRevHigh, storedRev(t, database, gateURI))
}

func TestApplyGated_EmptyRevBypassesTheGate(t *testing.T) {
	database := revGateTestDB(t)
	gate := NewRevGate(database)
	ctx := context.Background()

	applied := 0
	err := applyGated(ctx, gate, gateConsumer, gateDID, gateCommit("create", ""),
		func() error { applied++; return nil })

	require.NoError(t, err)
	assert.Equal(t, 1, applied,
		"a rev-less event (a synthetic test event, a legacy dead letter) applies rather "+
			"than being silently dropped")
	assert.Empty(t, storedRev(t, database, gateURI),
		"and writes no gate row: an empty rev would compare below every real TID and "+
			"permanently gate the record at the bottom")
}

func TestApplyGated_NilGateApplies(t *testing.T) {
	applied := 0
	err := applyGated(context.Background(), nil, gateConsumer, gateDID,
		gateCommit("create", gateRevMid), func() error { applied++; return nil })

	require.NoError(t, err)
	assert.Equal(t, 1, applied, "a nil gate disables gating entirely rather than panicking")
}

// ---------------------------------------------------------------------------
// C2 — the gate and the transaction
// ---------------------------------------------------------------------------

func TestApplyGated_ApplyErrorLeavesTheGateUnadvanced(t *testing.T) {
	database := revGateTestDB(t)
	gate := NewRevGate(database)
	ctx := context.Background()

	wantErr := fmt.Errorf("outbound_objects write failed")
	err := applyGated(ctx, gate, gateConsumer, gateDID, gateCommit("create", gateRevMid),
		func() error { return wantErr })

	require.ErrorIs(t, err, wantErr, "the apply error reaches the connector's retry/DLQ path")
	assert.Empty(t, storedRev(t, database, gateURI),
		"the claim is rolled back UN-ADVANCED: an event whose write failed must replay, "+
			"not be lost behind its own gate entry")

	// And the replay genuinely works.
	applied := 0
	require.NoError(t, applyGated(ctx, gate, gateConsumer, gateDID,
		gateCommit("create", gateRevMid), func() error { applied++; return nil }))
	assert.Equal(t, 1, applied, "the retried event applies because the gate never advanced")
}

func TestApplyGated_ApplyPanicLeavesTheGateUnadvanced(t *testing.T) {
	database := revGateTestDB(t)
	gate := NewRevGate(database)
	ctx := context.Background()

	func() {
		defer func() {
			assert.NotNil(t, recover(), "the panic must propagate, not be swallowed")
		}()
		_ = applyGated(ctx, gate, gateConsumer, gateDID, gateCommit("create", gateRevMid),
			func() error { panic("handler bug") })
	}()

	assert.Empty(t, storedRev(t, database, gateURI),
		"a panicking apply must roll the claim back too — the deferred rollback covers "+
			"errors and panics alike, or one bug would permanently gate a record")
}

func TestApplyGated_ConcurrentHandlersForOneURISerialize(t *testing.T) {
	database := revGateTestDB(t)
	gate := NewRevGate(database)
	ctx := context.Background()

	// Two feeds carrying the same repo deliver the SAME event at once. The
	// claim's row lock is held for the full duration of apply, so there is no
	// check→write window for the loser to slip through: it blocks, then
	// observes the winner's rev and skips.
	var mu sync.Mutex
	applied := 0
	var errs [2]error

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = applyGated(ctx, gate, gateConsumer, gateDID,
				gateCommit("create", gateRevMid), func() error {
					mu.Lock()
					applied++
					mu.Unlock()
					return nil
				})
		}(i)
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	assert.Equal(t, 1, applied,
		"exactly ONE of two concurrent handlers for the same (uri, rev) may apply; the "+
			"other serializes on the claim's row lock and then skips")
	assert.Equal(t, gateRevMid, storedRev(t, database, gateURI))
}

func TestApplyGated_ConcurrentDifferentRevsConvergeOnTheHigher(t *testing.T) {
	database := revGateTestDB(t)
	gate := NewRevGate(database)
	ctx := context.Background()

	var mu sync.Mutex
	highApplied := 0

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = applyGated(ctx, gate, gateConsumer, gateDID, gateCommit("update", gateRevLow),
			func() error { return nil })
	}()
	go func() {
		defer wg.Done()
		_ = applyGated(ctx, gate, gateConsumer, gateDID, gateCommit("update", gateRevHigh),
			func() error {
				mu.Lock()
				highApplied++
				mu.Unlock()
				return nil
			})
	}()
	wg.Wait()

	// Whichever order they interleave in, the newer revision must win: if it
	// runs first the older one loses the gate, if it runs second it beats the
	// stored rev.
	assert.Equal(t, 1, highApplied, "the newer revision always applies exactly once")
	assert.Equal(t, gateRevHigh, storedRev(t, database, gateURI),
		"the gate settles on the highest rev regardless of arrival order")
}

// ---------------------------------------------------------------------------
// The non-transactional flavor (the profile path's check → write → advance)
// ---------------------------------------------------------------------------

func TestRevGate_IsStaleAndAdvance(t *testing.T) {
	database := revGateTestDB(t)
	gate := NewRevGate(database)
	ctx := context.Background()

	stale, err := gate.IsStale(ctx, gateURI, gateRevMid)
	require.NoError(t, err)
	assert.False(t, stale, "no gate row means nothing to be stale against")

	require.NoError(t, gate.Advance(ctx, gateURI, gateRevMid))

	for _, tc := range []struct {
		rev       string
		wantStale bool
		why       string
	}{
		{gateRevLow, true, "an older rev is superseded"},
		{gateRevMid, true, "the same rev is a replay"},
		{gateRevHigh, false, "a newer rev is not stale"},
		{"", false, "an empty rev bypasses the gate"},
	} {
		stale, err := gate.IsStale(ctx, gateURI, tc.rev)
		require.NoError(t, err)
		assert.Equal(t, tc.wantStale, stale, tc.why)
	}

	// Advance keeps whichever rev is greater — a late Advance for an older
	// event must not lower the gate.
	require.NoError(t, gate.Advance(ctx, gateURI, gateRevLow))
	assert.Equal(t, gateRevMid, storedRev(t, database, gateURI),
		"Advance never lowers the stored rev")

	require.NoError(t, gate.Advance(ctx, gateURI, gateRevHigh))
	assert.Equal(t, gateRevHigh, storedRev(t, database, gateURI))
}

func TestRevGate_NilIsSafe(t *testing.T) {
	var gate *RevGate
	ctx := context.Background()

	stale, err := gate.IsStale(ctx, gateURI, gateRevMid)
	require.NoError(t, err)
	assert.False(t, stale, "a nil gate never reports stale")
	require.NoError(t, gate.Advance(ctx, gateURI, gateRevMid), "a nil gate's Advance is a no-op")
}

func TestTryAdvanceRecordRev_RunsInsideACallersTransaction(t *testing.T) {
	database := revGateTestDB(t)
	ctx := context.Background()

	// The transactional integration pattern: the claim is the first statement
	// of the CALLER's transaction, so a rollback reverts the gate together
	// with the writes it guarded.
	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	won, err := tryAdvanceRecordRev(ctx, tx, gateURI, gateRevMid)
	require.NoError(t, err)
	assert.True(t, won, "an unclaimed record URI is won")
	require.NoError(t, tx.Rollback())

	assert.Empty(t, storedRev(t, database, gateURI),
		"the claim rolls back with the caller's transaction")

	tx, err = database.BeginTx(ctx, nil)
	require.NoError(t, err)
	won, err = tryAdvanceRecordRev(ctx, tx, gateURI, gateRevMid)
	require.NoError(t, err)
	require.True(t, won)
	require.NoError(t, tx.Commit())

	assert.Equal(t, gateRevMid, storedRev(t, database, gateURI))

	won, err = tryAdvanceRecordRev(ctx, database, gateURI, gateRevMid)
	require.NoError(t, err)
	assert.False(t, won, "re-claiming the same rev loses")

	won, err = tryAdvanceRecordRev(ctx, database, gateURI, "")
	require.NoError(t, err)
	assert.True(t, won, "an empty rev bypasses the gate without writing")
	assert.Equal(t, gateRevMid, storedRev(t, database, gateURI))
}

func TestRecordRevIsStale(t *testing.T) {
	database := revGateTestDB(t)
	ctx := context.Background()

	stale, err := recordRevIsStale(ctx, database, gateURI, gateRevMid)
	require.NoError(t, err)
	assert.False(t, stale, "a missing gate row is not stale")

	_, err = tryAdvanceRecordRev(ctx, database, gateURI, gateRevMid)
	require.NoError(t, err)

	stale, err = recordRevIsStale(ctx, database, gateURI, gateRevHigh)
	require.NoError(t, err)
	assert.False(t, stale)

	stale, err = recordRevIsStale(ctx, database, gateURI, gateRevLow)
	require.NoError(t, err)
	assert.True(t, stale)
}
