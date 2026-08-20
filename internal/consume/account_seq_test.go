package consume

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Second-opinion C4: #account frames need ordering protection of their own.
//
// The rev gate protects COMMIT events, but #account and #identity go straight
// to their handlers ungated. So a stale #account replay — which the reconnect
// rewind produces on every reconnect — is applied verbatim: a pause at seq N,
// then an unpause at seq N+1, then the rewind re-delivers the seq-N pause and
// the account flips back to paused. The user is silently un-delivered until
// the next live event. #account carries a monotonic seq per DID exactly so a
// consumer can reject the stale one.

// accountFrameSeq builds an #account frame with an explicit seq.
func accountFrameSeq(did string, active bool, status string, seq int64) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":%d,"kind":"account",`+
			`"account":{"did":%q,"seq":%d,"time":"2026-08-13T10:00:00.000Z","active":%t,"status":%q}}`,
		did, 7000+seq, did, seq, active, status))
}

func TestAccountSeq_StaleReplayDoesNotUndoANewerState(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	// In order: pause at seq 5, then reactivate at seq 6.
	require.NoError(t, fixture.handle(t, accountFrameSeq(dispatchNativeDID, false, "deactivated", 5)))
	require.True(t, deliveryPaused(t, database, dispatchNativeDID))

	require.NoError(t, fixture.handle(t, accountFrameSeq(dispatchNativeDID, true, "active", 6)))
	require.False(t, deliveryPaused(t, database, dispatchNativeDID))

	// The reconnect rewind re-delivers the OLD seq-5 pause.
	require.NoError(t, fixture.handle(t, accountFrameSeq(dispatchNativeDID, false, "deactivated", 5)))

	assert.False(t, deliveryPaused(t, database, dispatchNativeDID),
		"a stale #account (seq 5) arriving after a newer one (seq 6) must be a no-op: "+
			"re-applying it flips a reactivated user back to paused and un-delivers them "+
			"until the next live event — the exact silent regression the rev gate "+
			"prevents for commits")
}

func TestAccountSeq_EqualSeqReplayIsANoOp(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t, accountFrameSeq(dispatchNativeDID, true, "active", 9)))
	require.NoError(t, fixture.handle(t, accountFrameSeq(dispatchNativeDID, false, "deactivated", 9)),
		"an equal-seq frame is the same event redelivered")

	assert.False(t, deliveryPaused(t, database, dispatchNativeDID),
		"equal seq is a duplicate, not a new decision: the second frame at seq 9 must "+
			"not override the first, or a redelivery would let stale contents win a tie")
}

func TestAccountSeq_NewerSeqStillApplies(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t, accountFrameSeq(dispatchNativeDID, true, "active", 3)))
	require.NoError(t, fixture.handle(t, accountFrameSeq(dispatchNativeDID, false, "suspended", 4)))

	assert.True(t, deliveryPaused(t, database, dispatchNativeDID),
		"the gate must not over-reject: a genuinely newer seq is the user's next real "+
			"state and applies")
}

func TestAccount_NestedDIDMismatchingTheEnvelopeIsRejected(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	// The envelope names one DID; the nested account payload names another.
	// A consumer that trusted the inner DID could be steered to pause or
	// delete an account the frame's envelope never authorized.
	frame := []byte(fmt.Sprintf(
		`{"did":%q,"time_us":7100,"kind":"account",`+
			`"account":{"did":%q,"seq":1,"time":"2026-08-13T10:00:00.000Z","active":false,"status":"deactivated"}}`,
		dispatchNativeDID, "did:plc:someoneelse00000000000"))

	err := fixture.handle(t, frame)
	require.Error(t, err,
		"an #account whose inner DID disagrees with the envelope DID is malformed or "+
			"hostile: acting on the inner one lets a frame about DID A mutate DID B")
	assert.ErrorIs(t, err, ErrPermanentEvent,
		"the disagreement cannot resolve itself on retry, so it is dead-lettered "+
			"exhausted rather than replayed")

	assert.False(t, deliveryPaused(t, database, dispatchNativeDID),
		"and neither DID's state is touched")
}
