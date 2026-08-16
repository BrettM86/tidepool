package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// THE UPSERT IS THE INTENT WRITER. SetDeliveredState IS THE SETTLEMENT WRITER.
// They are the two writers of one column, and they do NOT obey the same rule.
//
// A re-cast — the user flipping up→down — reaches this table as an ordinary
// upsert carrying `pending`, because the consumer records intent and never
// claims delivery. But a flip is a REPLACEMENT, not a retraction: the peer is
// still holding a vote from this actor, and nothing has come back off the wire
// to say otherwise. Letting `pending` win erases the only record that a
// delivery ever happened, and the loss is permanent — no event re-fires it, so
// the vote silently drops out of ListStandingForActor and the erasure purge can
// no longer retract it.
//
// The guard is therefore narrow, and its narrowness is the whole design:
//
//	incoming pending over stored delivered -> KEEP delivered   (the re-cast)
//	incoming pending over stored undone    -> take pending     (a NEW intent)
//
// The second line is the one that rules out the tempting shortcut. "Keep the
// stored value whenever the incoming write says pending" satisfies the re-cast
// and breaks the purge: `undone` is a decision about an IDENTITY, and an upsert
// arriving over it is a live human casting a new vote, not a stale fact about
// an old message catching up. Terminality guards LATE SETTLEMENTS. It has no
// business refusing new intents, which is why SetDeliveredState refuses
// delivered-over-undone and this path does not. The asymmetry is deliberate.

// ---------------------------------------------------------------------------
// B1 — the guard itself
// ---------------------------------------------------------------------------

// recastActivityID is the id the FLIPPED vote goes out under: a re-cast is a new
// activity, so it must not reuse the delivered Like's id.
var recastActivityID = "https://coves.social/ap/activity/" + repeatHex('b')

func TestOutboundVotes_UpsertKeepsDeliveredThroughARecast(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundVotes(database)
	ctx := context.Background()

	_, err := repo.Upsert(ctx, testOutboundVote())
	require.NoError(t, err)
	require.NoError(t, repo.SetDeliveredState(ctx, testVoteATURI, DeliveredStateDelivered),
		"the peer accepted the Like — without this the test proves nothing")

	// The consumer's re-cast write, verbatim: applyVoteWrite always states
	// pending, because it records intent and cannot know what the wire said.
	recast := testOutboundVote()
	recast.Direction = "down"
	recast.CurrentActivityID = recastActivityID
	recast.DeliveredState = DeliveredStatePending

	returned, err := repo.Upsert(ctx, recast)
	require.NoError(t, err)
	require.NotNil(t, returned)

	// Pinned on the RETURNING value, not just on a re-read: applyVoteWrite builds
	// the outgoing intent straight from this struct and never reloads the row, so
	// a guard that fixed only the stored value would still hand the caller a lie.
	assert.Equal(t, DeliveredStateDelivered, returned.DeliveredState,
		"a flip REPLACES a vote the peer still holds; it does not withdraw it. Resetting to "+
			"pending discards the only evidence a delivery ever happened, and nothing "+
			"re-establishes it — the vote then vanishes from the standing list the erasure "+
			"purge enumerates, un-retractable on someone else's instance")

	assert.Equal(t, "down", returned.Direction,
		"while everything the re-cast actually carries still lands: a guard that froze the "+
			"whole row would leave the peer counting the vote the user changed away from")
	assert.Equal(t, recastActivityID, returned.CurrentActivityID,
		"including the new id — the Undo embeds whatever this column holds")
	assert.Equal(t, 1, returned.ActivitySeq,
		"and the seq still bumps: an id colliding with the delivered Like's would be "+
			"swallowed as a duplicate by any peer that already has it")

	got, err := repo.GetByATURI(ctx, testVoteATURI)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, DeliveredStateDelivered, got.DeliveredState,
		"and the row agrees with what the upsert returned")
	assert.Equal(t, recastActivityID, got.CurrentActivityID)
	assert.Equal(t, 1, got.ActivitySeq)
}

// ---------------------------------------------------------------------------
// B2 — everything the guard must NOT change
// ---------------------------------------------------------------------------

// TestOutboundVotes_UpsertDeliveredStateMatrix pins the six conflict
// transitions that already work — plus the plain INSERT branch — so the fix
// above cannot be bought by freezing the column.
//
// Each of these is a live production path, named in its own case. They pass
// before the guard exists and must pass after it.
func TestOutboundVotes_UpsertDeliveredStateMatrix(t *testing.T) {
	tests := []struct {
		name string
		// seed is the state the row is in before the write under test. Empty
		// means NO PRIOR ROW — the plain INSERT branch, which has no stored
		// value to defend and so cannot reach the guard at all.
		seed    DeliveredState
		write   DeliveredState
		want    DeliveredState
		wantSeq int
		why     string
	}{
		{
			name: "a first cast asks for pending and gets pending",
			seed: "", write: DeliveredStatePending, want: DeliveredStatePending, wantSeq: 0,
			why: "the ordinary first vote: the consumer records intent, and only the " +
				"settlement callback may ever claim more than that",
		},
		{
			name: "pending over pending stays pending, and still bumps",
			seed: DeliveredStatePending, write: DeliveredStatePending, want: DeliveredStatePending, wantSeq: 1,
			why: "a re-cast of a vote that never reached the peer has nothing to protect — " +
				"but it is still a second activity, so the seq moves",
		},
		{
			name: "delivered over delivered applies",
			seed: DeliveredStateDelivered, write: DeliveredStateDelivered, want: DeliveredStateDelivered, wantSeq: 1,
			why: "applyVoteDelete re-upserts the row it just read back, verbatim, to bump the " +
				"seq for the Undo — so a delivered row writes its own state straight through",
		},
		{
			name: "undone over delivered applies",
			seed: DeliveredStateDelivered, write: DeliveredStateUndone, want: DeliveredStateUndone, wantSeq: 1,
			why: "THE PURGE WRITES `undone` THROUGH THIS UPSERT (outbound.Purger.undoLiveVotes), " +
				"in the one statement that also bumps the seq for the Undo it enqueues. A " +
				"guard that defended `delivered` against everything would silently drop the " +
				"retraction of a withdrawn actor's vote",
		},
		{
			name: "pending over undone applies",
			seed: DeliveredStateUndone, write: DeliveredStatePending, want: DeliveredStatePending, wantSeq: 1,
			why: "no production path reaches this today, and it is allowed anyway: this is the " +
				"INTENT writer, and terminality guards late settlements — stale facts about " +
				"an old message — not a new vote cast by a live human. Refusing it here is " +
				"the shortcut that would break the purge case above, since both arrive as " +
				"`pending`-shaped writes over a non-pending row",
		},
		{
			name: "undone over pending applies",
			seed: DeliveredStatePending, write: DeliveredStateUndone, want: DeliveredStateUndone, wantSeq: 1,
			why: "a REAL production path, not a hypothetical: the purge retracts votes whose " +
				"delivery is HELD FOR SETTLEMENT — the peer accepted the POST, only our " +
				"bookkeeping lagged — and those rows still read `pending` " +
				"(ListStandingForActor's second term). A guard shaped 'only a delivered row " +
				"may take undone' would pass every other case here and break that purge at " +
				"the store level",
		},
		{
			name: "delivered over undone applies",
			seed: DeliveredStateUndone, write: DeliveredStateDelivered, want: DeliveredStateDelivered, wantSeq: 1,
			why: "the same rationale, and the deliberate asymmetry: SetDeliveredState REFUSES " +
				"this exact transition, because a settlement landing after a retraction is a " +
				"stale fact. An upsert carrying it is a caller restating the whole row, and " +
				"the intent path does not second-guess that",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			database := outboundTestDB(t)
			repo := NewOutboundVotes(database)
			ctx := context.Background()

			if tc.seed != "" {
				_, err := repo.Upsert(ctx, testOutboundVote())
				require.NoError(t, err, "seed the row")
				if tc.seed != DeliveredStatePending {
					require.NoError(t, repo.SetDeliveredState(ctx, testVoteATURI, tc.seed),
						"seed the row into %s", tc.seed)
				}
			}

			vote := testOutboundVote()
			vote.DeliveredState = tc.write
			returned, err := repo.Upsert(ctx, vote)
			require.NoError(t, err)
			require.NotNil(t, returned)

			assert.Equal(t, tc.want, returned.DeliveredState, tc.why)
			assert.Equal(t, tc.wantSeq, returned.ActivitySeq,
				"the seq bump is independent of the delivered_state decision")

			got, err := repo.GetByATURI(ctx, testVoteATURI)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tc.want, got.DeliveredState,
				"the stored row must agree with what the upsert returned")
		})
	}
}
