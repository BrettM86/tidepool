package outbound

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// TASK 17d REVIEW — THE VOTE WHOSE COMMUNITY IS GONE.
//
// A community deleted or unfollowed since the vote was cast has no row and no
// inbox, so its Undo has nowhere to go — and skipping the DELIVERY is right.
// But the purge makes TWO decisions per vote, and only one of them needs a
// peer: "tell the instance to drop it" needs an address; "stop counting this
// actor's vote" is the purge's own, and it is recorded by flipping the row to
// `undone`. Skipping the vote ENTIRELY conflates the two: the row keeps
// delivered_state='delivered' for a tombstoned actor, 17b's reseed subtracts it
// from served scores forever — the precise outcome the Purger's own header says
// the tier exists to prevent — and the 17e recast sweep's first exclusion
// suppresses exactly this row, so the only trace ever is one purge-time log
// line.

const puGoneCommunityDID = "did:plc:gonecommunity0000000000"

// puGoneVoteATURI is a second vote by the same actor, so the unaddressable vote
// and an ordinary addressable one travel through ONE purge — without the
// addressable one beside it, "the purge retracted nothing" and "the purge
// handled the unaddressable vote" would be the same observation.
const puGoneVoteATURI = "at://" + wActorDID + "/social.coves.feed.vote/3lzpuvote002"

func TestPurge_AnUnaddressableVoteIsStillRetractedLocally(t *testing.T) {
	world := newHeldVoteWorld(t)
	ctx := context.Background()
	conn := world.conn

	// A DELIVERED vote whose community row does not exist: the peer holds it,
	// but there is no inbox left to address its Undo to.
	goneActivityID := "https://coves.social/ap/activity/" + repeatHex64("Gone")
	_, err := world.votes.Upsert(ctx, store.OutboundVote{
		VoteATURI:         puGoneVoteATURI,
		ActorDID:          wActorDID,
		SubjectATURI:      "at://" + puGoneCommunityDID + "/social.coves.community.postv2/3lzgonepost",
		SubjectAPID:       "https://gone.example/post/1",
		CommunityDID:      puGoneCommunityDID,
		Direction:         "up",
		CurrentActivityID: goneActivityID,
		DeliveredState:    store.DeliveredStateDelivered,
	})
	require.NoError(t, err)

	// --- WHEN: the withdrawal runs. It must not be held hostage by the missing
	//     community — the anti-hostage rationale in addressableVotes stands.
	require.NoError(t, world.purger.DeleteRemoteContent(ctx, wActorDID))

	// --- THEN: the local decision was still made for the unaddressable vote.
	gone, err := world.votes.GetByATURI(ctx, puGoneVoteATURI)
	require.NoError(t, err)
	assert.Equal(t, store.DeliveredStateUndone, gone.DeliveredState,
		"the vote must read undone even though its Undo could not be sent. 'Stop counting "+
			"this actor's votes' is the purge's own decision, independent of whether a peer "+
			"can still be told — left 'delivered', the reseed subtracts a tombstoned actor's "+
			"vote from served scores forever, and the recast sweep is structurally blind to "+
			"the row, so nothing downstream ever corrects it")

	// And no Undo was enqueued FOR THAT SUBJECT — there is no inbox, so an
	// enqueue could only dead-letter — while the addressable held vote in the
	// same purge still got its retraction.
	assert.Equal(t, 1, activitiesOfKind(t, conn, "Undo"),
		"exactly one Undo: the addressable vote's. The unaddressable one flips locally "+
			"and enqueues nothing")
	assert.Equal(t, 0, undosForSubject(t, conn, gone.SubjectATURI),
		"and the one Undo is not addressed to the vanished community's subject")
}

// undosForSubject counts Undo activities whose causal parent is the given
// subject — undoLiveVotes enqueues each Undo under its vote's SubjectATURI.
func undosForSubject(t *testing.T, conn *sql.DB, subjectATURI string) int {
	t.Helper()
	var n int
	require.NoError(t, conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM outbound_activities WHERE kind = 'Undo' AND parent_at_uri = $1`,
		subjectATURI).Scan(&n))
	return n
}
