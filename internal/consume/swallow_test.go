package consume

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// Second-opinion: two swallowed DB errors that must PROPAGATE.
//
// recordedDepth and communityAPID both catch every error from a store read and
// return a default (depth 0, empty community). That is correct for a genuine
// miss — a subject the bridge does not track depth for, a community with no
// row — but a NON-NotFound error (postgres down, a timeout) is not a miss. On
// one it federates a deeply nested comment as depth 0 (past Lemmy's cap the
// wrong way) or addresses an Undo to nobody, silently, and never retries. Both
// must return the error so the connector retries and, if it persists,
// dead-letters it where an operator can see it.

func TestSwallow_RecordedDepthPropagatesANonNotFoundError(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	// The parent is a MAPPED subject, so resolveSubject takes the recordedDepth
	// path (a second outbound_objects read for the parent's own depth).
	seedThreadRoot(t, database)

	failing := &failingOutboundObjects{
		OutboundObjects: store.NewOutboundObjects(database),
		err:             fmt.Errorf("connection reset by peer"),
		failGet:         true,
	}
	fixture := newDispatchFixture(t, database, func(opts *Options) { opts.Objects = failing })

	err := fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRev, "3lzswal000001", "create", "reply", acceptRootATURI))

	require.Error(t, err,
		"a failed depth read must FAIL the event: recording depth 0 off a dropped "+
			"connection would federate a comment at the wrong nesting and, past the "+
			"cap, keep federating ones Lemmy will reject — silently, with no retry")
	assert.False(t, errors.IsNotFound(err),
		"and it is the real error, not remapped to a miss")

	assert.Zero(t, countRows(t, database, "jetstream_record_revs"),
		"the failed event advances no gate, so the retry can recover")
}

func TestSwallow_CommunityAPIDPropagatesANonNotFoundError(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	ctx := context.Background()

	// Seed a real vote so the delete path has state to read and reaches
	// communityAPID (which only runs when CommunityDID != "").
	const rkey = "3lzswal000002"
	voteATURI := voteATURIFor(dispatchNativeDID, rkey)
	healthy := newDispatchFixture(t, database)
	require.NoError(t, healthy.handle(t, voteFrame(dispatchNativeDID, dispatchRev, rkey, acceptRootATURI, "up")))
	seeded, err := store.NewOutboundVotes(database).GetByATURI(ctx, voteATURI)
	require.NoError(t, err)
	require.NotEmpty(t, seeded.CommunityDID, "the seeded vote must carry a community to resolve")

	// Now run the DELETE with a communities store that fails GetByDID.
	broken := newDispatchFixture(t, database, func(opts *Options) {
		opts.Communities = &failingCommunities{
			Communities: store.NewCommunities(database),
			err:         fmt.Errorf("statement timeout"),
		}
	})
	err = broken.handle(t, voteDeleteFrame(dispatchNativeDID, dispatchRevHigher, rkey))

	require.Error(t, err,
		"a failed community lookup on the Undo path must FAIL the event: addressing the "+
			"withdrawal to an empty community off a timeout would drop it into the void "+
			"and never retry")
	assert.False(t, errors.IsNotFound(err))
}
