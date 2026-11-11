package materialize

import (
	"context"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Account `!takedown` labels on a bridged account's bare DID (chunk 04): an
// account-scope withhold covering every blob the DID owns, including blobs
// stored after the label.

// labelExpLayout is the RFC 3339 UTC form a label exp is written in.
const labelExpLayout = "2006-01-02T15:04:05.000Z"

// dbLabelExp is the database clock plus ahead, formatted as a label exp.
func dbLabelExp(t *testing.T, ahead time.Duration) string {
	t.Helper()
	var now time.Time
	require.NoError(t, testutil.DB(t).QueryRow(`SELECT now()`).Scan(&now))
	return now.Add(ahead).UTC().Format(labelExpLayout)
}

// awaitDBClockPast waits (deadline 10 s) until the database clock is past exp.
func awaitDBClockPast(t *testing.T, exp string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var past bool
		require.NoError(t, testutil.DB(t).QueryRow(`SELECT now() > $1::timestamptz`, exp).Scan(&past))
		return past
	}, 10*time.Second, 50*time.Millisecond, "the database clock did not pass exp %s within 10s", exp)
}

// requireActorType requires did to be a bridged actor of actorType.
func requireActorType(t *testing.T, h *harness, did string, actorType store.ActorType) {
	t.Helper()
	actor, err := h.actors.GetByDID(context.Background(), did)
	require.NoError(t, err, "setup: %s is a bridged actor", did)
	require.Equal(t, actorType, actor.ActorType, "setup: %s is a bridged %s", did, actorType)
}

// TestAccountTakedown_BridgedPersonsBlobsAreWithheldUntilTheLabelLifts (B8):
// an account `!takedown` on a bridged person's DID makes Covers refuse the
// person's stored image and an image stored for that DID afterwards; when the
// label lifts (negation, or its exp lapsing and ApplyLapsed) every CID of the
// DID serves again except one an operator withhold still covers. The same
// label on a bridged community (Group) DID writes no account-wide withhold for
// the community's own blobs (community-scope withholds are task 05's).
func TestAccountTakedown_BridgedPersonsBlobsAreWithheldUntilTheLabelLifts(t *testing.T) {
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	communityDID := testDIDFor("technology", "lemmy.world")

	for _, tc := range []struct {
		name string
		// ban commits L on authorDID at seq 1; lift lifts it.
		ban  func(t *testing.T, rig *labelRig) (exp string)
		lift func(t *testing.T, rig *labelRig, exp string)
	}{
		{
			name: "negated",
			ban: func(t *testing.T, rig *labelRig) string {
				rig.commit(t, 1, labeling.Label{URI: authorDID, Val: "!takedown"})
				return ""
			},
			lift: func(t *testing.T, rig *labelRig, _ string) {
				rig.commit(t, 2, labeling.Label{URI: authorDID, Val: "!takedown", Negated: negated()})
			},
		},
		{
			name: "exp lapses",
			ban: func(t *testing.T, rig *labelRig) string {
				exp := dbLabelExp(t, 2*time.Second)
				rig.commit(t, 1, labeling.Label{URI: authorDID, Val: "!takedown", ExpiresAt: &exp})
				return exp
			},
			lift: func(t *testing.T, rig *labelRig, exp string) {
				awaitDBClockPast(t, exp)
				applied, err := rig.store.ApplyLapsed(context.Background())
				require.NoError(t, err)
				assert.Equal(t, 1, applied, "the lapsed account label is applied once")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.serveLemmyWorldFixtures()
			rig := newLabelRig(t, h)
			withholds := blobwithhold.NewStore(testutil.DB(t))
			const firstPostID = "https://lemmy.world/post/b8-account-label-first"
			const laterPostID = "https://lemmy.world/post/b8-account-label-later"
			const operatorPostID = "https://lemmy.world/post/b8-account-label-operator"

			materializeImagePost(t, h, firstPostID, "/media/b8-account-first.png", []byte("b8-account-label-first-image"))
			materializeImagePost(t, h, operatorPostID, "/media/b8-account-operator.png", []byte("b8-account-label-operator-image"))
			requireActorType(t, h, authorDID, store.ActorTypePerson)
			first := embedImageRef(t, h.recordFor(t, firstPostID)).CID
			operatorHeld := embedImageRef(t, h.recordFor(t, operatorPostID)).CID
			require.False(t, covered(t, withholds, authorDID, first), "setup: nothing withholds the image yet")
			requireActorType(t, h, communityDID, store.ActorTypeGroup)
			otherRef, err := h.manager.PutBlob(context.Background(), communityDID, "image/png", []byte("b8-account-label-other-did-image"))
			require.NoError(t, err, "setup: store a blob under a different DID")
			otherDIDImage := cid.Cid(otherRef.Ref).String()

			exp := tc.ban(t, rig)
			assert.True(t, covered(t, withholds, authorDID, first), "the account label withholds the person's stored image")
			assert.True(t, covered(t, withholds, authorDID, operatorHeld), "the account label withholds every CID of the DID")
			assert.False(t, covered(t, withholds, communityDID, otherDIDImage),
				"the account label withholds no blob of a different DID")

			materializeImagePost(t, h, laterPostID, "/media/b8-account-later.png", []byte("b8-account-label-later-image"))
			later := embedImageRef(t, h.recordFor(t, laterPostID)).CID
			assert.True(t, covered(t, withholds, authorDID, later), "an image stored for the DID after the label is withheld")

			operatorWithhold(t, withholds, operatorHeld)

			tc.lift(t, rig, exp)
			assert.False(t, covered(t, withholds, authorDID, first), "after the lift the stored image serves again")
			assert.False(t, covered(t, withholds, authorDID, later), "after the lift the later image serves again")
			assert.True(t, covered(t, withholds, authorDID, operatorHeld), "the operator withhold still covers its CID")
			assert.Equal(t, []string{blobwithhold.SourceOperator}, activeSourcesOfCID(t, operatorHeld),
				"the lift leaves only the operator withhold active on its CID")
		})
	}

	t.Run("a bridged community DID gets no withhold on its own blobs", func(t *testing.T) {
		h := newHarness(t)
		h.serveLemmyWorldFixtures()
		rig := newLabelRig(t, h)
		withholds := blobwithhold.NewStore(testutil.DB(t))
		ctx := context.Background()
		const postID = "https://lemmy.world/post/b8-account-label-group"

		materializeImagePost(t, h, postID, "/media/b8-account-group.png", []byte("b8-account-label-group-post-image"))
		requireActorType(t, h, communityDID, store.ActorTypeGroup)
		ref, err := h.manager.PutBlob(ctx, communityDID, "image/png", []byte("b8-account-label-group-own-image"))
		require.NoError(t, err, "setup: store a blob under the community's DID")
		groupImage := cid.Cid(ref.Ref).String()

		rig.commit(t, 1, labeling.Label{URI: communityDID, Val: "!takedown"})
		assert.False(t, covered(t, withholds, communityDID, groupImage), "a community DID's label withholds none of its own blobs")
		assert.Empty(t, activeSourcesOfCID(t, groupImage), "no withhold row is written for the community's blob")
	})
}

// TestAccountTakedown_StoredLabelCoversABridgedPersonOnceActionable (B15): a
// stored account `!takedown` takes effect on a bridged person without another
// frame: (a) the label arrives while the DID is unknown, then the person is
// minted through the materializer's bridged-actor insertion path and its image
// is covered; (b) the label was committed by a store without the materializer
// applier, and a consumer start (activation before subscribe) with the person
// existing covers its image before any new frame arrives.
func TestAccountTakedown_StoredLabelCoversABridgedPersonOnceActionable(t *testing.T) {
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")

	t.Run("label stored before the person is minted", func(t *testing.T) {
		h := newHarness(t)
		h.serveLemmyWorldFixtures()
		rig := newLabelRig(t, h)
		withholds := blobwithhold.NewStore(testutil.DB(t))
		const postID = "https://lemmy.world/post/b15-account-label-before-mint"

		_, err := h.actors.GetByDID(context.Background(), authorDID)
		require.True(t, errors.IsNotFound(err), "setup: the DID is unknown when the label arrives, got %v", err)
		rig.commit(t, 1, labeling.Label{URI: authorDID, Val: "!takedown"})

		materializeImagePost(t, h, postID, "/media/b15-before-mint.png", []byte("b15-account-label-before-mint-image"))
		requireActorType(t, h, authorDID, store.ActorTypePerson)
		image := embedImageRef(t, h.recordFor(t, postID)).CID
		assert.True(t, covered(t, withholds, authorDID, image),
			"the person minted after its stored account label has its image withheld")
	})

	t.Run("label committed without the applier, activated at start", func(t *testing.T) {
		h := newHarness(t)
		h.serveLemmyWorldFixtures()
		rig := newLabelRig(t, h)
		withholds := blobwithhold.NewStore(testutil.DB(t))
		const postID = "https://lemmy.world/post/b15-account-label-activation"

		materializeImagePost(t, h, postID, "/media/b15-activation.png", []byte("b15-account-label-activation-image"))
		requireActorType(t, h, authorDID, store.ActorTypePerson)
		image := embedImageRef(t, h.recordFor(t, postID)).CID

		// Task 02's consumer: the same tables, no materializer applier.
		withoutApplier := labels.NewPostgresStore(testutil.DB(t))
		require.NoError(t, withoutApplier.Commit(context.Background(), labels.Cursor{Epoch: 0, Seq: 1},
			rig.signed(t, labeling.Label{URI: authorDID, Val: "!takedown"})))
		require.False(t, covered(t, withholds, authorDID, image), "setup: a store without the applier withholds nothing")

		server := labelstest.NewServer(t, func(int64) labelstest.Reply { return labelstest.Reply{Hold: true} })
		returned, _ := startLabelConsumer(t, rig.store,
			staticLabeler{endpoint: server.URL, key: labelstest.PublicKey(t, rig.key)})
		cursor, ok := awaitLabelConnection(server, returned)
		require.True(t, ok, "the consumer connects")
		assert.Equal(t, int64(1), cursor, "the consumer resumes from the stored cursor")
		assert.True(t, covered(t, withholds, authorDID, image),
			"the stored account label covers the person's image before any new frame")
	})
}
