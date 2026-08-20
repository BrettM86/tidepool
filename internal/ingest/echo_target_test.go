package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/outbound"
	"tidepool/internal/store"
)

// TARGET vs PAYLOAD — the shape every other test in this suite missed.
//
// An activity's `object` means two different things depending on the verb:
//
//   - for the WRAPPER verbs (Announce, Create, Update, Undo) it is the PAYLOAD:
//     the activity or object being carried, and the thing the envelope is
//     really about;
//   - for Like, Dislike, Delete, Flag, Block and Remove it is the TARGET: what
//     somebody else's activity is being done TO.
//
// A walk that descends unconditionally reaches the TARGET of a genuine remote
// activity and answers "ours" for the whole envelope — because the target is
// ours. That is the single most common interaction in the product: a fediverse
// user acting on content a Coves user wrote.
//
// Every "genuine remote traffic survives" control we wrote before this one
// targets LEMMY-origin content (W5 votes on a lemmy post, the mod removal of a
// lemmy page). They prove remote traffic survives only when it never touches our
// content, which is exactly the case that cannot break. These tests construct
// the case that can.
//
// The echoes the guards exist for do NOT need the descent: an echoed Delete is
// ours by its OWN activity id at depth 2, and an echoed vote by its ACTOR at
// depth 2. Nothing below a target-bearing verb may decide the envelope.
const (
	tgUserOrigin = "https://coves.social"
	tgAuthorDID  = "did:plc:tgtargetauthor0001"
	tgActorID    = tgUserOrigin + "/ap/actor/" + tgAuthorDID
	tgPostRKey   = "3lztargetpost01"
	tgPostATURI  = "at://" + tgAuthorDID + "/social.coves.community.postv2/" + tgPostRKey
	tgPostAPID   = tgUserOrigin + "/ap/object/" + tgAuthorDID +
		"/social.coves.community.postv2/" + tgPostRKey
	tgPostCID = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"

	// The fediverse humans acting on it: a voter and a moderator, both real
	// people on lemmy.world with no ap_actors row anywhere.
	tgVoter = personID
)

// targetWorld is a native post federated into the bridged community, with the
// acceptance the admission wrote — the state a remote vote or removal arrives
// into.
type targetWorld struct {
	group        *remoteActor
	voter        *remoteActor
	communityDID string
	digestRKey   string
}

func setupTargetWorld(t *testing.T, h *harness) targetWorld {
	t.Helper()
	ctx := context.Background()
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	voter := h.newRemoteActor(tgVoter, person(tgVoter, "LeftLeaningFreedomFighters", nil))
	communityDID := testDIDFor("technology", "lemmy.world")

	_, err := store.NewAPActors(h.db).Create(ctx, store.APActor{
		DID:              tgAuthorDID,
		Kind:             store.ActorTypePerson,
		ActorID:          tgActorID,
		NormalizedOrigin: "coves.social",
		LocalPart:        "tgauthor",
		RSAKeySealed:     []byte{0x01, 0x02, 0x03},
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err)
	_, err = store.NewOutboundObjects(h.db).Upsert(ctx, store.OutboundObject{
		ATURI:              tgPostATURI,
		APObjectID:         tgPostAPID,
		LastCID:            tgPostCID,
		LastRev:            "3lztgrev000001",
		CommunityDID:       communityDID,
		CommunityAPID:      groupID,
		TranslatedSnapshot: tgSnapshot(t),
	})
	require.NoError(t, err)

	enqueuer, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         h.db,
		Translator: outbound.NewTranslator(tgUserOrigin),
		Inboxes:    outbound.NewInboxResolver(h.client, time.Minute),
		Actors:     store.NewAPActors(h.db),
		UserOrigin: tgUserOrigin,
	})
	require.NoError(t, err)
	enqueueAs(t, h.db, enqueuer, tgAuthorDID, consume.PostIntent{
		Op:            "create",
		ATURI:         tgPostATURI,
		ID:            consume.ActivityID(tgUserOrigin, tgPostATURI, "create", 0),
		CommunityAPID: groupID,
		Snapshot:      tgSnapshot(t),
	})

	// The mapping carries its community, the way 17c will leave it.
	//
	// SECOND LATENT BUG, worth recording: today's enqueuer writes no
	// community_did (enqueuer.go:196-205), and CommunityDIDOf then falls
	// through to reading the postv2 out of the AUTHOR's repo — which the bridge
	// does not host. So an announced vote on a native post fails
	// subjectBelongsToCommunity, and an announced removal fails
	// authorizeAnnouncedContentDelete, INDEPENDENTLY of the classifier. Both
	// paths need this column, so the fixture supplies it; otherwise these tests
	// would be unsatisfiable even after the classifier is fixed.
	mapping, err := h.objects.GetByAPID(ctx, tgPostAPID)
	require.NoError(t, err)
	require.Equal(t, store.OriginBridge, mapping.Origin)
	mapping.CommunityDID = communityDID
	_, err = h.objects.PutMapping(ctx, *mapping)
	require.NoError(t, err)

	digest := testDigestRKey(tgPostATURI)
	_, err = h.manager.PutRecord(ctx, communityDID, materialize.CollectionAcceptance, digest,
		map[string]any{
			"$type":     materialize.CollectionAcceptance,
			"subject":   map[string]any{"uri": tgPostATURI, "cid": tgPostCID},
			"createdAt": "2026-08-13T09:00:00.000Z",
		})
	require.NoError(t, err)

	return targetWorld{group: group, voter: voter, communityDID: communityDID, digestRKey: digest}
}

func tgSnapshot(t *testing.T) []byte {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      tgPostATURI,
		"cid":        tgPostCID,
		"rev":        "3lztgrev000001",
		"collection": "social.coves.community.postv2",
		"record": map[string]any{
			"$type":     "social.coves.community.postv2",
			"community": testDIDFor("technology", "lemmy.world"),
			"title":     "A native post the fediverse votes on and moderates",
			"content":   "the target, not the payload",
			"createdAt": "2026-08-13T09:00:00.000Z",
		},
		"communityApId": groupID,
	})
	require.NoError(t, err)
	return snap
}

// liveVoteRows counts the voter's live (non-undone) vote_events rows.
func liveVoteRows(t *testing.T, h *harness, voter string) int {
	t.Helper()
	var n int
	require.NoError(t, h.db.QueryRow(
		`SELECT COUNT(*) FROM vote_events WHERE voter_ap_id = $1 AND NOT undone`, voter).Scan(&n))
	return n
}

// TestFediverseVotesOnOurFederatedPostAreCounted: the highest-volume genuine
// interaction in the product. A Lemmy human upvotes a post a Coves user wrote;
// the vote's TARGET is our object, its actor and activity id are theirs.
//
// If the envelope walk descends into that target, every vote on every native
// post is discarded and their tallies sit at zero forever — with no error, no
// dead letter, and a counter that says the bridge is working.
func TestFediverseVotesOnOurFederatedPostAreCounted(t *testing.T) {
	h := newHarness(t)
	world := setupTargetWorld(t, h)
	withRealAggregator(t, h)
	before := dropSnapshot()

	// Each shape is its own subtest: they run in sequence (an Undo needs a live
	// vote), but a failure in one must not hide the others — the four shapes
	// reach the guard by four different routes.

	t.Run("announced Like (depth 3)", func(t *testing.T) {
		require.Equal(t, http.StatusAccepted, h.deliver(world.group,
			echoAnnounce("https://lemmy.world/activities/announce/like/tg-1", map[string]any{
				"id":       "https://lemmy.world/activities/like/tg-1",
				"type":     "Like",
				"actor":    tgVoter,
				"object":   tgPostAPID,
				"audience": groupID,
			})))
		h.drain()

		assert.Equal(t, 1, voteRows(t, h, tgVoter),
			"a Lemmy human's upvote on a NATIVE post must be recorded: our id is the TARGET "+
				"of their activity, not an activity of ours")
		up, _, found := aggregateOf(t, h, tgPostAPID)
		assert.True(t, found, "the aggregate must exist — this is the post's only score")
		assert.Equal(t, 1, up)
	})

	t.Run("announced Undo{Like} (depth 4)", func(t *testing.T) {
		require.Equal(t, http.StatusAccepted, h.deliver(world.group,
			echoAnnounce("https://lemmy.world/activities/announce/undo/tg-2", map[string]any{
				"id":    "https://lemmy.world/activities/undo/tg-2",
				"type":  "Undo",
				"actor": tgVoter,
				"object": map[string]any{
					"id":       "https://lemmy.world/activities/like/tg-regenerated",
					"type":     "Like",
					"actor":    tgVoter,
					"object":   tgPostAPID,
					"audience": groupID,
				},
			})))
		h.drain()
		// Asserted on the ROW, not only the total: a zero aggregate is also what
		// a vote that was never counted looks like, and this subtest is about
		// the retraction landing — not about the sum happening to be right.
		assert.GreaterOrEqual(t, voteRows(t, h, tgVoter), 1,
			"their vote must be on record before it can be withdrawn")
		assert.Equal(t, 0, liveVoteRows(t, h, tgVoter),
			"and the Undo must mark it undone — depth 4 (Announce{Undo{Like}}) is where "+
				"Lemmy sends retractions")
		up, _, _ := aggregateOf(t, h, tgPostAPID)
		assert.Equal(t, 0, up,
			"a vote that can be cast but not withdrawn leaves a score nobody can correct")
	})

	t.Run("bare Like", func(t *testing.T) {
		require.Equal(t, http.StatusAccepted, h.deliver(world.voter, map[string]any{
			"id":     "https://lemmy.world/activities/like/tg-bare",
			"type":   "Like",
			"actor":  tgVoter,
			"object": tgPostAPID,
		}))
		h.drain()
		up, _, _ := aggregateOf(t, h, tgPostAPID)
		assert.Equal(t, 1, up, "a bare Like on our object is still their vote")
	})

	t.Run("bare Undo{Like} through Process's guard", func(t *testing.T) {
		require.Equal(t, http.StatusAccepted, h.deliver(world.voter, map[string]any{
			"id":    "https://lemmy.world/activities/undo/tg-bare",
			"type":  "Undo",
			"actor": tgVoter,
			"object": map[string]any{
				"id":     "https://lemmy.world/activities/like/tg-bare",
				"type":   "Like",
				"actor":  tgVoter,
				"object": tgPostAPID,
			},
		}))
		h.drain()
		up, _, _ := aggregateOf(t, h, tgPostAPID)
		assert.Equal(t, 0, up,
			"the bare Undo branch runs the classifier BEFORE dispatch, so a walk that "+
				"descends into the vote's target strands every retraction on our own content")
	})

	// The mirror-image pin: none of this is an echo, so no counter may move.
	for _, class := range echoClasses {
		assert.Equal(t, before[class], echo.Drops(class),
			"no echo counter may move for a remote actor acting on OUR content (%s): a drop "+
				"here is invisible — the votes simply never appear", class)
	}
}

// TestModeratorRemovalOfNativePostIsHonored: a Lemmy moderator removing a post
// a Coves user wrote. The Delete's TARGET is our postv2; its actor is the
// moderator and its id is the community's.
//
// This is the transition 17c exists to deliver. If the walk descends into the
// target, the removal is dropped as an echo and no native post can ever be
// moderated by the community hosting it — while the M1 case (our OWN delete
// coming home) must stay dropped. The difference is target versus payload, and
// nothing else.
func TestModeratorRemovalOfNativePostIsHonored(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := setupTargetWorld(t, h)
	before := dropSnapshot()

	reason := "off topic for this community"
	h.announceDeleteWithSummary(world.group,
		"https://lemmy.world/activities/announce/delete/tg-removal", tgPostAPID, &reason)

	removal, _, err := h.manager.GetRecord(ctx,
		world.communityDID, materialize.CollectionRemoval, world.digestRKey)
	require.NoError(t, err,
		"a moderator's removal of a NATIVE post must be honored: our id is the TARGET of "+
			"the community's activity, and moderating bridged content is the whole point "+
			"of the acceptance model")
	assert.Equal(t, reason, removal["reason"])
	assert.Equal(t, "moderator-discretion", removal["code"])

	_, _, err = h.manager.GetRecord(ctx,
		world.communityDID, materialize.CollectionAcceptance, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"and the acceptance is withdrawn in the same commit (err=%v)", err)

	for _, class := range echoClasses {
		assert.Equal(t, before[class], echo.Drops(class),
			"no echo counter may move for genuine moderation (%s): counting it here would "+
				"also be the only evidence anyone ever sees of the content it swallowed", class)
	}
}
