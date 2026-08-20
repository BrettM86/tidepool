package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
	"tidepool/internal/echo"
	"tidepool/internal/materialize"
	"tidepool/internal/outbound"
	"tidepool/internal/store"
	"tidepool/internal/votes"
)

// The vote half of task 17a, driven through the real inbound path with the
// REAL aggregator behind the dispatcher.
//
// Two guards, two questions, and they are not redundant:
//
//   - the ingest classifier asks an ENVELOPE question — "is this announced
//     traffic ours?" — at the dispatch boundary, and it only ever sees
//     handleAnnounce;
//   - the aggregator asks a VOTER question — "may this voter appear in
//     vote_events?" — at the mutation site, which is the ONLY guard on the
//     paths that never reach handleAnnounce: Process's bare Like/Dislike
//     branch, the community-outbox backfill, and the seeder.
//
// M2 exercises the second on the path the first cannot see.
const (
	evUserOrigin  = "https://coves.social"
	evPersonaDID  = "did:plc:evpersona000000001"
	evPersonaID   = evUserOrigin + "/ap/actor/" + evPersonaDID
	evVoteATURI   = "at://" + evPersonaDID + "/social.coves.interaction.vote/3lzevvote0001"
	evLemmyVoter  = personID
	evVoteSubject = pageID
)

// withRealAggregator puts the production vote aggregator behind the harness's
// recorder, wired with the same voter probe production uses.
func withRealAggregator(t *testing.T, h *harness) {
	t.Helper()
	probe, err := echo.New(echo.Options{
		Objects:         h.objects,
		OutboundObjects: store.NewOutboundObjects(h.db),
		Activities:      store.NewOutboundActivities(h.db),
		Actors:          store.NewAPActors(h.db),
	})
	require.NoError(t, err)
	aggregator, err := votes.NewAggregator(h.db, h.objects, h.communities, h.manager, probe,
		slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	require.NoError(t, err)
	h.votes.mu.Lock()
	h.votes.delegate = aggregator
	h.votes.mu.Unlock()
}

// mintPersona creates the native persona's ap_actors row and serves an actor
// document for it, so activities attributed to that persona can be delivered
// with a verifiable signature.
func mintPersona(t *testing.T, h *harness) *remoteActor {
	t.Helper()
	_, err := store.NewAPActors(h.db).Create(context.Background(), store.APActor{
		DID:              evPersonaDID,
		Kind:             store.ActorTypePerson,
		ActorID:          evPersonaID,
		NormalizedOrigin: "coves.social",
		LocalPart:        "evpersona",
		RSAKeySealed:     []byte{0x01, 0x02, 0x03},
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err, "mint the native persona")
	return h.newRemoteActor(evPersonaID, person(evPersonaID, "evpersona", nil))
}

// voteRows counts vote_events rows for one voter, undone included.
func voteRows(t *testing.T, h *harness, voter string) int {
	t.Helper()
	var n int
	require.NoError(t, h.db.QueryRow(
		`SELECT COUNT(*) FROM vote_events WHERE voter_ap_id = $1`, voter).Scan(&n))
	return n
}

// aggregateOf reads the served totals for a subject.
func aggregateOf(t *testing.T, h *harness, subject string) (up, down int, found bool) {
	t.Helper()
	err := h.db.QueryRow(
		`SELECT upvotes, downvotes FROM vote_aggregates WHERE subject_ap_id = $1`, subject).
		Scan(&up, &down)
	if err != nil {
		return 0, 0, false
	}
	return up, down, true
}

// bridgeLemmyPost materializes the captured lemmy.world page so it is a votable
// subject, and returns the announcing community.
func bridgeLemmyPost(t *testing.T, h *harness) *remoteActor {
	t.Helper()
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()
	mapping, err := h.objects.GetByAPID(context.Background(), pageID)
	require.NoError(t, err, "precondition: the subject must be bridged, or votes drop for the wrong reason")
	require.Equal(t, materialize.CollectionPostV2, mapping.Collection)
	return group
}

// TestBareEchoedVoteFromOurPersonaNeverCounts is M2: the path the envelope
// classifier cannot see. Process dispatches a bare Like/Dislike straight to the
// aggregator — no Announce, no envelope — so the only thing standing between
// our own vote and vote_events is the voter probe.
func TestBareEchoedVoteFromOurPersonaNeverCounts(t *testing.T) {
	h := newHarness(t)
	bridgeLemmyPost(t, h)
	withRealAggregator(t, h)
	persona := mintPersona(t, h)
	lemmyVoter := h.newRemoteActor(evLemmyVoter, person(evLemmyVoter, "LeftLeaningFreedomFighters", nil))

	require.Equal(t, http.StatusAccepted, h.deliver(persona, map[string]any{
		"id":     evUserOrigin + "/ap/activity/bare-echo-like",
		"type":   "Like",
		"actor":  evPersonaID,
		"object": evVoteSubject,
	}))
	require.Equal(t, http.StatusAccepted, h.deliver(persona, map[string]any{
		"id":     evUserOrigin + "/ap/activity/bare-echo-dislike",
		"type":   "Dislike",
		"actor":  evPersonaID,
		"object": evVoteSubject,
	}))
	h.drain()

	assert.Equal(t, 0, voteRows(t, h, evPersonaID),
		"a vote cast BY ONE OF OUR PERSONAS must never reach vote_events, however it arrives: "+
			"the bare branch has no envelope for the classifier to read")
	_, _, found := aggregateOf(t, h, evVoteSubject)
	assert.False(t, found,
		"and no aggregate row: minting a 0/0 row tells the XRPC contract this subject has votes")

	// Control: the identical bare shape from a real Lemmy human IS counted, so
	// the assertions above cannot be passing because the path is dead.
	require.Equal(t, http.StatusAccepted, h.deliver(lemmyVoter, map[string]any{
		"id":     "https://lemmy.world/activities/like/genuine-bare",
		"type":   "Like",
		"actor":  evLemmyVoter,
		"object": evVoteSubject,
	}))
	h.drain()
	assert.Equal(t, 1, voteRows(t, h, evLemmyVoter), "a genuine bare vote still counts")
	up, down, found := aggregateOf(t, h, evVoteSubject)
	require.True(t, found)
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)
}

// TestNativeVoteRoundTripLeavesNoVoteEvents is M6, the invariant 17b's seeder
// subtraction is built on: after a native vote goes out and the community
// announces it back, vote_events holds ZERO rows for that persona — so the
// baseline the seeder subtracts from is the ONLY place our own vote is
// represented. If this does not hold, 17b double-subtracts.
//
// It also pins that the two guards are independent: the same echoed vote handed
// straight to the aggregator (the backfill/seed path, which never passes through
// handleAnnounce) must be refused there too.
func TestNativeVoteRoundTripLeavesNoVoteEvents(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	group := bridgeLemmyPost(t, h)
	withRealAggregator(t, h)
	mintPersona(t, h)

	// The native vote goes out through the REAL enqueuer: its payload is the
	// exact body the community echoes back.
	enqueuer, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         h.db,
		Translator: outbound.NewTranslator(evUserOrigin),
		Inboxes:    outbound.NewInboxResolver(h.client, time.Minute),
		Actors:     store.NewAPActors(h.db),
		UserOrigin: evUserOrigin,
	})
	require.NoError(t, err)
	voteIntent := consume.VoteIntent{
		Op:            "create",
		VoteATURI:     evVoteATURI,
		SubjectAPID:   evVoteSubject,
		Direction:     "up",
		ID:            consume.ActivityID(evUserOrigin, evVoteATURI, "create", 0),
		CommunityAPID: groupID,
	}
	enqueueAs(t, h.db, enqueuer, evPersonaDID, voteIntent)

	stored, err := store.NewOutboundActivities(h.db).Get(ctx, voteIntent.ID)
	require.NoError(t, err)
	var like map[string]any
	require.NoError(t, json.Unmarshal(stored.Payload, &like))
	require.Equal(t, "Like", like["type"])

	// WHEN: the community announces our own Like straight back.
	require.Equal(t, http.StatusAccepted, h.deliver(group,
		echoAnnounce("https://lemmy.world/activities/announce/like/native-round-trip", like)))
	h.drain()

	assert.Equal(t, 0, voteRows(t, h, evPersonaID),
		"the 17b PRECONDITION: after a full round trip, vote_events must hold no row for "+
			"our persona — the seeder's subtraction assumes our vote lives only in the "+
			"remote baseline")
	_, _, found := aggregateOf(t, h, evVoteSubject)
	assert.False(t, found, "no aggregate may be minted by our own vote coming home")

	// The SAME echo handed directly to the aggregator — the backfill and seed
	// paths do exactly this, and no envelope classifier is in front of them.
	parsedLike, err := ap.ParseObject(stored.Payload)
	require.NoError(t, err)
	require.NoError(t, h.votes.delegate.ApplyVote(ctx, parsedLike, groupID),
		"an echoed vote is dropped, not an error")
	assert.Equal(t, 0, voteRows(t, h, evPersonaID),
		"the aggregator's own voter guard must refuse it too: the classifier never sees "+
			"the backfill or the seeder")
	_, _, found = aggregateOf(t, h, evVoteSubject)
	assert.False(t, found, "and still no aggregate")
}
