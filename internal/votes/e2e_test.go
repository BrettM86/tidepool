package votes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/ingest"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// The aggregator is the real implementation behind task 06's seam.
var _ ingest.VoteAggregator = (*Aggregator)(nil)

// The seeder is what backfill's optional hook expects.
var _ ingest.CountSeeder = (*LemmySeeder)(nil)

const (
	e2eGroupID   = "https://lemmy.world/c/technology"
	e2eServiceID = "https://bridge.test/actor"
)

// stubMaterializer fails loudly if the dispatcher ever routes a vote to the
// materializer — votes must stay bridge-side.
type stubMaterializer struct{ t *testing.T }

func (s *stubMaterializer) MaterializePost(context.Context, *ap.Object) (*materialize.Result, error) {
	s.t.Fatal("votes must never reach MaterializePost")
	return nil, nil
}

func (s *stubMaterializer) MaterializeComment(context.Context, *ap.Object) (*materialize.Result, error) {
	s.t.Fatal("votes must never reach MaterializeComment")
	return nil, nil
}

func (s *stubMaterializer) HandleUpdate(context.Context, *ap.Object) (*materialize.Result, error) {
	s.t.Fatal("votes must never reach HandleUpdate")
	return nil, nil
}

func (s *stubMaterializer) HandleDelete(context.Context, string) error {
	s.t.Fatal("votes must never reach HandleDelete")
	return nil
}

func (s *stubMaterializer) HandleDeleteRecord(context.Context, string) error {
	s.t.Fatal("votes must never reach HandleDeleteRecord")
	return nil
}

func (s *stubMaterializer) RefreshActor(context.Context, *ap.Object) (*store.BridgedActor, error) {
	s.t.Fatal("votes must never reach RefreshActor")
	return nil, nil
}

func (s *stubMaterializer) RefreshCommunity(context.Context, *ap.Object) (*store.Community, error) {
	s.t.Fatal("votes must never reach RefreshCommunity")
	return nil, nil
}

func (s *stubMaterializer) EnsureCommunity(context.Context, *ap.Object) (*store.Community, error) {
	s.t.Fatal("votes must never reach EnsureCommunity")
	return nil, nil
}

// stubFetcher fails loudly on any fetch: inline vote activities must be
// dispatched without touching the network.
type stubFetcher struct{ t *testing.T }

func (s *stubFetcher) FetchObject(_ context.Context, iri string) (*ap.Object, error) {
	s.t.Errorf("unexpected fetch of %s while dispatching votes", iri)
	return nil, errors.NewNotFoundError("object", iri)
}

func (s *stubFetcher) FetchObjectSameAuthority(ctx context.Context, iri string) (*ap.Object, error) {
	return s.FetchObject(ctx, iri)
}

// deliverVote runs one activity through Handler.Process the way the queue
// worker does (signature verification and dedupe happened at the inbox; the
// handler receives the verified payload + bound actor).
func deliverVote(t *testing.T, handler *ingest.Handler, signer string, activity map[string]any) {
	t.Helper()
	payload, err := json.Marshal(activity)
	require.NoError(t, err)
	require.NoError(t, handler.Process(context.Background(), &store.InboxEvent{
		ActivityID: activity["id"].(string),
		Payload:    payload,
		ActorID:    signer,
	}))
}

// announceVote wraps a Like/Dislike in FEP-1b12 group fan-out exactly as
// Lemmy emits it (inline inner activity).
func announceVote(announceID string, inner map[string]any) map[string]any {
	return map[string]any{
		"id":       announceID,
		"type":     "Announce",
		"actor":    e2eGroupID,
		"to":       []any{ap.PublicAudience},
		"audience": e2eGroupID,
		"object":   inner,
	}
}

func inlineVote(voteType, id, voter, subject string) map[string]any {
	return map[string]any{
		"id":       id,
		"type":     voteType,
		"actor":    voter,
		"object":   subject,
		"audience": e2eGroupID,
	}
}

// TestFakeLemmyVoteE2E is the definition-of-done flow: Announce{Like} from
// three distinct voters + one Announce{Dislike} + one Announce{Undo{Like}},
// dispatched through the real ingest handler, must serve {up: 2, down: 1}
// over the XRPC side channel.
func TestFakeLemmyVoteE2E(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	postURI := bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	communities := store.NewCommunities(database)
	_, err := communities.UpsertCommunity(ctx, store.Community{
		APGroupID:         e2eGroupID,
		DID:               testDID,
		PreferredUsername: "technology",
		Instance:          testInstance,
	})
	require.NoError(t, err)
	require.NoError(t, communities.SetFollowState(ctx, e2eGroupID, store.FollowStateAccepted))

	handler, err := ingest.NewHandler(ingest.HandlerOptions{
		Materializer:   &stubMaterializer{t: t},
		Fetcher:        &stubFetcher{t: t},
		Objects:        objects,
		Actors:         store.NewBridgedActors(database),
		Communities:    communities,
		Tombstones:     store.NewTombstones(database),
		Records:        &fakeRecords{records: map[string]map[string]any{}},
		Votes:          agg,
		ServiceActorID: e2eServiceID,
	})
	require.NoError(t, err)

	base := "https://lemmy.world/activities"
	like1 := inlineVote("Like", base+"/like/1", voterAlice, subjectPost)
	deliverVote(t, handler, e2eGroupID, announceVote(base+"/announce/1", like1))
	deliverVote(t, handler, e2eGroupID, announceVote(base+"/announce/2",
		inlineVote("Like", base+"/like/2", voterBob, subjectPost)))
	deliverVote(t, handler, e2eGroupID, announceVote(base+"/announce/3",
		inlineVote("Like", base+"/like/3", voterCarol, subjectPost)))
	deliverVote(t, handler, e2eGroupID, announceVote(base+"/announce/4",
		inlineVote("Dislike", base+"/dislike/4", "https://lemmy.world/u/dan", subjectPost)))
	// Alice takes her upvote back: Announce{Undo{Like}} with the original
	// Like inlined, the shape Lemmy sends.
	deliverVote(t, handler, e2eGroupID, announceVote(base+"/announce/5", map[string]any{
		"id":     base + "/undo/5",
		"type":   "Undo",
		"actor":  voterAlice,
		"object": like1,
	}))

	// ... and the AppView reads the net result over the side channel.
	router := newXRPCRouter(t, database, 1000, 1000)
	rec := get(t, router, "uris="+url.QueryEscape(postURI))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out xrpcResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Aggregates, 1)
	assert.Equal(t, postURI, out.Aggregates[0].URI)
	assert.Equal(t, 2, out.Aggregates[0].Upvotes)
	assert.Equal(t, 1, out.Aggregates[0].Downvotes)
}

// TestBareVoteDispatch: a Like delivered directly (not group-announced)
// rides Handler.Process's bare-vote arm with communityIRI "".
func TestBareVoteDispatch(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")

	handler, err := ingest.NewHandler(ingest.HandlerOptions{
		Materializer:   &stubMaterializer{t: t},
		Fetcher:        &stubFetcher{t: t},
		Objects:        objects,
		Actors:         store.NewBridgedActors(database),
		Communities:    store.NewCommunities(database),
		Tombstones:     store.NewTombstones(database),
		Records:        &fakeRecords{records: map[string]map[string]any{}},
		Votes:          agg,
		ServiceActorID: e2eServiceID,
	})
	require.NoError(t, err)

	deliverVote(t, handler, voterAlice,
		inlineVote("Like", "https://lemmy.world/activities/like/bare-1", voterAlice, subjectPost))

	up, down, found := counts(t, database, subjectPost)
	require.True(t, found)
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)
}
