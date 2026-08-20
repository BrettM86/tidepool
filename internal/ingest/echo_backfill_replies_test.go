package ingest

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/outbound"
	"tidepool/internal/personas"
	"tidepool/internal/store"
)

// THE REPLIES WALK IS THE OUTBOX WALK'S TWIN, AND UNPINNED UNTIL NOW.
//
// backfillReplies calls MaterializeComment directly — no envelope, no
// dispatcher guard — and a Lemmy post's replies collection holds OUR federated
// comments the moment a native user replies in the thread. The suppressEcho
// call at that site is the only thing standing between an ordinary re-backfill
// and the same double-materialization the outbox test pins
// (TestBackfillDoesNotReMaterializeOurOwnContent): a bridged actor minted for
// our own persona, and our bridge-origin mapping rewritten to origin=fediverse,
// which disables the echo guard for that comment forever.
//
// The failure DIRECTION at that site is the second half. suppressEcho's
// contract says a classification that cannot be made propagates as an error so
// the run stays resumable — and the outbox path honors it (the error feeds the
// run's failures counter, last_backfill_at stays unset, the next trigger
// re-walks). The reply walk must honor it the same way: a Warn-and-skip there
// turns a transient DB blip into a genuine Lemmy reply silently dropped
// forever, because the "clean" completion stamps the freshness window that
// blocks the re-walk.
const (
	brUserOrigin = "https://coves.social"
	brAuthorDID  = "did:plc:brbackfillreplier1"
	brActorID    = brUserOrigin + "/ap/actor/" + brAuthorDID
	brReplyRKey  = "3lzbrreply00001"
	brReplyATURI = "at://" + brAuthorDID + "/social.coves.community.comment/" + brReplyRKey
	brReplyAPID  = brUserOrigin + "/ap/object/" + brAuthorDID +
		"/social.coves.community.comment/" + brReplyRKey
	brReplyCID = "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4"

	// The genuine Lemmy reply sharing the collection (serveOutboxFixtures
	// serves its author, /u/replier).
	brLemmyReplyID = "https://lemmy.world/comment/3001"
)

// TestBackfillRepliesDoNotReMaterializeOurOwnContent is the replies-collection
// twin of TestBackfillDoesNotReMaterializeOurOwnContent.
//
// The post's replies collection carries BOTH kinds of history — a comment we
// federated out and a comment a Lemmy human wrote — because that is what a
// bridged thread looks like once anyone native has replied. The walk must skip
// the first and materialize the second.
func TestBackfillRepliesDoNotReMaterializeOurOwnContent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)

	// Our origin serves the comment for real: it is cross-authority with the
	// replies collection's host, so without the guard the walk RE-FETCHES it —
	// and that fetch would succeed. The bypass is not a fixture artifact.
	userOrigin, err := personas.New(personas.Options{
		DB:         h.db,
		Custodian:  h.custodian,
		UserOrigin: brUserOrigin,
	})
	require.NoError(t, err)
	selfFetches := &atomic.Int64{}
	h.mux.Handle("/ap/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		selfFetches.Add(1)
		userOrigin.ServeHTTP(w, r)
	}))
	_, err = userOrigin.CreateActorForDID(ctx, brAuthorDID, "brreplier.coves.social")
	require.NoError(t, err)

	// The comment federates through the real Enqueuer with the CommentIntent
	// the Jetstream consumer builds — this is what writes the bridge-origin
	// ap_objects mapping the classifier answers from.
	enqueuer, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         h.db,
		Translator: outbound.NewTranslator(brUserOrigin),
		Inboxes:    outbound.NewInboxResolver(h.client, time.Minute),
		Actors:     store.NewAPActors(h.db),
		UserOrigin: brUserOrigin,
	})
	require.NoError(t, err)
	enqueueAs(t, h.db, enqueuer, brAuthorDID, consume.CommentIntent{
		Op:            "create",
		ATURI:         brReplyATURI,
		ID:            consume.ActivityID(brUserOrigin, brReplyATURI, "create", 0),
		CommunityAPID: groupID,
		ParentAPID:    pageID,
		Snapshot:      brSnapshot(t),
	})
	ourMapping, err := h.objects.GetByAPID(ctx, brReplyAPID)
	require.NoError(t, err, "precondition: our comment is mapped bridge-origin")
	require.Equal(t, store.OriginBridge, ourMapping.Origin)

	// The post's replies collection: our federated comment, then a genuine
	// Lemmy reply — re-served over the fixture registered above.
	h.serveObject("/post/49131386/replies", map[string]any{
		"type":       "OrderedCollection",
		"id":         pageID + "/replies",
		"totalItems": 2,
		"orderedItems": []any{
			note(brReplyAPID, brActorID, pageID, "a native reply already federated out",
				"2026-08-13T10:00:00.000000Z"),
			note(brLemmyReplyID, "https://lemmy.world/u/replier", pageID, "a backfilled reply",
				"2026-07-07T05:00:00.000000Z"),
		},
	})

	before := dropSnapshot()
	selfFetches.Store(0)

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	backfill := newBackfill(t, h, 10)
	require.NoError(t, backfill.Run(ctx, community, true),
		"walking past our own reply is a SKIP, not an error: an errored item would leave the "+
			"community's backfill retrying forever over its own expected history")

	// --- Our own comment: untouched. ---
	_, err = h.actors.GetByAPActorID(ctx, brActorID)
	assert.True(t, errors.IsNotFound(err),
		"NO bridged actor may be minted for our own persona: MaterializeComment calls "+
			"EnsureActor before it reads any mapping, so an ordinary re-backfill is a mint "+
			"oracle for every native user who has replied (err=%v)", err)

	after, err := h.objects.GetByAPID(ctx, brReplyAPID)
	require.NoError(t, err)
	assert.Equal(t, store.OriginBridge, after.Origin,
		"our mapping must stay origin=bridge: re-materializing rewrites it to the default "+
			"origin=fediverse, and the classifier then answers ClassNone for this id forever "+
			"— the echo guard silently disabling itself for the comment")
	assert.Equal(t, ourMapping.ATURI, after.ATURI, "and it must still name the record we federated")
	assert.Equal(t, int64(0), selfFetches.Load(),
		"our own comment must not even be dereferenced: the reply walk has no envelope, so "+
			"the id itself is the only thing that can stop it")

	// --- The genuine Lemmy reply in the SAME collection: backfilled normally. ---
	lemmyMapping, err := h.objects.GetByAPID(ctx, brLemmyReplyID)
	require.NoError(t, err,
		"the thread's real replies must still backfill: suppression here must key on whose "+
			"comment it is, never on 'this collection contains something of ours'")
	assert.Equal(t, store.OriginFediverse, lemmyMapping.Origin)
	assert.Equal(t, materialize.CollectionComment, lemmyMapping.Collection)

	// A skipped echo is not a failure: the run is a clean completion.
	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.NotNil(t, community.LastBackfillAt,
		"a run that only walked past our own reply still stamps last_backfill_at")

	// Attribution: the drop is counted like every other, under the class that
	// identified it (the object id is ours).
	assertOnlyClassMoved(t, before, echo.ClassMappedObject,
		"an uncounted drop on the reply walk is exactly how a bypass would stay invisible")
}

func brSnapshot(t *testing.T) []byte {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      brReplyATURI,
		"cid":        brReplyCID,
		"rev":        "3lzbrrev0000001",
		"collection": "social.coves.community.comment",
		"record": map[string]any{
			"$type":     "social.coves.community.comment",
			"content":   "a native reply in a Lemmy thread",
			"createdAt": "2026-08-13T10:00:00.000Z",
		},
		"parentApId":    pageID,
		"communityApId": groupID,
	})
	require.NoError(t, err)
	return snap
}

// flakyReplyClassifier fails classification for exactly one object id until
// recovered, delegating everything else to the real classifier — so the
// outbox pass stays healthy and the failure lands inside the reply walk,
// which is the site under test.
type flakyReplyClassifier struct {
	inner  EchoClassifier
	failID string

	mu  sync.Mutex
	err error
}

func (f *flakyReplyClassifier) Classify(ctx context.Context, node *ap.Object) (echo.Identity, error) {
	f.mu.Lock()
	err := f.err
	f.mu.Unlock()
	if err != nil && node.ID == f.failID {
		return echo.Identity{Class: echo.ClassNone}, err
	}
	return f.inner.Classify(ctx, node)
}

func (f *flakyReplyClassifier) recover() {
	f.mu.Lock()
	f.err = nil
	f.mu.Unlock()
}

// TestBackfillReplyClassifierFailureLeavesResumable is the reply walk's half of
// the fail-safe direction (TestEchoClassificationFailureRetriesAndRecovers pins
// the dispatcher's).
//
// A classifier that cannot answer for one reply has produced no verdict about
// it, so the run must NOT complete cleanly: the failure surfaces the way an
// outbox-item failure does, last_backfill_at stays unset, and the next
// un-forced trigger actually re-walks — where a healthy classifier answers for
// real and the genuine reply finally lands. The wrong answer available here is
// quieter than the dispatcher's: a Warn-and-skip stamps the freshness window,
// and "heals on the next re-seed" means never.
func TestBackfillReplyClassifierFailureLeavesResumable(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)

	flaky := &flakyReplyClassifier{
		inner:  h.classifier,
		failID: brLemmyReplyID,
		err:    stderrors.New("connection reset by peer"),
	}
	backfill, err := NewBackfill(BackfillOptions{
		Fetcher:      h.client,
		Materializer: h.mat,
		Communities:  h.communities,
		Tombstones:   h.tombstones,
		Echo:         flaky,
		MaxPosts:     10,
	})
	require.NoError(t, err)

	dropsBefore := dropSnapshot()
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.Error(t, backfill.Run(ctx, community, true),
		"a classification that could not be MADE is not a verdict about the reply: the run "+
			"must surface a failure, the way an outbox-item failure surfaces")

	// The failing attempt dropped nothing silently and decided nothing.
	_, err = h.objects.GetByAPID(ctx, brLemmyReplyID)
	assert.True(t, errors.IsNotFound(err),
		"the unclassifiable reply must not materialize on the failing attempt (err=%v)", err)
	for _, class := range echoClasses {
		assert.Equal(t, dropsBefore[class], echo.Drops(class),
			"no drop counter may move (%s): a failure is not a suppression, and counting it "+
				"as one would hide the outage inside the metric that reports on the guard", class)
	}
	// The healthy items still landed — the failure is resumable-by-redo, not
	// all-or-nothing.
	_, err = h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err, "the post carrying the replies collection still materializes")

	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.Nil(t, community.LastBackfillAt,
		"last_backfill_at must stay unset: stamping it puts the dropped reply behind the "+
			"freshness window, and 'heals on the next re-seed' means never")

	// The retry is the whole point: with the classifier healthy again, an
	// UN-FORCED re-trigger re-walks (nil last_backfill_at) and the genuine
	// reply finally lands.
	flaky.recover()
	require.NoError(t, backfill.Run(ctx, community, false))

	mapping, err := h.objects.GetByAPID(ctx, brLemmyReplyID)
	require.NoError(t, err,
		"the re-run materializes the genuine Lemmy reply — which is what makes the failure "+
			"the recoverable direction")
	assert.Equal(t, materialize.CollectionComment, mapping.Collection)
	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.NotNil(t, community.LastBackfillAt, "the recovered run is a clean completion")
}
