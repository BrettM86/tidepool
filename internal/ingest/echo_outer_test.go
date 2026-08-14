package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
	"tidepool/internal/echo"
	"tidepool/internal/outbound"
	"tidepool/internal/personas"
	"tidepool/internal/store"
)

// The native side of the world this acceptance test builds: a Coves user whose
// post was accepted into the bridged !technology@lemmy.world community and
// federated out under the coves.social user origin.
const (
	echoUserOrigin = "https://coves.social"

	echoAuthorDID    = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	echoAuthorHandle = "alice.coves.social"

	echoPostRKey  = "3lzecho2222aa"
	echoPostATURI = "at://" + echoAuthorDID + "/social.coves.community.postv2/" + echoPostRKey
	echoPostAPID  = echoUserOrigin + "/ap/object/" + echoAuthorDID +
		"/social.coves.community.postv2/" + echoPostRKey
	echoPostCID = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"

	echoVoteATURI = "at://" + echoAuthorDID + "/social.coves.interaction.vote/3lzechovote1a"
)

// echoClasses are the four classification counters task 17a splits drops into
// (false-positive detection depends on the split, so they are asserted as a
// vector, never as a total).
var echoClasses = []echo.Class{
	echo.ClassMappedObject,
	echo.ClassLocalActivity,
	echo.ClassLocalActor,
	echo.ClassAncestorShortCircuit,
}

// TestAnnouncedEchoOfOurOwnDeliveriesChangesNothing is the OUTER acceptance
// test for task 17a (echo suppression).
//
// GIVEN a native Coves user's post accepted into a bridged community and
// FEDERATED through the real enqueuer + delivery worker (so outbound_activities,
// its bridge-origin ap_objects mapping, outbound_objects and a delivered
// outbound_deliveries row all exist, and coves.social really serves those ids),
// plus a native vote delivered to the same community,
//
// WHEN the community announces our own activities back at the bridge inbox —
// the echo — in every envelope shape FEP-1b12 permits:
//
//	A. Announce{Create{Page}}  the embedded ACTIVITY (Lemmy's own shape)
//	B. Announce{Page}          the embedded OBJECT (implementations that
//	                           announce the object rather than its Create)
//	C. Announce{<activity IRI>} a BARE IRI
//	D. Announce{Like}          an echoed vote, whose object is the LEMMY
//	                           subject — only the inner ACTOR identifies it
//
// THEN each echo is DROPPED at the envelope: no new mapping, record, firehose
// op or outbound row; no vote reaches the aggregator; the bridge never dials its
// own origin to find out; the event is processed once (not retried, not
// poisoned); and the drop is OBSERVABLE — the counter for its class advances by
// exactly one and no other class moves.
//
// The GIVEN drives the real outbound.Enqueuer/Worker and the real
// personas.Service serving surface, so "ours" is decided against ids the bridge
// actually minted and actually serves — not against hand-written fixtures. The
// acceptance ENGINE (task 16) is represented by the PostIntent it produces:
// this test owns the inbound contract, not admission.
func TestAnnouncedEchoOfOurOwnDeliveriesChangesNothing(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()

	// --- The user origin really serves our ids (the "is it ours?" oracle is
	//     entity existence on this surface, so it must be a real surface). ---
	userOrigin, err := personas.New(personas.Options{
		DB:         h.db,
		Custodian:  h.custodian,
		UserOrigin: echoUserOrigin,
	})
	require.NoError(t, err, "build the coves.social persona service")
	var selfFetches atomic.Int64
	h.mux.Handle("/ap/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		selfFetches.Add(1)
		userOrigin.ServeHTTP(w, r)
	}))
	actor, err := userOrigin.CreateActorForDID(ctx, echoAuthorDID, echoAuthorHandle)
	require.NoError(t, err, "mint the native author's persona")
	require.Equal(t, echoUserOrigin+"/ap/actor/"+echoAuthorDID, actor.ActorID)

	// --- The accepted post's outbound state (the acceptance engine's row). ---
	communityDID := testDIDFor("technology", "lemmy.world")
	_, err = store.NewOutboundObjects(h.db).Upsert(ctx, store.OutboundObject{
		ATURI:              echoPostATURI,
		APObjectID:         echoPostAPID,
		LastCID:            echoPostCID,
		LastRev:            "3lzechorev0001",
		CommunityDID:       communityDID,
		CommunityAPID:      groupID,
		TranslatedSnapshot: echoPageSnapshot(t),
	})
	require.NoError(t, err, "seed the accepted post's outbound state")

	// --- Federate it for real: enqueue + deliver. ---
	enqueuer, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         h.db,
		Translator: outbound.NewTranslator(echoUserOrigin),
		Inboxes:    outbound.NewInboxResolver(h.client, time.Minute),
		Actors:     store.NewAPActors(h.db),
		UserOrigin: echoUserOrigin,
	})
	require.NoError(t, err, "build the outbound enqueuer")
	worker, err := outbound.NewWorker(outbound.WorkerOptions{
		DB:      h.db,
		Actors:  store.NewAPActors(h.db),
		Objects: store.NewOutboundObjects(h.db),
		Signers: userOrigin,
		Inboxes: outbound.NewInboxResolver(h.client, time.Minute),
		Sender:  h.client,
		Lease:   time.Minute,
	})
	require.NoError(t, err, "build the delivery worker")

	postIntent := consume.PostIntent{
		Op:            "create",
		ATURI:         echoPostATURI,
		ID:            consume.ActivityID(echoUserOrigin, echoPostATURI, "create", 0),
		CommunityAPID: groupID,
		Snapshot:      echoPageSnapshot(t),
	}
	// The native user also upvotes a LEMMY post in the same community: the
	// echoed Like's object is that Lemmy id, so no object-origin check can ever
	// recognize it — the inner ACTOR is the only handle on it.
	voteIntent := consume.VoteIntent{
		Op:            "create",
		VoteATURI:     echoVoteATURI,
		SubjectAPID:   pageID,
		Direction:     "up",
		ID:            consume.ActivityID(echoUserOrigin, echoVoteATURI, "create", 0),
		CommunityAPID: groupID,
	}
	enqueueOutbound(t, h.db, enqueuer, postIntent)
	enqueueOutbound(t, h.db, enqueuer, voteIntent)

	deliveredFrom := h.deliveryCount()
	drainDeliveries(t, ctx, worker)
	delivered := h.deliveriesSince(t, deliveredFrom)
	require.Len(t, delivered, 2,
		"the GIVEN is two real deliveries to the community: Create{Page} and Like")
	create := deliveredOfType(t, delivered, "Create")
	like := deliveredOfType(t, delivered, "Like")
	page, isMap := create["object"].(map[string]any)
	require.True(t, isMap, "the delivered Create embeds the Page")
	require.Equal(t, echoPostAPID, page["id"])

	activityID, _ := create["id"].(string)
	require.NotEmpty(t, activityID, "the delivered Create carries the id we minted for it")

	// Everything the bridge dialed while federating is GIVEN, not echo
	// behaviour: reset the self-fetch counter here.
	selfFetches.Store(0)

	// -------------------------------------------------------------------
	// WHEN: the community announces each of them straight back at us.
	// -------------------------------------------------------------------
	cases := []struct {
		name       string
		announceID string
		object     any
		wantClass  echo.Class
		why        string
	}{
		{
			name:       "embedded activity",
			announceID: "https://lemmy.world/activities/announce/create/echo-a",
			object:     create,
			wantClass:  echo.ClassLocalActivity,
			why: "the walk runs outer-in (outer activity id, inner activity id, inner " +
				"actor, inner object): the inner Create's id is an activity WE sent, and " +
				"that is the first identity that is ours",
		},
		{
			name:       "embedded object",
			announceID: "https://lemmy.world/activities/announce/page/echo-b",
			object:     page,
			wantClass:  echo.ClassMappedObject,
			why: "an announced bare object carries no activity id — it is ours because the " +
				"object id maps to a record the bridge federated",
		},
		{
			name:       "bare IRI",
			announceID: "https://lemmy.world/activities/announce/iri/echo-c",
			object:     activityID,
			wantClass:  echo.ClassLocalActivity,
			why: "a bare IRI must be classified BEFORE it is fetched: an id we minted is " +
				"ours by lookup, and dereferencing our own URL to find out is the mint " +
				"oracle in reverse",
		},
		{
			name:       "echoed vote",
			announceID: "https://lemmy.world/activities/announce/like/echo-d",
			object:     like,
			wantClass:  echo.ClassLocalActor,
			why: "an Announce{Like}'s object is the LEMMY subject and its id is ours only " +
				"as an activity — but the decisive identity is the inner ACTOR, our persona",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := h.snapshotWorld(t, selfFetches.Load())

			require.Equal(t, http.StatusAccepted,
				h.deliver(group, echoAnnounce(tc.announceID, tc.object)),
				"the inbox accepts the announce (the drop is a PROCESSING decision, not a 4xx)")
			h.drain()

			// The drop is a processed skip: never a retry (which would replay
			// the echo forever) and never a poison (which would dead-letter our
			// own traffic).
			event, err := h.events.GetEvent(ctx, tc.announceID)
			require.NoError(t, err)
			assert.NotNil(t, event.ProcessedAt, "an echo is processed (skipped), not left pending")
			assert.Nil(t, event.FailedAt, "an echo must never poison: %s", event.Error)
			assert.Equal(t, 1, event.Attempts, "an echo is decided on the first attempt, never retried")

			after := h.snapshotWorld(t, selfFetches.Load())

			assert.Equal(t, before.apObjects, after.apObjects,
				"an echo must not create or rewrite an ap_objects mapping")
			assert.Equal(t, before.firehoseOps, after.firehoseOps,
				"an echo must not produce a repo commit — our own content coming back is not new content")
			assert.Equal(t, before.outActivities, after.outActivities,
				"an echo must not enqueue anything outbound (no boomerang)")
			assert.Equal(t, before.outDeliveries, after.outDeliveries,
				"...and no new delivery either")
			assert.Equal(t, before.votesApplied, after.votesApplied,
				"an echoed vote must never reach the aggregator: that is the double-count")
			assert.Equal(t, before.votesRetracted, after.votesRetracted,
				"...nor the retraction path")
			assert.Equal(t, before.selfFetches, after.selfFetches,
				"an echo is recognized from our own state; dialing coves.social to decide "+
					"whether something is ours is both a round trip and a trust inversion")

			// Observability: exactly one class advances, by exactly one. A drop
			// nobody can count is indistinguishable from content silently lost.
			for _, class := range echoClasses {
				want := before.drops[class]
				if class == tc.wantClass {
					want++
				}
				assert.Equal(t, want, echo.Drops(class),
					"drop counter for class %q after an echo classified %q: %s",
					class, tc.wantClass, tc.why)
			}
		})
	}

	// The post's mapping is still the bridge's own, unmoved by four echoes.
	mapping, err := h.objects.GetByAPID(ctx, echoPostAPID)
	require.NoError(t, err)
	assert.Equal(t, store.OriginBridge, mapping.Origin)
	assert.Equal(t, echoPostATURI, "at://"+mapping.DID+"/"+mapping.Collection+"/"+mapping.RKey,
		"the echo must leave the native post's identity exactly where the acceptance put it")
}

// ---------------------------------------------------------------------------
// Fixtures + harness support
// ---------------------------------------------------------------------------

// echoPageSnapshot is the durable snapshot the acceptance engine stores for a
// native post: the postv2 record plus the context the Page is rendered from.
func echoPageSnapshot(t *testing.T) []byte {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      echoPostATURI,
		"cid":        echoPostCID,
		"rev":        "3lzechorev0001",
		"collection": "social.coves.community.postv2",
		"record": map[string]any{
			"$type":     "social.coves.community.postv2",
			"community": testDIDFor("technology", "lemmy.world"),
			"title":     "A native post that must not come back to haunt us",
			"content":   "federated out through the bridge, announced straight back",
			"createdAt": "2026-08-13T09:00:00.000Z",
		},
		"communityApId": groupID,
	})
	require.NoError(t, err)
	return snap
}

// echoAnnounce wraps object in the community's FEP-1b12 fan-out envelope.
func echoAnnounce(id string, object any) map[string]any {
	return map[string]any{
		"@context": "https://www.w3.org/ns/activitystreams",
		"id":       id,
		"type":     "Announce",
		"actor":    groupID,
		"to":       []any{ap.PublicAudience},
		"cc":       []any{groupID},
		"object":   object,
	}
}

// enqueueOutbound runs the real enqueuer inside the transaction the consumer's
// rev gate would be holding, and commits it.
func enqueueOutbound(t *testing.T, db *sql.DB, enqueuer *outbound.Enqueuer, intent consume.Intent) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, enqueuer.EnqueueActivity(ctx, tx, echoAuthorDID, groupID, "", intent),
		"enqueue %s", intent.ActivityID())
	require.NoError(t, tx.Commit())
}

// drainDeliveries runs the delivery worker until its queue is empty.
func drainDeliveries(t *testing.T, ctx context.Context, worker *outbound.Worker) {
	t.Helper()
	for i := 0; i < 20; i++ {
		worked, err := worker.DeliverNext(ctx)
		require.NoError(t, err, "DeliverNext must not error on a healthy delivery")
		if !worked {
			return
		}
	}
	t.Fatal("the delivery worker did not drain within 20 iterations")
}

// deliveryCount is how many activities the fake Lemmy inbox has captured.
func (h *harness) deliveryCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.inboxLog)
}

// deliveriesSince parses everything the fake Lemmy inbox captured after from —
// the exact bytes the community received, which are the exact bytes it echoes.
func (h *harness) deliveriesSince(t *testing.T, from int) []map[string]any {
	t.Helper()
	h.mu.Lock()
	raw := append([][]byte(nil), h.inboxLog[from:]...)
	h.mu.Unlock()
	out := make([]map[string]any, 0, len(raw))
	for _, body := range raw {
		var doc map[string]any
		require.NoError(t, json.Unmarshal(body, &doc), "parse delivered activity")
		out = append(out, doc)
	}
	return out
}

// deliveredOfType picks the one delivered activity of the given AP type.
func deliveredOfType(t *testing.T, delivered []map[string]any, apType string) map[string]any {
	t.Helper()
	for _, activity := range delivered {
		if activity["type"] == apType {
			return activity
		}
	}
	t.Fatalf("no delivered activity of type %s in %v", apType, delivered)
	return nil
}

// worldState is everything an echo must leave untouched, plus the drop
// counters that make the decision visible.
type worldState struct {
	apObjects      int
	outActivities  int
	outDeliveries  int
	firehoseOps    int
	votesApplied   int
	votesRetracted int
	selfFetches    int64
	drops          map[echo.Class]int64
}

func (h *harness) snapshotWorld(t *testing.T, selfFetches int64) worldState {
	t.Helper()
	h.votes.mu.Lock()
	applied, retracted := len(h.votes.applied), len(h.votes.retracted)
	h.votes.mu.Unlock()
	state := worldState{
		apObjects:      rowCount(t, h.db, "ap_objects"),
		outActivities:  rowCount(t, h.db, "outbound_activities"),
		outDeliveries:  rowCount(t, h.db, "outbound_deliveries"),
		firehoseOps:    len(h.firehoseOps()),
		votesApplied:   applied,
		votesRetracted: retracted,
		selfFetches:    selfFetches,
		drops:          map[echo.Class]int64{},
	}
	for _, class := range echoClasses {
		state.drops[class] = echo.Drops(class)
	}
	return state
}

func rowCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM `+table).Scan(&n))
	return n
}
