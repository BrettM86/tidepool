package outbound

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Task 15 cycle F: the delivery Worker. DeliverNext claims a delivery, rechecks
// consent, resolves the signer, POSTs the STORED activity payload verbatim, and
// records the fenced outcome. The retry taxonomy, the consent/retraction
// asymmetry, and the vote-delivery callbacks are pinned here; the signed-wire +
// addressing + verify-against-served-doc happy path is the OUTER test.

const (
	wActorDID      = "did:plc:workeractor000000000000"
	wActorID       = "https://coves.social/ap/actor/" + wActorDID
	wCommunityDID  = "did:plc:44ybard66vv44zksje25o7dz"
	wCommunityAPID = "https://lemmy.world/c/tech"
	wInbox         = "https://lemmy.world/c/tech/inbox"
)

func workerTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"outbound_deliveries", "outbound_activities", "outbound_objects",
		"outbound_votes", "ap_actors", "federation_prefs")
	return database
}

// ---- fakes ----

type sentPost struct {
	inbox   string
	payload []byte
}

// fakeSender records POSTs and returns a scripted outcome per call.
type fakeSender struct {
	mu      sync.Mutex
	posts   []sentPost
	respond func(callNo int, inbox string) error // nil => 2xx success
}

func (s *fakeSender) SendActivityAs(_ context.Context, _ *ap.Signer, inbox string, activity any) error {
	payload, _ := json.Marshal(activity)
	s.mu.Lock()
	s.posts = append(s.posts, sentPost{inbox: inbox, payload: payload})
	call := len(s.posts)
	respond := s.respond
	s.mu.Unlock()
	if respond != nil {
		return respond(call, inbox)
	}
	return nil
}

func (s *fakeSender) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.posts)
}

// alwaysFail returns a sender whose every POST returns err.
func senderReturning(err error) *fakeSender {
	return &fakeSender{respond: func(int, string) error { return err }}
}

// httpErr builds the *ap.HTTPError a delivery POST surfaces on a non-2xx
// response (status + bounded body excerpt).
func httpErr(status int, body string) error {
	return ap.HTTPError{URL: wInbox, StatusCode: status, Body: body}
}

// fakeSigners returns one generated signer for any DID.
type fakeSigners struct {
	signer *ap.Signer
	err    error
}

func newFakeSigners(t *testing.T) fakeSigners {
	t.Helper()
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	return fakeSigners{signer: ap.NewSigner(wActorID+"#main-key", key)}
}

func (f fakeSigners) SignerFor(context.Context, string) (*ap.Signer, error) {
	return f.signer, f.err
}

// fakeSwitches records the scopes it was consulted with.
type fakeSwitches struct {
	mu      sync.Mutex
	allow   bool
	dryRun  bool
	scopes  []DeliveryScope
}

func (s *fakeSwitches) OutboundAllowed(scope DeliveryScope) bool {
	s.mu.Lock()
	s.scopes = append(s.scopes, scope)
	s.mu.Unlock()
	return s.allow
}

func (s *fakeSwitches) DryRun() bool { return s.dryRun }

// staticInbox always resolves to wInbox.
type staticInbox struct{ inbox string }

func (r staticInbox) ResolveInbox(context.Context, string) (string, error) { return r.inbox, nil }

// ---- seed helpers ----

func seedWorkerActor(t *testing.T, conn *sql.DB, enabled, paused bool) {
	t.Helper()
	ctx := context.Background()
	actors := store.NewAPActors(conn)
	_, err := actors.Create(ctx, store.APActor{
		DID:              wActorDID,
		Kind:             store.ActorTypePerson,
		ActorID:          wActorID,
		NormalizedOrigin: "coves.social",
		LocalPart:        "worker",
		RSAKeySealed:     []byte("sealed"),
		RSAKeyVersion:    1,
		PublicKeyPEM:     "pem",
	})
	require.NoError(t, err)
	if !enabled {
		require.NoError(t, actors.SetEnabled(ctx, wActorDID, false))
	}
	if paused {
		require.NoError(t, actors.SetPaused(ctx, wActorDID, true))
	}
}

// seedDelivery inserts an activity + its single pending delivery and returns the
// activity id.
func seedDelivery(t *testing.T, conn *sql.DB, kind, parentATURI string, payload []byte) string {
	t.Helper()
	ctx := context.Background()
	activityID := "https://coves.social/ap/activity/" + repeatHex64(kind)
	inserted, err := store.NewOutboundActivities(conn).Insert(ctx, store.OutboundActivity{
		ActivityID:  activityID,
		ActorDID:    wActorDID,
		Kind:        kind,
		Payload:     payload,
		ParentATURI: parentATURI,
	})
	require.NoError(t, err)
	require.True(t, inserted)
	_, err = store.NewOutboundDeliveries(conn).Enqueue(ctx, store.OutboundDelivery{
		ActivityID:  activityID,
		TargetInbox: wInbox,
		OrderingKey: wCommunityAPID,
	})
	require.NoError(t, err)
	return activityID
}

func repeatHex64(seed string) string {
	// A deterministic 64-hex digest of the seed's CONTENT (not its length), so
	// distinct seeds get distinct activity ids and the SAME seed reproduces the
	// same id (the vote tests rely on repeatHex64("Like") matching in two
	// places). sha256 is exactly 32 bytes → 64 hex chars.
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}

func createPayload(activityID string) []byte {
	return []byte(fmt.Sprintf(`{"@context":"https://www.w3.org/ns/activitystreams",`+
		`"id":%q,"type":"Create","actor":%q,"object":{"type":"Note","content":"hi"}}`,
		activityID, wActorID))
}

func setAttempts(t *testing.T, conn *sql.DB, activityID string, n int) {
	t.Helper()
	_, err := conn.ExecContext(context.Background(),
		`UPDATE outbound_deliveries SET attempts = $2 WHERE activity_id = $1`, activityID, n)
	require.NoError(t, err)
}

func getDelivery(t *testing.T, conn *sql.DB, activityID string) *store.OutboundDelivery {
	t.Helper()
	d, err := store.NewOutboundDeliveries(conn).Get(context.Background(), activityID, wInbox)
	require.NoError(t, err)
	require.NotNil(t, d)
	return d
}

func newWorker(t *testing.T, conn *sql.DB, sender ActivitySender, opts func(*WorkerOptions)) *Worker {
	t.Helper()
	o := WorkerOptions{
		DB:          conn,
		Actors:      store.NewAPActors(conn),
		Signers:     newFakeSigners(t),
		Inboxes:     staticInbox{inbox: wInbox},
		Sender:      sender,
		Lease:       time.Minute,
		MaxAttempts: 3,
		BackoffBase: time.Millisecond,
	}
	if opts != nil {
		opts(&o)
	}
	w, err := NewWorker(o)
	require.NoError(t, err)
	return w
}

// ---------------------------------------------------------------------------
// F: retry taxonomy
// ---------------------------------------------------------------------------

func TestWorker_DuplicateActivityResponseIsDelivered(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))

	// The fake Lemmy in the OUTER test returns 400 + {"error":"activity was
	// already received"} on a duplicate. THIS is the shape the worker keys on.
	sender := senderReturning(httpErr(http.StatusBadRequest, `{"error":"activity was already received"}`))
	w := newWorker(t, conn, sender, nil)

	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)
	assert.Equal(t, store.DeliveryStateDelivered, getDelivery(t, conn, id).State,
		"a 400 whose body reports an already-received activity is DELIVERED, never poisoned — "+
			"redelivery after a crash is expected and safe")
}

func TestWorker_TransientFailuresRelease(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"transport", fmt.Errorf("dial tcp 1.2.3.4:443: connect: connection refused")},
		{"408", httpErr(http.StatusRequestTimeout, "")},
		{"429", httpErr(http.StatusTooManyRequests, "")},
		{"500", httpErr(http.StatusInternalServerError, "")},
		{"503", httpErr(http.StatusServiceUnavailable, "boom")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := workerTestDB(t)
			seedWorkerActor(t, conn, true, false)
			id := seedDelivery(t, conn, "Create", "", createPayload("x"))

			w := newWorker(t, conn, senderReturning(tc.err), nil)
			worked, err := w.DeliverNext(context.Background())
			require.NoError(t, err)
			assert.True(t, worked)

			d := getDelivery(t, conn, id)
			assert.Equal(t, store.DeliveryStatePending, d.State,
				"a transport error / 408 / 429 / 5xx is a RETRY: the delivery stays pending")
			assert.NotEmpty(t, d.LastErrorClass, "the retry taxonomy label is recorded")
			assert.True(t, d.NextAttemptAt.After(time.Now().Add(-time.Second)),
				"next_attempt_at is advanced for the backoff")
		})
	}
}

func TestWorker_AttemptCapPoisons(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))
	// One attempt below the cap; ClaimNext bumps to the cap, and the persistent
	// 5xx then poisons rather than releasing forever.
	setAttempts(t, conn, id, 2) // MaxAttempts is 3

	w := newWorker(t, conn, senderReturning(httpErr(http.StatusBadGateway, "")), nil)
	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)
	assert.Equal(t, store.DeliveryStatePoisoned, getDelivery(t, conn, id).State,
		"a retryable failure at the attempt cap poisons")
}

func TestWorker_OtherClientErrorPoisons(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))
	setAttempts(t, conn, id, 2) // small fixed retry budget exhausted

	// A non-duplicate 4xx (a genuine rejection) poisons after the small budget.
	w := newWorker(t, conn, senderReturning(httpErr(http.StatusBadRequest, `{"error":"invalid_object"}`)), nil)
	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)

	d := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePoisoned, d.State,
		"a non-duplicate 4xx is a permanent rejection → poisoned")
	require.NotNil(t, d.LastStatusCode)
	assert.Equal(t, http.StatusBadRequest, *d.LastStatusCode)
}

// ---------------------------------------------------------------------------
// F: consent recheck at claim (retraction asymmetry)
// ---------------------------------------------------------------------------

func TestWorker_ConsentAsymmetry(t *testing.T) {
	cases := []struct {
		name        string
		disable     func(t *testing.T, conn *sql.DB)
		kind        string
		wantState   store.DeliveryState
		wantPosted  bool
		explanation string
	}{
		{
			name:        "disabled actor cancels a create",
			disable:     func(t *testing.T, conn *sql.DB) { seedWorkerActor(t, conn, false, false) },
			kind:        "Create",
			wantState:   store.DeliveryStateCancelled,
			wantPosted:  false,
			explanation: "a disabled actor's create is cancelled, not delivered and not poisoned",
		},
		{
			name:        "paused actor cancels a create",
			disable:     func(t *testing.T, conn *sql.DB) { seedWorkerActor(t, conn, true, true) },
			kind:        "Create",
			wantState:   store.DeliveryStateCancelled,
			wantPosted:  false,
			explanation: "a delivery_paused actor's create is cancelled (transient #account state)",
		},
		{
			name: "opted-out actor cancels a create",
			disable: func(t *testing.T, conn *sql.DB) {
				seedWorkerActor(t, conn, true, false)
				_, err := store.NewFederationPrefs(conn).Upsert(context.Background(), store.FederationPref{
					DID: wActorDID, Enabled: false, Source: store.FederationPrefSourceRecord,
				})
				require.NoError(t, err)
			},
			kind:        "Create",
			wantState:   store.DeliveryStateCancelled,
			wantPosted:  false,
			explanation: "a federation opt-out cancels an outward create",
		},
		{
			name:        "disabled actor STILL delivers a delete",
			disable:     func(t *testing.T, conn *sql.DB) { seedWorkerActor(t, conn, false, false) },
			kind:        "Delete",
			wantState:   store.DeliveryStateDelivered,
			wantPosted:  true,
			explanation: "retraction asymmetry: a Delete goes out even for a disabled actor — it is the " +
				"only way an opted-out user takes down what is already federated",
		},
		{
			name:        "disabled actor STILL delivers an undo",
			disable:     func(t *testing.T, conn *sql.DB) { seedWorkerActor(t, conn, false, false) },
			kind:        "Undo",
			wantState:   store.DeliveryStateDelivered,
			wantPosted:  true,
			explanation: "an Undo (vote retraction) is also exempt from the consent recheck",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := workerTestDB(t)
			tc.disable(t, conn)
			id := seedDelivery(t, conn, tc.kind, "", createPayload("x"))

			sender := &fakeSender{}
			w := newWorker(t, conn, sender, nil)
			worked, err := w.DeliverNext(context.Background())
			require.NoError(t, err)
			assert.True(t, worked)

			assert.Equal(t, tc.wantState, getDelivery(t, conn, id).State, tc.explanation)
			if tc.wantPosted {
				assert.Equal(t, 1, sender.count(), "a retraction must actually be POSTed")
			} else {
				assert.Zero(t, sender.count(), "a cancelled delivery must POST nothing")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// F: vote delivery-success callbacks (decision 16)
// ---------------------------------------------------------------------------

func TestWorker_LikeDeliverySuccessFlipsVoteState(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)

	likeID := "https://coves.social/ap/activity/" + repeatHex64("Like")
	voteATURI := "at://" + wActorDID + "/social.coves.feed.vote/3lzvoteaaaa"
	_, err := store.NewOutboundVotes(conn).Upsert(context.Background(), store.OutboundVote{
		VoteATURI:         voteATURI,
		ActorDID:          wActorDID,
		SubjectATURI:      "at://" + wCommunityDID + "/social.coves.community.postv2/3lzpost",
		SubjectAPID:       "https://lemmy.world/post/1",
		CommunityDID:      wCommunityDID,
		Direction:         "up",
		CurrentActivityID: likeID,
	})
	require.NoError(t, err)

	seedDelivery(t, conn, "Like", "", []byte(fmt.Sprintf(
		`{"id":%q,"type":"Like","actor":%q,"object":"https://lemmy.world/post/1"}`, likeID, wActorID)))

	w := newWorker(t, conn, &fakeSender{}, nil)
	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)

	vote, err := store.NewOutboundVotes(conn).GetByATURI(context.Background(), voteATURI)
	require.NoError(t, err)
	assert.Equal(t, store.DeliveredStateDelivered, vote.DeliveredState,
		"a Like delivery success flips outbound_votes.delivered_state to delivered (decision 16)")
}

func TestWorker_UndoDeliverySuccessClearsVoteState(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)

	likeID := "https://coves.social/ap/activity/" + repeatHex64("Like")
	voteATURI := "at://" + wActorDID + "/social.coves.feed.vote/3lzvotebbbb"
	_, err := store.NewOutboundVotes(conn).Upsert(context.Background(), store.OutboundVote{
		VoteATURI:         voteATURI,
		ActorDID:          wActorDID,
		SubjectATURI:      "at://" + wCommunityDID + "/social.coves.community.postv2/3lzpost",
		SubjectAPID:       "https://lemmy.world/post/1",
		CommunityDID:      wCommunityDID,
		Direction:         "up",
		CurrentActivityID: likeID,
		DeliveredState:    store.DeliveredStateDelivered,
	})
	require.NoError(t, err)

	// The Undo embeds the Like's id as its inner object.id — the worker resolves
	// the vote row from that.
	seedDelivery(t, conn, "Undo", "", []byte(fmt.Sprintf(
		`{"id":%q,"type":"Undo","actor":%q,"object":{"type":"Like","id":%q,"actor":%q,`+
			`"object":"https://lemmy.world/post/1"}}`,
		"https://coves.social/ap/activity/"+repeatHex64("Undo"), wActorID, likeID, wActorID)))

	w := newWorker(t, conn, &fakeSender{}, nil)
	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)

	_, err = store.NewOutboundVotes(conn).GetByATURI(context.Background(), voteATURI)
	assert.Truef(t, errors.IsNotFound(err),
		"an Undo delivery success CLEARS the vote row (clear-on-Undo), got %v", err)
}

// ---------------------------------------------------------------------------
// I: kill switches + dry-run
// ---------------------------------------------------------------------------

func TestWorker_KillSwitchParksPending(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))

	switches := &fakeSwitches{allow: false}
	sender := &fakeSender{}
	w := newWorker(t, conn, sender, func(o *WorkerOptions) { o.Switches = switches })

	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)

	assert.Zero(t, sender.count(), "a kill switch POSTs nothing")
	assert.Equal(t, store.DeliveryStatePending, getDelivery(t, conn, id).State,
		"a killed delivery is PARKED (stays pending, resumable when the switch clears) — never "+
			"poisoned, never cancelled")

	// The switch is consulted with the full scope so global/per-host/
	// per-community/per-actor can all be expressed.
	require.NotEmpty(t, switches.scopes)
	scope := switches.scopes[0]
	assert.Equal(t, wActorDID, scope.ActorDID, "the actor is in the scope")
	assert.Equal(t, wCommunityAPID, scope.CommunityAPID, "the community is in the scope")
	assert.Equal(t, "lemmy.world", scope.InboxHost, "the inbox host is in the scope")
}

func TestWorker_DryRunPostsNothing(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, func(o *WorkerOptions) {
		o.Switches = &fakeSwitches{allow: true, dryRun: true}
	})

	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)

	assert.Zero(t, sender.count(), "dry-run translates + logs but POSTs nothing")
	assert.Equal(t, store.DeliveryStatePending, getDelivery(t, conn, id).State,
		"dry-run does not mark delivered — the delivery stays pending")
}
