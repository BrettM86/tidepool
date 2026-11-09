package outbound

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"strings"
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
	mu     sync.Mutex
	allow  bool
	dryRun bool
	scopes []DeliveryScope
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
		DB:               conn,
		Actors:           store.NewAPActors(conn),
		Signers:          newFakeSigners(t),
		Inboxes:          staticInbox{inbox: wInbox},
		Sender:           sender,
		Lease:            time.Minute,
		MaxAttempts:      3,
		BackoffBase:      time.Millisecond,
		CausalWaitBudget: time.Hour, // long by default: recent deliveries never time out
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
			name:       "disabled actor STILL delivers a delete",
			disable:    func(t *testing.T, conn *sql.DB) { seedWorkerActor(t, conn, false, false) },
			kind:       "Delete",
			wantState:  store.DeliveryStateDelivered,
			wantPosted: true,
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

// TestWorker_VoteUndoRejectionPoisonsAtOnceAndFreesTheLine: Lemmy holds one
// vote per (person, post), so a 4xx rejection of an Undo{Like|Dislike} is an
// answer about a vote the peer no longer holds — retrying it only blocks the
// ordering key behind it. It poisons on its first attempt, the withdrawn vote
// row is cleared, and the next delivery on the same key goes out. Retryable
// statuses, the duplicate dedupe and non-vote activities keep today's handling.
func TestWorker_VoteUndoRejectionPoisonsAtOnceAndFreesTheLine(t *testing.T) {
	const subjectAPID = "https://lemmy.world/post/1"
	tests := []struct {
		name string
		// kind is the activity under test; inner is the Undo's object type.
		kind, inner string
		answer      error
		// freshAnswer is the re-resolved inbox's answer; nil means answer.
		freshAnswer error
		wantState   store.DeliveryState
		// wantVoteGone: the outbound_votes row the Undo withdraws is deleted.
		wantVoteGone bool
		// wantFollowerDelivered: the next DeliverNext delivers the follower.
		wantFollowerDelivered bool
	}{
		{"Undo{Like} answered 400", "Undo", "Like",
			httpErr(http.StatusBadRequest, `{"error":"couldnt_find_object"}`), nil, store.DeliveryStatePoisoned, true, true},
		{"Undo{Dislike} answered 403", "Undo", "Dislike",
			httpErr(http.StatusForbidden, `{"error":"banned_from_community"}`), nil, store.DeliveryStatePoisoned, true, true},
		{"Undo{Like} answered 404 at both inboxes", "Undo", "Like",
			httpErr(http.StatusNotFound, ""), nil, store.DeliveryStatePoisoned, true, true},
		{"Undo{Dislike} answered 410 at both inboxes", "Undo", "Dislike",
			httpErr(http.StatusGone, ""), nil, store.DeliveryStatePoisoned, true, true},
		{"Undo{Like} answered 503 is retried", "Undo", "Like",
			httpErr(http.StatusServiceUnavailable, ""), nil, store.DeliveryStatePending, false, false},
		{"Undo{Dislike} answered 401 is retried", "Undo", "Dislike",
			httpErr(http.StatusUnauthorized, ""), nil, store.DeliveryStatePending, false, false},
		{"Undo{Like} answered 408 is retried", "Undo", "Like",
			httpErr(http.StatusRequestTimeout, ""), nil, store.DeliveryStatePending, false, false},
		{"Undo{Dislike} answered 429 is retried", "Undo", "Dislike",
			httpErr(http.StatusTooManyRequests, ""), nil, store.DeliveryStatePending, false, false},
		{"Undo{Like} answered the duplicate 400 is delivered", "Undo", "Like",
			httpErr(http.StatusBadRequest, `{"error":"activity was already received"}`), nil, store.DeliveryStateDelivered, true, true},
		{"Undo{Follow} answered 400 is retried", "Undo", "Follow",
			httpErr(http.StatusBadRequest, `{"error":"couldnt_find_object"}`), nil, store.DeliveryStatePending, false, false},
		{"Delete answered 400 is retried", "Delete", "",
			httpErr(http.StatusBadRequest, `{"error":"couldnt_find_object"}`), nil, store.DeliveryStatePending, false, false},
		{"Undo{Like} answered 404 then 503 at the fresh inbox is retried", "Undo", "Like",
			httpErr(http.StatusNotFound, ""), httpErr(http.StatusServiceUnavailable, ""), store.DeliveryStatePending, false, false},
		{"Undo{Dislike} answered 410 then 503 at the fresh inbox is retried", "Undo", "Dislike",
			httpErr(http.StatusGone, ""), httpErr(http.StatusServiceUnavailable, ""), store.DeliveryStatePending, false, false},
		{"Undo{Like} answered 404 then a transport error at the fresh inbox is retried", "Undo", "Like",
			httpErr(http.StatusNotFound, ""), stderrors.New("dial tcp: connection refused"), store.DeliveryStatePending, false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := workerTestDB(t)
			ctx := context.Background()
			seedWorkerActor(t, conn, true, false)

			innerID := "https://coves.social/ap/activity/" + repeatHex64("inner-"+tc.inner)
			undoID := "https://coves.social/ap/activity/" + repeatHex64(tc.kind)
			voteATURI := "at://" + wActorDID + "/social.coves.feed.vote/3lzvoteundo"
			isVote := tc.inner == "Like" || tc.inner == "Dislike"
			if isVote {
				direction := "up"
				if tc.inner == "Dislike" {
					direction = "down"
				}
				_, err := store.NewOutboundVotes(conn).Upsert(ctx, store.OutboundVote{
					VoteATURI:            voteATURI,
					ActorDID:             wActorDID,
					SubjectATURI:         "at://" + wCommunityDID + "/social.coves.community.postv2/3lzpost",
					SubjectAPID:          subjectAPID,
					CommunityDID:         wCommunityDID,
					Direction:            direction,
					CurrentActivityID:    innerID,
					DeliveredState:       store.DeliveredStateDelivered,
					RetractionActivityID: undoID,
				})
				require.NoError(t, err)
			}

			var payload []byte
			if tc.kind == "Undo" {
				payload = []byte(fmt.Sprintf(
					`{"id":%q,"type":"Undo","actor":%q,"object":{"type":%q,"id":%q,"actor":%q,"object":%q}}`,
					undoID, wActorID, tc.inner, innerID, wActorID, subjectAPID))
			} else {
				payload = []byte(fmt.Sprintf(
					`{"id":%q,"type":"Delete","actor":%q,"object":"https://coves.social/ap/note/1"}`,
					undoID, wActorID))
			}
			subjectID := seedDelivery(t, conn, tc.kind, "", payload)
			// The follower is queued on the same ordering key BEFORE any claim.
			followerID := seedDelivery(t, conn, "Create", "", createPayload("follower"))

			// The peer answers the activity under test with tc.answer at every
			// inbox (including the re-resolved one) and accepts the follower.
			const freshInbox = "https://lemmy.world/c/tech/inbox-rotated"
			sender := &fakeSender{}
			sender.respond = func(call int, inbox string) error {
				sender.mu.Lock()
				posted := sender.posts[call-1].payload
				sender.mu.Unlock()
				var doc struct {
					ID string `json:"id"`
				}
				require.NoError(t, json.Unmarshal(posted, &doc))
				if doc.ID == undoID {
					if inbox == freshInbox && tc.freshAnswer != nil {
						return tc.freshAnswer
					}
					return tc.answer
				}
				return nil
			}
			resolver := &rotatingResolver{normal: wInbox, fresh: freshInbox}
			w := newWorker(t, conn, sender, func(o *WorkerOptions) {
				o.Inboxes = resolver
				o.MaxAttempts = 5
				o.BackoffBase = time.Hour // a retried row is not re-claimable within the test
			})

			worked, err := w.DeliverNext(ctx)
			require.NoError(t, err)
			require.True(t, worked)

			// 404/410 is an endpoint-gone signal for every kind, vote Undos
			// included: the inbox is re-resolved once and the Undo re-sent there
			// before the refusal is treated as terminal.
			gone := false
			if he := (ap.HTTPError{}); stderrors.As(tc.answer, &he) {
				gone = he.StatusCode == http.StatusNotFound || he.StatusCode == http.StatusGone
			}
			if gone {
				assert.Equal(t, 1, resolver.freshCalls, "a 404/410 re-resolves the inbox once")
				sender.mu.Lock()
				var undoInboxes []string
				for _, post := range sender.posts {
					if strings.Contains(string(post.payload), undoID) {
						undoInboxes = append(undoInboxes, post.inbox)
					}
				}
				sender.mu.Unlock()
				assert.Equal(t, []string{wInbox, freshInbox}, undoInboxes,
					"the Undo is sent to the cached inbox, then once to the re-resolved one")
			} else {
				assert.Zero(t, resolver.freshCalls, "only a 404/410 re-resolves the inbox")
			}

			subject := getDelivery(t, conn, subjectID)
			assert.Equal(t, tc.wantState, subject.State, "state after one attempt")
			assert.Equal(t, 1, subject.Attempts, "attempts after one DeliverNext")

			_, voteErr := store.NewOutboundVotes(conn).GetByATURI(ctx, voteATURI)
			if tc.wantVoteGone {
				assert.Truef(t, errors.IsNotFound(voteErr), "the withdrawn vote row is cleared, got %v", voteErr)
			} else if isVote {
				assert.NoError(t, voteErr, "the vote row is still present while its Undo is retried")
			}

			if tc.wantFollowerDelivered {
				worked, err = w.DeliverNext(ctx)
				require.NoError(t, err)
				assert.True(t, worked, "the follower is claimable next")
				assert.Equal(t, store.DeliveryStateDelivered, getDelivery(t, conn, followerID).State,
					"the follower on the same ordering key is delivered")
			}
		})
	}
}

// failingDeleteVotes fails the Undo-side ledger write (the row delete) on demand.
type failingDeleteVotes struct {
	store.OutboundVotes
	failDelete bool
}

func (f *failingDeleteVotes) Delete(ctx context.Context, voteATURI string) error {
	if f.failDelete {
		return stderrors.New("ledger delete failed")
	}
	return f.OutboundVotes.Delete(ctx, voteATURI)
}

// TestWorker_VoteUndoRefusalHeldUntilClearedLocally: when the peer refuses a
// vote Undo but the local row delete fails, the delivery is not poisoned with
// the row still counted. It is held pending under its own class, and the next
// claim resumes at the local delete without POSTing the Undo again.
func TestWorker_VoteUndoRefusalHeldUntilClearedLocally(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)

	const subjectAPID = "https://lemmy.world/post/1"
	innerID := "https://coves.social/ap/activity/" + repeatHex64("inner-Like")
	undoID := "https://coves.social/ap/activity/" + repeatHex64("Undo")
	voteATURI := "at://" + wActorDID + "/social.coves.feed.vote/3lzvoteheld"
	votes := store.NewOutboundVotes(conn)
	_, err := votes.Upsert(ctx, store.OutboundVote{
		VoteATURI:            voteATURI,
		ActorDID:             wActorDID,
		SubjectATURI:         "at://" + wCommunityDID + "/social.coves.community.postv2/3lzpost",
		SubjectAPID:          subjectAPID,
		CommunityDID:         wCommunityDID,
		Direction:            "up",
		CurrentActivityID:    innerID,
		DeliveredState:       store.DeliveredStateDelivered,
		RetractionActivityID: undoID,
	})
	require.NoError(t, err)
	subjectID := seedDelivery(t, conn, "Undo", "", []byte(fmt.Sprintf(
		`{"id":%q,"type":"Undo","actor":%q,"object":{"type":"Like","id":%q,"actor":%q,"object":%q}}`,
		undoID, wActorID, innerID, wActorID, subjectAPID)))

	sender := &fakeSender{respond: func(int, string) error {
		return httpErr(http.StatusForbidden, `{"error":"banned_from_community"}`)
	}}
	faulty := &failingDeleteVotes{OutboundVotes: votes, failDelete: true}
	w := newWorker(t, conn, sender, func(o *WorkerOptions) { o.Votes = faulty })

	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)

	held := getDelivery(t, conn, subjectID)
	assert.Equal(t, store.DeliveryStatePending, held.State,
		"a refusal whose local delete failed is not poisoned while the row still counts")
	assert.Equal(t, store.DeliveryHeldForRefusalSettlement, held.LastErrorClass,
		"the peer's verdict is recorded so the retry skips the wire")
	_, err = votes.GetByATURI(ctx, voteATURI)
	require.NoError(t, err, "the vote row is still present while the delete is outstanding")
	require.Equal(t, 1, sender.count())

	// The held refusal already went out: neither the actor nor the community
	// cancellation may strand it with the vote row still counted.
	deliveries := store.NewOutboundDeliveries(conn)
	_, err = deliveries.CancelForActor(ctx, wActorDID)
	require.NoError(t, err)
	_, err = deliveries.CancelForCommunity(ctx, wCommunityAPID)
	require.NoError(t, err)
	held = getDelivery(t, conn, subjectID)
	assert.Equal(t, store.DeliveryStatePending, held.State,
		"a delivery held for refusal settlement is not cancellable")
	assert.Equal(t, store.DeliveryHeldForRefusalSettlement, held.LastErrorClass)

	faulty.failDelete = false
	require.Eventually(t, func() bool {
		worked, err := w.DeliverNext(ctx)
		require.NoError(t, err)
		return worked
	}, 5*time.Second, 5*time.Millisecond, "the held refusal is re-claimed")

	assert.Equal(t, 1, sender.count(), "the resumed refusal does not POST the Undo again")
	assert.Equal(t, store.DeliveryStatePoisoned, getDelivery(t, conn, subjectID).State,
		"once the row is cleared, the refusal is terminal")
	_, err = votes.GetByATURI(ctx, voteATURI)
	assert.Truef(t, errors.IsNotFound(err), "the withdrawn vote row is cleared, got %v", err)
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
