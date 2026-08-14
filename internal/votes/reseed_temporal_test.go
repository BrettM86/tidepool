package votes

import (
	"context"
	"crypto/rsa"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
	"tidepool/internal/echo"
	"tidepool/internal/outbound"
	"tidepool/internal/store"
)

// TASK 17b, THE TEMPORAL CASES.
//
// The states that break this arithmetic are not the ones a fixture author
// chooses; they are the ones the WRITE PATH produces transiently. A hand-written
// row is always in some steady state somebody decided on — a clean 'delivered',
// a clean 'pending' — and asserting against it proves only that the SQL matches
// the string that was typed. The fixture nobody writes is the one where the
// vote's history has MORE THAN ONE STEP.
//
// So the lifecycle below is driven through the REAL path end to end:
//
//	consume.Dispatcher.HandleEvent (a vote commit off the firehose)
//	  → applyVoteWrite            → outbound_votes 'pending' + one activity
//	outbound.Worker.DeliverNext   → voteCallback → SetDeliveredState('delivered')
//	consume.Dispatcher.HandleEvent (the vote record's DELETE)
//	  → applyVoteDelete           → re-upsert PRESERVING 'delivered' + Undo enqueued
//	outbound.Worker.DeliverNext   → voteCallback → the row is DELETED
//
// Every state these tests assert against is a state that path actually reached.

const (
	tpUserOrigin   = "https://coves.social"
	tpNativeDID    = "did:plc:temporalnative001"
	tpNativeHandle = "temporal.coves.social"
	tpCommunityDID = "did:plc:temporalcommunity"
	tpCommunityAP  = "https://lemmy.world/c/technology"
	tpSubject      = "https://lemmy.world/post/700"
	tpSubjectRKey  = "3lztemporalpost"
	tpVoteRKey     = "3lztemporalvote"
)

// tpRSAKey is generated once: the signing key is not under test and RSA keygen
// dominates the runtime of every test in this file otherwise.
var (
	tpRSAKey     *rsa.PrivateKey
	tpRSAKeyOnce sync.Once
)

// ---------------------------------------------------------------------------
// The real write path, wired
// ---------------------------------------------------------------------------

// tpMinter is the personas seam: it really writes the ap_actors row, because
// the delivery worker's consent recheck reads it.
type tpMinter struct{ db *sql.DB }

func (m tpMinter) CreateActorForDID(ctx context.Context, did, _ string) (*store.APActor, error) {
	actors := store.NewAPActors(m.db)
	if existing, err := actors.GetByDID(ctx, did); err == nil {
		return existing, nil
	}
	tpRSAKeyOnce.Do(func() {
		key, err := ap.GenerateRSAKey()
		if err != nil {
			panic(err)
		}
		tpRSAKey = key
	})
	return actors.Create(ctx, store.APActor{
		DID:              did,
		Kind:             store.ActorTypePerson,
		ActorID:          tpUserOrigin + "/ap/actor/" + did,
		NormalizedOrigin: "coves.social",
		LocalPart:        "temporal",
		RSAKeySealed:     []byte{0x01, 0x02, 0x03},
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n",
	})
}

type tpResolver struct{}

func (tpResolver) ResolveDIDHandle(context.Context, string) (string, error) {
	return tpNativeHandle, nil
}

type tpSigners struct{}

func (tpSigners) SignerFor(context.Context, string) (*ap.Signer, error) {
	tpRSAKeyOnce.Do(func() {
		key, err := ap.GenerateRSAKey()
		if err != nil {
			panic(err)
		}
		tpRSAKey = key
	})
	return ap.NewSigner(tpUserOrigin+"/ap/actor/"+tpNativeDID+"#main-key", tpRSAKey), nil
}

type tpInbox struct{}

func (tpInbox) ResolveInbox(context.Context, string) (string, error) {
	return "https://lemmy.world/inbox", nil
}

// tpSender is the wire. It records what went out and can be told to fail, which
// is how a delivery is driven to POISON — the state no happy-path fixture ever
// produces and the one that makes a subtraction permanent.
type tpSender struct {
	mu     sync.Mutex
	sent   []string
	err    error
	onSend func()
}

func (s *tpSender) SendActivityAs(_ context.Context, _ *ap.Signer, _ string, activity any) error {
	s.mu.Lock()
	hook, err := s.onSend, s.err
	s.mu.Unlock()
	// The hook runs WHILE the request is on the wire — the only window in which
	// a lease can expire under a worker that is about to succeed.
	if hook != nil {
		hook()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(activity)
	var doc map[string]any
	_ = json.Unmarshal(raw, &doc)
	kind, _ := doc["type"].(string)
	s.sent = append(s.sent, kind)
	return nil
}

func (s *tpSender) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *tpSender) succeed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = nil
}

func (s *tpSender) kinds() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sent...)
}

// lifecycle is one native persona's vote, drivable through its whole life.
type lifecycle struct {
	db         *sql.DB
	agg        *Aggregator
	dispatcher *consume.Dispatcher
	worker     *outbound.Worker
	sender     *tpSender
	votes      store.OutboundVotes
	subjectURI string
	logs       *tpLogBuffer
}

// tpLogBuffer is a concurrency-safe log sink (the clamp Warn is asserted).
type tpLogBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *tpLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *tpLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// newLifecycle wires the real consumer, the real enqueuer and the real delivery
// worker over one test database. maxAttempts is the delivery cap: 1 makes the
// first failure poison, which is how the permanent states are reached.
func newLifecycle(t *testing.T, maxAttempts int, workerOpts ...func(*outbound.WorkerOptions)) *lifecycle {
	t.Helper()
	ctx := context.Background()
	database := testDB(t)

	objects := store.NewAPObjects(database)
	// The Lemmy post being voted on, bound to the community the consumer
	// resolves the vote's target through.
	_, err := store.NewCommunities(database).UpsertCommunity(ctx, store.Community{
		APGroupID:         tpCommunityAP,
		DID:               tpCommunityDID,
		PreferredUsername: "technology",
		Instance:          testInstance,
		FollowState:       store.FollowStateAccepted,
	})
	require.NoError(t, err)
	mapping, err := objects.PutMapping(ctx, store.APObjectMapping{
		APID:           tpSubject,
		APType:         "Page",
		OriginInstance: testInstance,
		Origin:         store.OriginFediverse,
		DID:            tpCommunityDID,
		CommunityDID:   tpCommunityDID,
		Collection:     testCollection,
		RKey:           tpSubjectRKey,
		CID:            testCID,
	})
	require.NoError(t, err)

	logs := &tpLogBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	probe, err := echo.New(echo.Options{
		Objects:         objects,
		OutboundObjects: store.NewOutboundObjects(database),
		Activities:      store.NewOutboundActivities(database),
		Actors:          store.NewAPActors(database),
	})
	require.NoError(t, err)
	agg, err := NewAggregator(database, objects, store.NewCommunities(database),
		&fakeRecords{records: map[string]map[string]any{}}, probe, logger)
	require.NoError(t, err)

	enqueuer, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         database,
		Translator: outbound.NewTranslator(tpUserOrigin),
		Inboxes:    tpInbox{},
		Actors:     store.NewAPActors(database),
		UserOrigin: tpUserOrigin,
		Logger:     logger,
	})
	require.NoError(t, err)

	dispatcher, err := consume.NewDispatcher(consume.Options{
		DB:         database,
		Actors:     tpMinter{db: database},
		Enqueuer:   enqueuer,
		Resolver:   tpResolver{},
		UserOrigin: tpUserOrigin,
		Logger:     logger,
	})
	require.NoError(t, err)

	sender := &tpSender{}
	workerOptions := outbound.WorkerOptions{
		DB:          database,
		Actors:      store.NewAPActors(database),
		Objects:     store.NewOutboundObjects(database),
		Votes:       store.NewOutboundVotes(database),
		Signers:     tpSigners{},
		Inboxes:     tpInbox{},
		Sender:      sender,
		Lease:       time.Minute,
		MaxAttempts: maxAttempts,
		BackoffBase: time.Nanosecond,
		Logger:      logger,
	}
	for _, apply := range workerOpts {
		apply(&workerOptions)
	}
	worker, err := outbound.NewWorker(workerOptions)
	require.NoError(t, err)

	return &lifecycle{
		db: database, agg: agg, dispatcher: dispatcher, worker: worker,
		sender: sender, votes: store.NewOutboundVotes(database),
		subjectURI: mapping.ATURI, logs: logs,
	}
}

// castVote drives a vote COMMIT through the real consumer.
func (l *lifecycle) castVote(t *testing.T, rev, direction string) {
	t.Helper()
	frame := fmt.Sprintf(
		`{"did":%q,"time_us":9500,"kind":"commit","commit":{"rev":%q,"operation":"create",`+
			`"collection":"social.coves.feed.vote","rkey":%q,"cid":%q,`+
			`"record":{"$type":"social.coves.feed.vote","subject":{"uri":%q,"cid":%q},`+
			`"direction":%q,"createdAt":"2026-08-13T10:00:00.000Z"}}}`,
		tpNativeDID, rev, tpVoteRKey, testCID, l.subjectURI, testCID, direction)
	l.handle(t, frame)
}

// deleteVote drives the vote record's DELETE through the real consumer — the
// step that enqueues the Undo while the row keeps its delivered state.
func (l *lifecycle) deleteVote(t *testing.T, rev string) {
	t.Helper()
	frame := fmt.Sprintf(
		`{"did":%q,"time_us":9600,"kind":"commit","commit":{"rev":%q,"operation":"delete",`+
			`"collection":"social.coves.feed.vote","rkey":%q}}`,
		tpNativeDID, rev, tpVoteRKey)
	l.handle(t, frame)
}

func (l *lifecycle) handle(t *testing.T, frame string) {
	t.Helper()
	var event consume.JetstreamEvent
	require.NoError(t, json.Unmarshal([]byte(frame), &event))
	require.NoError(t, l.dispatcher.HandleEvent(context.Background(), &event))
}

// deliver runs the delivery worker until its queue drains.
func (l *lifecycle) deliver(t *testing.T) {
	t.Helper()
	for i := 0; i < 20; i++ {
		worked, err := l.worker.DeliverNext(context.Background())
		require.NoError(t, err, "DeliverNext must not error")
		if !worked {
			return
		}
	}
	t.Fatal("the delivery worker did not drain")
}

// state reports the vote row's delivered_state, or "" when the row is gone.
func (l *lifecycle) state(t *testing.T) string {
	t.Helper()
	var s string
	err := l.db.QueryRow(
		`SELECT delivered_state FROM outbound_votes WHERE vote_at_uri = $1`,
		"at://"+tpNativeDID+"/social.coves.feed.vote/"+tpVoteRKey).Scan(&s)
	if err == sql.ErrNoRows {
		return ""
	}
	require.NoError(t, err)
	return s
}

// ---------------------------------------------------------------------------
// T1 + T2 — the same vote at two points in its life
// ---------------------------------------------------------------------------

// TestDeliveredVoteIsSubtractedUntilItsUndoIsDelivered pins the pair that only
// makes sense together: while the withdrawal is IN FLIGHT the origin still
// counts the vote, and once it lands the origin stops — and the served total
// must move by exactly one, at exactly that moment.
func TestDeliveredVoteIsSubtractedUntilItsUndoIsDelivered(t *testing.T) {
	l := newLifecycle(t, 5)
	ctx := context.Background()

	// A Lemmy human's live inbound up-vote, so the fixture is mixed and the two
	// directions are unequal.
	require.NoError(t, l.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, tpSubject), ""))

	// Our persona casts a DOWN-vote, and it is delivered for real.
	l.castVote(t, "3lztprev00001", directionDown)
	require.Equal(t, string(store.DeliveredStatePending), l.state(t),
		"the consumer records INTENT; only the wire makes it delivered")
	l.deliver(t)
	require.Equal(t, string(store.DeliveredStateDelivered), l.state(t))
	require.Equal(t, []string{"Dislike"}, l.sender.kinds())

	// T1: the user deletes their vote record. An Undo is enqueued, and the row
	// KEEPS delivered — applyVoteDelete re-upserts *stored on purpose.
	l.deleteVote(t, "3lztprev00002")
	require.Equal(t, string(store.DeliveredStateDelivered), l.state(t),
		"the withdrawal is in flight: Lemmy has not processed it, so the row must still "+
			"say the peer holds this vote")

	require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 3, 2))
	up, down, found := counts(t, l.db, tpSubject)
	require.True(t, found)
	assert.Equal(t, 3, up, "the inbound up-vote nets out of the baseline and back into the total")
	assert.Equal(t, 1, down,
		"T1: an Undo IN FLIGHT changes nothing about what the origin counts — its 2 still "+
			"includes our vote, so it must still be subtracted")

	// T2: the Undo delivers. voteCallback deletes the row, and the origin's
	// own count drops by one at the same moment.
	l.deliver(t)
	require.Equal(t, []string{"Dislike", "Undo"}, l.sender.kinds())
	require.Equal(t, "", l.state(t), "a withdrawn vote leaves no row to subtract")

	require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 3, 1))
	up, down, _ = counts(t, l.db, tpSubject)
	assert.Equal(t, 3, up)
	assert.Equal(t, 1, down,
		"T2: the origin's own count dropped by one (2→1) when it processed the withdrawal, "+
			"and our subtrahend dropped with it — so the SERVED total holds at 1. Subtracting "+
			"a row that is already gone would move it to 0 instead")
}

// ---------------------------------------------------------------------------
// T3 — pending is never subtracted, however it got there
// ---------------------------------------------------------------------------

// TestPendingVotesAreNeverSubtracted covers both flavours of pending: a first
// delivery still in flight, and a POISONED one that will never advance
// (voteCallback is the only writer of 'delivered', so a poisoned delivery
// leaves the row pending forever).
//
// Subtracting either would UNDERCOUNT: Lemmy cannot be holding a vote it never
// received.
func TestPendingVotesAreNeverSubtracted(t *testing.T) {
	ctx := context.Background()

	t.Run("first delivery still in flight", func(t *testing.T) {
		l := newLifecycle(t, 5)
		require.NoError(t, l.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, tpSubject), ""))
		l.castVote(t, "3lztprev00001", directionDown)
		require.Equal(t, string(store.DeliveredStatePending), l.state(t))

		require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 3, 2))
		up, down, found := counts(t, l.db, tpSubject)
		require.True(t, found)
		assert.Equal(t, 3, up)
		assert.Equal(t, 2, down,
			"a vote the peer has not received is not in the peer's count: subtracting it "+
				"would show the community one fewer downvote than it has")
	})

	t.Run("poisoned delivery, pending forever", func(t *testing.T) {
		l := newLifecycle(t, 1) // one attempt, so the first failure poisons
		require.NoError(t, l.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, tpSubject), ""))
		l.castVote(t, "3lztprev00001", directionDown)
		l.sender.fail(fmt.Errorf("lemmy is unreachable"))
		l.deliver(t)

		assert.Equal(t, string(store.DeliveredStatePending), l.state(t),
			"nothing but delivery success writes 'delivered', so a poisoned delivery leaves "+
				"the row pending — permanently")
		var poisoned int
		require.NoError(t, l.db.QueryRow(
			`SELECT COUNT(*) FROM outbound_deliveries WHERE state = 'poisoned'`).Scan(&poisoned))
		require.Equal(t, 1, poisoned, "precondition: the delivery really did poison")

		require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 3, 2))
		_, down, _ := counts(t, l.db, tpSubject)
		assert.Equal(t, 2, down,
			"permanence cuts the other way here: this vote will NEVER reach Lemmy, so it "+
				"must never be subtracted from Lemmy's count")

		// KNOWN AMBIGUITY, recorded rather than resolved: a poisoned delivery
		// does not prove what the peer holds. A timeout AFTER Lemmy applied the
		// vote leaves us treating a present vote as absent, and no arithmetic
		// here can tell the two apart — only reconciliation against the origin
		// can (17e). What this pins is that the uncertainty stays QUERYABLE:
		// the poisoned row keeps its activity id and error class, so a
		// reconciler has something to walk. Erasing it would turn "we do not
		// know" into "it never happened".
		var activityID, errorClass string
		require.NoError(t, l.db.QueryRow(
			`SELECT activity_id, last_error_class FROM outbound_deliveries WHERE state = 'poisoned'`).
			Scan(&activityID, &errorClass))
		assert.NotEmpty(t, activityID,
			"the poisoned delivery must remain identifiable: this row is the only record that "+
				"we do not know whether the peer holds this vote")
		assert.NotEmpty(t, errorClass, "with why it died, so a reconciler can triage it")
	})
}

// ---------------------------------------------------------------------------
// T4 — a delivered vote whose UNDO poisoned is subtracted FOREVER
// ---------------------------------------------------------------------------

// TestDeliveredVoteWithPoisonedUndoSubtractsForever is the case that decides the
// predicate's SHAPE. Nobody ever advances this row: the vote is delivered, the
// withdrawal is dead, and Lemmy holds the vote permanently.
//
// It is why decision 16 bans queue-history arithmetic. "Delivered Likes minus
// delivered Undos" gets this row wrong for as long as the post exists, and the
// error is invisible — the tally is simply one off, forever, with nothing to
// point at.
func TestDeliveredVoteWithPoisonedUndoSubtractsForever(t *testing.T) {
	l := newLifecycle(t, 1)
	ctx := context.Background()
	require.NoError(t, l.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, tpSubject), ""))

	l.castVote(t, "3lztprev00001", directionDown)
	l.deliver(t)
	require.Equal(t, string(store.DeliveredStateDelivered), l.state(t))

	// The withdrawal is enqueued and then dies on the wire.
	l.deleteVote(t, "3lztprev00002")
	l.sender.fail(fmt.Errorf("lemmy rejected the undo"))
	l.deliver(t)

	require.Equal(t, string(store.DeliveredStateDelivered), l.state(t),
		"the row is untouched by a failed Undo: the peer still holds the vote")
	var poisoned int
	require.NoError(t, l.db.QueryRow(
		`SELECT COUNT(*) FROM outbound_deliveries WHERE state = 'poisoned'`).Scan(&poisoned))
	require.Equal(t, 1, poisoned, "precondition: the Undo really did poison")

	require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 3, 2))
	up, down, _ := counts(t, l.db, tpSubject)
	assert.Equal(t, 3, up)
	assert.Equal(t, 1, down,
		"the vote stands on the origin forever, so we subtract it forever — an implementation "+
			"that reasons from the QUEUE (a delivered Like whose Undo was also delivered) "+
			"has no way to see that the Undo died, and is permanently wrong here")

	// And a re-seed keeps saying so: this is a standing state, not an event.
	require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 3, 2))
	_, down, _ = counts(t, l.db, tpSubject)
	assert.Equal(t, 1, down, "still subtracted on the next backfill, and the one after that")
}

// ---------------------------------------------------------------------------
// T5 — re-seed idempotence with an outbound vote present
// ---------------------------------------------------------------------------

// TestReseedWithOutboundVoteIsIdempotent is the outbound mirror of
// TestReseedDoesNotDoubleCountLiveVotes. A backfill re-run with unchanged origin
// totals must leave the served total exactly where it was — the baseline is
// REPLACED by each seed, never accumulated.
func TestReseedWithOutboundVoteIsIdempotent(t *testing.T) {
	l := newLifecycle(t, 5)
	ctx := context.Background()
	require.NoError(t, l.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, tpSubject), ""))
	l.castVote(t, "3lztprev00001", directionDown)
	l.deliver(t)
	require.Equal(t, string(store.DeliveredStateDelivered), l.state(t))

	require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 3, 2))
	firstUp, firstDown, found := counts(t, l.db, tpSubject)
	require.True(t, found)
	require.Equal(t, 3, firstUp)
	require.Equal(t, 1, firstDown)

	for i := 0; i < 3; i++ {
		require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 3, 2))
	}
	up, down, _ := counts(t, l.db, tpSubject)
	assert.Equal(t, firstUp, up, "a re-seed with unchanged origin totals must not move the total")
	assert.Equal(t, firstDown, down,
		"and the subtraction must not compound: three re-seeds subtracting our one vote each "+
			"time would walk the community's score down to zero over a few backfills")
}

// ---------------------------------------------------------------------------
// T8 — the predicate is a POSITIVE EQUALITY
// ---------------------------------------------------------------------------

// TestUndoneRowsAreNotSubtracted pins the shape of the predicate itself.
//
// No code path writes 'undone' today, so this row is written directly — and
// that is exactly why the test exists. A negation ('not undone', '!= pending')
// reads identically to the equality on every state the system currently
// produces, so nothing else here can tell them apart. If a future policy starts
// writing 'undone' it will mean THE PEER ACCEPTED THE WITHDRAWAL — not live —
// and every negation silently inverts on the day it appears, in the direction
// that removes real votes from the community's score.
func TestUndoneRowsAreNotSubtracted(t *testing.T) {
	l := newLifecycle(t, 5)
	ctx := context.Background()
	require.NoError(t, l.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, tpSubject), ""))
	l.castVote(t, "3lztprev00001", directionDown)
	l.deliver(t)
	require.Equal(t, string(store.DeliveredStateDelivered), l.state(t))

	_, err := l.db.ExecContext(ctx,
		`UPDATE outbound_votes SET delivered_state = 'undone' WHERE vote_at_uri = $1`,
		"at://"+tpNativeDID+"/social.coves.feed.vote/"+tpVoteRKey)
	require.NoError(t, err, "a state no writer produces today — the point is what happens WHEN one does")

	require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 3, 2))
	up, down, _ := counts(t, l.db, tpSubject)
	assert.Equal(t, 3, up)
	assert.Equal(t, 2, down,
		"'undone' will mean the peer accepted the withdrawal, so the origin's count no longer "+
			"includes our vote and there is nothing to subtract. Only a POSITIVE equality on "+
			"'delivered' gets this right without being rewritten")
}

// ---------------------------------------------------------------------------
// T6 — the subtraction is UNREACHABLE for the subjects it would be wrong for
// ---------------------------------------------------------------------------

// refusingTransport fails every request: no test here may reach a network.
type refusingTransport struct{}

func (refusingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("refusing outbound request to %s: this test never touches a network", req.URL)
}

// recordingSeedStore captures whether the seeder reached the arithmetic at all.
type recordingSeedStore struct {
	mu       sync.Mutex
	subjects []string
}

func (r *recordingSeedStore) SeedAggregates(_ context.Context, subjectAPID string, _, _ int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subjects = append(r.subjects, subjectAPID)
	return nil
}

func (r *recordingSeedStore) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.subjects)
}

// TestSeedingNeverInvokesTheArithmeticForRefusedSubjects is the BEHAVIOURAL
// half of the guard whose shapes TestLemmyPostAPIURL pins: a refused id must
// stop the seeder before SeedAggregates is reached, and before any request is
// made.
//
// It matters because the refusal is the only thing standing between a native
// post and a subtraction that would corrupt a total nobody seeded — there is no
// check downstream.
func TestSeedingNeverInvokesTheArithmeticForRefusedSubjects(t *testing.T) {
	refused := []string{
		"https://coves.social/ap/object/did:plc:ewvi7nxzyoun6zhxrhs64oiz/social.coves.community.postv2/3lznative0001",
		"https://lemmy.world/comment/27485395",
		"https://lemmy.world/post/49131386/replies",
	}
	// The transport REFUSES everything, so this stays offline even under a
	// deliberately relaxed parser: the assertion is that the guard holds BEFORE
	// any request is made.
	recorder := &recordingSeedStore{}
	seeder, err := NewLemmySeeder(recorder, &http.Client{Transport: refusingTransport{}}, "tidepool-test/0", nil)
	require.NoError(t, err)
	for _, apID := range refused {
		_ = seeder.SeedPostCounts(context.Background(), apID)
	}
	assert.Zero(t, recorder.count(),
		"SeedAggregates must never be invoked for a subject with no external total: the "+
			"guard is the URL shape, and it has to hold before the first packet")
}

// ---------------------------------------------------------------------------
// R1 — the symmetry trap, mirrored
// ---------------------------------------------------------------------------

// TestOutboundUpVoteIsSubtractedFromUpOnly closes the gap every other fixture
// in this file leaves open: they all cast DOWN votes, so an implementation that
// buckets the subtrahend into `down` unconditionally passes all of them. That is
// the same trap reseed_test.go's header warns about, reproduced in the other
// direction — a suite is only as directional as its least directional fixture.
func TestOutboundUpVoteIsSubtractedFromUpOnly(t *testing.T) {
	l := newLifecycle(t, 5)
	ctx := context.Background()

	// A Lemmy human's live inbound DOWN-vote, so the two directions carry
	// different populations and cannot be confused for each other.
	require.NoError(t, l.agg.ApplyVote(ctx, dislike(activityID(t, 1), voterAlice, tpSubject), ""))

	// Our persona's UP-vote, delivered for real.
	l.castVote(t, "3lztprev00001", directionUp)
	l.deliver(t)
	require.Equal(t, string(store.DeliveredStateDelivered), l.state(t))
	require.Equal(t, []string{"Like"}, l.sender.kinds())

	require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 4, 2))
	up, down, found := counts(t, l.db, tpSubject)
	require.True(t, found)
	assert.Equal(t, 3, up,
		"UP: the origin's 4 includes our persona's up-vote, so the bridged tally is 3")
	assert.Equal(t, 2, down,
		"DOWN: our vote was cast UP. Subtracting it here — or subtracting the outbound "+
			"TOTAL from both columns — moves a number no native user touched")

	seededUp, seededDown := seededCounts(t, l.db, tpSubject)
	assert.Equal(t, 3, seededUp, "4 origin − 0 live inbound up − 1 delivered outbound up = 3")
	assert.Equal(t, 1, seededDown, "2 origin − 1 live inbound down − 0 outbound down = 1")
}

// TestSubtractionIsScopedToTheSeededSubject pins the `subject_ap_id = $1`
// predicate on the outbound subquery. Without it every delivered vote in the
// TABLE is subtracted from whatever subject happens to be seeded — a bridge
// with a thousand native voters would walk every backfilled post's score toward
// zero, and no fixture with one subject in the database could ever see it.
func TestSubtractionIsScopedToTheSeededSubject(t *testing.T) {
	l := newLifecycle(t, 5)
	ctx := context.Background()

	// Our persona's delivered DOWN-vote — on a DIFFERENT post.
	l.castVote(t, "3lztprev00001", directionDown)
	l.deliver(t)
	require.Equal(t, string(store.DeliveredStateDelivered), l.state(t))

	// An unrelated bridged post, with no outbound vote of ours on it at all.
	other := "https://lemmy.world/post/701"
	_, err := store.NewAPObjects(l.db).PutMapping(ctx, store.APObjectMapping{
		APID:           other,
		APType:         "Page",
		OriginInstance: testInstance,
		Origin:         store.OriginFediverse,
		DID:            tpCommunityDID,
		CommunityDID:   tpCommunityDID,
		Collection:     testCollection,
		RKey:           "3lzotherpost01",
		CID:            testCID,
	})
	require.NoError(t, err)
	require.NoError(t, l.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, other), ""))

	require.NoError(t, l.agg.SeedAggregates(ctx, other, 5, 3))
	up, down, found := counts(t, l.db, other)
	require.True(t, found)
	assert.Equal(t, 5, up, "the unrelated subject keeps the origin's total")
	assert.Equal(t, 3, down,
		"our vote lives on ANOTHER post: subtracting it here is a table-wide subtraction "+
			"masquerading as a per-subject one, and it grows with every native voter")
}
