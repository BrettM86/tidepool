package consume

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/optout"
	"tidepool/internal/store"
)

// THE #account TIER IS A CHECK-THEN-ACT, AND TWO GOROUTINES RUN IT.
//
// main starts connector.Start and the DLQ redriver over the SAME Dispatcher, so
// a redriven frame and a live frame are handled CONCURRENTLY. Commit events are
// safe: applyGatedTx claims the gate row as the first statement of a
// transaction and holds it across apply, so a racer blocks on the claim and
// then observes it. #account had none of that — read last_account_seq, decide,
// write, advance, four separate autocommit statements — which is two distinct
// bugs at once:
//
//  1. a stale pause can be applied AFTER a newer reactivation, because both
//     racers read the same watermark before either advanced it, leaving the
//     user un-delivered until the next live event; and
//  2. status="deleted" reaches the TERMINAL, IRREVERSIBLE seam TWICE, which is
//     two Delete{Person} activities for one deletion.
//
// The claim these tests demand is applyGatedTx's shape: exclusive first, apply
// under it, commit last.

// blockingTerminator is the terminal seam held OPEN. It is what makes the race
// deterministic rather than probabilistic: while one racer is parked inside the
// irreversible call, the other has all the time it needs to reach its own
// decision — which is exactly the window a claim has to close.
type blockingTerminator struct {
	mu      sync.Mutex
	calls   []string
	entered chan string
	release chan struct{}
}

func newBlockingTerminator() *blockingTerminator {
	return &blockingTerminator{
		entered: make(chan string, 4),
		release: make(chan struct{}),
	}
}

func (t *blockingTerminator) TerminateAccount(_ context.Context, did string) error {
	t.mu.Lock()
	t.calls = append(t.calls, did)
	t.mu.Unlock()
	t.entered <- did
	<-t.release
	return nil
}

func (t *blockingTerminator) Calls() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.calls...)
}

// handleConcurrently dispatches every frame at once and returns their errors in
// order. Each frame is parsed on THIS goroutine (a redriver parses its own copy
// of the frame anyway) so no testing.T call happens off the test goroutine.
func handleConcurrently(t *testing.T, dispatcher *Dispatcher, frames ...[]byte) []error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	events := make([]*JetstreamEvent, len(frames))
	for i, frame := range frames {
		events[i] = parseFrame(t, frame)
	}
	errs := make([]error, len(frames))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range events {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = dispatcher.HandleEvent(ctx, events[i])
		}(i)
	}
	close(start) // released together, so the check-then-act window is real
	wg.Wait()
	return errs
}

// ---------------------------------------------------------------------------
// Finding 1 — the claim
// ---------------------------------------------------------------------------

// TestAccountClaim_ConcurrentDeletedReplayTerminatesExactlyOnce is the probe
// from the review, as a test. A redriven deletion and its live twin arrive
// together; without a claim BOTH pass the seq gate and BOTH call the terminal
// tier, and the terminal tier is the one that cannot be taken back.
func TestAccountClaim_ConcurrentDeletedReplayTerminatesExactlyOnce(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	terminator := newBlockingTerminator()
	fixture := newDispatchFixture(t, database,
		func(opts *Options) { opts.Terminator = terminator })

	var errs []error
	done := make(chan struct{})
	go func() {
		defer close(done)
		errs = handleConcurrently(t, fixture.dispatcher,
			accountFrameSeq(dispatchNativeDID, false, "deleted", 41),
			accountFrameSeq(dispatchNativeDID, false, "deleted", 41))
	}()

	select {
	case <-terminator.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("no racer reached the terminal seam")
	}
	// The other racer now has an unhurried window to read the watermark, decide
	// and act. Under a claim it spends that window BLOCKED on the claim; without
	// one it spends it sending a second Delete{Person}.
	time.Sleep(250 * time.Millisecond)
	close(terminator.release)
	<-done

	for _, err := range errs {
		require.NoError(t, err)
	}
	assert.Len(t, terminator.Calls(), 1,
		"ONE deletion, ONE withdrawal. The terminal tier is irreversible at every peer "+
			"at once, so a redriven frame racing its live twin must be serialized by a "+
			"claim — not by hoping the two reads do not overlap")
	assert.Equal(t, int64(41), appliedSeq(t, database, dispatchNativeDID))
}

// TestAccountClaim_StalePauseCannotLandAfterAReactivation is the other half of
// the same missing claim: the seq gate is read-then-write, so two racers can
// both pass it and the LOSER's write can land last.
func TestAccountClaim_StalePauseCannotLandAfterAReactivation(t *testing.T) {
	database := dispatchTestDB(t)
	fixture := newDispatchFixture(t, database)

	// Rounds, because an unguarded interleaving is a race rather than a
	// certainty: each round is a fresh actor, and ONE bad round is the bug.
	const rounds = 25
	for round := range rounds {
		did := fmt.Sprintf("did:plc:racer%019d", round)
		seedAPActor(t, database, did, fmt.Sprintf("racer%d", round))

		errs := handleConcurrently(t, fixture.dispatcher,
			accountFrameSeq(did, false, "deactivated", 5), // the redriven stale pause
			accountFrameSeq(did, true, "active", 6),       // the live reactivation
		)
		for _, err := range errs {
			require.NoError(t, err, "round %d", round)
		}

		require.False(t, deliveryPaused(t, database, did),
			"round %d: the seq-5 pause is STALE — seq 6 says the user is back. Applied in "+
				"either order the outcome must be the newer state, because the only thing "+
				"that repairs a wrongly paused actor is the NEXT live #account frame, and "+
				"there may not be one for days", round)
		require.Equal(t, int64(6), appliedSeq(t, database, did), "round %d", round)
	}
}

// ---------------------------------------------------------------------------
// Finding 2 — a nil terminator must not eat the deletion
// ---------------------------------------------------------------------------

// TestAccountDeleted_NilTerminalTierLeavesTheEventReplayable pins the one
// asymmetry that makes an unwired seam unrecoverable rather than merely
// unimplemented. Advancing the seq CLOSES the event: every redelivery of it is
// then rejected as stale, so a deployment that wires the terminal tier tomorrow
// can never act on the deletion that arrived today.
//
// The sibling RemoteContentDeleter path gets this right by making the durable
// record (the preference) BEFORE it reaches the seam. This path has nothing to
// record, so the un-advanced seq IS the record.
func TestAccountDeleted_NilTerminalTierLeavesTheEventReplayable(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	unwired := newDispatchFixture(t, database, func(opts *Options) { opts.Terminator = nil })

	require.NoError(t, unwired.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 51)),
		"a missing terminal tier must not fail the event")
	assert.Zero(t, appliedSeq(t, database, dispatchNativeDID),
		"and must not CONSUME it either: nothing was recorded and nothing was sent, so "+
			"advancing the seq would make the user's deletion permanently unreachable — "+
			"every later redelivery rejected as stale, by a bridge that never acted on it")

	// The deployment that wires the tier gets the event.
	terminator := &recordingTerminator{}
	wired := newDispatchFixture(t, database,
		func(opts *Options) { opts.Terminator = terminator })
	require.NoError(t, wired.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 51)))
	assert.Equal(t, []string{dispatchNativeDID}, terminator.DIDs(),
		"the same frame, redelivered to a build with the tier wired, still reaches it")
	assert.Equal(t, int64(51), appliedSeq(t, database, dispatchNativeDID),
		"and NOW the event is closed")
}

// ---------------------------------------------------------------------------
// Finding 3 — the terminal marker
// ---------------------------------------------------------------------------

// actorTombstoned reports whether the destructive tier withdrew the identity.
func actorTombstoned(t *testing.T, database *sql.DB, did string) bool {
	t.Helper()
	var tombstoned bool
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT tombstoned_at IS NOT NULL FROM ap_actors WHERE did = $1`, did).Scan(&tombstoned))
	return tombstoned
}

// TestAccountDeleted_TerminalStateReachesTheActorAndSurvivesAReactivation is
// the deployment where the DESTRUCTIVE seam has not landed: the terminal tier
// confirms the deletion and RECORDS it, and that record is the only terminal
// state in the database — no tombstone, because nothing was sent.
//
// The consume side wrote nothing at all: consent stayed OK on the actor row,
// which then disagreed with the preference that says this user is gone. Every
// reader that consults ap_actors WITHOUT the preference — webfinger, the actor
// document, the admission gate — went on treating a confirmed-deleted identity
// as live, and a later active=true frame with a greater seq is welcomed by a row
// that never learned anything happened.
func TestAccountDeleted_TerminalStateReachesTheActorAndSurvivesAReactivation(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	world := newTerminalWorld(t, database, false) // no destructive seam wired
	world.confirmer.set(true, nil)

	require.NoError(t, world.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 61)))

	pref := storedPref(t, database, dispatchNativeDID)
	require.NotNil(t, pref, "precondition: the terminal tier recorded the deletion")
	require.False(t, pref.Enabled)

	enabled, _ := actorEnabled(t, database, dispatchNativeDID)
	assert.False(t, enabled,
		"THE ACTOR ROW MUST AGREE WITH THE PREFERENCE. The opt-out door mirrors exactly "+
			"this onto ap_actors; the deletion door — the stronger of the two — left the "+
			"identity enabled, so it kept resolving through webfinger for a user the "+
			"bridge had just confirmed is gone")

	// The reactivation that must find a closed door.
	require.NoError(t, world.handle(t, accountFrameSeq(dispatchNativeDID, true, "active", 62)))

	enabled, _ = actorEnabled(t, database, dispatchNativeDID)
	assert.False(t, enabled,
		"an active=true frame at a greater seq must NOT re-open an identity whose deletion "+
			"was already acted on: the transient tier pauses and unpauses delivery, and it "+
			"has no business undoing a terminal decision")
	assert.False(t, storedPref(t, database, dispatchNativeDID).Enabled,
		"and the terminal record still stands")
	assert.Equal(t, int64(62), appliedSeq(t, database, dispatchNativeDID))
}

// tombstoningDeleter is the destructive seam modelled on the REAL one: an
// outbound.Purger's last act is ap_actors.TombstoneTx, and that tombstone —
// not anything the consumer writes — is where a purged deployment's terminality
// actually lives. The double writes it so this package can test what the
// consume side must NOT undo, without depending on outbound.
type tombstoningDeleter struct {
	actors store.APActors
}

func (d *tombstoningDeleter) DeleteRemoteContent(ctx context.Context, did string) error {
	return d.actors.Tombstone(ctx, did)
}

// TestAccountDeleted_PurgedIdentityIsNotResurrectedByAReactivation is the same
// question with the destructive tier WIRED, where terminality comes from the
// purge (tombstoned_at, which no code path clears). It pins that the consume
// side's own marker COMPOSES with the purge's rather than fighting it.
func TestAccountDeleted_PurgedIdentityIsNotResurrectedByAReactivation(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	confirmer := &stubConfirmer{}
	confirmer.set(true, nil)
	terminator, err := optout.NewTerminator(optout.Options{
		Confirmer: confirmer,
		Prefs:     store.NewFederationPrefs(database),
		Deleter:   &tombstoningDeleter{actors: store.NewAPActors(database)},
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	world := newDispatchFixture(t, database, func(opts *Options) { opts.Terminator = terminator })

	require.NoError(t, world.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 71)))
	require.True(t, actorTombstoned(t, database, dispatchNativeDID),
		"precondition: the purge withdrew the identity")

	require.NoError(t, world.handle(t, accountFrameSeq(dispatchNativeDID, true, "active", 72)))

	assert.True(t, actorTombstoned(t, database, dispatchNativeDID),
		"a withdrawal is terminal: peers were told this identity is gone and no peer "+
			"un-deletes, so nothing an #account frame says may clear it")
	enabled, _ := actorEnabled(t, database, dispatchNativeDID)
	assert.False(t, enabled,
		"and it stays disabled — signing new content as an actor peers were told is gone "+
			"is the one outcome the tombstone exists to prevent")
}

// ---------------------------------------------------------------------------
// Finding 4 — a frame with no seq
// ---------------------------------------------------------------------------

// TestAccount_FrameWithoutAPositiveSeqIsPermanentlyRejected closes the silent
// hole under the seq gate. last_account_seq defaults to 0 and the gate skips
// anything at or below it, so a frame carrying seq 0 — or none at all, which
// decodes to the same thing — is indistinguishable from a stale replay ON THE
// VERY FIRST EVENT. A Jetstream build or proxy that stopped emitting seq would
// take the whole account tier dark with clean metrics and a debug line reading
// "stale or duplicate".
//
// time_us gets exactly this treatment for every kind, for exactly this reason.
func TestAccount_FrameWithoutAPositiveSeqIsPermanentlyRejected(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{"zero", []byte(fmt.Sprintf(
			`{"did":%q,"time_us":7300,"kind":"account",`+
				`"account":{"did":%q,"seq":0,"time":"2026-08-13T10:00:00.000Z",`+
				`"active":false,"status":"deactivated"}}`,
			dispatchNativeDID, dispatchNativeDID))},
		{"absent", []byte(fmt.Sprintf(
			`{"did":%q,"time_us":7301,"kind":"account",`+
				`"account":{"did":%q,"time":"2026-08-13T10:00:00.000Z",`+
				`"active":false,"status":"deactivated"}}`,
			dispatchNativeDID, dispatchNativeDID))},
		{"negative", []byte(fmt.Sprintf(
			`{"did":%q,"time_us":7302,"kind":"account",`+
				`"account":{"did":%q,"seq":-3,"time":"2026-08-13T10:00:00.000Z",`+
				`"active":false,"status":"deactivated"}}`,
			dispatchNativeDID, dispatchNativeDID))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database := dispatchTestDB(t)
			seedAPActor(t, database, dispatchNativeDID, "alice")
			terminator := &recordingTerminator{}
			fixture := newDispatchFixture(t, database,
				func(opts *Options) { opts.Terminator = terminator })

			err := fixture.handle(t, tc.frame)
			require.Error(t, err,
				"a frame with no usable seq cannot be ORDERED, and an #account tier that "+
					"cannot order is not a tier — it is a coin flip between the stale copy "+
					"and the live one")
			assert.ErrorIs(t, err, ErrPermanentEvent,
				"and no retry adds a seq to a frame that has none, so it is dead-lettered "+
					"where an operator can SEE the tier went dark instead of reading a debug "+
					"line that says 'stale or duplicate'")

			assert.False(t, deliveryPaused(t, database, dispatchNativeDID),
				"nothing is applied from a frame that cannot be ordered")
			assert.Zero(t, appliedSeq(t, database, dispatchNativeDID))
			assert.Empty(t, terminator.DIDs())
		})
	}
}
