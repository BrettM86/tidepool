package consume

import (
	"context"
	"database/sql"
	stderrors "errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/optout"
	"tidepool/internal/store"
)

// TASK 17d, CYCLE 4 — THE FOUR OUTCOMES OF THE CONFIRM (decision 19).
//
// A #account frame is a CLAIM about a moment that may have passed: the reconnect
// rewind replays it, a user can reactivate, and what was true an hour ago need
// not be true now. The action it proposes — asking every peer to purge a user's
// content — is one no peer undoes. So the terminal tier confirms against the
// identity's own sources first, and the confirm has THREE answers, not two:
//
//	deleted → act;  live → do nothing, and be DONE with the event;
//	error   → we do not know, which is not "not deleted".
//
// Collapsing the third into either of the others is the bug the seam exists to
// prevent, and it is 17c-3's P1-c again (absent and unparseable both becoming
// nil, so an unreadable expiry meant a permanent ban): read as live it silently
// drops a real deletion and leaves the user federated forever; read as deleted it
// erases someone who never left.
//
// THE SEQ IS THE OTHER HALF, and it is what separates "live" from "unknown"
// operationally. #account is ungated by the rev gate — its own per-DID seq is
// the ordering guard — so an outcome that does not advance it makes the event
// redrivable, and one that does closes it. A stale deletion for a live account
// must NOT wedge every later status change for that DID behind itself; an
// unconfirmable one must not be silently consumed.
//
// The confirmer here is a stub on purpose: the real PLC→#atproto_pds→
// getRepoStatus resolver is covered where it lives. What is under test is what
// the tier DOES with each of the three answers.

// stubConfirmer is the AccountConfirmer seam, driven to each of its three
// outcomes. It counts calls so "nothing destructive happened" can be told apart
// from "the tier was never reached".
type stubConfirmer struct {
	mu      sync.Mutex
	deleted bool
	err     error
	calls   []string
}

func (c *stubConfirmer) AccountStatus(_ context.Context, did string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, did)
	return c.deleted, c.err
}

func (c *stubConfirmer) Calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func (c *stubConfirmer) set(deleted bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted, c.err = deleted, err
}

// terminalWorld is a dispatcher whose terminal tier is the REAL
// optout.Terminator — the thing that owns the confirm-then-act sequence —
// wired to a stub confirmer and a recording destructive seam.
type terminalWorld struct {
	*dispatchFixture
	confirmer *stubConfirmer
	purger    *recordingDeleter
}

// newTerminalWorld builds that world. deleterWired=false is the deployment
// where the destructive tier has not landed: the Terminator's Deleter is nil,
// which is a different thing from a deleter that is never called.
func newTerminalWorld(t *testing.T, database *sql.DB, deleterWired bool) *terminalWorld {
	t.Helper()
	confirmer := &stubConfirmer{}
	purger := &recordingDeleter{}
	opts := optout.Options{
		Confirmer: confirmer,
		Prefs:     store.NewFederationPrefs(database),
		// Discarded because NewTerminator warns at CONSTRUCTION when the seam is
		// absent: a log assertion here would pass without the handler ever
		// announcing anything.
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if deleterWired {
		opts.Deleter = purger
	}
	terminator, err := optout.NewTerminator(opts)
	require.NoError(t, err)

	fixture := newDispatchFixture(t, database,
		func(o *Options) { o.Terminator = terminator })
	return &terminalWorld{dispatchFixture: fixture, confirmer: confirmer, purger: purger}
}

// appliedSeq is the last #account seq the consumer recorded for a DID. It is
// the cursor for that DID's status history: an event that does not advance it
// is redelivered, and one that does is closed.
func appliedSeq(t *testing.T, database *sql.DB, did string) int64 {
	t.Helper()
	var seq int64
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT last_account_seq FROM ap_actors WHERE did = $1`, did).Scan(&seq))
	return seq
}

// storedPref reads the terminal preference, or nil when none was recorded.
//
// NOT-FOUND IS THE ONLY ABSENCE. Swallowing every error here would make
// "nothing was recorded" true of a query that failed, a table that was dropped
// and a typo in the column list — three different nothings, all reading as the
// assertion passing, on the tests whose whole content is that a row does not
// exist.
func storedPref(t *testing.T, database *sql.DB, did string) *store.FederationPref {
	t.Helper()
	pref, err := store.NewFederationPrefs(database).Get(context.Background(), did)
	if errors.IsNotFound(err) {
		return nil
	}
	require.NoError(t, err, "read the federation preference for %s", did)
	return pref
}

// ---------------------------------------------------------------------------
// (1) CONFIRMED DELETED — the destructive path runs
// ---------------------------------------------------------------------------

func TestConfirmedDeletion_RunsTheDestructivePathAndClosesTheEvent(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	world := newTerminalWorld(t, database, true)
	world.confirmer.set(true, nil)

	require.NoError(t, world.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 11)))

	require.Equal(t, []string{dispatchNativeDID}, world.confirmer.Calls(),
		"the event is never acted on directly: the DID's own sources are asked first, "+
			"because a deletion that was true when the frame was written may have been "+
			"reversed by the time we replay it")
	assert.Equal(t, []string{dispatchNativeDID}, world.purger.DIDs(),
		"and a CONFIRMED deletion runs the destructive tier — a user who is gone must not "+
			"keep a federated identity speaking for them on every instance that holds "+
			"their content")

	pref := storedPref(t, database, dispatchNativeDID)
	require.NotNil(t, pref,
		"the terminal state is RECORDED: peers that honour a Delete cannot restore what "+
			"they dropped, so the decision has to survive a crash between deciding and sending")
	assert.False(t, pref.Enabled)
	assert.True(t, pref.DeleteRemote, "recorded as the destructive tier, not as a soft opt-out")
	assert.Equal(t, store.FederationPrefSourceAccount, pref.Source,
		"and attributed to the ACCOUNT: the user wrote no record and there is none left to "+
			"fetch, so this column is the only place an operator can tell a deletion from "+
			"an opt-out")

	assert.Equal(t, int64(11), appliedSeq(t, database, dispatchNativeDID),
		"the event is closed: a handled deletion must not be replayed into a second "+
			"purge on the next reconnect")
}

// ---------------------------------------------------------------------------
// (2) CONFIRMED LIVE — nothing destructive, and the seq STILL ADVANCES
// ---------------------------------------------------------------------------

func TestConfirmedLiveAccountIsLeftAloneAndTheEventStillAdvances(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	world := newTerminalWorld(t, database, true)
	// The frame says deleted; the identity says otherwise. This is the ordinary
	// shape of a rewound cursor replaying a deletion a user has since reversed.
	world.confirmer.set(false, nil)

	require.NoError(t, world.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 12)),
		"a divergence between the firehose and the identity is not a failure: it is the "+
			"answer the confirm exists to get")

	require.NotEmpty(t, world.confirmer.Calls(), "precondition: the confirm really ran")
	assert.Empty(t, world.purger.DIDs(),
		"NOTHING destructive: an unconfirmed deletion that purged anyway would erase a "+
			"living user's content from every instance holding it, on the strength of an "+
			"event we just proved wrong")
	assert.Nil(t, storedPref(t, database, dispatchNativeDID),
		"and nothing is recorded either — a preference row here would stop federating for "+
			"a user who never asked, with no record left to contradict it")

	paused := deliveryPaused(t, database, dispatchNativeDID)
	assert.False(t, paused, "the live account keeps delivering")
	enabled, _ := actorEnabled(t, database, dispatchNativeDID)
	assert.True(t, enabled, "under its original identity")

	assert.Equal(t, int64(12), appliedSeq(t, database, dispatchNativeDID),
		"AND THE SEQ ADVANCES. Leaving it un-advanced would hold a stale deletion open "+
			"forever: the same frame redelivers on every reconnect, and — because the seq "+
			"gate rejects anything at or below the last APPLIED one — every later status "+
			"change for this DID queues behind an event that can never succeed")
}

// ---------------------------------------------------------------------------
// (3) UNKNOWN (the confirm failed) — retryable, nothing sent, seq NOT advanced
// ---------------------------------------------------------------------------

// TestUnknownConfirmOutcomeIsRetryableAndAdvancesNothing is the case the seam
// exists for. "We could not confirm" must not collapse into either verdict: the
// event has to come back, which means it must NOT be closed.
func TestUnknownConfirmOutcomeIsRetryableAndAdvancesNothing(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	world := newTerminalWorld(t, database, true)
	world.confirmer.set(false, stderrors.New("plc directory unreachable"))

	err := world.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 13))
	require.Error(t, err,
		"an unconfirmable deletion is an ERROR, which is what keeps the event redrivable: "+
			"returning nil would consume the user's deletion because a directory was down "+
			"for a minute")
	assert.NotErrorIs(t, err, ErrPermanentEvent,
		"and a TRANSIENT one: dead-lettering it exhausted turns a network blip into a "+
			"deletion that never happens")

	assert.Empty(t, world.purger.DIDs(),
		"nothing is sent on an unknown: acting on a stale deletion is the unrecoverable "+
			"direction, and it is unrecoverable at every peer at once")
	assert.Nil(t, storedPref(t, database, dispatchNativeDID),
		"and nothing is recorded, so a later confirm decides on the evidence rather than "+
			"on a half-written state")
	assert.Zero(t, appliedSeq(t, database, dispatchNativeDID),
		"the seq must NOT advance: it is the only thing that brings this event back, and a "+
			"deletion consumed by an advance is gone — the user stays federated forever "+
			"with nothing left to notice it")

	// And the retry is a real one: the same frame, once the directory answers.
	world.confirmer.set(true, nil)
	require.NoError(t, world.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 13)),
		"the redelivered frame is not rejected as stale — an un-advanced seq is what makes "+
			"the redrive possible")
	assert.Equal(t, []string{dispatchNativeDID}, world.purger.DIDs(),
		"and now it acts, on a confirmation instead of an assumption")
	assert.Equal(t, int64(13), appliedSeq(t, database, dispatchNativeDID))
}

// ---------------------------------------------------------------------------
// (4) CONFIRMED DELETED, NO DESTRUCTIVE SEAM — recorded, nothing sent
// ---------------------------------------------------------------------------

// TestConfirmedDeletionWithNoDestructiveSeamRecordsTheBacklogAndSendsNothing
// pins the deployment where the tier has not landed. It matches what
// deleteRemote=true already does with no seam wired — record the intent, leave
// the work in the backlog — rather than degrading into a pause, which looks
// handled in the database and is not.
func TestConfirmedDeletionWithNoDestructiveSeamRecordsTheBacklogAndSendsNothing(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	world := newTerminalWorld(t, database, false) // no Deleter on the Terminator
	world.confirmer.set(true, nil)

	require.NoError(t, world.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 14)),
		"a missing destructive seam must not panic and must not fail the event")

	pref := storedPref(t, database, dispatchNativeDID)
	require.NotNil(t, pref,
		"the preference is recorded even with nowhere to send it, so the deletion can be "+
			"acted on from the backlog when the tier lands instead of being lost")
	assert.False(t, pref.Enabled)
	assert.True(t, pref.DeleteRemote)
	assert.Equal(t, store.FederationPrefSourceAccount, pref.Source)

	assert.Zero(t, countRows(t, database, "outbound_activities"),
		"and NOTHING is sent: there is no seam to send it with, and a tier that half-acts "+
			"is worse than one that has not landed")
	assert.Zero(t, countRows(t, database, "outbound_deliveries"))

	assert.False(t, deliveryPaused(t, database, dispatchNativeDID),
		"it is NOT degraded into a pause: a pause says 'this user is coming back', which is "+
			"the one thing a confirmed deletion rules out, and it would satisfy an operator "+
			"looking for evidence the deletion was handled")

	assert.Equal(t, int64(14), appliedSeq(t, database, dispatchNativeDID),
		"the event is closed on the strength of the RECORD: the backlog is durable, so "+
			"replaying the frame forever adds nothing")
}
