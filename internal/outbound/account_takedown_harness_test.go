package outbound

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/store"
)

// The chunk-04 account-label additions to the takedown harness. An account
// `!takedown` names the persona's BARE DID (not at://did); a temporary ban
// carries exp as RFC 3339 text anchored to the database clock.

// labelExpLayout is the RFC 3339 UTC form an exp is written in.
const labelExpLayout = "2006-01-02T15:04:05.000Z"

// commitAccountLabel commits a `!takedown` (or its negation) on the bare DID
// did at the next seq through the store with the takedowns applier.
func (w *takedownWorld) commitAccountLabel(t *testing.T, did string, negate bool) {
	t.Helper()
	w.commitLabelExp(t, did, negate, nil)
}

// commitLabelExp is commitLabel with an exp (nil for none).
func (w *takedownWorld) commitLabelExp(t *testing.T, subject string, negate bool, exp *string) {
	t.Helper()
	w.labelSeq++
	commitTakedownLabelExp(t, w.labelStore, w.labelKey, w.labelSeq, subject, negate, exp)
}

// dbExp is the database clock plus ahead, formatted as a label exp.
func (w *takedownWorld) dbExp(t *testing.T, ahead time.Duration) string {
	t.Helper()
	var now time.Time
	require.NoError(t, w.conn.QueryRow(`SELECT now()`).Scan(&now))
	return now.Add(ahead).UTC().Format(labelExpLayout)
}

// awaitDBClockPast waits (deadline 10 s) until the database clock is past exp.
func (w *takedownWorld) awaitDBClockPast(t *testing.T, exp string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var past bool
		require.NoError(t, w.conn.QueryRow(`SELECT now() > $1::timestamptz`, exp).Scan(&past))
		return past
	}, 10*time.Second, 50*time.Millisecond, "the database clock did not pass exp %s within 10s", exp)
}

// paused is did's persona's DeliveryPaused.
func (w *takedownWorld) paused(t *testing.T, did string) bool {
	t.Helper()
	actor, err := store.NewAPActors(w.conn).GetByDID(context.Background(), did)
	require.NoError(t, err, "read %s's persona", did)
	return actor.DeliveryPaused
}

// deliverPosts enqueues each post's Create and drains, so Lemmy received and
// accepted every one; it requires Lemmy received exactly those Creates.
func (w *takedownWorld) deliverPosts(t *testing.T, posts ...tdNativePost) {
	t.Helper()
	want := []string{}
	for _, post := range posts {
		w.enqueuePost(t, post, "create", 0)
		want = append(want, "Create "+post.apID())
	}
	w.drain(t)
	require.Equal(t, want, w.receivedLines(t), "precondition: Lemmy received each post's Create")
	for _, post := range posts {
		require.True(t, rawAccepted(t, w.conn, post.atURI()), "precondition: %s is accepted", post.atURI())
	}
}

// receivedLinesSince is receivedLines after the first n activities.
func (w *takedownWorld) receivedLinesSince(t *testing.T, n int) []string {
	t.Helper()
	lines := w.receivedLines(t)
	if n >= len(lines) {
		return []string{}
	}
	return lines[n:]
}

// commitTakedownLabelExp is commitTakedownLabel with an exp (nil for none).
func commitTakedownLabelExp(t *testing.T, labelStore *labels.PostgresStore, key atcrypto.PrivateKey,
	seq int64, subject string, negate bool, exp *string) {
	t.Helper()
	l := labeling.Label{SourceDID: tdLabelerDID, URI: subject, Val: tdLabelTakedown, ExpiresAt: exp}
	if negate {
		l.Negated = &negate
	}
	require.NoError(t, labelStore.Commit(context.Background(), labels.Cursor{Epoch: 0, Seq: seq},
		labelstest.Sign(t, key, l)), "commit label seq %d", seq)
}

// startLabelConsumer runs a labels.Consumer with opts (LabelerDID, Resolver,
// Logger and backoffs filled for server and key) until the test ends.
func startLabelConsumer(t *testing.T, opts labels.ConsumerOptions, server *labelstest.Server,
	key atcrypto.PrivateKey) {
	t.Helper()
	opts.LabelerDID = tdLabelerDID
	opts.Resolver = takedownResolver{endpoint: server.URL, key: labelstest.PublicKey(t, key)}
	opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	opts.MinBackoff, opts.MaxBackoff = 10*time.Millisecond, 50*time.Millisecond
	consumer, err := labels.NewConsumer(opts)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		consumer.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-returned:
		case <-time.After(5 * time.Second):
			t.Error("the label consumer did not stop within 5s")
		}
	})
}

// fakeAccountStatus is the PDS confirmation read the dispatcher makes for an
// #account event: it answers status, or fails with err when err is set.
type fakeAccountStatus struct {
	mu     sync.Mutex
	status consume.RepoStatus
	err    error
	// reads answers the next reads, one each, before status and err apply;
	// each runs without the mutex held, so it may handle another frame.
	reads []func() (consume.RepoStatus, error)
}

func (f *fakeAccountStatus) RepoStatus(context.Context, string) (consume.RepoStatus, error) {
	f.mu.Lock()
	if len(f.reads) > 0 {
		read := f.reads[0]
		f.reads = f.reads[1:]
		f.mu.Unlock()
		return read()
	}
	defer f.mu.Unlock()
	if f.err != nil {
		return consume.RepoStatus{}, f.err
	}
	return f.status, nil
}

// onReads makes the next reads answer from reads, in order.
func (f *fakeAccountStatus) onReads(reads ...func() consume.RepoStatus) {
	for _, read := range reads {
		f.onReadResults(func() (consume.RepoStatus, error) { return read(), nil })
	}
}

// onReadResults is onReads for reads that may fail.
func (f *fakeAccountStatus) onReadResults(reads ...func() (consume.RepoStatus, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads = append(f.reads, reads...)
}

// answer makes the PDS answer status from now on.
func (f *fakeAccountStatus) answer(status consume.RepoStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status, f.err = status, nil
}

// fail makes the confirmation read fail from now on.
func (f *fakeAccountStatus) fail() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = fmt.Errorf("pds getRepoStatus unreachable")
}

// accountStatusWorld is the takedown world plus a real consume.Dispatcher
// whose #account confirmation reads the fake PDS and whose account takedown
// seam is the world's Takedowns.
type accountStatusWorld struct {
	*takedownWorld
	dispatcher *consume.Dispatcher
	pds        *fakeAccountStatus
	accountSeq int64
}

func newAccountStatusWorld(t *testing.T) *accountStatusWorld {
	t.Helper()
	w := newTakedownWorld(t)
	pds := &fakeAccountStatus{status: consume.RepoStatus{Active: true}}
	dispatcher, err := consume.NewDispatcher(consume.Options{
		DB:               w.conn,
		Actors:           w.personas,
		Enqueuer:         w.enqueuer,
		Resolver:         noHandleLookups{},
		UserOrigin:       outUserOrigin,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		AccountStatus:    pds,
		AccountTakedowns: w.takedowns,
	})
	require.NoError(t, err)
	return &accountStatusWorld{takedownWorld: w, dispatcher: dispatcher, pds: pds}
}

// nextAccountEvent is the relay's #account frame for tdAuthorDID at the next
// seq, decoded from wire JSON the way the connector does. An empty status is
// omitted, as the relay omits it for an active account.
func (s *accountStatusWorld) nextAccountEvent(t *testing.T, active bool, status string) *consume.JetstreamEvent {
	t.Helper()
	s.accountSeq++
	statusField := ""
	if status != "" {
		statusField = fmt.Sprintf(`,"status":%q`, status)
	}
	frame := fmt.Sprintf(`{"did":%q,"time_us":%d,"kind":"account",`+
		`"account":{"did":%q,"seq":%d,"time":"2026-11-09T10:00:00.000Z","active":%t%s}}`,
		tdAuthorDID, 1_793_527_200_000_000+s.accountSeq, tdAuthorDID, s.accountSeq, active, statusField)
	var event consume.JetstreamEvent
	require.NoError(t, json.Unmarshal([]byte(frame), &event), "the test frame is valid wire JSON")
	return &event
}

// handle runs one event through the dispatcher.
func (s *accountStatusWorld) handle(event *consume.JetstreamEvent) error {
	return s.dispatcher.HandleEvent(context.Background(), event)
}

// confirmed delivers the relay #account frame for status (active when status
// is "") while the PDS answers the same status, and requires it handled.
func (s *accountStatusWorld) confirmed(t *testing.T, status string) {
	t.Helper()
	active := status == ""
	s.pds.answer(consume.RepoStatus{Active: active, Status: status})
	require.NoError(t, s.handle(s.nextAccountEvent(t, active, status)),
		"a confirmed #account %q is handled", status)
}

// accountStep is one step of an account-status scenario and what follows it,
// once the queue is drained.
type accountStep struct {
	name string
	do   func(t *testing.T, s *accountStatusWorld)
	// wantSent is what Lemmy receives after this step, in order.
	wantSent   []string
	wantPaused bool
	// wantDeletes and wantUndos are the outbound Delete and Undo activities
	// enqueued so far.
	wantDeletes, wantUndos int
}

// banStep commits L on tdAuthorDID; unbanStep commits its negation.
func banStep(t *testing.T, s *accountStatusWorld) { s.commitAccountLabel(t, tdAuthorDID, false) }

func unbanStep(t *testing.T, s *accountStatusWorld) { s.commitAccountLabel(t, tdAuthorDID, true) }

// confirmedStep is a confirmed #account status ("" for active).
func confirmedStep(status string) func(t *testing.T, s *accountStatusWorld) {
	return func(t *testing.T, s *accountStatusWorld) { s.confirmed(t, status) }
}

// runAccountSteps runs steps in order after the first sent activities,
// draining after each and checking what it sent, the pause and the counts.
func (s *accountStatusWorld) runAccountSteps(t *testing.T, sent int, steps []accountStep) {
	t.Helper()
	for i, step := range steps {
		step.do(t, s)
		s.drain(t)
		got := s.receivedLinesSince(t, sent)
		assert.Equal(t, step.wantSent, got, "step %d (%s) sends", i+1, step.name)
		assert.Equal(t, step.wantPaused, s.paused(t, tdAuthorDID), "P's pause after step %d (%s)", i+1, step.name)
		assert.Equal(t, step.wantDeletes, activityCount(t, s.conn, "Delete"),
			"Deletes enqueued after step %d (%s)", i+1, step.name)
		assert.Equal(t, step.wantUndos, activityCount(t, s.conn, "Undo"),
			"Undos enqueued after step %d (%s)", i+1, step.name)
		sent += len(got)
	}
}
