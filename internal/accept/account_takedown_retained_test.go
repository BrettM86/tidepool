package accept

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/outbound"
	"tidepool/internal/repo"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Chunk 04 B6: work of a persona paused by a takedown source (an account
// `!takedown` label, or a confirmed `takendown` #account status) is retained:
// a queued comment is held, not cancelled, and an edit of an accepted post is
// kept as the object's latest state instead of becoming an admission removal.
// Both are delivered once the pause clears.

const (
	atLabelerDID = "did:web:coves.social"

	atCommentRKey   = "3lzcmntaaaa11"
	atCommentRev    = "3lzcmntrev001"
	atCommentURI    = "at://" + acAuthorDID + "/social.coves.community.comment/" + atCommentRKey
	atCommentAPID   = acUserOrigin + "/ap/object/" + acAuthorDID + "/social.coves.community.comment/" + atCommentRKey
	atPostAPID      = acUserOrigin + "/ap/object/" + acAuthorDID + "/social.coves.community.postv2/" + acPostRKey
	atEditedTitle   = "hello again, edited during the ban"
	atEditedContent = "the body of the post, edited while the account was down"
)

// atRecorder is the community inbox: it records every activity the worker
// POSTs, in order, and accepts each one.
type atRecorder struct {
	mu       sync.Mutex
	received []map[string]any
}

func (r *atRecorder) SendActivityAs(_ context.Context, _ *ap.Signer, _ string, activity any) error {
	raw, err := json.Marshal(activity)
	if err != nil {
		return err
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.received = append(r.received, body)
	return nil
}

func (r *atRecorder) all() []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]map[string]any(nil), r.received...)
}

// atLine is "Type object" for an activity: the AP id a Create/Update carries
// or a Delete names; an Undo reads "Undo{Delete} <object>".
func atLine(body map[string]any) string {
	kind, _ := body["type"].(string)
	switch object := body["object"].(type) {
	case string:
		return kind + " " + object
	case map[string]any:
		if inner, _ := object["type"].(string); kind == "Undo" {
			return "Undo{" + inner + "} " + strings.TrimPrefix(atLine(object), inner+" ")
		}
		id, _ := object["id"].(string)
		return kind + " " + id
	default:
		return kind
	}
}

// atRepoStatus is the PDS confirmation read for #account events.
// A non-nil err fails every read.
type atRepoStatus struct {
	mu     sync.Mutex
	status consume.RepoStatus
	err    error
}

func (f *atRepoStatus) RepoStatus(context.Context, string) (consume.RepoStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status, f.err
}

func (f *atRepoStatus) answer(status consume.RepoStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status, f.err = status, nil
}

func (f *atRepoStatus) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// atWorld is the accept engine behind a dispatcher with the account-status
// seams, the outbound Takedowns and its label store, and a worker POSTing to
// atRecorder.
type atWorld struct {
	conn       *sql.DB
	repos      *repo.Manager
	engine     *Engine
	dispatcher *consume.Dispatcher
	worker     *outbound.Worker
	inbox      *atRecorder
	pds        *atRepoStatus
	takedowns  *outbound.Takedowns
	labelStore *labels.PostgresStore
	labelKey   atcrypto.PrivateKey
	labelSeq   int64
	accountSeq int64
	sent       int
}

func newATWorld(t *testing.T) *atWorld {
	t.Helper()
	conn := acceptanceDB(t)
	testutil.Truncate(t, conn, "coves_labels", "coves_label_cursor",
		"outbound_takedown_sources", "outbound_takedowns")
	// acceptanceDB does not reset these, so a takedown a test leaves standing
	// would remove the next test's post.
	t.Cleanup(func() {
		testutil.Truncate(t, conn, "coves_labels", "coves_label_cursor",
			"outbound_takedown_sources", "outbound_takedowns")
	})
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enqueuer := realEnqueuer(t, conn)
	engine := engineWith(t, conn, repos, enqueuer)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	takedowns, err := outbound.NewTakedowns(outbound.TakedownsOptions{DB: conn, Enqueuer: enqueuer, Logger: logger})
	require.NoError(t, err)
	pds := &atRepoStatus{status: consume.RepoStatus{Active: true}}
	dispatcher, err := consume.NewDispatcher(consume.Options{
		DB:               conn,
		Actors:           newMinter(t, conn),
		Enqueuer:         enqueuer,
		Resolver:         stubResolver{},
		Engine:           engine,
		UserOrigin:       acUserOrigin,
		Logger:           logger,
		AccountStatus:    pds,
		AccountTakedowns: takedowns,
	})
	require.NoError(t, err)
	inbox := &atRecorder{}
	worker, err := outbound.NewWorker(outbound.WorkerOptions{
		DB:          conn,
		Actors:      store.NewAPActors(conn),
		Signers:     newMinter(t, conn),
		Inboxes:     stubInboxResolver{},
		Sender:      inbox,
		Lease:       time.Minute,
		BackoffBase: time.Millisecond,
	})
	require.NoError(t, err)
	return &atWorld{
		conn:       conn,
		repos:      repos,
		engine:     engine,
		dispatcher: dispatcher,
		worker:     worker,
		inbox:      inbox,
		pds:        pds,
		takedowns:  takedowns,
		labelStore: labels.NewPostgresStore(conn, takedowns.LabelApplier()),
		labelKey:   labelstest.NewKey(t),
	}
}

// drain makes every pending delivery due and delivers until none is left.
func (w *atWorld) drain(t *testing.T) {
	t.Helper()
	_, err := w.conn.Exec(`UPDATE outbound_deliveries SET next_attempt_at = now() WHERE state = 'pending'`)
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		worked, err := w.worker.DeliverNext(context.Background())
		require.NoError(t, err)
		if !worked {
			return
		}
	}
	t.Fatal("the worker did not drain within 20 deliveries")
}

// newlySent is what the inbox received since the last call, as lines, and
// the activities themselves.
func (w *atWorld) newlySent() ([]string, []map[string]any) {
	all := w.inbox.all()
	fresh := all[w.sent:]
	w.sent = len(all)
	lines := []string{}
	for _, body := range fresh {
		lines = append(lines, atLine(body))
	}
	return lines, fresh
}

// commitAccountLabel commits L (or its negation) on the author's bare DID.
func (w *atWorld) commitAccountLabel(t *testing.T, negate bool) {
	t.Helper()
	w.commitTakedownLabel(t, acAuthorDID, negate)
}

// commitTakedownLabel commits `!takedown` (or its negation) on subject, at
// the next cursor seq, through w.labelStore.
func (w *atWorld) commitTakedownLabel(t *testing.T, subject string, negate bool) {
	t.Helper()
	w.labelSeq++
	l := labeling.Label{SourceDID: atLabelerDID, URI: subject, Val: "!takedown"}
	if negate {
		l.Negated = &negate
	}
	require.NoError(t, w.labelStore.Commit(context.Background(), labels.Cursor{Epoch: 0, Seq: w.labelSeq},
		labelstest.Sign(t, w.labelKey, l)), "commit label seq %d", w.labelSeq)
}

// confirmed handles the relay's #account frame for status ("" for active)
// while the PDS answers the same status.
func (w *atWorld) confirmed(t *testing.T, status string) {
	t.Helper()
	active := status == ""
	w.relayed(t, active, status, consume.RepoStatus{Active: active, Status: status})
}

// relayed handles the relay's #account frame (active, status) while the PDS
// answers pds.
func (w *atWorld) relayed(t *testing.T, active bool, status string, pds consume.RepoStatus) {
	t.Helper()
	w.pds.answer(pds)
	require.NoError(t, w.handleAccountFrame(active, status), "a confirmed #account %q is handled", status)
}

// handleAccountFrame hands the dispatcher the relay's next #account frame
// (active, status) and returns what HandleEvent returned.
func (w *atWorld) handleAccountFrame(active bool, status string) error {
	w.accountSeq++
	statusField := ""
	if status != "" {
		statusField = fmt.Sprintf(`,"status":%q`, status)
	}
	frame := fmt.Sprintf(`{"did":%q,"time_us":%d,"kind":"account",`+
		`"account":{"did":%q,"seq":%d,"time":"2026-11-09T10:00:00.000Z","active":%t%s}}`,
		acAuthorDID, acPostTimeUS+100+w.accountSeq, acAuthorDID, w.accountSeq, active, statusField)
	var event consume.JetstreamEvent
	if err := json.Unmarshal([]byte(frame), &event); err != nil {
		return fmt.Errorf("decode the test #account frame: %w", err)
	}
	return w.dispatcher.HandleEvent(context.Background(), &event)
}

// commentEvent is the author's comment replying to the accepted post.
func commentEvent() *consume.JetstreamEvent {
	return &consume.JetstreamEvent{
		DID:    acAuthorDID,
		TimeUS: acPostTimeUS + 10,
		Kind:   "commit",
		Commit: &consume.CommitEvent{
			Rev:        atCommentRev,
			Operation:  "create",
			Collection: "social.coves.community.comment",
			RKey:       atCommentRKey,
			CID:        acPostCID,
			Record: map[string]any{
				"$type": "social.coves.community.comment",
				"reply": map[string]any{
					"root":   map[string]any{"uri": acPostURI, "cid": acPostCID},
					"parent": map[string]any{"uri": acPostURI, "cid": acPostCID},
				},
				"content":   "a comment queued before the ban",
				"createdAt": "2026-08-12T10:05:00.000Z",
			},
		},
	}
}

func (w *atWorld) postTombstoned(t *testing.T) bool {
	t.Helper()
	var tombstoned bool
	require.NoError(t, w.conn.QueryRow(`SELECT tombstoned_at IS NOT NULL FROM outbound_objects WHERE at_uri = $1`,
		acPostURI).Scan(&tombstoned))
	return tombstoned
}

func (w *atWorld) commentDeliveryState(t *testing.T) store.DeliveryState {
	t.Helper()
	delivery, err := store.NewOutboundDeliveries(w.conn).Get(context.Background(),
		consume.ActivityID(acUserOrigin, atCommentURI, "create", 0), acCommunityInbox)
	require.NoError(t, err, "the comment's Create delivery exists")
	return delivery.State
}

// requireNoRemoval asserts the post's admission still stands: accepted in the
// ledger, no removal record, and its outbound object never tombstoned.
func (w *atWorld) requireNoRemoval(t *testing.T, when string) {
	t.Helper()
	status, code := admissionOf(t, w.conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusAccepted, status, "%s: the post's admission stays accepted", when)
	assert.Empty(t, code, "%s: no decision code is recorded against the post", when)
	assert.Zero(t, countWhere(t, w.conn, "admissions", "status", StatusRemoved),
		"%s: no admission removal is recorded", when)
	_, removed := removalStandsAt(t, w.repos, acCommunityDID, acPostURI)
	assert.False(t, removed, "%s: no removal record stands in the community repo", when)
	assert.False(t, w.postTombstoned(t), "%s: outbound_objects.tombstoned_at is not set", when)
}

// TestAccountTakedown_PausedAuthorsQueuedCommentAndEditAreDeliveredOnRestore
// (B6): the author's post is accepted and delivered, a comment is queued and
// never delivered, then a takedown source pauses the author (row 1: account
// label L; row 2: a confirmed `takendown` #account status). The post gets its
// Delete and the comment is held. The author edits the post during the ban:
// the edit is admitted through the accept engine without an admission removal
// or a tombstone, and nothing is sent. When the source lifts (L negated;
// confirmed `active`), the comment's Create is delivered and the post's edit
// reaches Lemmy as an Update with the edited content after its Undo{Delete},
// with no further upstream edit.
func TestAccountTakedown_PausedAuthorsQueuedCommentAndEditAreDeliveredOnRestore(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ban, lift func(t *testing.T, w *atWorld)
	}{
		{
			name: "account label",
			ban:  func(t *testing.T, w *atWorld) { w.commitAccountLabel(t, false) },
			lift: func(t *testing.T, w *atWorld) { w.commitAccountLabel(t, true) },
		},
		{
			name: "confirmed takendown status",
			ban:  func(t *testing.T, w *atWorld) { w.confirmed(t, "takendown") },
			lift: func(t *testing.T, w *atWorld) { w.confirmed(t, "") },
		},
		{
			// The pause phase 1 writes carries the relay's deactivated; the
			// PDS's takendown is what holds the work.
			name: "relay deactivated, PDS answers takendown",
			ban: func(t *testing.T, w *atWorld) {
				w.relayed(t, false, "deactivated", consume.RepoStatus{Active: false, Status: "takendown"})
			},
			lift: func(t *testing.T, w *atWorld) { w.confirmed(t, "") },
		},
		{
			name: "relay inactive with no status, PDS answers takendown",
			ban: func(t *testing.T, w *atWorld) {
				w.relayed(t, false, "", consume.RepoStatus{Active: false, Status: "takendown"})
			},
			lift: func(t *testing.T, w *atWorld) { w.confirmed(t, "") },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newATWorld(t)
			ctx := context.Background()

			admittedCreate(t, w.dispatcher)
			w.drain(t)
			sent, _ := w.newlySent()
			require.Equal(t, []string{"Create " + atPostAPID}, sent, "setup: the post is delivered")

			require.NoError(t, w.dispatcher.HandleEvent(ctx, commentEvent()), "the comment is enqueued")

			tc.ban(t, w)
			w.drain(t)
			sent, _ = w.newlySent()
			assert.Equal(t, []string{"Delete " + atPostAPID}, sent,
				"the ban deletes the delivered post and sends nothing for the comment")
			assert.Equal(t, store.DeliveryStateHeld, w.commentDeliveryState(t), "the queued comment is held, not cancelled")

			editedRecord := pv2Record(func(r map[string]any) {
				r["title"] = atEditedTitle
				r["content"] = atEditedContent
			})
			require.NoError(t, w.dispatcher.HandleEvent(ctx,
				postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1, editedRecord)),
				"the edit during the ban is handled")
			w.requireNoRemoval(t, "after the edit")
			w.drain(t)
			sent, _ = w.newlySent()
			assert.Equal(t, []string{}, sent, "nothing is sent for the edit while the author is down")

			tc.lift(t, w)
			w.drain(t)
			sent, bodies := w.newlySent()
			assert.ElementsMatch(t, []string{
				"Undo{Delete} " + atPostAPID,
				"Update " + atPostAPID,
				"Create " + atCommentAPID,
			}, sent, "the restore sends the post's Undo{Delete}, its latest edit and the held comment")
			undoAt, updateAt := -1, -1
			for i, line := range sent {
				switch line {
				case "Undo{Delete} " + atPostAPID:
					undoAt = i
				case "Update " + atPostAPID:
					updateAt = i
					object, _ := bodies[i]["object"].(map[string]any)
					assert.Equal(t, atEditedTitle, object["name"], "the delivered Update carries the edited title")
					source, _ := object["source"].(map[string]any)
					assert.Equal(t, atEditedContent, source["content"], "the delivered Update carries the edited content")
				}
			}
			assert.Less(t, undoAt, updateAt, "the post's edit is delivered after its Undo{Delete}")
			assert.Equal(t, store.DeliveryStateDelivered, w.commentDeliveryState(t), "the held comment is delivered")
			w.requireNoRemoval(t, "after the restore")
		})
	}
}

// TestAccountTakedown_ADeactivatedAuthorsEditOfAnAcceptedPostStaysPaused (B6):
// the edit carve-out is for a takedown pause only. An author paused by a
// confirmed `deactivated` status alone (no account label, no takendown or
// suspended status) who edits an accepted post gets DecisionPaused.
func TestAccountTakedown_ADeactivatedAuthorsEditOfAnAcceptedPostStaysPaused(t *testing.T) {
	w := newATWorld(t)
	ctx := context.Background()

	admittedCreate(t, w.dispatcher)
	w.drain(t)
	sent, _ := w.newlySent()
	require.Equal(t, []string{"Create " + atPostAPID}, sent, "setup: the post is delivered")

	w.confirmed(t, "deactivated")

	editedRecord := pv2Record(func(r map[string]any) {
		r["title"] = atEditedTitle
		r["content"] = atEditedContent
	})
	require.NoError(t, w.dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1, editedRecord)),
		"the edit during the deactivation is handled")

	_, code := admissionOf(t, w.conn, acCommunityDID, acPostURI)
	assert.Equal(t, DecisionPaused, code,
		"a deactivated pause alone does not admit the edit of an accepted post")
}

// TestAccountTakedown_AnUnconfirmedFrameDoesNotDowngradeAConfirmedTakedown
// (review round 2): under a confirmed `takendown`, with the author's comment
// held on the pause and an accepted post, a frame the PDS does not confirm
// leaves the takedown status in place: (i) a relay `takendown` the PDS
// answers with an unknown status, and (ii) a relay `deactivated` whose PDS
// read fails. The held comment survives and an edit of the accepted post is
// kept, with the post's outbound object never tombstoned.
func TestAccountTakedown_AnUnconfirmedFrameDoesNotDowngradeAConfirmedTakedown(t *testing.T) {
	for _, tc := range []struct {
		name    string
		unknown func(t *testing.T, w *atWorld)
	}{
		{
			name: "(i) inactive frame, PDS answers an unknown status",
			unknown: func(t *testing.T, w *atWorld) {
				w.relayed(t, false, "takendown", consume.RepoStatus{Active: false, Status: "frozen"})
			},
		},
		{
			name: "(ii) relay deactivated, PDS read fails",
			unknown: func(t *testing.T, w *atWorld) {
				w.pds.fail(fmt.Errorf("the PDS is unreachable"))
				require.Error(t, w.handleAccountFrame(false, "deactivated"),
					"a frame whose PDS read fails is retried")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newATWorld(t)
			ctx := context.Background()

			admittedCreate(t, w.dispatcher)
			w.drain(t)
			sent, _ := w.newlySent()
			require.Equal(t, []string{"Create " + atPostAPID}, sent, "setup: the post is delivered")
			require.NoError(t, w.dispatcher.HandleEvent(ctx, commentEvent()), "the comment is enqueued")

			w.confirmed(t, "takendown")
			w.drain(t)
			require.Equal(t, store.DeliveryStateHeld, w.commentDeliveryState(t),
				"precondition: the queued comment is held on the confirmed takedown")

			tc.unknown(t, w)
			w.drain(t)
			assert.Equal(t, store.DeliveryStateHeld, w.commentDeliveryState(t),
				"the unconfirmed frame leaves the held comment held")

			editedRecord := pv2Record(func(r map[string]any) {
				r["title"] = atEditedTitle
				r["content"] = atEditedContent
			})
			require.NoError(t, w.dispatcher.HandleEvent(ctx,
				postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1, editedRecord)),
				"the edit is handled")
			w.requireNoRemoval(t, "after the edit")
		})
	}
}

// atSecondPostRKey is a second post by the author, first created while the
// author is paused by a takedown source.
const atSecondPostRKey = "3lzpostbbbb22"

// atTakedownBans are the two takedown sources the edit carve-out admits an
// edit under: an account label, and a confirmed `takendown` status.
var atTakedownBans = []struct {
	name string
	ban  func(t *testing.T, w *atWorld)
}{
	{name: "account label", ban: func(t *testing.T, w *atWorld) { w.commitAccountLabel(t, false) }},
	{name: "confirmed takendown status", ban: func(t *testing.T, w *atWorld) { w.confirmed(t, "takendown") }},
}

// TestAccountTakedown_ANewPostByATakedownPausedAuthorIsRejected (B6): the
// edit carve-out is for a post accepted before the pause. A post the author
// first creates while paused by a takedown source is rejected DecisionPaused:
// no acceptance is signed and nothing is enqueued for it.
func TestAccountTakedown_ANewPostByATakedownPausedAuthorIsRejected(t *testing.T) {
	for _, tc := range atTakedownBans {
		t.Run(tc.name, func(t *testing.T) {
			w := newATWorld(t)
			ctx := context.Background()
			secondURI := "at://" + acAuthorDID + "/social.coves.community.postv2/" + atSecondPostRKey

			admittedCreate(t, w.dispatcher)
			w.drain(t)
			tc.ban(t, w)
			w.drain(t)
			_, _ = w.newlySent()
			activitiesBefore := countRows(t, w.conn, "outbound_activities")

			require.NoError(t, w.dispatcher.HandleEvent(ctx,
				postEvent("create", atSecondPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+2, pv2Record())),
				"the new post during the ban is handled")

			status, code := admissionOf(t, w.conn, acCommunityDID, secondURI)
			assert.Equal(t, StatusRejected, status, "the new post is rejected")
			assert.Equal(t, DecisionPaused, code, "with DecisionPaused")
			_, signed := acceptanceSubjectCID(t, w.repos, acCommunityDID, secondURI)
			assert.False(t, signed, "no acceptance is signed for the new post")
			assert.Equal(t, activitiesBefore, countRows(t, w.conn, "outbound_activities"),
				"nothing is enqueued for the new post")
			w.drain(t)
			sent, _ := w.newlySent()
			assert.Equal(t, []string{}, sent, "nothing is sent")
		})
	}
}

// TestAccountTakedown_ReadmitOfAnAcceptedPostUnderATakedownPauseKeepsIt (B6):
// an operator readmit of an accepted, delivered post while its author is
// paused by a takedown source takes the edit carve-out as AdmitPost does: the
// acceptance stands and no further Delete is enqueued beyond the ban's own.
func TestAccountTakedown_ReadmitOfAnAcceptedPostUnderATakedownPauseKeepsIt(t *testing.T) {
	for _, tc := range atTakedownBans {
		t.Run(tc.name, func(t *testing.T) {
			w := newATWorld(t)
			ctx := context.Background()

			admittedCreate(t, w.dispatcher)
			w.drain(t)
			sent, _ := w.newlySent()
			require.Equal(t, []string{"Create " + atPostAPID}, sent, "setup: the post is delivered")
			tc.ban(t, w)
			w.drain(t)
			sent, _ = w.newlySent()
			require.Equal(t, []string{"Delete " + atPostAPID}, sent, "setup: the ban deletes the post")
			deletesBefore := activityKindCount(t, w.conn, "Delete")

			result, err := w.engine.Readmit(ctx, acPostURI)
			require.NoError(t, err)
			assert.Equal(t, StatusAccepted, result.Status, "the readmit keeps the post accepted")
			w.requireNoRemoval(t, "after the readmit")
			assert.Equal(t, deletesBefore, activityKindCount(t, w.conn, "Delete"),
				"the readmit enqueues no further Delete")
			w.drain(t)
			sent, _ = w.newlySent()
			assert.Equal(t, []string{}, sent, "nothing is sent while the author is down")
		})
	}
}
