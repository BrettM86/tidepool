package outbound

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/store"
)

// TestTakedown_ARestoreCommittedDuringTheWorkersHoldDecisionIsNotLost: the
// worker claims a Create whose object, or whose reply parent, is taken down,
// and finds the blocker; the label's negation commits before the worker holds
// the row. The row is not left held on a subject whose release already ran:
// the worker delivers it in the same pass.
func TestTakedown_ARestoreCommittedDuringTheWorkersHoldDecisionIsNotLost(t *testing.T) {
	for _, tc := range []struct {
		name string
		// arrange leaves a pending Create claimable while subject is taken
		// down, and returns that Create's activity id and object AP id.
		arrange func(t *testing.T, w *takedownWorld) (createID, objectAPID string)
		subject string
	}{
		{
			name: "own object, a retry after a failed in-flight POST",
			arrange: func(t *testing.T, w *takedownWorld) (string, string) {
				createID := consume.ActivityID(outUserOrigin, tdPostATURI, "create", 0)
				w.enqueuePostCreate(t)
				w.answerUnavailable(1)
				w.commitLabelWhilePOSTInFlight(t, tdPostATURI)
				require.Equal(t, store.DeliveryStatePending, w.deliveryStateOf(t, createID),
					"precondition: the failed Create is pending for a retry")
				w.makeDue(t, createID)
				return createID, tdPostAPID
			},
			subject: tdPostATURI,
		},
		{
			name: "reply queued before its native parent's takedown",
			arrange: func(t *testing.T, w *takedownWorld) (string, string) {
				w.deliverPost(t)
				w.enqueueReply(t, tdReplyToPost)
				w.commitLabel(t, tdPostATURI, false)
				return tdReplyToPost.createID(), tdReplyToPost.apID()
			},
			subject: tdPostATURI,
		},
		{
			name: "reply queued before its bridged parent's takedown",
			arrange: func(t *testing.T, w *takedownWorld) (string, string) {
				w.enqueueReply(t, tdReplyToBridged)
				w.commitLabel(t, tdBridgedParentATURI, false)
				return tdReplyToBridged.createID(), tdReplyToBridged.apID()
			},
			subject: tdBridgedParentATURI,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newTakedownWorld(t)
			createID, objectAPID := tc.arrange(t, w)

			fired := false
			w.worker.beforeTakedownHold = func() {
				if fired {
					return
				}
				fired = true
				w.commitLabel(t, tc.subject, true)
			}
			worked, err := w.worker.DeliverNext(context.Background())
			require.NoError(t, err)
			require.True(t, worked, "the Create was claimed")
			require.True(t, fired, "precondition: the worker found the blocker and the restore committed")

			assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, createID),
				"the Create is delivered, not held on a subject already released")
			assert.Contains(t, w.receivedLines(t), "Create "+objectAPID, "Lemmy receives the Create")
		})
	}
}

// TestTakedown_ARestoreCommittingDuringAnEnqueueIsNotLost: a label negation is
// mid-commit, past its release, when a Create is enqueued that the standing
// label would hold: a post first seen while its own label stands, or a reply
// to a bridged parent under a label. The enqueue's hold decision waits for the
// negation's commit and reads it, so the Create is queued, not held, and is
// delivered.
func TestTakedown_ARestoreCommittingDuringAnEnqueueIsNotLost(t *testing.T) {
	for _, tc := range []struct {
		name       string
		subject    string
		enqueue    func(t *testing.T, w *takedownWorld)
		createID   string
		objectAPID string
	}{
		{
			name:       "a post first seen under its own label",
			subject:    tdPostATURI,
			enqueue:    func(t *testing.T, w *takedownWorld) { w.enqueuePostCreate(t) },
			createID:   consume.ActivityID(outUserOrigin, tdPostATURI, "create", 0),
			objectAPID: tdPostAPID,
		},
		{
			name:       "a reply to a bridged parent under a label",
			subject:    tdBridgedParentATURI,
			enqueue:    func(t *testing.T, w *takedownWorld) { w.enqueueReply(t, tdReplyToBridged) },
			createID:   tdReplyToBridged.createID(),
			objectAPID: tdReplyToBridged.apID(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newTakedownWorld(t)
			w.commitLabel(t, tc.subject, false)

			// The negation commits through a label store whose last applier
			// parks inside the label's transaction, after the takedowns
			// applier ran its release, until release is closed.
			gate := &parkingApplier{entered: make(chan struct{}), release: make(chan struct{})}
			parked := labels.NewPostgresStore(w.conn, w.takedowns.LabelApplier(), gate)
			w.labelSeq++
			at := labels.Cursor{Epoch: 0, Seq: w.labelSeq}
			negate := true
			negation := labelstest.Sign(t, w.labelKey, labeling.Label{
				SourceDID: tdLabelerDID, URI: tc.subject, Val: tdLabelTakedown, Negated: &negate})
			committed := make(chan error, 1)
			go func() { committed <- parked.Commit(context.Background(), at, negation) }()
			select {
			case <-gate.entered:
			case err := <-committed:
				t.Fatalf("the negation returned before parking: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("the negation did not reach its last applier within 5s")
			}

			// Let the negation commit once the enqueue is waiting on a
			// takedown-subject lock; if the enqueue never waits, it is let
			// go after the enqueue returns.
			var once sync.Once
			releaseGate := func() { once.Do(func() { close(gate.release) }) }
			stop := make(chan struct{})
			watched := make(chan bool, 1)
			go func() {
				deadline := time.After(5 * time.Second)
				poll := time.NewTicker(5 * time.Millisecond)
				defer poll.Stop()
				for {
					if waitingOnTakedownSubjectLock(w.conn) {
						releaseGate()
						watched <- true
						return
					}
					select {
					case <-stop:
						watched <- true
						return
					case <-deadline:
						// Unblock whatever the enqueue waits on, so the test
						// fails instead of hanging.
						releaseGate()
						watched <- false
						return
					case <-poll.C:
					}
				}
			}()
			tc.enqueue(t, w)
			close(stop)
			require.True(t, <-watched, "the enqueue neither returned nor waited on the subject lock within 5s")
			releaseGate()
			select {
			case err := <-committed:
				require.NoError(t, err, "the negation commits")
			case <-time.After(5 * time.Second):
				t.Fatal("the negation did not commit within 5s")
			}

			assert.Equal(t, store.DeliveryStatePending, w.deliveryStateOf(t, tc.createID),
				"the Create is queued, not held on a subject already released")
			w.drain(t)
			assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, tc.createID),
				"the Create is delivered")
			assert.Contains(t, w.receivedLines(t), "Create "+tc.objectAPID, "Lemmy receives the Create")
		})
	}
}

// parkingApplier is a labels.Applier that announces each label on entered and
// blocks inside the label's transaction until release is closed.
type parkingApplier struct {
	entered chan struct{}
	release chan struct{}
}

func (a *parkingApplier) ApplyLabel(context.Context, *sql.Tx, labels.Stored) error {
	a.entered <- struct{}{}
	<-a.release
	return nil
}

// waitingOnTakedownSubjectLock reports whether a session in this database is
// waiting for a takedown-subject advisory lock: the two-key form with the
// "td" namespace (0x7464 = 29796) as its first key.
func waitingOnTakedownSubjectLock(conn *sql.DB) bool {
	var waiting bool
	err := conn.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM pg_locks
			WHERE locktype = 'advisory' AND NOT granted
			  AND classid = 29796 AND objsubid = 2
			  AND database = (SELECT oid FROM pg_database WHERE datname = current_database()))`).Scan(&waiting)
	return err == nil && waiting
}

// TestTakedown_ATakedownCommittedBeforeTheAcceptedStampIsDeleted: the
// worker's Create POST succeeds and its first takedown check finds the post
// not taken down; the label commits before the plain accepted_at stamp, so the
// takedown sees the post unaccepted and owes no Delete yet. The post is not
// left live on Lemmy: exactly one takedown Delete follows the Create.
func TestTakedown_ATakedownCommittedBeforeTheAcceptedStampIsDeleted(t *testing.T) {
	w := newTakedownWorld(t)
	createID := consume.ActivityID(outUserOrigin, tdPostATURI, "create", 0)
	w.enqueuePostCreate(t)

	fired := false
	w.worker.beforeAcceptedStamp = func() {
		if fired {
			return
		}
		fired = true
		require.False(t, rawAccepted(t, w.conn, tdPostATURI), "precondition: the post is not yet accepted")
		w.commitLabel(t, tdPostATURI, false)
	}
	w.drain(t)
	require.True(t, fired, "precondition: the takedown committed between the first check and the stamp")

	assert.True(t, rawAccepted(t, w.conn, tdPostATURI), "the delivered post is accepted")
	assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, createID), "the Create is delivered")
	require.Equal(t, []string{"Create " + tdPostAPID, "Delete " + tdPostAPID}, w.receivedLines(t),
		"Lemmy has the Create, then exactly one takedown Delete")
	assert.Equal(t, 1, activityCount(t, w.conn, "Delete"), "one takedown Delete is minted")
}
