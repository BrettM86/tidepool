package outbound

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"tidepool/internal/consume"
	"tidepool/internal/store"
)

// Chunk 04, the #account half of the shared pause source set, driven through
// a real consume.Dispatcher: a confirmed takendown/suspended is an object
// takedown source as well as a pause; deactivated/throttled pause only; every
// change of the object restriction is confirmed against the PDS first.

// TestAccountStatus_LabelAndStatusPausesLiftIndependently (B2): L and a
// confirmed takendown both pause P and both take its post down. Delivery
// stays paused until both are gone, the post restores only when its last
// source goes, and neither lift clears the other; a confirmed active alone
// never unpauses while L stands.
func TestAccountStatus_LabelAndStatusPausesLiftIndependently(t *testing.T) {
	rows := []struct {
		name  string
		steps []accountStep
	}{
		{name: "status lifts first, then L", steps: []accountStep{
			{name: "L", do: banStep, wantSent: []string{"Delete " + acctPostA}, wantPaused: true, wantDeletes: 1},
			{name: "confirmed takendown", do: confirmedStep("takendown"),
				wantSent: []string{}, wantPaused: true, wantDeletes: 1},
			{name: "confirmed active", do: confirmedStep(""),
				wantSent: []string{}, wantPaused: true, wantDeletes: 1},
			{name: "L negated", do: unbanStep,
				wantSent: []string{"Undo{Delete} " + acctPostA}, wantPaused: false, wantDeletes: 1, wantUndos: 1},
		}},
		{name: "L lifts first, then status", steps: []accountStep{
			{name: "L", do: banStep, wantSent: []string{"Delete " + acctPostA}, wantPaused: true, wantDeletes: 1},
			{name: "confirmed takendown", do: confirmedStep("takendown"),
				wantSent: []string{}, wantPaused: true, wantDeletes: 1},
			{name: "L negated", do: unbanStep,
				wantSent: []string{}, wantPaused: true, wantDeletes: 1},
			{name: "confirmed active", do: confirmedStep(""),
				wantSent: []string{"Undo{Delete} " + acctPostA}, wantPaused: false, wantDeletes: 1, wantUndos: 1},
		}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			s := newAccountStatusWorld(t)
			s.deliverPosts(t, tdPost)
			s.runAccountSteps(t, 1, row.steps)
		})
	}
}

// TestAccountStatus_RestrictionChangesOnlyOnAConfirmedStatus (B3): the
// account-status source is added only on a confirmed takendown/suspended and
// lifted only on a confirmed active. A relay active the PDS does not confirm
// changes nothing; a failed confirmation read leaves the restriction as it was
// and the event retryable (the seq is not advanced, so re-handling the same
// frame applies it).
func TestAccountStatus_RestrictionChangesOnlyOnAConfirmedStatus(t *testing.T) {
	type read struct {
		answer consume.RepoStatus
		fails  bool
	}
	type outcome struct {
		// wantRetryable is a non-nil error that is not ErrPermanentEvent;
		// otherwise checkErr decides whether a nil error is required.
		wantRetryable bool
		checkErr      bool
		wantPaused    bool
		wantSent      []string
		wantDeletes   int
		wantUndos     int
	}
	rows := []struct {
		name string
		// takenDown starts P under a confirmed takendown (A deleted).
		takenDown      bool
		active         bool
		status         string
		first          read
		want           outcome
		retry          *read
		wantAfterRetry outcome
	}{
		{name: "(a) relay active while the PDS answers suspended", takenDown: true, active: true,
			first: read{answer: consume.RepoStatus{Active: false, Status: "suspended"}},
			want:  outcome{checkErr: true, wantPaused: true, wantSent: []string{}, wantDeletes: 1}},
		{name: "(b) relay active and the confirmation read fails", takenDown: true, active: true,
			first: read{fails: true},
			want:  outcome{wantRetryable: true, wantPaused: true, wantSent: []string{}, wantDeletes: 1}},
		{name: "(c) the same frame re-handled once the PDS answers active", takenDown: true, active: true,
			first: read{fails: true},
			want:  outcome{wantRetryable: true, wantPaused: true, wantSent: []string{}, wantDeletes: 1},
			retry: &read{answer: consume.RepoStatus{Active: true}},
			wantAfterRetry: outcome{checkErr: true, wantPaused: false,
				wantSent: []string{"Undo{Delete} " + acctPostA}, wantDeletes: 1, wantUndos: 1}},
		{name: "(d) relay active while the PDS answers inactive with no status", takenDown: true, active: true,
			first: read{answer: consume.RepoStatus{Active: false, Status: ""}},
			want:  outcome{wantPaused: true, wantSent: []string{}, wantDeletes: 1}},
		{name: "(d) relay active while the PDS answers an unknown inactive status", takenDown: true, active: true,
			first: read{answer: consume.RepoStatus{Active: false, Status: "frozen"}},
			want:  outcome{wantPaused: true, wantSent: []string{}, wantDeletes: 1}},
		{name: "(d) relay active while the PDS answers deleted", takenDown: true, active: true,
			first: read{answer: consume.RepoStatus{Active: false, Status: "deleted"}},
			want:  outcome{wantPaused: true, wantSent: []string{}, wantDeletes: 1}},
		{name: "(e) relay suspended on an unpaused P and the confirmation read fails",
			active: false, status: "suspended",
			first: read{fails: true},
			want:  outcome{wantRetryable: true, wantPaused: true, wantSent: []string{}, wantDeletes: 0},
			retry: &read{answer: consume.RepoStatus{Active: false, Status: "suspended"}},
			wantAfterRetry: outcome{checkErr: true, wantPaused: true,
				wantSent: []string{"Delete " + acctPostA}, wantDeletes: 1}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			s := newAccountStatusWorld(t)
			s.deliverPosts(t, tdPost)
			if row.takenDown {
				s.confirmed(t, "takendown")
				s.drain(t)
				assert.True(t, s.paused(t, tdAuthorDID), "precondition: the confirmed takendown pauses P")
				assert.Equal(t, []string{"Delete " + acctPostA}, s.receivedLinesSince(t, 1),
					"precondition: the confirmed takendown deletes A")
			}
			sent := len(s.received())

			check := func(phase string, err error, want outcome) {
				t.Helper()
				switch {
				case want.wantRetryable:
					if assert.Error(t, err, "%s: an unconfirmed change is retried", phase) {
						assert.False(t, errors.Is(err, consume.ErrPermanentEvent),
							"%s: the error is retryable, not permanent", phase)
					}
				case want.checkErr:
					assert.NoError(t, err, "%s: the event is handled", phase)
				}
				s.drain(t)
				got := s.receivedLinesSince(t, sent)
				assert.Equal(t, want.wantSent, got, "%s: Lemmy receives", phase)
				assert.Equal(t, want.wantPaused, s.paused(t, tdAuthorDID), "%s: P's pause", phase)
				assert.Equal(t, want.wantDeletes, activityCount(t, s.conn, "Delete"), "%s: Deletes enqueued", phase)
				assert.Equal(t, want.wantUndos, activityCount(t, s.conn, "Undo"), "%s: Undos enqueued", phase)
				sent += len(got)
			}
			setRead := func(r read) {
				if r.fails {
					s.pds.fail()
				} else {
					s.pds.answer(r.answer)
				}
			}

			event := s.nextAccountEvent(t, row.active, row.status)
			setRead(row.first)
			check("first handling", s.handle(event), row.want)
			if row.retry != nil {
				setRead(*row.retry)
				check("re-handling the same frame", s.handle(event), row.wantAfterRetry)
			}
		})
	}
}

// TestAccountStatus_ConfirmedTransitionsMoveTheObjectSource (B4, with B5 as a
// row): takendown/suspended carry the object source, deactivated/throttled
// pause only. Leaving a takedown status for a pause-only one lifts the source
// (the Undo{Delete} waits while P stays paused); entering one adds it.
func TestAccountStatus_ConfirmedTransitionsMoveTheObjectSource(t *testing.T) {
	rows := []struct {
		name  string
		steps []accountStep
	}{
		{name: "(a) suspended, then deactivated, then active", steps: []accountStep{
			{name: "confirmed suspended", do: confirmedStep("suspended"),
				wantSent: []string{"Delete " + acctPostA}, wantPaused: true, wantDeletes: 1},
			{name: "confirmed deactivated", do: confirmedStep("deactivated"),
				wantSent: []string{}, wantPaused: true, wantDeletes: 1, wantUndos: 1},
			{name: "confirmed active", do: confirmedStep(""),
				wantSent: []string{"Undo{Delete} " + acctPostA}, wantPaused: false, wantDeletes: 1, wantUndos: 1},
		}},
		{name: "(b) deactivated, then takendown", steps: []accountStep{
			{name: "confirmed deactivated", do: confirmedStep("deactivated"),
				wantSent: []string{}, wantPaused: true},
			{name: "confirmed takendown", do: confirmedStep("takendown"),
				wantSent: []string{"Delete " + acctPostA}, wantPaused: true, wantDeletes: 1},
		}},
		{name: "(c) throttled, then suspended", steps: []accountStep{
			{name: "confirmed throttled", do: confirmedStep("throttled"),
				wantSent: []string{}, wantPaused: true},
			{name: "confirmed suspended", do: confirmedStep("suspended"),
				wantSent: []string{"Delete " + acctPostA}, wantPaused: true, wantDeletes: 1},
		}},
		{name: "(B5 guard) deactivated pauses and deletes nothing", steps: []accountStep{
			{name: "confirmed deactivated", do: confirmedStep("deactivated"),
				wantSent: []string{}, wantPaused: true},
		}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			s := newAccountStatusWorld(t)
			s.deliverPosts(t, tdPost)
			s.runAccountSteps(t, 1, row.steps)
		})
	}
}

// TestAccountStatus_WorkClaimedWhileTakenDownIsHeldNotCancelled (B6w): a
// native comment enqueued after P is paused by a takedown source is claimed by
// the worker while paused and held, then delivered once the pause clears. A
// pause that comes only from deactivated keeps today's cancel.
func TestAccountStatus_WorkClaimedWhileTakenDownIsHeldNotCancelled(t *testing.T) {
	commentCreate := "Create " + outCommentAPID
	rows := []struct {
		name            string
		pause, clear    func(t *testing.T, s *accountStatusWorld)
		wantWhilePaused store.DeliveryState
		wantAfterClear  []string
	}{
		{name: "paused by L, cleared by its negation", pause: banStep, clear: unbanStep,
			wantWhilePaused: store.DeliveryStateHeld, wantAfterClear: []string{commentCreate}},
		{name: "paused by a relay takendown, cleared by a confirmed active",
			pause: confirmedStep("takendown"), clear: confirmedStep(""),
			wantWhilePaused: store.DeliveryStateHeld, wantAfterClear: []string{commentCreate}},
		{name: "(guard) paused only by deactivated: the claimed comment is cancelled",
			pause: confirmedStep("deactivated"), clear: confirmedStep(""),
			wantWhilePaused: store.DeliveryStateCancelled, wantAfterClear: []string{}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			s := newAccountStatusWorld(t)
			seedAcceptedParent(t, s.conn)

			row.pause(t, s)
			s.drain(t)
			assert.True(t, s.paused(t, tdAuthorDID), "precondition: P is paused")

			s.enqueueCommentCreate(t)
			s.drain(t)
			assert.Equal(t, row.wantWhilePaused, s.deliveryStateOf(t, commentEnqueueIntent(t).ID),
				"the comment claimed while P is paused")
			assert.Equal(t, []string{}, s.receivedLines(t), "nothing is sent while P is paused")

			row.clear(t, s)
			s.drain(t)
			assert.False(t, s.paused(t, tdAuthorDID), "the pause cleared")
			assert.Equal(t, row.wantAfterClear, s.receivedLines(t), "after the pause clears Lemmy receives")
		})
	}
}
