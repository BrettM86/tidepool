package outbound

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Review round 1 of chunk 04: the confirmed #account transition re-asserts
// the pause with the PDS's status, and a withdrawn consent cancels work held
// on a takedown pause.

// accountStatusPause is the status of did's account-status pause source, and
// whether the source stands.
func accountStatusPause(t *testing.T, conn *sql.DB, did string) (string, bool) {
	t.Helper()
	var status sql.NullString
	require.NoError(t, conn.QueryRow(
		`SELECT pause_sources ->> ($2 || did) FROM ap_actors WHERE did = $1`,
		did, store.PauseSourceAccountStatus+":").Scan(&status))
	return status.String, status.Valid
}

// TestAccountStatus_OlderActiveFrameDoesNotUnpauseANewerConfirmedTakedown:
// an older `active` frame reads the PDS (active) before a newer `takendown`
// frame's phase 1 commits; the older frame's phase 2 then removes that pause,
// and the newer frame's phase 2, confirmed takendown, must put it back.
func TestAccountStatus_OlderActiveFrameDoesNotUnpauseANewerConfirmedTakedown(t *testing.T) {
	s := newAccountStatusWorld(t)
	s.deliverPosts(t, tdPost)

	older := s.nextAccountEvent(t, true, "")
	newer := s.nextAccountEvent(t, false, "takendown")
	newerReading := make(chan struct{})
	releaseNewer := make(chan struct{})
	newerDone := make(chan error, 1)
	s.pds.onReads(
		// The older frame's read: the newer frame runs up to its own read,
		// so its phase-1 pause has committed, and the PDS still says active.
		func() consume.RepoStatus {
			go func() { newerDone <- s.handle(newer) }()
			select {
			case <-newerReading:
			case <-time.After(10 * time.Second):
				t.Error("the newer frame did not reach its PDS read within 10s")
			}
			return consume.RepoStatus{Active: true}
		},
		// The newer frame's read waits until the older frame has finished.
		func() consume.RepoStatus {
			close(newerReading)
			select {
			case <-releaseNewer:
			case <-time.After(10 * time.Second):
			}
			return consume.RepoStatus{Active: false, Status: "takendown"}
		},
	)

	require.NoError(t, s.handle(older), "the older active frame is handled")
	close(releaseNewer)
	select {
	case err := <-newerDone:
		require.NoError(t, err, "the newer takendown frame is handled")
	case <-time.After(10 * time.Second):
		t.Fatal("the newer frame did not finish within 10s")
	}
	s.drain(t)

	assert.True(t, s.paused(t, tdAuthorDID), "the confirmed takendown leaves P paused")
	status, stands := accountStatusPause(t, s.conn, tdAuthorDID)
	assert.True(t, stands, "the account-status pause source stands")
	assert.Equal(t, "takendown", status, "the pause carries the confirmed status")
	assert.Equal(t, []string{"Delete " + acctPostA}, s.receivedLinesSince(t, 1),
		"the confirmed takendown takes A down")
}

// TestAccountStatus_OlderActiveFrameDoesNotUnpauseANewerFrameWhoseReadFails:
// an older `active` frame reads the PDS (active) after a newer `takendown`
// frame's phase 1 committed its pause, and the newer frame's own PDS read
// then fails. The older frame is stale against the newer pending pause and
// clears nothing, so P stays paused while the newer frame is retried, and a
// comment queued meanwhile is not delivered.
func TestAccountStatus_OlderActiveFrameDoesNotUnpauseANewerFrameWhoseReadFails(t *testing.T) {
	s := newAccountStatusWorld(t)
	s.deliverPosts(t, tdPost)

	older := s.nextAccountEvent(t, true, "")
	newer := s.nextAccountEvent(t, false, "takendown")
	newerReading := make(chan struct{})
	releaseNewer := make(chan struct{})
	newerDone := make(chan error, 1)
	s.pds.onReadResults(
		// The older frame's read: the newer frame runs up to its own read,
		// so its phase-1 pause has committed, and the PDS still says active.
		func() (consume.RepoStatus, error) {
			go func() { newerDone <- s.handle(newer) }()
			select {
			case <-newerReading:
			case <-time.After(10 * time.Second):
				t.Error("the newer frame did not reach its PDS read within 10s")
			}
			return consume.RepoStatus{Active: true}, nil
		},
		// The newer frame's read waits until the older frame has finished,
		// then fails.
		func() (consume.RepoStatus, error) {
			close(newerReading)
			select {
			case <-releaseNewer:
			case <-time.After(10 * time.Second):
			}
			return consume.RepoStatus{}, fmt.Errorf("pds getRepoStatus unreachable")
		},
	)

	require.NoError(t, s.handle(older), "the older active frame is handled")
	close(releaseNewer)
	select {
	case err := <-newerDone:
		require.Error(t, err, "the newer frame's failed read leaves it to be retried")
	case <-time.After(10 * time.Second):
		t.Fatal("the newer frame did not finish within 10s")
	}

	assert.True(t, s.paused(t, tdAuthorDID), "the newer frame's pause still stands")
	_, stands := accountStatusPause(t, s.conn, tdAuthorDID)
	assert.True(t, stands, "the account-status pause source stands")
	sent := len(s.received())
	s.enqueueCommentCreate(t)
	s.drain(t)
	assert.Equal(t, []string{}, s.receivedLinesSince(t, sent), "nothing is delivered while the pause stands")
}

// TestAccountStatus_UnconfirmedInactiveFrameKeepsThePauseWithNoTakedownStatus:
// a relay `takendown` the PDS answers with active or an unknown status keeps
// the pause (applied unconfirmed), but the pause no longer claims a takedown:
// work claimed while paused is cancelled, not held, and nothing is taken down.
func TestAccountStatus_UnconfirmedInactiveFrameKeepsThePauseWithNoTakedownStatus(t *testing.T) {
	for _, row := range []struct {
		name string
		pds  consume.RepoStatus
	}{
		{name: "PDS answers active", pds: consume.RepoStatus{Active: true}},
		{name: "PDS answers an unknown inactive status", pds: consume.RepoStatus{Active: false, Status: "frozen"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			s := newAccountStatusWorld(t)
			seedAcceptedParent(t, s.conn)

			s.pds.answer(row.pds)
			require.NoError(t, s.handle(s.nextAccountEvent(t, false, "takendown")), "the frame is handled")
			s.drain(t)
			assert.True(t, s.paused(t, tdAuthorDID), "the unconfirmed pause still applies")
			status, stands := accountStatusPause(t, s.conn, tdAuthorDID)
			assert.True(t, stands, "the account-status pause source stands")
			assert.Equal(t, "", status, "the pause records no unconfirmed takedown status")

			s.enqueueCommentCreate(t)
			s.drain(t)
			assert.Equal(t, store.DeliveryStateCancelled, s.deliveryStateOf(t, commentEnqueueIntent(t).ID),
				"work claimed under the unconfirmed pause is cancelled, not held")
			assert.Equal(t, 0, activityCount(t, s.conn, "Delete"), "nothing is taken down")
		})
	}
}

// federationEvent is tdAuthorDID's social.coves.bridge.federation commit at
// rev: enabled true or false, or a record delete when operation is "delete".
func federationEvent(t *testing.T, rev, operation string, enabled bool) *consume.JetstreamEvent {
	t.Helper()
	record := ""
	if operation != "delete" {
		record = fmt.Sprintf(`,"cid":%q,"record":{"$type":%q,"enabled":%t,"deleteRemote":false}`,
			outCommentCID, consume.CollectionFederation, enabled)
	}
	frame := fmt.Sprintf(
		`{"did":%q,"time_us":%d,"kind":"commit","commit":{"rev":%q,"operation":%q,`+
			`"collection":%q,"rkey":"self"%s}}`,
		tdAuthorDID, 1_793_527_300_000_000, rev, operation, consume.CollectionFederation, record)
	var event consume.JetstreamEvent
	require.NoError(t, json.Unmarshal([]byte(frame), &event), "the test frame is valid wire JSON")
	return &event
}

// TestAccountStatus_WithdrawnConsentCancelsWorkHeldOnATakedownPause: a
// comment claimed while P is takedown-paused is held on the pause. When
// consent is withdrawn (an opt-out, or the takedown confirmed down to a
// pause-only status) the held comment is cancelled, so it is never published
// when P later comes back.
func TestAccountStatus_WithdrawnConsentCancelsWorkHeldOnATakedownPause(t *testing.T) {
	optOut := func(t *testing.T, s *accountStatusWorld) {
		require.NoError(t, s.handle(federationEvent(t, "3lzfedrev0001", "create", false)), "the opt-out is handled")
	}
	optIn := func(t *testing.T, s *accountStatusWorld) {
		require.NoError(t, s.handle(federationEvent(t, "3lzfedrev0002", "update", true)), "the opt-in is handled")
	}
	rows := []struct {
		name     string
		pause    func(t *testing.T, s *accountStatusWorld)
		withdraw func(t *testing.T, s *accountStatusWorld)
		comeBack []func(t *testing.T, s *accountStatusWorld)
	}{
		{name: "(a) opt-out while L stands, opt back in, L negated",
			pause: banStep, withdraw: optOut,
			comeBack: []func(t *testing.T, s *accountStatusWorld){optIn, unbanStep}},
		{name: "(a) opt-out while L stands, L negated, opt back in",
			pause: banStep, withdraw: optOut,
			comeBack: []func(t *testing.T, s *accountStatusWorld){unbanStep, optIn}},
		{name: "(b) confirmed takendown, then confirmed deactivated, then active",
			pause: confirmedStep("takendown"), withdraw: confirmedStep("deactivated"),
			comeBack: []func(t *testing.T, s *accountStatusWorld){confirmedStep("")}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			s := newAccountStatusWorld(t)
			// The rev gate's table outlives the world; each row starts the
			// federation record afresh.
			testutil.Truncate(t, s.conn, "jetstream_record_revs")
			seedAcceptedParent(t, s.conn)
			commentID := commentEnqueueIntent(t).ID

			row.pause(t, s)
			s.drain(t)
			s.enqueueCommentCreate(t)
			s.drain(t)
			require.Equal(t, store.DeliveryStateHeld, s.deliveryStateOf(t, commentID),
				"precondition: the comment claimed under the takedown pause is held")

			row.withdraw(t, s)
			s.drain(t)
			assert.Equal(t, store.DeliveryStateCancelled, s.deliveryStateOf(t, commentID),
				"the withdrawn consent cancels the held comment")

			for _, step := range row.comeBack {
				step(t, s)
				s.drain(t)
			}
			assert.False(t, s.paused(t, tdAuthorDID), "P is back")
			assert.Equal(t, store.DeliveryStateCancelled, s.deliveryStateOf(t, commentID),
				"the comment withdrawn with consent stays cancelled")
			assert.Equal(t, []string{}, s.receivedLines(t), "the withdrawn comment is never published")
		})
	}
}
