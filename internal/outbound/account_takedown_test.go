package outbound

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/store"
)

// Literal AP object ids of the two delivered posts every account-label test
// uses: A is tdPost, B is tdSecondPost.
const (
	acctPostA = "https://coves.social/ap/object/" + tdAuthorDID + "/social.coves.community.postv2/3lztkdnpost01"
	acctPostB = "https://coves.social/ap/object/" + tdAuthorDID + "/social.coves.community.postv2/3lztkdnpost02"
)

// TestAccountTakedown_LabelDeletesOnlyTheUnremovedPostAndRestoresInEveryLiftOrder
// (B1, acceptance): P has delivered posts A and B, B also under its own record
// label R. The account label L pauses P and sends one Delete, for A. The lift
// order decides when each post gets Undo{Delete}: A only when L lifts, B only
// once both L and R are gone.
func TestAccountTakedown_LabelDeletesOnlyTheUnremovedPostAndRestoresInEveryLiftOrder(t *testing.T) {
	type lift struct {
		subject string // tdAuthorDID for L, B's at-uri for R
		// wantSent is what Lemmy receives after this lift, in any order.
		wantSent []string
		// wantPaused is P's DeliveryPaused after this lift.
		wantPaused bool
	}
	rows := []struct {
		name  string
		lifts []lift
	}{
		{name: "L lifts, R stays", lifts: []lift{
			{subject: tdAuthorDID, wantSent: []string{"Undo{Delete} " + acctPostA}, wantPaused: false},
		}},
		{name: "R lifts first, then L", lifts: []lift{
			{subject: tdSecondPost.atURI(), wantSent: []string{}, wantPaused: true},
			{subject: tdAuthorDID, wantSent: []string{"Undo{Delete} " + acctPostA, "Undo{Delete} " + acctPostB},
				wantPaused: false},
		}},
		{name: "L lifts first, then R", lifts: []lift{
			{subject: tdAuthorDID, wantSent: []string{"Undo{Delete} " + acctPostA}, wantPaused: false},
			{subject: tdSecondPost.atURI(), wantSent: []string{"Undo{Delete} " + acctPostB}, wantPaused: false},
		}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			w := newTakedownWorld(t)
			w.deliverPosts(t, tdPost, tdSecondPost)

			w.commitLabel(t, tdSecondPost.atURI(), false)
			w.drain(t)
			require.Equal(t, []string{"Delete " + acctPostB}, w.receivedLinesSince(t, 2),
				"precondition: R deletes B")

			w.commitAccountLabel(t, tdAuthorDID, false)
			w.drain(t)
			assert.True(t, w.paused(t, tdAuthorDID), "L pauses P")
			require.Equal(t, []string{"Delete " + acctPostA}, w.receivedLinesSince(t, 3),
				"L sends exactly one Delete, for A; B was already deleted")

			sent := 4
			for i, step := range row.lifts {
				w.commitLabel(t, step.subject, true)
				w.drain(t)
				got := w.receivedLinesSince(t, sent)
				assert.ElementsMatch(t, step.wantSent, got, "lift %d (%s) sends", i+1, step.subject)
				assert.Equal(t, step.wantPaused, w.paused(t, tdAuthorDID), "P's pause after lift %d", i+1)
				sent += len(got)
			}
		})
	}
}

// TestAccountTakedown_LapsedLabelLiftsLikeANegation (B7): L carries an exp
// about 1 s away on the database clock. Once the database clock is past it,
// ApplyLapsed lifts L as a negation would: P resumes and A gets Undo{Delete}.
// A re-ban committed before the old exp keeps P down; a record label on B
// stays in force after L lapses.
func TestAccountTakedown_LapsedLabelLiftsLikeANegation(t *testing.T) {
	rows := []struct {
		name string
		// recordLabelOnB puts R on B before L.
		recordLabelOnB bool
		// reban commits a later L with exp 1 h ahead before the old exp.
		reban       bool
		wantApplied int
		wantSent    []string
		wantPaused  bool
	}{
		{name: "lapse restores the persona", wantApplied: 1,
			wantSent:   []string{"Undo{Delete} " + acctPostA, "Undo{Delete} " + acctPostB},
			wantPaused: false},
		{name: "a re-ban before the old exp keeps the account down", reban: true, wantApplied: 0,
			wantSent: []string{}, wantPaused: true},
		{name: "a record label stays in force after the lapse", recordLabelOnB: true, wantApplied: 1,
			wantSent: []string{"Undo{Delete} " + acctPostA}, wantPaused: false},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			ctx := context.Background()
			w := newTakedownWorld(t)
			w.deliverPosts(t, tdPost, tdSecondPost)
			wantDeletes := []string{"Delete " + acctPostA, "Delete " + acctPostB}
			if row.recordLabelOnB {
				w.commitLabel(t, tdSecondPost.atURI(), false)
				w.drain(t)
				require.Equal(t, []string{"Delete " + acctPostB}, w.receivedLinesSince(t, 2),
					"precondition: R deletes B")
				wantDeletes = []string{"Delete " + acctPostA}
			}
			sent := len(w.received())

			exp := w.dbExp(t, time.Second)
			w.commitLabelExp(t, tdAuthorDID, false, &exp)
			if row.reban {
				later := w.dbExp(t, time.Hour)
				w.commitLabelExp(t, tdAuthorDID, false, &later)
			}
			w.drain(t)
			require.True(t, w.paused(t, tdAuthorDID), "the ban pauses P")
			got := w.receivedLinesSince(t, sent)
			require.ElementsMatch(t, wantDeletes, got, "the ban deletes every post not already removed")
			sent += len(got)

			w.awaitDBClockPast(t, exp)
			applied, err := w.labelStore.ApplyLapsed(ctx)
			require.NoError(t, err)
			w.drain(t)
			assert.Equal(t, row.wantPaused, w.paused(t, tdAuthorDID), "P's pause after the lapse check")
			assert.ElementsMatch(t, row.wantSent, w.receivedLinesSince(t, sent), "the lapse check sends")
			assert.Equal(t, row.wantApplied, applied, "labels the lapse check applied")
		})
	}
}

// TestAccountTakedown_LiveConsumerLiftsALapsedLabelOnItsOwnTicker (B7c): the
// consumer's periodic lapse check runs while the stream is silent. L with an
// exp about 1 s away; a consumer with a 200 ms LapseInterval on a stream that
// sends no frame lifts it without the test calling ApplyLapsed.
func TestAccountTakedown_LiveConsumerLiftsALapsedLabelOnItsOwnTicker(t *testing.T) {
	w := newTakedownWorld(t)
	w.deliverPost(t)

	exp := w.dbExp(t, time.Second)
	w.commitLabelExp(t, tdAuthorDID, false, &exp)
	w.drain(t)
	require.True(t, w.paused(t, tdAuthorDID), "the ban pauses P")
	require.Equal(t, []string{"Delete " + acctPostA}, w.receivedLinesSince(t, 1), "the ban deletes the post")

	server := labelstest.NewServer(t, func(int64) labelstest.Reply { return labelstest.Reply{Hold: true} })
	startLabelConsumer(t, labels.ConsumerOptions{
		Store:         labels.NewPostgresStore(w.conn, w.takedowns.LabelApplier()),
		LapseInterval: 200 * time.Millisecond,
	}, server, w.labelKey)

	assert.Eventually(t, func() bool { return !w.paused(t, tdAuthorDID) }, 10*time.Second, 50*time.Millisecond,
		"the live consumer lifts the lapsed L within 10s")
	w.drain(t)
	assert.Equal(t, []string{"Undo{Delete} " + acctPostA}, w.receivedLinesSince(t, 2),
		"the lapse restores the post")
}

// TestAccountTakedown_StartActivatesAnAccountLabelStoredByTask02 (B10): task
// 02's store (no takedowns applier) committed L at seq N for P with a
// delivered post. A consumer with this chunk starting at cursor N, on a
// stream that sends no frame, pauses P and enqueues one takedown Delete; a
// second start enqueues nothing more.
func TestAccountTakedown_StartActivatesAnAccountLabelStoredByTask02(t *testing.T) {
	const storedSeq = 9
	w := newTakedownWorld(t)
	w.deliverPost(t)

	commitTakedownLabel(t, labels.NewPostgresStore(w.conn), w.labelKey, storedSeq, tdAuthorDID, false)
	require.False(t, w.paused(t, tdAuthorDID), "precondition: task 02's commit does not pause P")
	require.Zero(t, activityCount(t, w.conn, "Delete"), "precondition: task 02's commit enqueued nothing")

	server := labelstest.NewServer(t, func(int64) labelstest.Reply { return labelstest.Reply{Hold: true} })
	runLabelConsumer(t, w.labelStore, server, w.labelKey, storedSeq)
	assert.True(t, w.paused(t, tdAuthorDID), "the start pauses P")
	assert.Equal(t, 1, activityCount(t, w.conn, "Delete"), "the start enqueues one takedown Delete")

	runLabelConsumer(t, labels.NewPostgresStore(w.conn, w.takedowns.LabelApplier()), server, w.labelKey, storedSeq)
	assert.Equal(t, 1, activityCount(t, w.conn, "Delete"), "a second start enqueues nothing more")

	w.drain(t)
	assert.Equal(t, []string{"Create " + acctPostA, "Delete " + acctPostA}, w.receivedLines(t),
		"Lemmy receives the one Delete")
}

// TestAccountTakedown_NegationSendsNothingForAPurgerTombstonedPersona (B11):
// L applied to P (one Delete delivered); the purger then tombstones P; the
// negation sends nothing (no Undo{Delete}, no Delete{Person}).
func TestAccountTakedown_NegationSendsNothingForAPurgerTombstonedPersona(t *testing.T) {
	w := newTakedownWorld(t)
	w.deliverPost(t)

	w.commitAccountLabel(t, tdAuthorDID, false)
	w.drain(t)
	require.Equal(t, []string{"Delete " + acctPostA}, w.receivedLinesSince(t, 1),
		"precondition: L delivers one Delete")

	require.NoError(t, store.NewAPActors(w.conn).Tombstone(context.Background(), tdAuthorDID),
		"the purger tombstones P")
	require.Equal(t, 2, countRows(t, w.conn, "outbound_deliveries"),
		"precondition: the Create and the Delete are the only deliveries")

	w.commitAccountLabel(t, tdAuthorDID, true)
	assert.Equal(t, 2, countRows(t, w.conn, "outbound_deliveries"),
		"the negation owes the tombstoned persona's queue nothing")
	w.drain(t)
	assert.Equal(t, []string{}, w.receivedLinesSince(t, 2), "the negation sends nothing for a tombstoned persona")
}

// TestAccountTakedown_PersonaMintedUnderAStoredLabelIsPaused (B12): L is
// committed for a DID with no persona and no bridged account; the persona
// minted later is minted paused, and a negation afterwards unpauses it.
func TestAccountTakedown_PersonaMintedUnderAStoredLabelIsPaused(t *testing.T) {
	rows := []struct {
		name string
		mint func(t *testing.T, w *takedownWorld)
	}{
		{name: "personas CreateActorForDID", mint: func(t *testing.T, w *takedownWorld) { w.mintOtherAuthor(t) }},
		{name: "store APActors.Create", mint: func(t *testing.T, w *takedownWorld) {
			_, err := store.NewAPActors(w.conn).Create(context.Background(), store.APActor{
				DID:              tdOtherAuthorDID,
				Kind:             store.ActorTypePerson,
				ActorID:          tdOtherAuthorActorID,
				NormalizedOrigin: "coves.social",
				LocalPart:        "bob",
				RSAKeySealed:     []byte("sealed"),
				RSAKeyVersion:    1,
				PublicKeyPEM:     "pem",
			})
			require.NoError(t, err, "mint the persona through the store")
		}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			w := newTakedownWorld(t)

			w.commitAccountLabel(t, tdOtherAuthorDID, false)
			row.mint(t, w)
			assert.True(t, w.paused(t, tdOtherAuthorDID), "the persona is minted paused under the standing L")

			w.commitAccountLabel(t, tdOtherAuthorDID, true)
			assert.False(t, w.paused(t, tdOtherAuthorDID), "the negation unpauses the persona")
		})
	}
}
