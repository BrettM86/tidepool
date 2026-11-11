package outbound

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// tdBridgedAuthorDID is the bridged Lemmy person whose hosted repo holds
// tdBridgedParentATURI.
const tdBridgedAuthorDID = "did:plc:n3wq5hbrbz7ydnkxlpqegrty"

// tdLateReplyToBridged is a second native reply to the bridged post, enqueued
// after the bridged account's label.
var tdLateReplyToBridged = tdReply{rkey: "3lztkdncmnt03", rev: "3lztkdncrev003",
	parentATURI: tdBridgedParentATURI, parentAPID: tdBridgedParentAPID}

// seedBridgedPerson registers tdBridgedAuthorDID as a bridged Lemmy person
// (bridged_actors, actor_type person) whose repo Tidepool hosts (repo_state).
func (w *takedownWorld) seedBridgedPerson(t *testing.T) {
	t.Helper()
	testutil.Truncate(t, w.conn, "bridged_actors")
	_, err := w.conn.Exec(`
		INSERT INTO bridged_actors (ap_actor_id, actor_type, did, handle, consent_state)
		VALUES ('https://lemmy.world/u/bridgedauthor', 'person', $1,
		        'bridgedauthor.lemmy-world.tidepool.test', 'ok')`, tdBridgedAuthorDID)
	require.NoError(t, err, "seed the bridged person")
	_, err = w.conn.Exec(`INSERT INTO repo_state (did, head_cid, rev) VALUES ($1, $2, $3)`,
		tdBridgedAuthorDID, "bafyreihostedbridgedparenthead", "3lztkdnbrrev01")
	require.NoError(t, err, "seed repo_state for the bridged person's hosted repo")
}

// TestAccountTakedown_BridgedAccountHoldsNativeRepliesBeneathItsPosts (B9): a
// native reply to a bridged person's post queued before an account `!takedown`
// on that person's DID, and another enqueued after it, are both held while the
// label stands and nothing reaches Lemmy for them; when the label lifts, by
// negation or by its exp lapsing, both are delivered.
func TestAccountTakedown_BridgedAccountHoldsNativeRepliesBeneathItsPosts(t *testing.T) {
	tests := []struct {
		name string
		// ban commits L on the bridged DID; lift lifts it.
		ban  func(t *testing.T, w *takedownWorld) (exp string)
		lift func(t *testing.T, w *takedownWorld, exp string)
	}{
		{
			name: "negated",
			ban: func(t *testing.T, w *takedownWorld) string {
				w.commitAccountLabel(t, tdBridgedAuthorDID, false)
				return ""
			},
			lift: func(t *testing.T, w *takedownWorld, _ string) {
				w.commitAccountLabel(t, tdBridgedAuthorDID, true)
			},
		},
		{
			name: "exp lapses",
			ban: func(t *testing.T, w *takedownWorld) string {
				exp := w.dbExp(t, 2*time.Second)
				w.commitLabelExp(t, tdBridgedAuthorDID, false, &exp)
				return exp
			},
			lift: func(t *testing.T, w *takedownWorld, exp string) {
				w.awaitDBClockPast(t, exp)
				applied, err := w.labelStore.ApplyLapsed(context.Background())
				require.NoError(t, err)
				assert.Equal(t, 1, applied, "the lapsed account label is applied once")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newTakedownWorld(t)
			w.seedBridgedPerson(t)

			w.enqueueReply(t, tdReplyToBridged)
			exp := tc.ban(t, w)
			w.enqueueReply(t, tdLateReplyToBridged)
			assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, tdLateReplyToBridged.createID()),
				"a reply enqueued under the bridged account's label is held at enqueue")

			w.drain(t)
			assert.Equal(t, []string{}, w.receivedLines(t), "nothing is delivered for either reply while the label stands")
			assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, tdReplyToBridged.createID()),
				"the reply queued before the label is held")
			assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, tdLateReplyToBridged.createID()),
				"the reply enqueued after the label is held")

			tc.lift(t, w, exp)
			w.drain(t)
			assert.ElementsMatch(t, []string{
				"Create " + tdReplyToBridged.apID(),
				"Create " + tdLateReplyToBridged.apID(),
			}, w.receivedLines(t), "both replies are delivered once the label lifts")
			assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, tdReplyToBridged.createID()))
			assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, tdLateReplyToBridged.createID()))
			assert.Zero(t, activityCount(t, w.conn, "Delete"), "a bridged account's label mints no Delete")
		})
	}
}
