package outbound

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Task 15 cycle G: causal gating. A reply must never be delivered before the
// thing it replies to — but ONLY when that parent is a BRIDGE-origin object we
// are still delivering. A reply to a Lemmy post (fediverse-origin: already on
// Lemmy) must ALWAYS be eligible. Getting this wrong poisons every reply to a
// Lemmy post, so the bridge-vs-fediverse distinction is the crux.

const gParentATURI = "at://" + wCommunityDID + "/social.coves.community.postv2/3lzparentpost"

// seedBridgeParent writes an outbound_objects row for the parent (a bridge-origin
// object Tidepool federates). accepted=false leaves accepted_at NULL.
func seedBridgeParent(t *testing.T, conn *sql.DB, accepted bool) {
	t.Helper()
	ctx := context.Background()
	_, err := store.NewOutboundObjects(conn).Upsert(ctx, store.OutboundObject{
		ATURI:              gParentATURI,
		APObjectID:         "https://coves.social/ap/object/" + wCommunityDID + "/social.coves.community.postv2/3lzparentpost",
		CommunityDID:       wCommunityDID,
		CommunityAPID:      wCommunityAPID,
		TranslatedSnapshot: []byte(`{"type":"Page"}`),
	})
	require.NoError(t, err)
	if accepted {
		require.NoError(t, store.NewOutboundObjects(conn).SetAccepted(ctx, gParentATURI))
	}
}

func TestCausalGating_BridgeParentUnacceptedIsIneligible(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, false) // accepted_at NULL
	id := seedDelivery(t, conn, "Create", gParentATURI, createPayload("x"))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, nil)
	_, err := w.DeliverNext(context.Background())
	require.NoError(t, err)

	assert.Zero(t, sender.count(),
		"a reply to a not-yet-accepted BRIDGE parent must NOT be delivered")
	assert.Equal(t, store.DeliveryStatePending, getDelivery(t, conn, id).State,
		"it stays pending (ineligible), waiting for the parent to land")

	// Flip the parent to accepted: the SAME delivery must now go out. This is
	// what keeps the negative non-vacuous — the gate opens, it delivers.
	require.NoError(t, store.NewOutboundObjects(conn).SetAccepted(context.Background(), gParentATURI))
	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)
	assert.Equal(t, 1, sender.count(), "once the parent is accepted, the held reply delivers")
	assert.Equal(t, store.DeliveryStateDelivered, getDelivery(t, conn, id).State)
}

func TestCausalGating_BridgeParentAcceptedIsEligible(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, true) // accepted_at set
	id := seedDelivery(t, conn, "Create", gParentATURI, createPayload("x"))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, nil)
	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)

	assert.Equal(t, 1, sender.count(), "once the parent is accepted, the reply delivers")
	assert.Equal(t, store.DeliveryStateDelivered, getDelivery(t, conn, id).State)
}

func TestCausalGating_FediverseParentIsAlwaysEligible(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	// NO outbound_objects row for the parent: it is a fediverse-origin object
	// (a Lemmy post we never federated outward) — already on Lemmy, so the reply
	// is eligible immediately. This is the crux: a naive gate poisons every
	// reply-to-a-Lemmy-post.
	id := seedDelivery(t, conn, "Create", "at://did:plc:someone/social.coves.community.comment/lemmyparent",
		createPayload("x"))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, nil)
	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)

	assert.Equal(t, 1, sender.count(),
		"a reply whose parent has no outbound_objects row (fediverse-origin) delivers immediately")
	assert.Equal(t, store.DeliveryStateDelivered, getDelivery(t, conn, id).State)
}

// Bounded-wait and poisoned-parent gating moved to causal_hardening_test.go —
// they are now TIME-bounded (not attempt-bounded, H4b) and keyed on the child's
// ACTUAL parent (not any lower-seq poison on the line, H6).
