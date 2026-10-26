package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/outbound"
	"tidepool/internal/store"
)

// THE OPERATOR PURGE: POST /admin/actors/purge.
//
// The destructive tier (optout_destructive_test.go) is reachable only from the
// user's own deleteRemote record or a confirmed account deletion. A spammer
// writes neither, and Tidepool is that actor's home instance — the only server
// that can send Delete{Person, removeData:true} for it. This endpoint is the
// operator's way to send it, reusing the same purger.
//
// The spammer case is the one the fixture is built around: by the time an
// operator acts, the actor is already DISABLED and PAUSED, and the worker's
// consent recheck cancels a disabled actor's create/update. The Delete must be
// exempt from that recheck and actually reach the peers, or the endpoint
// reports success while the content stays up.

const (
	apPurgeInARKey = "3lzappurge0001"
	apPurgeInBRKey = "3lzappurge0002"
	apFarRKey      = "3lzappurge0003"

	apFarCommunity = "https://lemmy.zip/c/apurgetest"
	apFarName      = "apurgetest"

	// apActorID is the persona the moderation world mints for mtAuthorDID.
	apActorID = "https://coves.social/ap/actor/did:plc:mtnativeauthor0001"

	// apGroupActorDID is a native actor row of kind group: a community must
	// never be purgeable through this endpoint.
	apGroupActorDID = "did:plc:appurgegroup00001"
	// apUnknownDID is well-formed and has no actor row.
	apUnknownDID = "did:plc:appurgeunknown001"
)

// newPurgeWorld is the spammer: content delivered to TWO distinct peer inboxes
// (lemmy.world, shared by communities A and B, and lemmy.zip), one standing
// delivered vote, and — when disabledAndPaused — the actor already disabled
// and paused (otherwise left enabled and unpaused). The admin is wired with
// the moderation world's real purger. It returns the world and the standing
// vote's at-uri.
func newPurgeWorld(t *testing.T, h *harness, disabledAndPaused bool) (moderationWorld, string) {
	t.Helper()
	ctx := context.Background()
	world := newModerationWorld(t, h)

	admitPost(t, world, mtAuthorDID, apPurgeInARKey, world.communityADID, "3lzaprev000110", 1_775_000_040_000_001)
	admitPost(t, world, mtAuthorDID, apPurgeInBRKey, world.communityBDID, "3lzaprev000111", 1_775_000_040_000_002)
	h.subscribeCommunityURL(apFarCommunity, apFarName)
	admitPost(t, world, mtAuthorDID, apFarRKey, testDIDFor(apFarName, "lemmy.zip"),
		"3lzaprev000112", 1_775_000_040_000_003)
	require.Len(t, deliveryInboxesFor(t, h.db, mtAuthorDID), 2,
		"precondition: the actor's content reached two distinct inboxes")

	vote := seedDeliveredVote(t, h.db, mtAuthorDID, mtPostATURI, world.communityADID, "delivered")

	actors := store.NewAPActors(h.db)
	if disabledAndPaused {
		require.NoError(t, actors.SetEnabled(ctx, mtAuthorDID, false))
		require.NoError(t, actors.SetPaused(ctx, mtAuthorDID, true))
	}
	actor, err := actors.GetByDID(ctx, mtAuthorDID)
	require.NoError(t, err)
	require.Equal(t, apActorID, actor.ActorID, "precondition: the persona's actor id")
	require.Equal(t, !disabledAndPaused, actor.Enabled, "precondition: the actor's enabled state")
	require.Equal(t, disabledAndPaused, actor.DeliveryPaused, "precondition: the actor's paused state")
	require.Nil(t, actor.TombstonedAt, "precondition: the actor is not tombstoned")

	h.admin.SetActorPurger(world.purger)
	return world, vote
}

// purgeResponse decodes the endpoint's JSON answer.
func purgeResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body),
		"the purge answers JSON: %s", rec.Body.String())
	return body
}

// hasInfoRecord reports whether the harness log sink holds an INFO record
// carrying every one of the given structured fields.
func hasInfoRecord(logs string, fields map[string]string) bool {
	for _, line := range strings.Split(logs, "\n") {
		if !strings.Contains(line, "level=INFO") {
			continue
		}
		matched := true
		for key, value := range fields {
			if !strings.Contains(line, " "+key+"="+value) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

// objectIDOf reads an activity's object id whether it is inlined or a bare IRI.
func objectIDOf(object any) string {
	switch value := object.(type) {
	case string:
		return value
	case map[string]any:
		id, _ := value["id"].(string)
		return id
	}
	return ""
}

// B1 — the acceptance contract: the operator purges a disabled, paused actor,
// and the Delete{Person} really reaches both peers.
func TestOperatorPurgeWithdrawsADisabledPausedActorEverywhere(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world, vote := newPurgeWorld(t, h, true)
	inboxes := deliveryInboxesFor(t, h.db, mtAuthorDID)

	rec := h.adminRequest(http.MethodPost, "/admin/actors/purge",
		map[string]any{"did": mtAuthorDID, "dryRun": false})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, map[string]any{
		"did":            "did:plc:mtnativeauthor0001",
		"actorId":        "https://coves.social/ap/actor/did:plc:mtnativeauthor0001",
		"kind":           "person",
		"enabled":        false,
		"deliveryPaused": true,
		"tombstoned":     true,
		"inboxes":        float64(2),
		"standingVotes":  float64(1),
		"dryRun":         false,
	}, purgeResponse(t, rec))

	// One Delete{Person}, addressed to every inbox the content reached.
	deleteActivity := activityOfKind(t, h.db, mtAuthorDID, "Delete")
	require.NotEmpty(t, deleteActivity, "the purge enqueues a Delete{Person}")
	assert.ElementsMatch(t, inboxes, deliveryInboxesForActivity(t, h.db, deleteActivity),
		"the Delete is addressed to both inboxes the actor's content reached")

	// The standing vote is retracted.
	assert.Equal(t, 1, undoActivitiesFor(t, h.db, mtAuthorDID), "one Undo for the standing vote")
	assert.Equal(t, "undone", voteState(t, h.db, vote))

	// The actor document is gone.
	assert.Equal(t, http.StatusGone, actorDocStatus(t, h, mtAuthorDID))

	// The operator's log record carries the did and the counts, and nothing
	// anywhere carries the bearer token.
	logs := h.logs.String()
	assert.True(t, hasInfoRecord(logs, map[string]string{
		"did":           "did:plc:mtnativeauthor0001",
		"dryRun":        "false",
		"inboxes":       "2",
		"standingVotes": "1",
	}), "an INFO record with did, dryRun, inboxes and standingVotes: %s", logs)
	assert.NotContains(t, logs, testAdminToken, "no log record carries the bearer token")

	// --- The real delivery worker, over the same database, into the harness's
	//     httptest peer inbox — with the actor still disabled and paused.
	worker, err := outbound.NewWorker(outbound.WorkerOptions{
		DB:      h.db,
		Actors:  store.NewAPActors(h.db),
		Objects: store.NewOutboundObjects(h.db),
		Signers: world.personas,
		Inboxes: outbound.NewInboxResolver(h.client, time.Minute),
		Sender:  h.client,
		Lease:   time.Minute,
	})
	require.NoError(t, err)
	from := h.deliveryCount()
	drainDeliveries(t, ctx, worker)

	actor, err := store.NewAPActors(h.db).GetByDID(ctx, mtAuthorDID)
	require.NoError(t, err)
	assert.False(t, actor.Enabled, "the actor stays disabled while the Delete delivers")
	assert.True(t, actor.DeliveryPaused, "the actor stays paused while the Delete delivers")

	assert.Equal(t, []string{"delivered", "delivered"}, deliveryStatesForActivity(t, h.db, deleteActivity),
		"both Delete deliveries are delivered, not cancelled by the consent recheck")
	var received []map[string]any
	for _, activity := range h.deliveriesSince(t, from) {
		if activity["type"] == "Delete" {
			received = append(received, activity)
		}
	}
	require.Len(t, received, 2, "the peer inbox received the Delete once per inbox")
	for _, activity := range received {
		assert.Equal(t, "https://coves.social/ap/actor/did:plc:mtnativeauthor0001", activity["actor"])
		assert.Equal(t, "https://coves.social/ap/actor/did:plc:mtnativeauthor0001", objectIDOf(activity["object"]),
			"the Delete's object is the Person")
	}
}

// B2 — a dry run reports the actor and the counts and changes nothing.
func TestOperatorPurgeDryRunChangesNothing(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, vote := newPurgeWorld(t, h, true)
	activitiesBefore := rowCount(t, h.db, "outbound_activities")
	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")

	rec := h.adminRequest(http.MethodPost, "/admin/actors/purge",
		map[string]any{"did": mtAuthorDID, "dryRun": true})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, map[string]any{
		"did":            "did:plc:mtnativeauthor0001",
		"actorId":        "https://coves.social/ap/actor/did:plc:mtnativeauthor0001",
		"kind":           "person",
		"enabled":        false,
		"deliveryPaused": true,
		"tombstoned":     false,
		"inboxes":        float64(2),
		"standingVotes":  float64(1),
		"dryRun":         true,
	}, purgeResponse(t, rec))

	assert.Empty(t, activityOfKind(t, h.db, mtAuthorDID, "Delete"), "no Delete is enqueued")
	assert.Equal(t, 0, undoActivitiesFor(t, h.db, mtAuthorDID), "no Undo is enqueued")
	assert.Equal(t, "delivered", voteState(t, h.db, vote), "the vote still stands")
	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"))
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"))

	actor, err := store.NewAPActors(h.db).GetByDID(ctx, mtAuthorDID)
	require.NoError(t, err)
	assert.Nil(t, actor.TombstonedAt, "the actor is not tombstoned")
	assert.Equal(t, http.StatusOK, actorDocStatus(t, h, mtAuthorDID), "the actor document still resolves")

	logs := h.logs.String()
	assert.True(t, hasInfoRecord(logs, map[string]string{
		"did":           "did:plc:mtnativeauthor0001",
		"dryRun":        "true",
		"inboxes":       "2",
		"standingVotes": "1",
	}), "an INFO record with did, dryRun, inboxes and standingVotes: %s", logs)
	assert.NotContains(t, logs, testAdminToken, "no log record carries the bearer token")
}

// A real purge on an actor the operator never disabled or paused stops its
// outward publication first, so content published while the purge prepares is
// not delivered after the Delete targets were snapshotted. A dry run on the
// same actor leaves it enabled and unpaused.
func TestOperatorPurgeStopsPublicationOfAnEnabledUnpausedActor(t *testing.T) {
	cases := []struct {
		name               string
		dryRun             bool
		wantEnabled        bool
		wantDeliveryPaused bool
		wantTombstoned     bool
	}{
		{name: "real run pauses and disables", dryRun: false,
			wantEnabled: false, wantDeliveryPaused: true, wantTombstoned: true},
		{name: "dry run changes nothing", dryRun: true,
			wantEnabled: true, wantDeliveryPaused: false, wantTombstoned: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			newPurgeWorld(t, h, false)

			rec := h.adminRequest(http.MethodPost, "/admin/actors/purge",
				map[string]any{"did": mtAuthorDID, "dryRun": tc.dryRun})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			body := purgeResponse(t, rec)
			assert.Equal(t, tc.wantDeliveryPaused, body["deliveryPaused"], "response deliveryPaused")
			assert.Equal(t, tc.wantEnabled, body["enabled"], "response enabled")
			assert.Equal(t, tc.wantTombstoned, body["tombstoned"], "response tombstoned")

			actor, err := store.NewAPActors(h.db).GetByDID(ctx, mtAuthorDID)
			require.NoError(t, err)
			assert.Equal(t, tc.wantDeliveryPaused, actor.DeliveryPaused, "stored delivery_paused")
			assert.Equal(t, tc.wantEnabled, actor.Enabled, "stored enabled")
		})
	}
}

// B3 — a retried purge on an already-tombstoned person succeeds and sends
// nothing twice.
func TestOperatorPurgeIsRetryable(t *testing.T) {
	h := newHarness(t)
	newPurgeWorld(t, h, true)

	first := h.adminRequest(http.MethodPost, "/admin/actors/purge",
		map[string]any{"did": mtAuthorDID, "dryRun": false})
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	second := h.adminRequest(http.MethodPost, "/admin/actors/purge",
		map[string]any{"did": mtAuthorDID, "dryRun": false})
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	assert.Equal(t, true, purgeResponse(t, second)["tombstoned"])

	deleteActivity := activityOfKind(t, h.db, mtAuthorDID, "Delete")
	require.NotEmpty(t, deleteActivity, "exactly one Delete{Person}")
	assert.Len(t, deliveryInboxesForActivity(t, h.db, deleteActivity), 2,
		"still exactly two deliveries for the Delete")
	assert.Equal(t, 1, undoActivitiesFor(t, h.db, mtAuthorDID), "still one Undo for the vote")
}

// B4 — every refusal answers its status and changes nothing.
func TestOperatorPurgeRefusals(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	newPurgeWorld(t, h, true)

	_, err := store.NewAPActors(h.db).Create(ctx, store.APActor{
		DID:              apGroupActorDID,
		Kind:             store.ActorTypeGroup,
		ActorID:          "https://coves.social/ap/actor/" + apGroupActorDID,
		NormalizedOrigin: "coves.social",
		LocalPart:        "apurgegroup",
		RSAKeySealed:     []byte("sealed"),
		RSAKeyVersion:    1,
		PublicKeyPEM:     "pem",
	})
	require.NoError(t, err, "seed a native actor of kind group")

	// An Admin with no purger wired, over the same dependencies.
	unwired, err := NewAdmin(h.adminOptions())
	require.NoError(t, err)
	unwiredRouter := chi.NewRouter()
	unwired.Routes(unwiredRouter)

	cases := []struct {
		name   string
		body   map[string]any
		bearer bool
		router http.Handler
		want   int
	}{
		{name: "missing did", body: map[string]any{"dryRun": false}, bearer: true, want: http.StatusBadRequest},
		{name: "invalid did", body: map[string]any{"did": "not-a-did", "dryRun": false}, bearer: true, want: http.StatusBadRequest},
		{name: "missing dryRun", body: map[string]any{"did": mtAuthorDID}, bearer: true, want: http.StatusBadRequest},
		{name: "unknown did", body: map[string]any{"did": apUnknownDID, "dryRun": false}, bearer: true, want: http.StatusNotFound},
		{name: "group actor", body: map[string]any{"did": apGroupActorDID, "dryRun": false}, bearer: true, want: http.StatusUnprocessableEntity},
		{name: "no bearer", body: map[string]any{"did": mtAuthorDID, "dryRun": false}, bearer: false, want: http.StatusUnauthorized},
		{name: "purger not wired", body: map[string]any{"did": mtAuthorDID, "dryRun": false}, bearer: true, router: unwiredRouter, want: http.StatusNotImplemented},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.body)
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+"/admin/actors/purge", bytes.NewReader(raw))
			if tc.bearer {
				req.Header.Set("Authorization", "Bearer "+testAdminToken)
			}
			router := tc.router
			if router == nil {
				router = h.router
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			assert.Equal(t, tc.want, rec.Code, rec.Body.String())

			assert.Empty(t, activityOfKind(t, h.db, mtAuthorDID, "Delete"), "no Delete is enqueued")
			assert.Empty(t, activityOfKind(t, h.db, apGroupActorDID, "Delete"), "no Delete is enqueued for the group")
			for _, did := range []string{mtAuthorDID, apGroupActorDID} {
				actor, err := store.NewAPActors(h.db).GetByDID(ctx, did)
				require.NoError(t, err)
				assert.Nil(t, actor.TombstonedAt, "%s is not tombstoned", did)
			}
		})
	}
}
