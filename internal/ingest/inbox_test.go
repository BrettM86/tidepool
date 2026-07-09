package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
)

// likeActivity is a small, valid activity for signature-path tests (it
// dispatches to the recording vote stub, no fixtures needed).
func likeActivity(id string, actor string) map[string]any {
	return map[string]any{
		"id":     id,
		"type":   "Like",
		"actor":  actor,
		"object": pageID,
	}
}

// TestInboxValidSignature: a correctly signed delivery is accepted (202),
// recorded, and processed by the queue.
func TestInboxValidSignature(t *testing.T) {
	h := newHarness(t)
	alice := h.newRemoteActor("https://lemmy.world/u/alice", person("https://lemmy.world/u/alice", "alice", nil))

	status := h.deliver(alice, likeActivity("https://lemmy.world/activities/like/1", alice.id))
	require.Equal(t, http.StatusAccepted, status)

	event, err := h.events.GetEvent(context.Background(), "https://lemmy.world/activities/like/1")
	require.NoError(t, err)
	assert.Equal(t, "Like", event.Type)
	assert.Equal(t, alice.id, event.ActorID)

	h.drain()
	event, err = h.events.GetEvent(context.Background(), "https://lemmy.world/activities/like/1")
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "valid deliveries must be processed")
	assert.Len(t, h.votes.applied, 1, "bare Like goes to the vote aggregator")
}

// TestInboxActorInboxAlias: the per-actor inbox path accepts the same
// deliveries as the shared inbox.
func TestInboxActorInboxAlias(t *testing.T) {
	h := newHarness(t)
	alice := h.newRemoteActor("https://lemmy.world/u/alice2", person("https://lemmy.world/u/alice2", "alice2", nil))

	status := h.deliverTo("/actor/inbox", alice, likeActivity("https://lemmy.world/activities/like/alias", alice.id))
	require.Equal(t, http.StatusAccepted, status)
}

// TestInboxBadDigest: a body that does not match the signed Digest header
// is rejected before any key resolution.
func TestInboxBadDigest(t *testing.T) {
	h := newHarness(t)
	alice := h.newRemoteActor("https://lemmy.world/u/badd", person("https://lemmy.world/u/badd", "badd", nil))

	signed, err := json.Marshal(likeActivity("https://lemmy.world/activities/like/2", alice.id))
	require.NoError(t, err)
	tampered, err := json.Marshal(likeActivity("https://lemmy.world/activities/like/TAMPERED", alice.id))
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+"/inbox", bytes.NewReader(tampered))
	req.Header.Set("Content-Type", ap.ContentTypeActivityJSON)
	require.NoError(t, alice.signer().SignRequest(req, signed)) // digest over the ORIGINAL body

	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	_, err = h.events.GetEvent(context.Background(), "https://lemmy.world/activities/like/TAMPERED")
	assert.True(t, errors.IsNotFound(err), "rejected deliveries must not be enqueued")
}

// TestInboxExpiredDate: a Date header outside the 1h skew window is
// rejected (Lemmy's EXPIRES_AFTER).
func TestInboxExpiredDate(t *testing.T) {
	h := newHarness(t)
	alice := h.newRemoteActor("https://lemmy.world/u/late", person("https://lemmy.world/u/late", "late", nil))

	body, err := json.Marshal(likeActivity("https://lemmy.world/activities/like/3", alice.id))
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+"/inbox", bytes.NewReader(body))
	req.Header.Set("Content-Type", ap.ContentTypeActivityJSON)
	require.NoError(t, alice.signer().SignRequest(req, body))
	// Backdate the request past the skew window. (The stale Date also breaks
	// the signature, but the skew check runs first and is what must reject.)
	req.Header.Set("Date", time.Now().Add(-2*time.Hour).UTC().Format(http.TimeFormat))

	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

// TestInboxUnsignedRejected: no Signature header → 401.
func TestInboxUnsignedRejected(t *testing.T) {
	h := newHarness(t)
	body, err := json.Marshal(likeActivity("https://lemmy.world/activities/like/4", "https://lemmy.world/u/ghost"))
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+"/inbox", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

// TestInboxKeyRotationRefetch: a delivery signed with a ROTATED key still
// verifies — the stale cached key fails, the verifier re-fetches once
// (because the key came from cache) and recovers.
func TestInboxKeyRotationRefetch(t *testing.T) {
	h := newHarness(t)
	rotator := h.newRemoteActor("https://lemmy.world/u/rotator", person("https://lemmy.world/u/rotator", "rotator", nil))

	// Prime the key cache with a valid delivery under the ORIGINAL key.
	status := h.deliver(rotator, likeActivity("https://lemmy.world/activities/like/rot-1", rotator.id))
	require.Equal(t, http.StatusAccepted, status)
	fetchesAfterPrime := h.hitCount("/u/rotator")

	// Rotate: publish a new key and sign the next delivery with it.
	newKey, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	h.serveActorDoc(rotator.id, person(rotator.id, "rotator", nil), &newKey.PublicKey)
	rotated := &remoteActor{id: rotator.id, key: newKey}

	status = h.deliver(rotated, likeActivity("https://lemmy.world/activities/like/rot-2", rotator.id))
	assert.Equal(t, http.StatusAccepted, status,
		"a rotated key must recover via the one-shot fresh re-fetch")
	assert.Equal(t, fetchesAfterPrime+1, h.hitCount("/u/rotator"),
		"rotation recovery costs exactly one extra actor fetch")
}

// TestInboxFreshKeyFailureNoRefetch is the amplification gate: when the
// failing key was NOT served from cache (it was just fetched), the verifier
// must NOT fetch again — a forged delivery costs at most one outbound
// request.
func TestInboxFreshKeyFailureNoRefetch(t *testing.T) {
	h := newHarness(t)
	victim := h.newRemoteActor("https://lemmy.world/u/victim", person("https://lemmy.world/u/victim", "victim", nil))

	// Sign with a key the victim never published; nothing cached yet.
	forgerKey, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	forger := &remoteActor{id: victim.id, key: forgerKey}

	status := h.deliver(forger, likeActivity("https://lemmy.world/activities/like/forged", victim.id))
	assert.Equal(t, http.StatusUnauthorized, status)
	assert.Equal(t, 1, h.hitCount("/u/victim"),
		"a signature failing against a FRESH key must not trigger a second fetch")
}

// TestInboxActorSignerMismatch: an activity claiming an actor on a
// different authority than the verified signer is rejected.
func TestInboxActorSignerMismatch(t *testing.T) {
	h := newHarness(t)
	mallory := h.newRemoteActor("https://evil.example/u/mallory", person("https://evil.example/u/mallory", "mallory", nil))

	status := h.deliver(mallory, likeActivity("https://evil.example/activities/like/5", "https://lemmy.world/u/alice"))
	assert.Equal(t, http.StatusForbidden, status,
		"host A must not deliver activities attributed to host B")
}

// TestInboxDedupe: re-delivering the same activity id acknowledges (200)
// without re-enqueueing or re-processing.
func TestInboxDedupe(t *testing.T) {
	h := newHarness(t)
	alice := h.newRemoteActor("https://lemmy.world/u/dedupe", person("https://lemmy.world/u/dedupe", "dedupe", nil))
	activity := likeActivity("https://lemmy.world/activities/like/once", alice.id)

	require.Equal(t, http.StatusAccepted, h.deliver(alice, activity))
	h.drain()
	require.Len(t, h.votes.applied, 1)

	// Second delivery: acknowledged but not re-processed.
	assert.Equal(t, http.StatusOK, h.deliver(alice, activity))
	h.drain()
	assert.Len(t, h.votes.applied, 1, "duplicate deliveries must not re-process")

	// Third delivery straight to the DB layer: still a single row.
	isNew, err := h.events.RecordEvent(context.Background(), "https://lemmy.world/activities/like/once", "Like")
	require.NoError(t, err)
	assert.False(t, isNew)
}

// TestServiceActorEndpoints: /actor, WebFinger, and nodeinfo serve what
// Lemmy needs to accept a Follow from the bridge.
func TestServiceActorEndpoints(t *testing.T) {
	h := newHarness(t)

	get := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "https://"+bridgeHost+path, nil)
		rec := httptest.NewRecorder()
		h.router.ServeHTTP(rec, req)
		return rec
	}

	// Actor document with the publicKey block Lemmy requires.
	rec := get("/actor")
	require.Equal(t, http.StatusOK, rec.Code)
	actor, err := ap.ParseObject(rec.Body.Bytes())
	require.NoError(t, err)
	assert.Equal(t, h.service.ID, actor.ID)
	assert.Equal(t, ap.TypeApplication, actor.Type)
	require.NotNil(t, actor.PublicKey)
	assert.Equal(t, h.service.KeyID(), actor.PublicKey.ID)
	assert.NotEmpty(t, actor.PublicKey.PublicKeyPem)

	// WebFinger resolves the service actor (and only it).
	rec = get("/.well-known/webfinger?resource=acct:" + bridgeHost + "@" + bridgeHost)
	require.Equal(t, http.StatusOK, rec.Code)
	var jrd ap.WebFingerResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &jrd))
	require.NotEmpty(t, jrd.Links)
	assert.Equal(t, h.service.ID, jrd.Links[0].Href)
	assert.Equal(t, http.StatusNotFound, get("/.well-known/webfinger?resource=acct:nobody@example.com").Code)

	// nodeinfo discovery + document (Lemmy matches software.name).
	rec = get("/.well-known/nodeinfo")
	require.Equal(t, http.StatusOK, rec.Code)
	var discovery struct {
		Links []struct {
			Href string `json:"href"`
		} `json:"links"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &discovery))
	require.NotEmpty(t, discovery.Links)

	rec = get("/nodeinfo/2.0")
	require.Equal(t, http.StatusOK, rec.Code)
	var nodeinfo struct {
		Software struct {
			Name string `json:"name"`
		} `json:"software"`
		Protocols []string `json:"protocols"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &nodeinfo))
	assert.Equal(t, "tidepool", nodeinfo.Software.Name)
	assert.Contains(t, nodeinfo.Protocols, "activitypub")
}
