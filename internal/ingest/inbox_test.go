package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
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

// selfDelete builds Lemmy's account-deletion shape: a bare Delete whose
// actor and object are the same person.
func selfDelete(id, actor string) map[string]any {
	return map[string]any{
		"id":     id,
		"type":   "Delete",
		"actor":  actor,
		"object": actor,
	}
}

// TestInboxTombstonedSelfDeleteAccepted: Lemmy federates an account
// deletion as ONE Delete{Person} signed by the deleted user — whose actor
// document the origin already serves as 410 Gone, so the signature can
// never verify (no cached key: users whose content only ever arrived via
// community Announces never signed anything at us). The inbox must accept
// it on the origin's tombstone evidence: verification failed WITH a
// tombstone, the payload is a self-Delete, and an independent fetch of the
// actor's own IRI confirms Gone. (Observed live against Lemmy 0.19.19 in
// the e2e Delete(Actor) scenario.)
func TestInboxTombstonedSelfDeleteAccepted(t *testing.T) {
	h := newHarness(t)
	const ghost = "https://lemmy.world/u/ghost"
	// The origin's answer for the deleted actor: 410, nothing fetchable.
	h.mux.HandleFunc("GET "+urlPath(h.t, ghost), func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGone)
	})
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	deleted := &remoteActor{id: ghost, key: key}

	const activityID = "https://lemmy.world/activities/delete/ghost-1"
	status := h.deliver(deleted, selfDelete(activityID, ghost))
	require.Equal(t, http.StatusAccepted, status,
		"a self-Delete from a tombstoned actor must be accepted, not rejected")

	event, err := h.events.GetEvent(context.Background(), activityID)
	require.NoError(t, err)
	assert.Equal(t, ghost, event.ActorID)

	h.drain()
	// Processed as a real delete: the tombstone marker (the
	// create-after-delete guard) is recorded for the actor id.
	gone, err := h.tombstones.Exists(context.Background(), ghost)
	require.NoError(t, err)
	assert.True(t, gone, "the accepted self-delete must reach handleDelete")
}

// TestInboxTombstonedSignerRejectedForNonDelete: a tombstoned signing key
// only ever unlocks the self-Delete path. Anything else is a DEFINITIVE
// 401 — not a 503, which would make the sender (whose per-instance queue
// retries head-of-line, forever) block every later delivery behind an
// activity that can never verify.
func TestInboxTombstonedSignerRejectedForNonDelete(t *testing.T) {
	h := newHarness(t)
	const ghost = "https://lemmy.world/u/ghost2"
	h.mux.HandleFunc("GET "+urlPath(h.t, ghost), func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGone)
	})
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	deleted := &remoteActor{id: ghost, key: key}

	const activityID = "https://lemmy.world/activities/like/ghost-vote"
	status := h.deliver(deleted, likeActivity(activityID, ghost))
	assert.Equal(t, http.StatusUnauthorized, status)
	_, err = h.events.GetEvent(context.Background(), activityID)
	assert.True(t, errors.IsNotFound(err), "rejected deliveries must not be enqueued")
}

// TestInboxTombstonedKeyCannotForgeDeleteOfLiveActor: the forgery attempt
// the independent confirmation exists for — signing a Delete of a LIVE
// same-host victim with a genuinely-tombstoned actor's keyId. The
// actor/signer authority binding cannot catch it (same host), so the
// deciding check is the fetch of the claimed actor's OWN IRI: alive →
// rejected.
func TestInboxTombstonedKeyCannotForgeDeleteOfLiveActor(t *testing.T) {
	h := newHarness(t)
	const ghost = "https://lemmy.world/u/ghost3"
	h.mux.HandleFunc("GET "+urlPath(h.t, ghost), func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGone)
	})
	victim := h.newRemoteActor("https://lemmy.world/u/victim9",
		person("https://lemmy.world/u/victim9", "victim9", nil))
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	forger := &remoteActor{id: ghost, key: key}

	const activityID = "https://lemmy.world/activities/delete/forged-victim9"
	status := h.deliver(forger, selfDelete(activityID, victim.id))
	assert.Equal(t, http.StatusUnauthorized, status,
		"a live actor's self-delete must not be forgeable via a tombstoned keyId")
	_, err = h.events.GetEvent(context.Background(), activityID)
	assert.True(t, errors.IsNotFound(err))
	gone, err := h.tombstones.Exists(context.Background(), victim.id)
	require.NoError(t, err)
	assert.False(t, gone, "no tombstone may be recorded for the live victim")
}

// TestInboxTombstoneConfirmationInconclusiveDeferred: when the independent
// confirmation fetch fails for a reason that says nothing about the actor
// (here: the origin answers 5xx), the inbox must answer 503 — retryable — so
// the sender's queue redelivers. A definitive 401 would make the sender drop
// the delivery forever, permanently losing a legitimate account deletion
// during a transient origin blip (there is no rediscovery path for a deleted
// actor).
func TestInboxTombstoneConfirmationInconclusiveDeferred(t *testing.T) {
	h := newHarness(t)
	const ghost = "https://lemmy.world/u/ghost-flaky"
	// First GET (key resolution during Verify): 410 → tombstoned verify
	// error. Second GET (the confirmation fetch): 503 → inconclusive.
	var mu sync.Mutex
	hits := 0
	h.mux.HandleFunc("GET "+urlPath(h.t, ghost), func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		first := hits == 1
		mu.Unlock()
		if first {
			w.WriteHeader(http.StatusGone)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	deleted := &remoteActor{id: ghost, key: key}

	const activityID = "https://lemmy.world/activities/delete/ghost-flaky-1"
	status := h.deliver(deleted, selfDelete(activityID, ghost))
	assert.Equal(t, http.StatusServiceUnavailable, status,
		"an inconclusive confirmation must defer (503), never definitively reject")
	_, err = h.events.GetEvent(context.Background(), activityID)
	assert.True(t, errors.IsNotFound(err), "deferred deliveries must not be enqueued")
	h.drain()
	gone, err := h.tombstones.Exists(context.Background(), ghost)
	require.NoError(t, err)
	assert.False(t, gone, "no tombstone may be recorded on an inconclusive confirmation")
}

// TestInboxTombstoneConfirmationFollowsSameAuthorityRedirect pins the
// redirect semantic of the confirmation fetch: redirect hops that STAY on
// the actor's own authority are followed (an origin may move its actor
// paths), and a 410 at the end of such a chain confirms the tombstone.
func TestInboxTombstoneConfirmationFollowsSameAuthorityRedirect(t *testing.T) {
	h := newHarness(t)
	const ghost = "https://lemmy.world/u/ghost-moved"
	h.mux.HandleFunc("GET "+urlPath(h.t, ghost), func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://lemmy.world/u/ghost-moved-target")
		w.WriteHeader(http.StatusMovedPermanently)
	})
	h.mux.HandleFunc("GET /u/ghost-moved-target", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGone)
	})
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	deleted := &remoteActor{id: ghost, key: key}

	const activityID = "https://lemmy.world/activities/delete/ghost-moved-1"
	status := h.deliver(deleted, selfDelete(activityID, ghost))
	require.Equal(t, http.StatusAccepted, status,
		"a same-authority redirect to 410 must confirm the tombstone")
	h.drain()
	gone, err := h.tombstones.Exists(context.Background(), ghost)
	require.NoError(t, err)
	assert.True(t, gone)
}

// TestInboxTombstoneConfirmationRejectsCrossAuthorityRedirect: the
// confirmation fetch is an authorization decision about the actor's OWN
// origin, so a redirect off that origin (an open-redirect bug, a compromised
// origin) to an attacker host serving 410 must NOT confirm the tombstone —
// and because the origin did answer, the outcome is a definitive 401, not a
// retryable 503.
func TestInboxTombstoneConfirmationRejectsCrossAuthorityRedirect(t *testing.T) {
	h := newHarness(t)
	const ghost = "https://lemmy.world/u/ghost-hijack"
	h.mux.HandleFunc("GET "+urlPath(h.t, ghost), func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://evil.example/u/attacker-tombstone")
		w.WriteHeader(http.StatusFound)
	})
	// The attacker host happily serves 410 for the "actor".
	h.mux.HandleFunc("GET /u/attacker-tombstone", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGone)
	})
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	forger := &remoteActor{id: ghost, key: key}

	const activityID = "https://lemmy.world/activities/delete/ghost-hijack-1"
	status := h.deliver(forger, selfDelete(activityID, ghost))
	assert.Equal(t, http.StatusUnauthorized, status,
		"a cross-authority redirect to 410 must not satisfy the tombstone confirmation")
	_, err = h.events.GetEvent(context.Background(), activityID)
	assert.True(t, errors.IsNotFound(err), "rejected deliveries must not be enqueued")
	h.drain()
	gone, err := h.tombstones.Exists(context.Background(), ghost)
	require.NoError(t, err)
	assert.False(t, gone, "no tombstone may be recorded via an off-origin redirect")
}

// TestInboxTombstonedSignerRejectedForNonSelfDelete: a Delete whose object
// is NOT the actor itself never unlocks the tombstone path — definitive 401
// before any confirmation fetch is spent.
func TestInboxTombstonedSignerRejectedForNonSelfDelete(t *testing.T) {
	h := newHarness(t)
	const ghost = "https://lemmy.world/u/ghost5"
	h.mux.HandleFunc("GET "+urlPath(h.t, ghost), func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGone)
	})
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	deleted := &remoteActor{id: ghost, key: key}

	const activityID = "https://lemmy.world/activities/delete/ghost5-other"
	status := h.deliver(deleted, map[string]any{
		"id":     activityID,
		"type":   "Delete",
		"actor":  ghost,
		"object": "https://lemmy.world/post/12345",
	})
	assert.Equal(t, http.StatusUnauthorized, status,
		"a tombstoned signer's Delete of anything but itself is a definitive 401")
	_, err = h.events.GetEvent(context.Background(), activityID)
	assert.True(t, errors.IsNotFound(err), "rejected deliveries must not be enqueued")
}

// TestInboxTombstonedSignerRejectedForAnnounceDelete: a Delete WRAPPED in an
// Announce (activity.Type == Announce) is not the bare self-Delete shape and
// must not unlock the tombstone path.
func TestInboxTombstonedSignerRejectedForAnnounceDelete(t *testing.T) {
	h := newHarness(t)
	const ghost = "https://lemmy.world/u/ghost6"
	h.mux.HandleFunc("GET "+urlPath(h.t, ghost), func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGone)
	})
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	deleted := &remoteActor{id: ghost, key: key}

	const activityID = "https://lemmy.world/activities/announce/ghost6-delete"
	status := h.deliver(deleted, map[string]any{
		"id":    activityID,
		"type":  "Announce",
		"actor": ghost,
		"object": map[string]any{
			"id":     "https://lemmy.world/activities/delete/ghost6-inner",
			"type":   "Delete",
			"actor":  ghost,
			"object": ghost,
		},
	})
	assert.Equal(t, http.StatusUnauthorized, status,
		"Announce{Delete} from a tombstoned signer is a definitive 401")
	_, err = h.events.GetEvent(context.Background(), activityID)
	assert.True(t, errors.IsNotFound(err), "rejected deliveries must not be enqueued")
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
	assert.Equal(t, ap.TypeService, actor.Type)
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
	assert.Equal(t, "https://"+bridgeHost+"/nodeinfo/2.0", discovery.Links[0].Href,
		"discovery href must be the BaseURL()-derived nodeinfo route")

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

// TestInstanceActorRoute: through the real router wiring, GET / serves the
// instance ("Site") actor document — 200, activity+json, parseable — the
// document Lemmy must resolve before it ever delivers send-to-all-instances
// activities (account deletions) to the bridge.
func TestInstanceActorRoute(t *testing.T) {
	h := newHarness(t)
	req := httptest.NewRequest(http.MethodGet, "https://"+bridgeHost+"/", nil)
	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/activity+json", rec.Header().Get("Content-Type"))
	doc, err := ap.ParseObject(rec.Body.Bytes())
	require.NoError(t, err)
	assert.Equal(t, h.service.InstanceActorID(), doc.ID)
	assert.Equal(t, ap.TypeApplication, doc.Type)
	require.NotNil(t, doc.PublicKey)
	assert.NotEmpty(t, doc.PublicKey.PublicKeyPem)
}
