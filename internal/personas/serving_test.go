package personas

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

const (
	vanityHost   = "vanity.example"
	vanityOrigin = "https://" + vanityHost
	// contentTypeJRD is what WebFinger answers with (v1 precedent:
	// ingest/inbox.go's service-actor webfinger).
	contentTypeJRD = "application/jrd+json"
	// contentTypeLDJSON is the second rel=self type Lemmy and Mastodon both
	// accept, offered after activity+json.
	contentTypeLDJSON = `application/ld+json; profile="https://www.w3.org/ns/activitystreams"`
)

// serveOnHost drives the handler with an explicit Host header — the routing
// input the whole user-origin surface keys on.
func serveOnHost(h http.Handler, host, method, target string, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "https://"+host+target, nil)
	req.Host = host
	for k, values := range header {
		for _, v := range values {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func actorPath(did string) string { return "/ap/actor/" + did }

func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var doc map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc),
		"response must be a JSON object, got %s", rec.Body.String())
	return doc
}

// contextIncludes reports whether the AS2 @context (a string or an array)
// names the ActivityStreams namespace.
func contextIncludes(raw any, want string) bool {
	switch v := raw.(type) {
	case string:
		return v == want
	case []any:
		for _, entry := range v {
			if s, ok := entry.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}

func webfingerTarget(resource string) string {
	return "/.well-known/webfinger?resource=" + url.QueryEscape(resource)
}

// mintTestActor mints one actor and returns it.
func mintTestActor(t *testing.T, svc *Service, handle string) *store.APActor {
	t.Helper()
	actor, err := svc.CreateActorForDID(t.Context(), testDID(t), handle)
	require.NoError(t, err)
	require.NotNil(t, actor)
	return actor
}

// TestServeActorDocument_Shape covers the document a bare actor (no profile
// cached yet) serves.
func TestServeActorDocument_Shape(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	actor := mintTestActor(t, svc, testHandle)

	rec := serveOnUserOrigin(svc, http.MethodGet, actorPath(actor.DID), nil)
	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	assert.Equal(t, ap.ContentTypeActivityJSON, rec.Header().Get("Content-Type"))

	doc := decodeJSON(t, rec)
	assert.True(t, contextIncludes(doc["@context"], "https://www.w3.org/ns/activitystreams"),
		"@context must name the ActivityStreams namespace, got %v", doc["@context"])
	assert.Equal(t, actor.ActorID, doc["id"])
	assert.Equal(t, "Person", doc["type"])
	assert.Equal(t, actor.LocalPart, doc["preferredUsername"])

	// With an empty profile cache the display name falls back to the local
	// part: Lemmy renders `name`, and an empty one shows as a blank user.
	assert.Equal(t, actor.LocalPart, doc["name"],
		"name falls back to the local part while the profile cache is empty")
	assert.NotContains(t, doc, "summary", "an empty summary is omitted, not served blank")
	assert.NotContains(t, doc, "icon", "an actor without an avatar publishes no icon")

	publicKey, ok := doc["publicKey"].(map[string]any)
	require.True(t, ok, "publicKey must be an object, got %v", doc["publicKey"])
	assert.Equal(t, actor.ActorID+"#main-key", publicKey["id"])
	assert.Equal(t, actor.ActorID, publicKey["owner"],
		"owner must equal the document id, which is the URL this was fetched from")
	pem, ok := publicKey["publicKeyPem"].(string)
	require.True(t, ok)
	assert.Equal(t, actor.PublicKeyPEM, pem)

	assert.Equal(t, userOrigin+"/ap/inbox", doc["inbox"])
	endpoints, ok := doc["endpoints"].(map[string]any)
	require.True(t, ok, "endpoints must be an object, got %v", doc["endpoints"])
	assert.Equal(t, userOrigin+"/ap/inbox", endpoints["sharedInbox"],
		"one shared inbox serves every actor on the origin")
	assert.Equal(t, actor.ActorID+"/outbox", doc["outbox"],
		"outbox is REQUIRED for Lemmy's Person deserialization")

	published, ok := doc["published"].(string)
	require.True(t, ok, "published must be a string, got %v", doc["published"])
	publishedAt, err := time.Parse(time.RFC3339, published)
	require.NoError(t, err, "published must be RFC3339, got %q", published)
	assert.WithinDuration(t, actor.CreatedAt, publishedAt, time.Second,
		"published is the row's created_at")
}

// TestServeActorDocument_ProfileCache covers the fields that appear once
// task 14's profile sync has filled the cache. icon is an Image OBJECT:
// Lemmy's parser rejects a bare URL string there.
func TestServeActorDocument_ProfileCache(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	actor := mintTestActor(t, svc, testHandle)

	const avatar = "https://cdn.example/alice.png"
	require.NoError(t, store.NewAPActors(database).UpdateProfile(t.Context(), actor.DID,
		store.APActorProfile{
			DisplayName: "Alice Liddell",
			Summary:     "posts about tide pools",
			AvatarURL:   avatar,
		}))

	rec := serveOnUserOrigin(svc, http.MethodGet, actorPath(actor.DID), nil)
	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	doc := decodeJSON(t, rec)

	assert.Equal(t, "Alice Liddell", doc["name"], "the cached display name wins over the local part")
	assert.Equal(t, "posts about tide pools", doc["summary"])
	assert.Equal(t, actor.LocalPart, doc["preferredUsername"],
		"the local part is frozen: a profile refresh never moves it")

	icon, ok := doc["icon"].(map[string]any)
	require.True(t, ok, "icon must be an Image object, not a bare string; got %v", doc["icon"])
	assert.Equal(t, "Image", icon["type"])
	assert.Equal(t, avatar, icon["url"])
}

// TestServeActorDocument_Lookup covers path handling: unknown DIDs 404, and
// a percent-escaped DID resolves the same actor (a DID's colons are legal
// unescaped, so both spellings arrive in the wild).
func TestServeActorDocument_Lookup(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	actor := mintTestActor(t, svc, testHandle)

	unknown := serveOnUserOrigin(svc, http.MethodGet, actorPath(testDID(t)), nil)
	assert.Equal(t, http.StatusNotFound, unknown.Code, "an unminted DID has no actor document")

	escaped := serveOnUserOrigin(svc, http.MethodGet,
		actorPath(strings.ReplaceAll(actor.DID, ":", "%3A")), nil)
	require.Equal(t, http.StatusOK, escaped.Code,
		"a percent-escaped DID must resolve the same actor; body=%s", escaped.Body.String())
	assert.Equal(t, actor.ActorID, decodeJSON(t, escaped)["id"],
		"the served id is the canonical stored actor_id, never the escaped request spelling")
}

// TestServeOutbox: the collection Lemmy dereferences after parsing the
// actor. Empty is fine; missing is not.
func TestServeOutbox(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	actor := mintTestActor(t, svc, testHandle)

	rec := serveOnUserOrigin(svc, http.MethodGet, actorPath(actor.DID)+"/outbox", nil)
	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	assert.Equal(t, ap.ContentTypeActivityJSON, rec.Header().Get("Content-Type"))

	doc := decodeJSON(t, rec)
	assert.True(t, contextIncludes(doc["@context"], "https://www.w3.org/ns/activitystreams"),
		"@context must name the ActivityStreams namespace, got %v", doc["@context"])
	assert.Equal(t, actor.ActorID+"/outbox", doc["id"])
	assert.Equal(t, "OrderedCollection", doc["type"])
	assert.EqualValues(t, 0, doc["totalItems"], "no content flows yet (task 13 mints identities only)")
}

// TestServeDisabledActor pins the split task 17 depends on: disabling
// removes an actor from DISCOVERY, not from the network. Its document must
// keep resolving so already-federated references do not become dangling.
func TestServeDisabledActor(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	actors := store.NewAPActors(database)
	actor := mintTestActor(t, svc, testHandle)

	require.NoError(t, actors.SetEnabled(t.Context(), actor.DID, false))

	doc := serveOnUserOrigin(svc, http.MethodGet, actorPath(actor.DID), nil)
	assert.Equal(t, http.StatusOK, doc.Code,
		"a disabled actor's document stays fetchable (task 17 owns scrub semantics)")

	finger := serveOnUserOrigin(svc, http.MethodGet,
		webfingerTarget("acct:"+actor.LocalPart+"@"+userHost), nil)
	assert.Equal(t, http.StatusNotFound, finger.Code,
		"a disabled actor's local part must not resolve via webfinger")
}

// TestServePausedActor: delivery_paused is about OUTBOUND delivery. It must
// be invisible to every read surface.
func TestServePausedActor(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	actors := store.NewAPActors(database)
	actor := mintTestActor(t, svc, testHandle)

	before := serveOnUserOrigin(svc, http.MethodGet, actorPath(actor.DID), nil)
	require.Equal(t, http.StatusOK, before.Code, "body=%s", before.Body.String())
	beforeDoc := decodeJSON(t, before)
	beforeFinger := serveOnUserOrigin(svc, http.MethodGet,
		webfingerTarget("acct:"+actor.LocalPart+"@"+userHost), nil)
	require.Equal(t, http.StatusOK, beforeFinger.Code, "body=%s", beforeFinger.Body.String())

	require.NoError(t, actors.SetPaused(t.Context(), actor.DID, true))

	after := serveOnUserOrigin(svc, http.MethodGet, actorPath(actor.DID), nil)
	require.Equal(t, http.StatusOK, after.Code)
	assert.Equal(t, beforeDoc, decodeJSON(t, after),
		"pausing delivery must not change the actor document")

	afterFinger := serveOnUserOrigin(svc, http.MethodGet,
		webfingerTarget("acct:"+actor.LocalPart+"@"+userHost), nil)
	assert.Equal(t, http.StatusOK, afterFinger.Code, "a paused actor still resolves")
	assert.JSONEq(t, beforeFinger.Body.String(), afterFinger.Body.String())
}

// TestWebFinger is the discovery document Lemmy resolves @alice@coves.social
// through.
func TestWebFinger(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	actor := mintTestActor(t, svc, testHandle)
	acct := "acct:" + actor.LocalPart + "@" + userHost

	rec := serveOnUserOrigin(svc, http.MethodGet, webfingerTarget(acct), nil)
	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	assert.Equal(t, contentTypeJRD, rec.Header().Get("Content-Type"))

	var jrd ap.WebFingerResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &jrd), "body=%s", rec.Body.String())
	assert.Equal(t, acct, jrd.Subject, "the subject echoes the acct form")
	assert.Contains(t, jrd.Aliases, actor.ActorID)

	// Two self links, activity+json FIRST: implementations that take the
	// first match must land on the AP document, and Lemmy's own resolver
	// looks for either type.
	var selfLinks []ap.WebFingerLink
	for _, link := range jrd.Links {
		if link.Rel == "self" {
			selfLinks = append(selfLinks, link)
		}
	}
	require.Len(t, selfLinks, 2, "expected two rel=self links, got %+v", jrd.Links)
	assert.Equal(t, ap.ContentTypeActivityJSON, selfLinks[0].Type)
	assert.Equal(t, contentTypeLDJSON, selfLinks[1].Type)
	for i, link := range selfLinks {
		assert.Equal(t, actor.ActorID, link.Href, "self link %d must href the actor URL", i)
	}

	// The actor URL itself is an accepted resource (v1 precedent:
	// ingest/inbox.go's service-actor webfinger accepts both spellings) and
	// answers identically.
	byURL := serveOnUserOrigin(svc, http.MethodGet, webfingerTarget(actor.ActorID), nil)
	require.Equal(t, http.StatusOK, byURL.Code, "body=%s", byURL.Body.String())
	assert.JSONEq(t, rec.Body.String(), byURL.Body.String(),
		"resolving by actor URL must answer exactly as resolving by acct")
}

// TestWebFinger_HostBinding: the resource's authority must be the routed
// Host. Otherwise this origin would answer for accounts it does not host.
func TestWebFinger_HostBinding(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	actor := mintTestActor(t, svc, testHandle)
	acct := "acct:" + actor.LocalPart + "@" + userHost

	foreign := serveOnUserOrigin(svc, http.MethodGet,
		webfingerTarget("acct:"+actor.LocalPart+"@other.example"), nil)
	assert.Equal(t, http.StatusNotFound, foreign.Code,
		"a resource authority that is not the routed Host must not resolve")

	// Host normalization: the default https port and letter case carry no
	// meaning, and a fully-qualified trailing dot is the same name.
	for _, host := range []string{userHost + ":443", strings.ToUpper(userHost), userHost + "."} {
		rec := serveOnHost(svc, host, http.MethodGet, webfingerTarget(acct), nil)
		assert.Equal(t, http.StatusOK, rec.Code,
			"Host %q must normalize to %q; body=%s", host, userHost, rec.Body.String())
	}

	// A NON-default port is a different authority, not noise to strip.
	odd := serveOnHost(svc, userHost+":8443", http.MethodGet, webfingerTarget(acct), nil)
	assert.Equal(t, http.StatusNotFound, odd.Code,
		"only the scheme's default port is stripped; %s:8443 is another origin", userHost)
}

// TestWebFinger_VanityOrigins is the vanity-origin proof: the same local
// part under two origins are two people, and each resolves only under its
// own Host. This is what forces ServeHTTP to consult r.Host rather than the
// configured user origin.
func TestWebFinger_VanityOrigins(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	actors := store.NewAPActors(database)

	native := mintTestActor(t, svc, testHandle)
	vanityDID := testDID(t)
	vanity, err := actors.Create(t.Context(), store.APActor{
		DID:              vanityDID,
		Kind:             store.ActorTypePerson,
		ActorID:          vanityOrigin + "/ap/actor/" + vanityDID,
		NormalizedOrigin: vanityHost,
		LocalPart:        native.LocalPart,
		RSAKeySealed:     []byte{0x01, 0x02, 0x03},
		RSAKeyVersion:    1,
		PublicKeyPEM:     native.PublicKeyPEM,
	})
	require.NoError(t, err, "the same local part under another origin must coexist")
	require.NotNil(t, vanity)

	acctNative := "acct:" + native.LocalPart + "@" + userHost
	acctVanity := "acct:" + native.LocalPart + "@" + vanityHost

	onNative := serveOnHost(svc, userHost, http.MethodGet, webfingerTarget(acctNative), nil)
	require.Equal(t, http.StatusOK, onNative.Code, "body=%s", onNative.Body.String())
	assert.Contains(t, onNative.Body.String(), native.ActorID)
	assert.NotContains(t, onNative.Body.String(), vanity.ActorID)

	onVanity := serveOnHost(svc, vanityHost, http.MethodGet, webfingerTarget(acctVanity), nil)
	require.Equal(t, http.StatusOK, onVanity.Code,
		"a registered actor origin must route; body=%s", onVanity.Body.String())
	assert.Contains(t, onVanity.Body.String(), vanity.ActorID)
	assert.NotContains(t, onVanity.Body.String(), native.ActorID)

	// A local part that exists only on the other origin does not leak across.
	bob := mintTestActor(t, svc, "bob."+userHost)
	crossed := serveOnHost(svc, vanityHost, http.MethodGet,
		webfingerTarget("acct:"+bob.LocalPart+"@"+vanityHost), nil)
	assert.Equal(t, http.StatusNotFound, crossed.Code,
		"bob exists on %s only: %s must not answer for him", userHost, vanityHost)
}

// TestWebFinger_Errors: a miss is 404, a malformed request is 400 — the
// distinction matters because remote resolvers cache 404s as "no such
// account" but treat 400s as our bug.
func TestWebFinger_Errors(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	mintTestActor(t, svc, testHandle)

	unknown := serveOnUserOrigin(svc, http.MethodGet,
		webfingerTarget("acct:nobody@"+userHost), nil)
	assert.Equal(t, http.StatusNotFound, unknown.Code, "unknown local part")

	missing := serveOnUserOrigin(svc, http.MethodGet, "/.well-known/webfinger", nil)
	assert.Equal(t, http.StatusBadRequest, missing.Code,
		"a webfinger request without a resource parameter is malformed, not a miss")
}
