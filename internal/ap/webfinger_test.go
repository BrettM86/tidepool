package ap

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

func TestParseHandle(t *testing.T) {
	cases := []struct {
		in       string
		user     string
		host     string
		expectOK bool
	}{
		{"technology@lemmy.world", "technology", "lemmy.world", true},
		{"!technology@lemmy.world", "technology", "lemmy.world", true},
		{"@alice@lemmy.world", "alice", "lemmy.world", true},
		{"acct:alice@lemmy.world", "alice", "lemmy.world", true},
		{" alice@lemmy.world ", "alice", "lemmy.world", true},
		{"alice@127.0.0.1:8443", "alice", "127.0.0.1:8443", true},
		{"alice", "", "", false},
		{"@lemmy.world", "", "", false},
		{"alice@", "", "", false},
		{"alice@lemmy.world/evil", "", "", false},
		{"alice@lemmy@world", "", "", false},
		{"", "", "", false},
	}
	for _, tc := range cases {
		user, host, err := ParseHandle(tc.in)
		if !tc.expectOK {
			assert.Error(t, err, "handle %q must be rejected", tc.in)
			assert.True(t, errors.IsValidation(err), "handle %q error must be a validation error", tc.in)
			continue
		}
		require.NoError(t, err, "handle %q", tc.in)
		assert.Equal(t, tc.user, user, "handle %q", tc.in)
		assert.Equal(t, tc.host, host, "handle %q", tc.in)
	}
}

// webfingerTestServer runs a TLS test server (ResolveHandle always speaks
// https) and returns it plus a client trusting its certificate.
func webfingerTestServer(t *testing.T, handler http.Handler) (*httptest.Server, *Client, string) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	host := strings.TrimPrefix(server.URL, "https://")

	httpClient := server.Client()
	client := NewClient(ClientOptions{
		UserAgent:             "tidepool-test/0",
		HTTPClient:            httpClient,
		AllowPrivateAddresses: true, // httptest server on 127.0.0.1
	})
	return server, client, host
}

func TestResolveHandle_Community(t *testing.T) {
	var sawResource string
	var host string
	_, client, h := webfingerTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/.well-known/webfinger", r.URL.Path)
		sawResource = r.URL.Query().Get("resource")
		w.Header().Set("Content-Type", "application/jrd+json")
		// Same shape as the live lemmy.world JRD, but self-hosted so the self
		// link's authority matches the queried WebFinger host (the interoperable
		// rule the host-confusion guard enforces).
		_, _ = fmt.Fprintf(w, `{"subject":"acct:technology@%s","links":[
			{"rel":"http://webfinger.net/rel/profile-page","type":"text/html","href":"https://%s/c/technology"},
			{"rel":"self","type":"application/activity+json","href":"https://%s/c/technology"}
		]}`, host, host, host)
	}))
	host = h

	actorURL, err := client.ResolveHandle(context.Background(), "!technology@"+host)
	require.NoError(t, err)
	assert.Equal(t, "https://"+host+"/c/technology", actorURL)
	assert.Equal(t, "acct:technology@"+host, sawResource,
		"the ! sigil must be stripped from the acct: resource")
}

// TestResolveHandle_RejectsCrossHostSelfLink covers the WebFinger host-confusion
// guard: instance A must not be able to hand back a self link pointing at
// instance B (claiming to speak for B's actors). The subject also does not
// match, so there is no legitimate override.
func TestResolveHandle_RejectsCrossHostSelfLink(t *testing.T) {
	_, client, host := webfingerTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// subject names a foreign account and the self href is on evil.example.
		_, _ = w.Write([]byte(`{"subject":"acct:alice@lemmy.world","links":[
			{"rel":"self","type":"application/activity+json","href":"https://evil.example/u/alice"}
		]}`))
	}))

	_, err := client.ResolveHandle(context.Background(), "alice@"+host)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err),
		"a self link on a different authority than the queried host must be rejected")
}

// TestResolveHandle_AcceptsSubjectMatch: when the self href is cross-host but
// the JRD subject exactly matches the requested acct, the interoperable rule
// permits it (some redirect setups legitimately do this).
func TestResolveHandle_AcceptsSubjectMatch(t *testing.T) {
	var host string
	_, client, h := webfingerTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"subject":"acct:alice@%s","links":[
			{"rel":"self","type":"application/activity+json","href":"https://actors.example/u/alice"}
		]}`, host)
	}))
	host = h

	actorURL, err := client.ResolveHandle(context.Background(), "alice@"+host)
	require.NoError(t, err)
	assert.Equal(t, "https://actors.example/u/alice", actorURL,
		"an exact subject match authorizes a cross-host self link")
}

// TestResolveHandle_Forbidden: a 403 on WebFinger (Cloudflare, defederation)
// must be distinguishable from a genuinely missing account, not flattened to
// not-found (finding 9).
func TestResolveHandle_Forbidden(t *testing.T) {
	_, client, host := webfingerTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))

	_, err := client.ResolveHandle(context.Background(), "alice@"+host)
	require.Error(t, err)
	assert.False(t, errors.IsNotFound(err),
		"a 403 must NOT read as account-does-not-exist")
	var httpErr HTTPError
	require.ErrorAs(t, err, &httpErr, "the underlying status must be preserved")
	assert.Equal(t, http.StatusForbidden, httpErr.StatusCode)
}

func TestResolveHandle_NoSelfLink(t *testing.T) {
	_, client, host := webfingerTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"subject":"acct:x@y","links":[{"rel":"http://webfinger.net/rel/profile-page","type":"text/html","href":"https://x.example/@x"}]}`))
	}))

	_, err := client.ResolveHandle(context.Background(), "x@"+host)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err))
}

func TestResolveHandle_UnknownAccount(t *testing.T) {
	_, client, host := webfingerTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	_, err := client.ResolveHandle(context.Background(), "ghost@"+host)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err))
}

func TestResolveHandle_PrefersActivityJSONLink(t *testing.T) {
	var host string
	_, client, h := webfingerTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Both self links are on the queried host; the AP-typed one wins.
		_, _ = fmt.Fprintf(w, `{"subject":"acct:x@%s","links":[
			{"rel":"self","type":"text/html","href":"https://%s/html"},
			{"rel":"self","type":"application/activity+json","href":"https://%s/ap"}
		]}`, host, host, host)
	}))
	host = h

	actorURL, err := client.ResolveHandle(context.Background(), "x@"+host)
	require.NoError(t, err)
	assert.Equal(t, "https://"+host+"/ap", actorURL)
}

func TestActorHandle(t *testing.T) {
	group := parseFixture(t, "group_lemmy_world.json")
	handle, err := ActorHandle(group)
	require.NoError(t, err)
	assert.Equal(t, "!technology@lemmy.world", handle, "groups get the Lemmy ! sigil")

	person := parseFixture(t, "person_lemmy_world.json")
	handle, err = ActorHandle(person)
	require.NoError(t, err)
	assert.Equal(t, "LeftLeaningFreedomFighters@lemmy.world", handle)

	_, err = ActorHandle(&Object{ID: "https://x.example/u/anon", Type: TypePerson})
	assert.True(t, errors.IsValidation(err), "actor without preferredUsername must fail")

	_, err = ActorHandle(nil)
	assert.True(t, errors.IsValidation(err))
}

func TestResolveActorHandle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(loadFixture(t, "person_lemmy_world.json"))
	}))
	defer server.Close()

	client := NewClient(ClientOptions{UserAgent: "tidepool-test/0", AllowPrivateAddresses: true})
	handle, actor, err := client.ResolveActorHandle(context.Background(), server.URL+"/u/x")
	require.NoError(t, err)
	// The handle host comes from the actor's canonical id, not the URL we
	// fetched from.
	assert.Equal(t, "LeftLeaningFreedomFighters@lemmy.world", handle)
	assert.Equal(t, TypePerson, actor.Type)
}
