package personas

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const serviceHost = "tdpl.io"

// marker is a stand-in handler that records what reached it and answers with
// its own name, so a test can tell WHICH surface served a request. Paths in
// notFound answer 404 instead — how the composed handler's fallback is
// exercised.
type marker struct {
	name     string
	calls    int
	hosts    []string
	notFound map[string]bool
	// statuses overrides the answer for a path, so a test can prove which
	// statuses the composed handler treats as "not mine".
	statuses map[string]int
}

func newMarker(name string, notFound ...string) *marker {
	m := &marker{name: name, notFound: map[string]bool{}}
	for _, path := range notFound {
		m.notFound[path] = true
	}
	return m
}

func (m *marker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.calls++
	m.hosts = append(m.hosts, r.Host)
	if m.notFound[r.URL.Path] {
		http.NotFound(w, r)
		return
	}
	if status, ok := m.statuses[r.URL.Path]; ok {
		w.Header().Set("X-Handler", m.name)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(m.name + " " + r.URL.Path))
		return
	}
	w.Header().Set("X-Handler", m.name)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(m.name + " " + r.URL.Path))
}

// requestOnHost builds a request with an explicit Host. scheme selects
// whether the server sees TLS, which a Host like "coves.social:443" must
// NOT depend on: in production TLS terminates at the proxy and the Go
// server sees plain HTTP carrying whatever Host was forwarded.
func requestOnHost(scheme, host, target string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, scheme+"://placeholder.invalid"+target, nil)
	req.Host = host
	return req
}

func routeHost(t *testing.T, h http.Handler, scheme, host, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, requestOnHost(scheme, host, target))
	return rec
}

func newTestRouter(t *testing.T, devFallthrough bool) (http.Handler, *marker, *marker) {
	t.Helper()
	service := newMarker("service")
	user := newMarker("user")
	router, err := NewHostRouter(HostRouterOptions{
		ServiceHost:    serviceHost,
		ServiceHandler: service,
		UserHost:       userHost,
		UserHandler:    user,
		DevFallthrough: devFallthrough,
	})
	require.NoError(t, err)
	require.NotNil(t, router)
	return router, service, user
}

// TestHostRouter_ServiceBucket: everything the bridge answers on today must
// keep reaching the same handler, byte for byte. The subdomain wildcard is
// the load-bearing one — 954 bridged handles resolve through r.Host.
func TestHostRouter_ServiceBucket(t *testing.T) {
	serviceHosts := []struct {
		name string
		host string
	}{
		{"the bridge hostname", serviceHost},
		{"uppercase", "TDPL.IO"},
		{"trailing dot", serviceHost + "."},
		{"default port", serviceHost + ":443"},
		{"bridged handle subdomain", "alice.lemmy-world." + serviceHost},
		{"deep subdomain", "a.b.c." + serviceHost},
		{"localhost", "localhost"},
		{"localhost with port", "localhost:80"},
		{"loopback v4", "127.0.0.1:8080"},
		{"loopback v6", "[::1]:8080"},
		{"absent Host", ""},
		// PUBLIC IP literals used to live here. The review narrowed the
		// address rule to LOOPBACK only: a bare public address names no
		// configured surface, and admitting it let anyone reaching the
		// process directly bypass whatever the proxy enforces per-name.
		// They are now refused in production — see
		// TestHostRouter_RejectsPublicIPLiterals, which also pins that dev
		// fallthrough still keeps them on the service bucket.
	}

	for _, tc := range serviceHosts {
		t.Run(tc.name, func(t *testing.T) {
			router, service, user := newTestRouter(t, false)
			rec := routeHost(t, router, "http", tc.host, "/healthz")
			assert.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
			assert.Equal(t, "service", rec.Header().Get("X-Handler"))
			assert.Equal(t, 1, service.calls, "Host %q belongs to the service surface", tc.host)
			assert.Zero(t, user.calls, "the user surface must not see Host %q", tc.host)
		})
	}
}

// TestHostRouter_HealthcheckReachesService pins the production healthcheck
// exactly: docker's probe sends Host localhost, and a rejection there takes
// the container down.
func TestHostRouter_HealthcheckReachesService(t *testing.T) {
	router, service, _ := newTestRouter(t, false)
	rec := routeHost(t, router, "http", "localhost:80", "/xrpc/_health")
	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	assert.Equal(t, "service /xrpc/_health", rec.Body.String(),
		"the service handler must answer byte-identically through the router")
	assert.Equal(t, 1, service.calls)
}

// TestHostRouter_UserBucket: the user origin's Host, in every spelling that
// names the same authority.
func TestHostRouter_UserBucket(t *testing.T) {
	for _, tc := range []struct {
		name   string
		scheme string
		host   string
	}{
		{"exact", "https", userHost},
		{"uppercase", "https", "COVES.SOCIAL"},
		{"trailing dot", "https", userHost + "."},
		{"default https port", "https", userHost + ":443"},
		// Behind a TLS-terminating proxy the Go server sees plain HTTP with
		// the forwarded Host, so ":443" must normalize without r.TLS.
		{"default https port, proxied", "http", userHost + ":443"},
		{"default http port", "http", userHost + ":80"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router, service, user := newTestRouter(t, false)
			rec := routeHost(t, router, tc.scheme, tc.host, "/.well-known/webfinger")
			assert.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
			assert.Equal(t, "user", rec.Header().Get("X-Handler"),
				"Host %q is the user origin", tc.host)
			assert.Equal(t, 1, user.calls)
			assert.Zero(t, service.calls, "the service surface must not see the user origin")
		})
	}
}

// TestHostRouter_RejectsUnknownHosts is the production posture: an
// authenticated write/AP service must not expose ANY surface under a Host
// an attacker chose.
func TestHostRouter_RejectsUnknownHosts(t *testing.T) {
	for _, tc := range []struct {
		name string
		host string
	}{
		{"unrelated host", "evil.example"},
		// Suffix matching must be on a label boundary in BOTH directions.
		{"suffix-adjacent to the service host", "nottdpl.io"},
		{"service host as a prefix label", serviceHost + ".evil.example"},
		{"suffix-adjacent to the user host", "notcoves.social"},
		{"user host as a prefix label", userHost + ".evil.example"},
		// A non-default port is a different authority (the dev origin runs
		// on :8091, so ports carry meaning here).
		{"user host on another port", userHost + ":8443"},
		// Vanity origins are OUT OF SCOPE for task 13: CreateActorForDID
		// only ever seeds the configured origin, so no ap_actors row can
		// name another host yet. The router gains an OriginAllowlist seam
		// when vanity minting lands; until then a vanity Host is unknown.
		{"registered-looking vanity origin", "vanity.example"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router, service, user := newTestRouter(t, false)
			rec := routeHost(t, router, "https", tc.host, "/healthz")
			assert.Equal(t, http.StatusMisdirectedRequest, rec.Code,
				"Host %q must be refused in production", tc.host)
			assert.Zero(t, service.calls, "a rejected Host must not touch the service surface")
			assert.Zero(t, user.calls, "a rejected Host must not touch the user surface")
		})
	}
}

// TestHostRouter_DevFallthrough: the dev escape hatch routes unknown Hosts
// to the service surface (a laptop is reached by IP, tunnel hostname, or
// whatever ngrok minted this morning).
func TestHostRouter_DevFallthrough(t *testing.T) {
	router, service, user := newTestRouter(t, true)

	rec := routeHost(t, router, "http", "whatever.ngrok.example", "/healthz")
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	assert.Equal(t, "service", rec.Header().Get("X-Handler"))
	assert.Equal(t, 1, service.calls, "dev fallthrough sends unknown Hosts to the service surface")
	assert.Zero(t, user.calls)

	// The user origin still wins its own Host with fallthrough on.
	rec = routeHost(t, router, "https", userHost, "/.well-known/webfinger")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "user", rec.Header().Get("X-Handler"))
	assert.Equal(t, 1, user.calls)
}

// TestHostRouter_CollidingHosts is the default dev configuration, where
// BRIDGE_HOSTNAME and AP_USER_ORIGIN name the same authority
// (localhost:8091). One Host cannot pick a bucket, so the composition is by
// PATH: the user surface is tried first and a 404 from it falls through to
// the service handler, whose response is the one that reaches the client.
func TestHostRouter_CollidingHosts(t *testing.T) {
	const shared = "localhost:8091"
	newRouter := func(t *testing.T) (http.Handler, *marker, *marker) {
		t.Helper()
		// The user surface owns webfinger and the actor paths; everything
		// else 404s out of it, exactly as personas.Service does. The service
		// surface answers its own routes and 404s the actor path — neither
		// side knows that DID, which is how a genuine miss stays a miss.
		service := newMarker("service", "/ap/actor/did:plc:missing")
		user := newMarker("user", "/xrpc/_health", "/healthz", "/ap/actor/did:plc:missing")
		router, err := NewHostRouter(HostRouterOptions{
			ServiceHost:    shared,
			ServiceHandler: service,
			UserHost:       shared,
			UserHandler:    user,
			DevFallthrough: true,
		})
		require.NoError(t, err)
		return router, service, user
	}

	t.Run("a user route is served by the user surface", func(t *testing.T) {
		router, service, user := newRouter(t)
		rec := routeHost(t, router, "http", shared, "/.well-known/webfinger")
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "user", rec.Header().Get("X-Handler"))
		assert.Equal(t, 1, user.calls)
		assert.Zero(t, service.calls, "a path the user surface answered must not be replayed")
	})

	t.Run("a service route falls through cleanly", func(t *testing.T) {
		router, service, user := newRouter(t)
		rec := routeHost(t, router, "http", shared, "/xrpc/_health")
		require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
		assert.Equal(t, 1, user.calls, "the user surface is tried first")
		assert.Equal(t, 1, service.calls)
		// The fallback must REPLACE the user surface's 404, not append to
		// it: buffering the first response is the only way this works.
		assert.Equal(t, "service /xrpc/_health", rec.Body.String())
		assert.Equal(t, "service", rec.Header().Get("X-Handler"),
			"the service response's headers must be the ones that land")
		assert.NotContains(t, rec.Body.String(), "404 page not found")
	})

	t.Run("neither surface knows the path", func(t *testing.T) {
		router, service, user := newRouter(t)
		rec := routeHost(t, router, "http", shared, "/ap/actor/did:plc:missing")
		assert.Equal(t, http.StatusNotFound, rec.Code,
			"a 404 from both surfaces stays a 404")
		assert.Equal(t, 1, user.calls)
		assert.Equal(t, 1, service.calls)
	})
}

// TestHostRouter_DevDefaultAuthorities is the wiring contract for the
// DEFAULT development configuration, where the two hosts are configured
// differently but name the same listener: BRIDGE_HOSTNAME is "localhost"
// while AP_USER_ORIGIN is "http://localhost:8091", so every local request
// arrives as Host "localhost:8091".
//
// That Host matches the user origin exactly AND satisfies the service
// bucket's loopback rule, so composition must key on "both buckets accept
// this authority", not on the two configured strings being equal. Keyed on
// string equality instead, the user surface swallows the whole listener and
// /healthz — the thing a developer hits first — disappears.
func TestHostRouter_DevDefaultAuthorities(t *testing.T) {
	const devServiceHost = "localhost" // BRIDGE_HOSTNAME dev default
	const devUserHost = "localhost:8091"

	newRouter := func(t *testing.T) (http.Handler, *marker, *marker) {
		t.Helper()
		service := newMarker("service")
		user := newMarker("user", "/xrpc/_health", "/healthz")
		router, err := NewHostRouter(HostRouterOptions{
			ServiceHost:    devServiceHost,
			ServiceHandler: service,
			UserHost:       devUserHost,
			UserHandler:    user,
			DevFallthrough: true,
		})
		require.NoError(t, err)
		return router, service, user
	}

	t.Run("the user surface answers its own routes", func(t *testing.T) {
		router, _, user := newRouter(t)
		rec := routeHost(t, router, "http", devUserHost, "/.well-known/webfinger")
		assert.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
		assert.Equal(t, "user", rec.Header().Get("X-Handler"))
		assert.Equal(t, 1, user.calls)
	})

	for _, path := range []string{"/healthz", "/xrpc/_health"} {
		t.Run("the service surface still answers "+path, func(t *testing.T) {
			router, service, _ := newRouter(t)
			rec := routeHost(t, router, "http", devUserHost, path)
			require.Equal(t, http.StatusOK, rec.Code,
				"a dev listener must keep serving %s; body=%s", path, rec.Body.String())
			assert.Equal(t, "service", rec.Header().Get("X-Handler"))
			assert.Equal(t, 1, service.calls)
		})
	}
}

// TestNewHostRouter_RequiresHandlers: a nil handler would nil-panic on the
// first request of whichever bucket it was meant to serve.
func TestNewHostRouter_RequiresHandlers(t *testing.T) {
	_, err := NewHostRouter(HostRouterOptions{
		ServiceHost: serviceHost,
		UserHost:    userHost,
		UserHandler: newMarker("user"),
	})
	assert.Error(t, err, "a router without a service handler is unusable")

	_, err = NewHostRouter(HostRouterOptions{
		ServiceHandler: newMarker("service"),
		UserHost:       userHost,
		UserHandler:    newMarker("user"),
	})
	assert.Error(t, err, "a router without a service host cannot classify anything")
}

// TestHostRouter_RejectsPublicIPLiterals: a bare IP Host has no registered
// name behind it, so it cannot be the user origin — but it can absolutely be
// an attacker probing the service surface directly, bypassing whatever the
// proxy enforces per-name. Loopback is the exception that must keep working:
// container healthchecks and local probes arrive that way.
func TestHostRouter_RejectsPublicIPLiterals(t *testing.T) {
	for _, tc := range []struct {
		name string
		host string
	}{
		{"public IPv4 literal", "192.0.2.10"},
		{"public IPv4 literal with port", "192.0.2.10:8091"},
		{"public IPv6 literal", "[2001:db8::1]:8080"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router, service, user := newTestRouter(t, false)
			rec := routeHost(t, router, "https", tc.host, "/healthz")
			assert.Equal(t, http.StatusMisdirectedRequest, rec.Code,
				"a public IP Host names no configured surface: %q", tc.host)
			assert.Zero(t, service.calls)
			assert.Zero(t, user.calls)
		})
	}

	for _, tc := range []struct {
		name string
		host string
	}{
		{"loopback v4", "127.0.0.1:8091"},
		{"loopback v6", "[::1]"},
		{"loopback name", "localhost"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router, service, _ := newTestRouter(t, false)
			rec := routeHost(t, router, "http", tc.host, "/healthz")
			assert.Equal(t, http.StatusOK, rec.Code,
				"loopback is how healthchecks and local probes arrive; body=%s", rec.Body.String())
			assert.Equal(t, 1, service.calls)
		})
	}

	t.Run("dev fallthrough keeps public IPs on the service surface", func(t *testing.T) {
		router, service, _ := newTestRouter(t, true)
		rec := routeHost(t, router, "http", "192.0.2.10", "/healthz")
		assert.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
		assert.Equal(t, 1, service.calls, "a dev box IS reached by its address")
	})
}

// TestHostRouter_ComposedFallbackOnlyOn404: falling through on any error
// status would replay a request the user surface already REFUSED — turning
// its 400 into a service 404 (or worse, letting a rate-limited 503 be
// retried immediately against another handler). Only "this path is not
// mine", spelled 404, hands over.
func TestHostRouter_ComposedFallbackOnlyOn404(t *testing.T) {
	const shared = "localhost:8091"
	newRouter := func(t *testing.T) (http.Handler, *marker, *marker) {
		t.Helper()
		service := newMarker("service")
		user := newMarker("user", "/xrpc/_health")
		user.statuses = map[string]int{
			// A malformed webfinger: the user surface OWNS this path and
			// has judged the request.
			"/.well-known/webfinger": http.StatusBadRequest,
			// A retryable refusal from the inbox.
			"/ap/inbox": http.StatusServiceUnavailable,
		}
		router, err := NewHostRouter(HostRouterOptions{
			ServiceHost:    shared,
			ServiceHandler: service,
			UserHost:       shared,
			UserHandler:    user,
			DevFallthrough: true,
		})
		require.NoError(t, err)
		return router, service, user
	}

	for _, tc := range []struct {
		path   string
		status int
	}{
		{"/.well-known/webfinger", http.StatusBadRequest},
		{"/ap/inbox", http.StatusServiceUnavailable},
	} {
		t.Run(tc.path, func(t *testing.T) {
			router, service, user := newRouter(t)
			rec := routeHost(t, router, "http", shared, tc.path)
			assert.Equal(t, tc.status, rec.Code,
				"the user surface's own refusal must reach the client unchanged")
			assert.Equal(t, "user", rec.Header().Get("X-Handler"))
			assert.Equal(t, 1, user.calls)
			assert.Zero(t, service.calls,
				"a judged request must not be replayed against the other surface")
		})
	}
}

// TestHostRouter_HostAgnosticPaths: Caddy's on-demand TLS ask reaches this
// process addressed by its DOCKER DNS NAME — the compose `ask` URL is
// http://tidepool:80/.well-known/tidepool-tls-ask, so the request carries
// Host "tidepool", a name that is neither configured surface. The ask gate is
// infrastructure: it must answer for whatever name the edge proxy happens to
// reach the container by, because refusing it with 421 silently breaks every
// NEW handle-cert issuance and every renewal (observed in production
// 2026-08-22). Paths listed as host-agnostic therefore go to the service
// handler before any Host judgment; every other path on the same unrecognized
// Host keeps the 421 posture.
func TestHostRouter_HostAgnosticPaths(t *testing.T) {
	const askPath = "/.well-known/tidepool-tls-ask"

	service := newMarker("service")
	user := newMarker("user")
	router, err := NewHostRouter(HostRouterOptions{
		ServiceHost:       serviceHost,
		ServiceHandler:    service,
		UserHost:          userHost,
		UserHandler:       user,
		HostAgnosticPaths: []string{askPath},
	})
	require.NoError(t, err)

	for _, host := range []string{
		"tidepool",          // the compose service name — production's actual ask Host
		"tidepool:80",       // with the port the ask URL names
		"anything.example",  // any other name the proxy might be told to use
	} {
		t.Run("ask via "+host, func(t *testing.T) {
			rec := routeHost(t, router, "http", host, askPath+"?domain=alice.lemmy-world."+serviceHost)
			assert.Equal(t, http.StatusOK, rec.Code,
				"the TLS ask must be served regardless of Host; a 421 here denies certificate issuance")
			assert.Equal(t, "service", rec.Header().Get("X-Handler"),
				"the ask gate lives on the service surface")
		})
	}

	// The exemption is the PATH, not the Host: the same unrecognized name
	// asking for anything else is still refused.
	rec := routeHost(t, router, "http", "tidepool", "/ap/inbox")
	assert.Equal(t, http.StatusMisdirectedRequest, rec.Code,
		"an unrecognized Host must stay refused for every path not listed as host-agnostic")

	// And on the recognized surfaces nothing changes: the ask path was
	// already served for the bridge hostname.
	rec = routeHost(t, router, "https", serviceHost, askPath)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "service", rec.Header().Get("X-Handler"))
}
