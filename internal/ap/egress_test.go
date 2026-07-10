package ap

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

func TestIsBlockedIP(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "::1", // loopback
		"10.0.0.5", "192.168.1.1", "172.16.0.1", // RFC1918 private
		"169.254.169.254",         // cloud metadata (link-local)
		"169.254.0.1",             // link-local
		"fe80::1",                 // link-local v6
		"fc00::1", "fd12:3456::1", // unique-local v6
		"224.0.0.1", "ff02::1", // multicast
		"0.0.0.0", "::", // unspecified
	}
	for _, s := range blocked {
		ip := net.ParseIP(s)
		require.NotNil(t, ip, s)
		assert.True(t, isBlockedIP(ip), "%s must be blocked", s)
	}

	allowed := []string{"8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:2800:220:1::1"}
	for _, s := range allowed {
		ip := net.ParseIP(s)
		require.NotNil(t, ip, s)
		assert.False(t, isBlockedIP(ip), "%s must be allowed", s)
	}
}

func TestCheckURL_SchemeAndUserinfo(t *testing.T) {
	g := newEgressGuard(false)
	cases := []string{
		"ftp://example.com/x",           // non-http scheme
		"file:///etc/passwd",            // non-http scheme
		"https://user:pass@example.com", // userinfo
		"https://127.0.0.1/x",           // blocked IP literal
		"https://169.254.169.254/x",     // metadata literal
	}
	for _, raw := range cases {
		u, err := url.Parse(raw)
		require.NoError(t, err, raw)
		assert.Error(t, g.checkURL(u), "%s must be refused", raw)
	}

	// A public hostname passes the URL-level check (its resolved IP is checked
	// later at dial time).
	u, _ := url.Parse("https://lemmy.world/c/technology")
	assert.NoError(t, g.checkURL(u))

	// With the guard relaxed (dev/test), private literals are allowed.
	off := newEgressGuard(true)
	u, _ = url.Parse("https://127.0.0.1:8443/x")
	assert.NoError(t, off.checkURL(u))
}

// TestFetch_BlocksLoopbackWhenGuardOn: with the guard on (production default),
// a fetch to a 127.0.0.1 httptest server is refused before any request. This
// is the initial-request egress check.
func TestFetch_BlocksLoopbackWhenGuardOn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"type":"Note","id":"https://x.example/1"}`))
	}))
	defer server.Close()

	// Guard ON: do NOT set AllowPrivateAddresses.
	client := NewClient(ClientOptions{UserAgent: "tidepool-test/0", Signer: NewSigner(testKeyID, testRSAKey(t))})
	_, err := client.FetchObject(context.Background(), server.URL+"/1")
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err), "loopback fetch must be an egress validation error, got %v", err)
	assert.Contains(t, err.Error(), "egress")
}

// TestFetch_AllowsLoopbackWhenGuardOff: the test/dev override lets the same
// fetch through.
func TestFetch_AllowsLoopbackWhenGuardOff(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"type":"Note","id":"https://x.example/1"}`))
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		UserAgent:             "tidepool-test/0",
		Signer:                NewSigner(testKeyID, testRSAKey(t)),
		AllowPrivateAddresses: true,
	})
	obj, err := client.FetchObject(context.Background(), server.URL+"/1")
	require.NoError(t, err)
	assert.Equal(t, "https://x.example/1", obj.ID)
}

// TestDialGuard_BlocksResolvedMetadata proves the DNS-rebinding / redirect
// defense: a hostname (which passes the URL-level check because it is not an
// IP literal) that RESOLVES to the cloud-metadata address is refused at dial
// time, when the actually-connected IP is validated. Every request — the
// initial one AND each redirect hop — dials through exactly this path.
func TestDialGuard_BlocksResolvedMetadata(t *testing.T) {
	g := newEgressGuard(false)
	g.lookupIPAddr = func(_ context.Context, _ string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.IPv4(169, 254, 169, 254)}}, nil
	}
	dialed := false
	base := func(_ context.Context, _, _ string) (net.Conn, error) {
		dialed = true
		return stubConn{}, nil
	}
	_, err := g.dialContext(base)(context.Background(), "tcp", "metadata.attacker.test:80")
	require.Error(t, err, "a hostname resolving to metadata must be refused at dial time")
	assert.False(t, dialed, "the guard must not dial a blocked resolved address")
}

// TestDialGuard_BlocksResolvedPrivate covers a rebind to an RFC1918 address.
func TestDialGuard_BlocksResolvedPrivate(t *testing.T) {
	g := newEgressGuard(false)
	g.lookupIPAddr = func(_ context.Context, _ string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.IPv4(10, 0, 0, 5)}}, nil
	}
	base := func(_ context.Context, _, _ string) (net.Conn, error) { return stubConn{}, nil }
	_, err := g.dialContext(base)(context.Background(), "tcp", "internal.attacker.test:80")
	require.Error(t, err)
}

// TestDialGuard_DialsValidatedIP: a hostname resolving to a public IP is
// dialed by its resolved IP literal (not the hostname), defeating a rebind
// between the DNS answer and the connect.
func TestDialGuard_DialsValidatedIP(t *testing.T) {
	g := newEgressGuard(false)
	g.lookupIPAddr = func(_ context.Context, _ string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.IPv4(93, 184, 216, 34)}}, nil
	}
	var dialedAddr string
	base := func(_ context.Context, _, addr string) (net.Conn, error) {
		dialedAddr = addr
		return stubConn{}, nil
	}
	_, err := g.dialContext(base)(context.Background(), "tcp", "example.test:443")
	require.NoError(t, err)
	assert.Equal(t, "93.184.216.34:443", dialedAddr,
		"the guard must dial the validated IP literal, not the hostname")
}

// TestDialGuard_IPLiteralAddr validates a blocked IP-literal address passed
// straight to the dialer (no resolution step).
func TestDialGuard_IPLiteralAddr(t *testing.T) {
	g := newEgressGuard(false)
	base := func(_ context.Context, _, _ string) (net.Conn, error) { return stubConn{}, nil }
	_, err := g.dialContext(base)(context.Background(), "tcp", "169.254.169.254:80")
	require.Error(t, err, "a blocked IP literal must be refused at dial time")
}

// TestPrivateOnlyHTTPClient_RefusesPublicDestinations: the dev-only client
// (ALLOW_DEV_REQUEST_CRAWL's crawl path) is the INVERSE of the SSRF guard —
// it must refuse anything public at dial time, so a misconfigured
// RELAY_HOSTS in dev can never contact live infrastructure. 203.0.113.10 is
// TEST-NET-3: the refusal happens before any dial, so nothing is contacted.
func TestPrivateOnlyHTTPClient_RefusesPublicDestinations(t *testing.T) {
	client := NewPrivateOnlyHTTPClient(2 * time.Second)
	resp, err := client.Get("http://203.0.113.10/")
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err, "a public literal-IP destination must be refused at dial time")
	assert.True(t, errors.IsValidation(err), "refusal must be the guard's validation error, got %v", err)
	assert.Contains(t, err.Error(), "public")
}

// TestPrivateOnlyHTTPClient_AllowsLoopback: the same client must still reach
// local servers — that is its whole purpose (local e2e relays).
func TestPrivateOnlyHTTPClient_AllowsLoopback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewPrivateOnlyHTTPClient(2 * time.Second)
	resp, err := client.Get(server.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

// TestPrivateOnlyDialGuard_ResolvedPublicIP: a hostname that resolves to a
// public address is refused by the private-only guard at dial time, same
// resolved-IP mechanics as the SSRF guard but inverted.
func TestPrivateOnlyDialGuard_ResolvedPublicIP(t *testing.T) {
	g := &egressGuard{privateOnly: true}
	g.lookupIPAddr = func(_ context.Context, _ string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.IPv4(93, 184, 216, 34)}}, nil
	}
	dialed := false
	base := func(_ context.Context, _, _ string) (net.Conn, error) {
		dialed = true
		return stubConn{}, nil
	}
	_, err := g.dialContext(base)(context.Background(), "tcp", "relay.example:443")
	require.Error(t, err, "a hostname resolving to a public address must be refused")
	assert.False(t, dialed, "the private-only guard must not dial a public resolved address")

	// The same guard dials a loopback resolution.
	g.lookupIPAddr = func(_ context.Context, _ string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.IPv4(127, 0, 0, 1)}}, nil
	}
	_, err = g.dialContext(base)(context.Background(), "tcp", "relay.local:443")
	require.NoError(t, err)
	assert.True(t, dialed)
}

// stubConn is a no-op net.Conn so the dial guard tests never touch the network.
type stubConn struct{ net.Conn }
