package ap

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"tidepool/internal/errors"
)

// NewGuardedHTTPClient returns an *http.Client whose transport enforces the
// same SSRF egress guard the AP client uses (resolved-IP validation at dial
// time). Non-AP outbound HTTP — the identity package's PLC directory client
// — uses this so every egress path in the bridge shares one guard. As with
// the AP client, allowPrivate must only be true in development/tests
// (config.AllowPrivateAddresses, env ALLOW_PRIVATE_FETCH).
func NewGuardedHTTPClient(allowPrivate bool, timeout time.Duration) *http.Client {
	if timeout == 0 {
		timeout = DefaultRequestTimeout
	}
	guard := newEgressGuard(allowPrivate)
	return &http.Client{
		Timeout:   timeout,
		Transport: guardedTransport(nil, guard),
	}
}

// egressGuard blocks outbound requests to addresses an SSRF attacker would
// pivot through: loopback, RFC1918 private, link-local, unique-local,
// multicast, unspecified, and the cloud-metadata endpoint. It rejects
// non-http(s) schemes and URL userinfo, validates any IP literal in a URL,
// and — crucially — validates the actually-resolved IP at dial time so a
// hostname that resolves to a blocked address (or rebinds between the DNS
// answer and the connect) is refused. This mirrors bridgy-fed's
// are_urls_safe egress check.
//
// The guard is config-gated (config.AllowPrivateAddresses /
// ClientOptions.AllowPrivateAddresses): production keeps it on; local dev and
// the httptest-based tests that hit 127.0.0.1 turn it off.
type egressGuard struct {
	allowPrivate bool
	// lookupIPAddr resolves a hostname to candidate IPs. It is a field (not a
	// direct net.Resolver call) so tests can inject a DNS-rebinding scenario —
	// a hostname that resolves to a blocked address — without a real DNS server.
	lookupIPAddr func(ctx context.Context, host string) ([]net.IPAddr, error)
}

func newEgressGuard(allowPrivate bool) *egressGuard {
	return &egressGuard{
		allowPrivate: allowPrivate,
		lookupIPAddr: net.DefaultResolver.LookupIPAddr,
	}
}

// checkURL validates scheme, userinfo, and any IP-literal host before a
// request is dialed. Hostname-based blocking happens at dial time (checkIP
// against the resolved address).
func (g *egressGuard) checkURL(u *url.URL) error {
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.NewValidationError("url", fmt.Sprintf("unsupported scheme %q", u.Scheme))
	}
	if u.User != nil {
		return errors.NewValidationError("url", "url must not contain userinfo")
	}
	host := u.Hostname()
	if host == "" {
		return errors.NewValidationError("url", fmt.Sprintf("url %q has no host", u.String()))
	}
	if ip := net.ParseIP(host); ip != nil {
		return g.checkIP(ip)
	}
	return nil
}

// checkIP rejects addresses in the blocked ranges unless the guard is
// disabled for dev/test.
func (g *egressGuard) checkIP(ip net.IP) error {
	if g.allowPrivate {
		return nil
	}
	if isBlockedIP(ip) {
		return errors.NewValidationError("url",
			fmt.Sprintf("address %s is not an allowed egress target", ip))
	}
	return nil
}

// metadataV4 is the cloud instance-metadata address (AWS/GCP/Azure/etc.). It
// is link-local so isBlockedIP already catches it; naming it makes intent
// explicit and covers the mapped forms.
var metadataV4 = net.IPv4(169, 254, 169, 254)

func isBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified() {
		return true
	}
	if ip.Equal(metadataV4) {
		return true
	}
	return false
}

type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// dialContext wraps a base dialer, resolving the host and validating every
// candidate IP before connecting, then dialing a validated IP directly so a
// DNS answer cannot rebind to a blocked address between check and connect.
func (g *egressGuard) dialContext(base dialFunc) dialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if g.allowPrivate {
			return base(ctx, network, addr)
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		// IP literal: validate and dial as-is.
		if ip := net.ParseIP(host); ip != nil {
			if err := g.checkIP(ip); err != nil {
				return nil, err
			}
			return base(ctx, network, addr)
		}
		ips, err := g.lookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		var lastErr error
		for _, ipa := range ips {
			if err := g.checkIP(ipa.IP); err != nil {
				lastErr = err
				continue
			}
			// Dial the exact IP we validated (not the hostname) to defeat DNS
			// rebinding.
			conn, err := base(ctx, network, net.JoinHostPort(ipa.IP.String(), port))
			if err != nil {
				lastErr = err
				continue
			}
			return conn, nil
		}
		if lastErr == nil {
			lastErr = errors.NewValidationError("url",
				fmt.Sprintf("host %q did not resolve to any address", host))
		}
		return nil, lastErr
	}
}
