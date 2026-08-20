package personas

import (
	"fmt"
	"net/url"
	"strings"

	"tidepool/internal/errors"
)

// CanonicalizeOrigin reduces an origin URL to its ONE canonical spelling and
// returns both the origin ("https://coves.social") and the authority
// ("coves.social") that Host routing and ap_actors.normalized_origin use.
//
// This is the single definition of "the same origin" for the whole bridge:
// config validates AP_USER_ORIGIN through it and stores the result, and
// personas.New runs it again for callers constructed directly. Two spellings
// of one authority must never mint two namespaces — an actor minted under
// "coves.social:443" would carry a normalized_origin that the routed Host,
// which arrives canonical, could never match, and the actor_id is FROZEN at
// mint, so the mistake is permanent.
//
// Anything past scheme://host is refused rather than trimmed: a path, query,
// fragment, or userinfo in this value would be concatenated into every
// actor_id, keyId, and webfinger href, and silently dropping it would mint
// identities the operator did not ask for.
//
// THE AUTHORITY IS REDUCED BY normalizeHost — the SAME function the Host
// router applies to every incoming request — so the canonical host is a fixed
// point of routing by construction. The two rules used to differ by one
// scheme test: this one dropped the default port only when it matched the
// scheme, the router drops :443 and :80 unconditionally (it must — in
// production TLS terminates at the proxy and this process sees plain HTTP
// carrying "Host: coves.social:443", so a scheme-keyed router would refuse the
// production shape). They agreed on "https + :443" and "http + :80" and
// disagreed on the mirror pairs, so an origin spelled "http://host:443" minted
// normalized_origin "host:443" that the routed Host — folded to "host" —
// could never match, under ANY spelling, forever. Widening the fold here is
// the only direction available: the router cannot learn the scheme.
func CanonicalizeOrigin(raw string) (origin, host string, err error) {
	parsed, parseErr := url.Parse(raw)
	if parseErr != nil {
		return "", "", errors.NewValidationError("origin",
			fmt.Sprintf("must be an absolute origin URL, got %q: %v", raw, parseErr))
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme == "" || parsed.Host == "" || parsed.Opaque != "" {
		return "", "", errors.NewValidationError("origin",
			fmt.Sprintf("must be an absolute origin URL (scheme and host), got %q", raw))
	}
	if scheme != "http" && scheme != "https" {
		return "", "", errors.NewValidationError("origin",
			fmt.Sprintf("must be http or https, got %q", scheme))
	}
	if parsed.Path != "" || parsed.RawQuery != "" || parsed.ForceQuery ||
		parsed.Fragment != "" || parsed.User != nil {
		return "", "", errors.NewValidationError("origin",
			fmt.Sprintf("must be a bare origin (scheme://host, no path, query, fragment, or userinfo), got %q", raw))
	}

	// A fully-qualified name's trailing dot names the same host, and NEITHER
	// scheme's default port is part of the authority. Any OTHER port is: the
	// dev origin runs on :8091 and that is a different origin.
	hostname := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if hostname == "" {
		return "", "", errors.NewValidationError("origin",
			fmt.Sprintf("must name a host, got %q", raw))
	}
	host = hostname
	if strings.Contains(host, ":") {
		// An IPv6 literal keeps the brackets url.URL.Hostname stripped.
		host = "[" + host + "]"
	}
	if port := parsed.Port(); port != "" {
		host += ":" + port
	}
	host = normalizeHost(host)

	// The round-trip property, asserted rather than assumed: normalizeHost is
	// idempotent today, and this is the line that fails loudly at startup
	// rather than minting a permanently unroutable namespace if a future edit
	// to either rule breaks the agreement.
	if reduced := normalizeHost(host); reduced != host {
		return "", "", errors.NewValidationError("origin",
			fmt.Sprintf("canonical host %q does not survive Host normalization (%q): "+
				"actors minted here would be unreachable under every Host spelling", host, reduced))
	}
	return scheme + "://" + host, host, nil
}
