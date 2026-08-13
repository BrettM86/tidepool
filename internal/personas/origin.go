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

	// A fully-qualified name's trailing dot names the same host, and the
	// scheme's default port is not part of the authority. Any OTHER port is:
	// the dev origin runs on :8091 and that is a different origin.
	hostname := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if hostname == "" {
		return "", "", errors.NewValidationError("origin",
			fmt.Sprintf("must name a host, got %q", raw))
	}
	port := parsed.Port()
	if (scheme == "https" && port == "443") || (scheme == "http" && port == "80") {
		port = ""
	}
	host = hostname
	if strings.Contains(host, ":") {
		// An IPv6 literal keeps the brackets url.URL.Hostname stripped.
		host = "[" + host + "]"
	}
	if port != "" {
		host += ":" + port
	}
	return scheme + "://" + host, host, nil
}
