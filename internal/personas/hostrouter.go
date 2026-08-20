package personas

import (
	"bytes"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"tidepool/internal/errors"
	"tidepool/internal/ratelimit"
)

// misdirectedLogInterval throttles the 421 refusal log: a scanner sweeping
// Hosts would otherwise write one line per probe, and the interesting signal
// is "this is happening at all", not each instance.
const misdirectedLogInterval = time.Second

// HostRouterOptions configures NewHostRouter.
type HostRouterOptions struct {
	// ServiceHost is BRIDGE_HOSTNAME: the bridge's own surface, including
	// every bridged handle's subdomain under it.
	ServiceHost    string
	ServiceHandler http.Handler
	// UserHost is AP_USER_ORIGIN's host: the Coves user origin.
	UserHost    string
	UserHandler http.Handler
	// DevFallthrough sends unknown Hosts to the service handler instead of
	// refusing them. A laptop is reached by IP, tunnel hostname, or whatever
	// the tunnel minted this morning; a production deployment is not.
	DevFallthrough bool
	// Logger receives a sampled warning for refused Hosts. Nil uses
	// slog.Default().
	Logger *slog.Logger
}

// NewHostRouter splits one listener between the bridge's service surface and
// the Coves user origin by request Host.
//
// Refusing an unknown Host with 421 is the production posture: this process
// serves an authenticated write surface and an AP inbox, and neither should be
// reachable under a name an attacker chose. Suffix matching is on a LABEL
// BOUNDARY in both directions — "nottdpl.io" is not the service host and
// "tdpl.io.evil.example" is not under it — because a bare substring test here
// would hand an attacker the whole service surface.
func NewHostRouter(opts HostRouterOptions) (http.Handler, error) {
	// A nil handler would nil-panic on the first request of whichever
	// bucket it was meant to serve, and a missing host cannot classify
	// anything: both are startup errors, not runtime surprises.
	if opts.ServiceHost == "" {
		return nil, errors.NewValidationError("service_host", "must not be empty")
	}
	if opts.ServiceHandler == nil {
		return nil, errors.NewValidationError("service_handler", "must not be nil")
	}
	if opts.UserHost == "" {
		return nil, errors.NewValidationError("user_host", "must not be empty")
	}
	if opts.UserHandler == nil {
		return nil, errors.NewValidationError("user_handler", "must not be nil")
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &hostRouter{
		serviceHost:    normalizeHost(opts.ServiceHost),
		serviceHandler: opts.ServiceHandler,
		userHost:       normalizeHost(opts.UserHost),
		userHandler:    opts.UserHandler,
		devFallthrough: opts.DevFallthrough,
		logger:         logger,
		refusalLog:     ratelimit.NewSampler(misdirectedLogInterval),
	}, nil
}

type hostRouter struct {
	serviceHost    string
	serviceHandler http.Handler
	userHost       string
	userHandler    http.Handler
	devFallthrough bool
	logger         *slog.Logger
	refusalLog     *ratelimit.Sampler
}

func (h *hostRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host := normalizeHost(r.Host)
	switch {
	case host == h.userHost && h.isServiceHost(host):
		// BOTH buckets accept this authority, so one Host cannot pick one
		// and the split moves to the path: see serveComposed. The test is
		// "both buckets accept it", not "the two configured strings are
		// equal" — the DEFAULT dev configuration sets BRIDGE_HOSTNAME to
		// "localhost" and AP_USER_ORIGIN to "http://localhost:8091", two
		// different strings naming one listener. Keyed on string equality,
		// the user surface would swallow the whole listener and /healthz —
		// the first thing a developer hits — would disappear.
		h.serveComposed(w, r)
	case host == h.userHost:
		h.userHandler.ServeHTTP(w, r)
	case h.isServiceHost(host):
		h.serviceHandler.ServeHTTP(w, r)
	case h.devFallthrough:
		h.serviceHandler.ServeHTTP(w, r)
	default:
		if h.refusalLog.Allow(time.Now()) {
			h.logger.Warn("refused request for an unrecognized Host (sampled)",
				"host", host, "path", r.URL.Path)
		}
		http.Error(w, "unrecognized Host", http.StatusMisdirectedRequest)
	}
}

// isServiceHost reports whether host belongs to the bridge's own surface:
// the configured hostname, any subdomain of it (954 bridged handles resolve
// through those), or a LOOPBACK address — an absent Host, "localhost", or
// 127.0.0.1/::1, which is how container healthchecks and local probes arrive.
//
// A PUBLIC IP literal is deliberately not in the bucket. It names no
// configured surface, and admitting it would hand an attacker a way to reach
// the service surface directly by address, bypassing whatever the proxy
// enforces per-name. Dev fallthrough still admits it — a dev box IS reached
// by its address — which is the whole reason that flag is refused in
// production.
func (h *hostRouter) isServiceHost(host string) bool {
	if host == "" || host == h.serviceHost || strings.HasSuffix(host, "."+h.serviceHost) {
		return true
	}
	name := hostnameOnly(host)
	if name == "localhost" {
		return true
	}
	ip := net.ParseIP(name)
	return ip != nil && ip.IsLoopback()
}

// serveComposed runs the user surface first and replaces its 404 with the
// service handler's response. The user response is BUFFERED rather than
// streamed: once a status line has reached the client there is no taking it
// back, so a fallback would append its body to the 404 instead of replacing
// it.
//
// INVARIANT: the user surface must not read the request body on any path it
// 404s. The replay hands the SAME *http.Request to the service handler, and a
// consumed body cannot be rewound — the service handler would see an empty
// one. personas.Service satisfies this (only POST /ap/inbox reads a body, and
// it never 404s after reading); a future handler that reads before deciding
// would break the fallback silently, in the direction of an inbox that
// accepts empty deliveries.
func (h *hostRouter) serveComposed(w http.ResponseWriter, r *http.Request) {
	buffered := &bufferedResponse{header: http.Header{}}
	h.userHandler.ServeHTTP(buffered, r)
	if buffered.status() == http.StatusNotFound {
		// The user surface does not serve this path; the service surface
		// gets the real writer, so its headers and status are the ones
		// that land.
		h.serviceHandler.ServeHTTP(w, r)
		return
	}
	buffered.flushTo(w)
}

// bufferedResponse captures a handler's response so the caller can decide
// whether to send it. It implements http.ResponseWriter and nothing else:
// no Flusher, no Hijacker, no ReaderFrom. Composition only happens when both
// configured hosts name one authority — the dev default — and nothing on the
// user surface streams, flushes, or upgrades. A future streaming route on a
// composed listener would need this to forward those interfaces.
type bufferedResponse struct {
	header http.Header
	code   int
	body   bytes.Buffer
}

func (b *bufferedResponse) Header() http.Header { return b.header }

func (b *bufferedResponse) WriteHeader(code int) {
	if b.code == 0 {
		b.code = code
	}
}

func (b *bufferedResponse) Write(p []byte) (int, error) {
	b.WriteHeader(http.StatusOK)
	return b.body.Write(p)
}

// status is the response's status, defaulting to 200 the way net/http does
// for a handler that wrote nothing at all.
func (b *bufferedResponse) status() int {
	if b.code == 0 {
		return http.StatusOK
	}
	return b.code
}

func (b *bufferedResponse) flushTo(w http.ResponseWriter) {
	for key, values := range b.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(b.status())
	_, _ = w.Write(b.body.Bytes())
}

// NormalizeHost reduces an authority to the ONE string this bridge compares:
// lowercase, no trailing dot, and no default port for either scheme.
//
// It is THE definition of "the same authority" for the whole process. The Host
// router keys on it, serving binds each actor to it, CanonicalizeOrigin
// reduces the minted normalized_origin through it, and config's shadow check
// compares BOTH sides with it — the last one matters because a check that
// reduces differently from the router can pass a pair the router then collapses
// into composed mode. One rule, one place, no site allowed to disagree.
//
// Both default ports are stripped unconditionally, without consulting r.TLS.
// In production TLS terminates at the proxy and the Go server sees plain HTTP
// carrying the forwarded Host, so a scheme-keyed rule would classify
// "coves.social:443" as an unknown authority precisely where it matters. A
// NON-default port still carries meaning — the dev origin runs on :8091 and
// coves.social:8443 is a different origin, not a sloppy spelling of one.
//
// internal/echo carries a byte-identical private twin for read-side actor
// classification; the two are documented as one rule and must move together.
func NormalizeHost(host string) string {
	normalized := strings.ToLower(strings.TrimSpace(host))
	for _, defaultPort := range []string{":443", ":80"} {
		if trimmed, found := strings.CutSuffix(normalized, defaultPort); found {
			normalized = trimmed
			break
		}
	}
	return strings.TrimSuffix(normalized, ".")
}

// normalizeHost is the package-internal spelling of NormalizeHost.
func normalizeHost(host string) string { return NormalizeHost(host) }

// hostnameOnly strips a port and IPv6 brackets, leaving the name or address.
func hostnameOnly(host string) string {
	if name, _, err := net.SplitHostPort(host); err == nil {
		return name
	}
	return strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
}
