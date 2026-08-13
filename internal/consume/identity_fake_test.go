package consume

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeIdentity is the atproto identity world a resolver test runs against: a
// PLC directory serving DID documents, and the handles' own
// /.well-known/atproto-did endpoints serving the reverse claim. Both live on
// ONE httptest listener, dispatched by request Host, so a single fake can
// stage every disagreement between the two directions.
//
// Local-only: identityRewriteTransport refuses any host the fake does not
// know, so a resolver bug cannot turn into a request to the real internet.
type fakeIdentity struct {
	server *httptest.Server

	mu sync.Mutex
	// alsoKnownAs is the handle each DID's document CLAIMS.
	alsoKnownAs map[string]string
	// wellKnown is the DID each handle claims BACK. A handle absent here has
	// no well-known endpoint (404) — the two maps are separate precisely so a
	// test can make the directions disagree.
	wellKnown map[string]string
	// plcStatus / plcBody force a directory response for one DID.
	plcStatus map[string]int
	plcBody   map[string]string
	// wellKnownStatus forces a status for one handle's endpoint.
	wellKnownStatus map[string]int

	plcHits       int
	wellKnownHits int
}

const wellKnownATProtoDIDPath = "/.well-known/atproto-did"

func newFakeIdentity(t *testing.T) *fakeIdentity {
	t.Helper()

	fake := &fakeIdentity{
		alsoKnownAs:     map[string]string{},
		wellKnown:       map[string]string{},
		plcStatus:       map[string]int{},
		plcBody:         map[string]string{},
		wellKnownStatus: map[string]int{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc(wellKnownATProtoDIDPath, func(w http.ResponseWriter, r *http.Request) {
		handle := strings.ToLower(hostOnly(r.Host))

		fake.mu.Lock()
		fake.wellKnownHits++
		status, forced := fake.wellKnownStatus[handle]
		did, claimed := fake.wellKnown[handle]
		fake.mu.Unlock()

		if forced {
			w.WriteHeader(status)
			_, _ = w.Write([]byte("forced well-known failure"))
			return
		}
		if !claimed {
			// A handle with no well-known endpoint. In the real world this is
			// often a DNS-TXT-only handle; here it is simply unverifiable.
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		// Real PDSes serve the DID with a trailing newline; a resolver that
		// compares without trimming would reject every genuine handle.
		_, _ = w.Write([]byte(did + "\n"))
	})

	// Everything else is the PLC directory: GET /{did}.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		did := strings.TrimPrefix(r.URL.Path, "/")

		fake.mu.Lock()
		fake.plcHits++
		status, forcedStatus := fake.plcStatus[did]
		body, forcedBody := fake.plcBody[did]
		handle, known := fake.alsoKnownAs[did]
		fake.mu.Unlock()

		if forcedStatus {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"message":"forced directory failure"}`))
			return
		}
		w.Header().Set("Content-Type", "application/did+ld+json")
		if forcedBody {
			_, _ = w.Write([]byte(body))
			return
		}
		if !known {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(fmt.Sprintf(`{
			"@context": ["https://www.w3.org/ns/did/v1"],
			"id": %q,
			"alsoKnownAs": ["at://%s"],
			"verificationMethod": [],
			"service": [{"id":"#atproto_pds","type":"AtprotoPersonalDataServer",
			             "serviceEndpoint":"https://pds.example"}]
		}`, did, handle)))
	})

	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

// claim wires BOTH directions: the DID document names the handle and the
// handle names the DID back. This is the only combination a resolver may
// accept.
func (f *fakeIdentity) claim(did, handle string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alsoKnownAs[did] = handle
	f.wellKnown[strings.ToLower(handle)] = did
}

// claimOneWay gives the DID document a handle that does NOT claim it back —
// the handle has no well-known endpoint at all.
func (f *fakeIdentity) claimOneWay(did, handle string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alsoKnownAs[did] = handle
}

// wellKnownReturns overrides who a handle says it belongs to. Pointing an
// already-claimed handle at a different DID is the impersonation case.
func (f *fakeIdentity) wellKnownReturns(handle, did string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wellKnown[strings.ToLower(handle)] = did
}

func (f *fakeIdentity) plcFails(did string, status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.plcStatus[did] = status
}

func (f *fakeIdentity) plcServes(did, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.plcBody[did] = body
}

func (f *fakeIdentity) wellKnownFails(handle string, status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wellKnownStatus[strings.ToLower(handle)] = status
}

func (f *fakeIdentity) PLCHits() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.plcHits
}

func (f *fakeIdentity) WellKnownHits() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.wellKnownHits
}

// resolver builds the REAL HandleResolver against this fake. Production wires
// ap.NewGuardedHTTPClient here; the test transport below plays the same role
// the ALLOW_PRIVATE_FETCH relaxation does in dev, except that it also refuses
// every host the fake does not serve.
func (f *fakeIdentity) resolver(t *testing.T) *HandleResolver {
	t.Helper()
	resolver, err := NewHandleResolver(ResolverOptions{
		PLCDirectoryURL: f.server.URL,
		HTTPClient: &http.Client{
			Transport: identityRewriteTransport{target: f.server.Listener.Addr().String()},
		},
		UserAgent: "tidepool-test/0.1",
	})
	require.NoError(t, err, "build handle resolver")
	require.NotNil(t, resolver)
	return resolver
}

// identityRewriteTransport sends requests for handle hosts to the fake's
// listener while preserving the request URL and Host, so the resolver believes
// it is talking to https://alice.coves.social. Anything outside the test
// namespace is refused: these tests never touch the network.
type identityRewriteTransport struct {
	target string
}

func (rt identityRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := hostOnly(req.URL.Host)
	allowed := host == hostOnly(rt.target) ||
		strings.HasSuffix(host, ".coves.social") ||
		strings.HasSuffix(host, ".example") ||
		host == "127.0.0.1" || host == "localhost"
	if !allowed {
		return nil, fmt.Errorf("refusing outbound request to %s: tests may not reach the network", req.URL)
	}
	clone := req.Clone(req.Context())
	clone.Host = req.URL.Host // the mux dispatches the well-known by Host
	clone.URL.Scheme = "http"
	clone.URL.Host = rt.target
	return http.DefaultTransport.RoundTrip(clone)
}

func hostOnly(hostport string) string {
	if idx := strings.LastIndex(hostport, ":"); idx != -1 {
		if !strings.Contains(hostport[idx+1:], "]") {
			return hostport[:idx]
		}
	}
	return hostport
}

// recordingResolver is the DIDResolver seam, recorded. Dispatcher-tier tests
// use it so they pin WHETHER resolution happened and WHAT handle reached the
// mint, without standing up the identity world for every case.
type recordingResolver struct {
	mu     sync.Mutex
	calls  []string
	handle string
	err    error
}

func (r *recordingResolver) ResolveDIDHandle(_ context.Context, did string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, did)
	if r.err != nil {
		return "", r.err
	}
	return r.handle, nil
}

func (r *recordingResolver) Calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}
