package consume

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
)

// TASK 17d — THE CONFIRM AT THE TRANSPORT (decision 19).
//
// The four dispatcher-tier cases in account_confirm_test.go pin what the bridge
// DOES with each of the confirm's three answers. They cannot pin where those
// answers come from: the confirmer is stubbed there, so the suite stays green
// against a resolver that picks the wrong service entry, follows a redirect off
// the PDS, accepts plain HTTP, or turns a non-200 into a verdict.
//
// THIS IS THE LAYER WHERE THE DANGER LIVES, and the danger is specific: the seam
// was built so that no FAILURE could become a VERDICT. Every route by which
// somebody other than the DID's own PDS gets to answer hands that verdict away.
// `{"active":false,"status":"deleted"}` from an on-path attacker, or from a
// server reached after an HTTPS→HTTP downgrade, is the irreversible verdict —
// Delete{Person, removeData:true} to every instance holding that user's content,
// which no peer undoes.
//
// So the rule these tests describe is one rule: THE ANSWER MUST COME FROM THE
// PDS THE DID DOCUMENT NAMES, OVER A CHANNEL THAT CANNOT BE READ OR REWRITTEN,
// AND ANYTHING ELSE IS AN ERROR. Not a "live" verdict — an error, because "we
// could not confirm" is its own outcome.
//
// Local-only: one httptest listener plays the PLC directory and every PDS, and
// the transport REFUSES any host the fixture does not serve, so a policy bug
// cannot become a request to the real internet.

const (
	asDID = "did:plc:accountstatus0000001"
	// The PDS the document names, and a second one that has no claim on this
	// DID. A redirect from the first to the second is the on-path case in its
	// most honest form: the peer that answers is not the peer we asked.
	asPDSHost      = "pds.example"
	asOtherPDSHost = "other-pds.example"

	asStatusPath = "/xrpc/com.atproto.sync.getRepoStatus"
)

// asAttempt is one outbound request the resolver made, recorded BEFORE the test
// transport rewrites it onto the local listener — so the scheme and host are the
// ones the resolver actually asked for.
type asAttempt struct {
	scheme string
	host   string
	path   string
}

// pdsWorld is a PLC directory plus one or more PDS hosts.
type pdsWorld struct {
	server *httptest.Server

	mu sync.Mutex
	// The DID document: what endpoint it names, or a raw body / status override.
	endpoint  string
	omitPDS   bool
	plcStatus int
	plcBody   string
	// Per-PDS-host behaviour for getRepoStatus.
	status   map[string]int
	body     map[string]string
	redirect map[string]string

	attempts []asAttempt
}

func newPDSWorld(t *testing.T) *pdsWorld {
	t.Helper()
	world := &pdsWorld{
		endpoint: "https://" + asPDSHost,
		status:   map[string]int{},
		body:     map[string]string{},
		redirect: map[string]string{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc(asStatusPath, func(w http.ResponseWriter, r *http.Request) {
		host := hostOnly(r.Host)
		world.mu.Lock()
		location, redirects := world.redirect[host]
		status, forcedStatus := world.status[host]
		body, forcedBody := world.body[host]
		world.mu.Unlock()

		if redirects {
			http.Redirect(w, r, location, http.StatusFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if forcedStatus {
			w.WriteHeader(status)
		}
		if forcedBody {
			_, _ = w.Write([]byte(body))
			return
		}
		// The default answer is a LIVE repo, so a test that reaches the wrong
		// server by accident cannot pass by inheriting a deleted verdict.
		_, _ = w.Write([]byte(`{"did":"` + asDID + `","active":true}`))
	})

	// Everything else is the PLC directory: GET /{did}.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		world.mu.Lock()
		status, endpoint, omit, body := world.plcStatus, world.endpoint, world.omitPDS, world.plcBody
		world.mu.Unlock()

		if status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"message":"forced directory failure"}`))
			return
		}
		w.Header().Set("Content-Type", "application/did+ld+json")
		if body != "" {
			_, _ = w.Write([]byte(body))
			return
		}
		service := fmt.Sprintf(
			`{"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":%q}`, endpoint)
		if omit {
			// A document whose only service is something else entirely. It is
			// NOT evidence of a deletion — the same shape appears while a
			// document is mid-rewrite or hosting is moving.
			service = `{"id":"#atproto_labeler","type":"AtprotoLabeler","serviceEndpoint":"https://labeler.example"}`
		}
		_, _ = w.Write([]byte(fmt.Sprintf(
			`{"id":%q,"alsoKnownAs":["at://alice.example"],"service":[%s]}`, asDID, service)))
	})

	world.server = httptest.NewServer(mux)
	t.Cleanup(world.server.Close)
	return world
}

// resolver builds the REAL HandleResolver over this world.
func (w *pdsWorld) resolver(t *testing.T) *HandleResolver {
	t.Helper()
	resolver, err := NewHandleResolver(ResolverOptions{
		PLCDirectoryURL: w.server.URL,
		HTTPClient:      &http.Client{Transport: &asRecordingTransport{world: w}},
		UserAgent:       "tidepool-test/0.1",
		LookupTXT:       func(context.Context, string) ([]string, error) { return nil, nil },
	})
	require.NoError(t, err)
	return resolver
}

func (w *pdsWorld) record(a asAttempt) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.attempts = append(w.attempts, a)
}

// statusAttempts lists the getRepoStatus requests the resolver made. The DID
// document fetch is excluded: every case here makes exactly one of those, and
// what is under test is who got asked for the VERDICT.
func (w *pdsWorld) statusAttempts() []asAttempt {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []asAttempt
	for _, attempt := range w.attempts {
		if strings.HasPrefix(attempt.path, asStatusPath) {
			out = append(out, attempt)
		}
	}
	return out
}

// asRecordingTransport records what the resolver asked for and then serves it
// from the local listener. It refuses every host outside the fixture, so the
// tests stay offline even while proving a guard is missing.
type asRecordingTransport struct{ world *pdsWorld }

func (rt *asRecordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := hostOnly(req.URL.Host)
	rt.world.record(asAttempt{scheme: req.URL.Scheme, host: host, path: req.URL.Path})

	listener := hostOnly(rt.world.server.Listener.Addr().String())
	if host != listener && !strings.HasSuffix(host, ".example") {
		return nil, fmt.Errorf("refusing outbound request to %s: these tests never touch a network", req.URL)
	}
	clone := req.Clone(req.Context())
	clone.Host = req.URL.Host // the mux dispatches PDS hosts by Host header
	clone.URL.Scheme = "http"
	clone.URL.Host = rt.world.server.Listener.Addr().String()
	return http.DefaultTransport.RoundTrip(clone)
}

// ---------------------------------------------------------------------------
// The verdicts themselves
// ---------------------------------------------------------------------------

// TestAccountStatus_OnlyAnInactiveDeletedRepoIsDeleted pins the predicate both
// halves of it: `active` alone reads a suspension as a deletion, and `status`
// alone believes a field a PDS may omit for a live repo.
func TestAccountStatus_OnlyAnInactiveDeletedRepoIsDeleted(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{"live repo", `{"active":true}`, false},
		{"live repo with a status field", `{"active":true,"status":"active"}`, false},
		{"deactivated", `{"active":false,"status":"deactivated"}`, false},
		{"suspended", `{"active":false,"status":"suspended"}`, false},
		{"takendown", `{"active":false,"status":"takendown"}`, false},
		{"throttled", `{"active":false,"status":"throttled"}`, false},
		{"inactive with no status at all", `{"active":false}`, false},
		{"active AND deleted, which is incoherent", `{"active":true,"status":"deleted"}`, false},
		{"the one shape that means gone", `{"active":false,"status":"deleted"}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newPDSWorld(t)
			world.body[asPDSHost] = tc.body

			deleted, err := world.resolver(t).AccountStatus(context.Background(), asDID)
			require.NoError(t, err)
			assert.Equal(t, tc.want, deleted,
				"only active=false AND status=deleted is a deletion: every other inactive "+
					"state is one a user comes back from, and withdrawing their identity "+
					"over a suspension destroys it irreversibly")
		})
	}
}

// ---------------------------------------------------------------------------
// The directory leg: no failure may become a verdict
// ---------------------------------------------------------------------------

func TestAccountStatus_DirectoryFailuresAreErrorsNeverVerdicts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*pdsWorld)
	}{
		{"directory 503", func(w *pdsWorld) { w.plcStatus = http.StatusServiceUnavailable }},
		{"directory 404", func(w *pdsWorld) { w.plcStatus = http.StatusNotFound }},
		{"directory serves a non-document", func(w *pdsWorld) { w.plcBody = `<html>maintenance</html>` }},
		{"document names no atproto PDS", func(w *pdsWorld) { w.omitPDS = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newPDSWorld(t)
			tc.setup(world)

			deleted, err := world.resolver(t).AccountStatus(context.Background(), asDID)
			require.Error(t, err,
				"an unanswerable directory is UNKNOWN: read as 'live' it silently drops a "+
					"real deletion and leaves the user federated forever, read as 'deleted' "+
					"it erases someone who never left. Only an error keeps the event redrivable")
			assert.False(t, deleted, "and no verdict may ride out alongside the error")
			assert.Empty(t, world.statusAttempts(),
				"nor may a missing or unreadable document be papered over by asking somebody "+
					"else: with no PDS named, there is no authority to ask")
		})
	}
}

// ---------------------------------------------------------------------------
// The endpoint the document names: it is attacker-influenced input
// ---------------------------------------------------------------------------

// TestAccountStatus_AnUnusablePDSEndpointIsRefusedBeforeAnyRequest covers the
// shapes that are not an absolute http(s) URL. The serviceEndpoint is written by
// the DID's own controller, so it reaches this code as untrusted text, and the
// check has to hold BEFORE the first packet.
func TestAccountStatus_AnUnusablePDSEndpointIsRefusedBeforeAnyRequest(t *testing.T) {
	for _, endpoint := range []string{
		"pds.example",                 // no scheme: url.Parse gives it no host
		"/xrpc",                       // relative
		"file:///etc/passwd",          // not http(s)
		"ftp://pds.example",           // not http(s)
		"",                            // present but empty
		"https://user:pw@pds.example", // userinfo: the credential is not ours to send
	} {
		t.Run(endpoint, func(t *testing.T) {
			world := newPDSWorld(t)
			world.endpoint = endpoint

			deleted, err := world.resolver(t).AccountStatus(context.Background(), asDID)
			require.Error(t, err, "an endpoint that is not a plain absolute http(s) URL is unusable")
			assert.False(t, deleted)
			assert.Empty(t, world.statusAttempts(),
				"and it is refused BEFORE any request: a guard that fires on the response has "+
					"already sent the bridge somewhere a stranger chose")
		})
	}
}

// TestAccountStatus_APlainHTTPPDSEndpointIsRefusedBeforeAnyRequest is the first
// of the three transport rules, and the simplest.
//
// A cleartext confirmation is a confirmation anyone on the path can WRITE. The
// answer decides whether a user's content is erased everywhere, so an attacker
// who can inject one response gets the irreversible verdict for free — no
// credentials, no compromise of either endpoint.
func TestAccountStatus_APlainHTTPPDSEndpointIsRefusedBeforeAnyRequest(t *testing.T) {
	world := newPDSWorld(t)
	world.endpoint = "http://" + asPDSHost
	world.body[asPDSHost] = `{"active":false,"status":"deleted"}`

	deleted, err := world.resolver(t).AccountStatus(context.Background(), asDID)

	assert.False(t, deleted,
		"a cleartext answer must never produce the deleted verdict: whoever is on the path "+
			"writes it, and Delete{Person, removeData:true} is not recallable")
	require.Error(t, err,
		"and it is an ERROR, not a quiet 'live': the confirmation did not happen, so the "+
			"event must come back rather than be consumed")
	assert.Empty(t, world.statusAttempts(),
		"refused before the first packet — a request already sent has already leaked which "+
			"DID this bridge is about to act on")
}

// TestAccountStatus_AnHTTPSToHTTPRedirectCannotProduceAVerdict is the same rule
// one hop later, and the hop is where it is usually lost: the endpoint is
// https, the policy looks satisfied, and the redirect hands the conversation to
// cleartext anyway.
func TestAccountStatus_AnHTTPSToHTTPRedirectCannotProduceAVerdict(t *testing.T) {
	world := newPDSWorld(t)
	world.endpoint = "https://" + asPDSHost
	world.redirect[asPDSHost] = "http://" + asPDSHost + asStatusPath + "?did=" + asDID
	world.body[asPDSHost] = `{"active":false,"status":"deleted"}`

	deleted, err := world.resolver(t).AccountStatus(context.Background(), asDID)

	assert.False(t, deleted,
		"a downgrade must not be able to answer: the guarantee an https endpoint bought is "+
			"gone the moment the client follows a redirect out of it, and what it bought was "+
			"the only thing standing between an on-path attacker and an irreversible erasure")
	require.Error(t, err, "and the failed confirmation stays an error")

	for _, attempt := range world.statusAttempts() {
		assert.Equal(t, "https", attempt.scheme,
			"no request in this confirmation may go out over cleartext, redirect or not")
	}
}

// TestAccountStatus_ARedirectOffThePDSAuthorityCannotProduceAVerdict closes the
// last route: the scheme stays https and the AUTHORITY changes.
//
// The DID document names one PDS. That naming is the entire authorization for
// the answer — it is what makes the response evidence about THIS repo rather
// than an opinion from a stranger. A redirect that leaves it means the bridge
// asked the peer the user chose and believed a peer somebody else chose.
func TestAccountStatus_ARedirectOffThePDSAuthorityCannotProduceAVerdict(t *testing.T) {
	world := newPDSWorld(t)
	world.endpoint = "https://" + asPDSHost
	world.redirect[asPDSHost] = "https://" + asOtherPDSHost + asStatusPath + "?did=" + asDID
	// The host the redirect points at is delighted to confirm the deletion.
	world.body[asOtherPDSHost] = `{"active":false,"status":"deleted"}`

	deleted, err := world.resolver(t).AccountStatus(context.Background(), asDID)

	assert.False(t, deleted,
		"a host the DID document never named must not be able to condemn the repo: the "+
			"redirect is written by the very server we are asking, so 'it told us to' is "+
			"exactly as trustworthy as the answer we are trying to verify")
	require.Error(t, err, "an answer from the wrong authority is a failed confirmation")

	for _, attempt := range world.statusAttempts() {
		assert.Equal(t, asPDSHost, attempt.host,
			"and the confirmation never left the named PDS's authority at all")
	}
}

// ---------------------------------------------------------------------------
// The PDS's answer: only something we fully read and understood decides
// ---------------------------------------------------------------------------

func TestAccountStatus_UnreadablePDSAnswersAreErrorsNeverVerdicts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"500", http.StatusInternalServerError, `{"active":false,"status":"deleted"}`},
		{"400 on an unrecognised repo", http.StatusBadRequest, `{"error":"RepoNotFound"}`},
		{"404", http.StatusNotFound, ``},
		{"empty body", http.StatusOK, ``},
		{"not JSON", http.StatusOK, `<html>hello</html>`},
		{"JSON that is not an object", http.StatusOK, `["deleted"]`},
		{
			"a body past the read cap",
			http.StatusOK,
			`{"padding":"` + strings.Repeat("x", maxRepoStatusBytes) + `","active":false,"status":"deleted"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newPDSWorld(t)
			if tc.status != http.StatusOK {
				world.status[asPDSHost] = tc.status
			}
			world.body[asPDSHost] = tc.body

			deleted, err := world.resolver(t).AccountStatus(context.Background(), asDID)
			require.Error(t, err,
				"an answer we could not read in full is not evidence of anything: a "+
					"misconfigured PDS, a proxy error page and a truncated body all look "+
					"alike, and none of them is a user deleting their account")
			assert.False(t, deleted)
		})
	}
}

// ---------------------------------------------------------------------------
// The egress guard, which is the WIRING half of the same rule
// ---------------------------------------------------------------------------

// TestAccountStatus_APrivatePDSEndpointIsRefusedByTheProductionEgressGuard
// pins the premise the tests above are allowed to relax.
//
// Everything else in this file rewrites the fixture's hosts onto a loopback
// listener, which is exactly what production must NOT do: the endpoint comes
// from a document a stranger controls, so a `serviceEndpoint` of
// http://169.254.169.254 or http://127.0.0.1:5432 would otherwise turn this
// confirmation into an SSRF probe of the bridge's own network — with the reply
// body deciding whether a user is erased.
//
// So this one wires the client production wires, and only the PDS leg goes
// through it: the directory is served locally so that a refusal here is
// attributable to the ENDPOINT rather than to the fixture being local.
func TestAccountStatus_APrivatePDSEndpointIsRefusedByTheProductionEgressGuard(t *testing.T) {
	world := newPDSWorld(t)
	// The endpoint the "DID document" names is the loopback listener itself,
	// which is the shape of every SSRF pivot — named over HTTPS, so that the
	// refusal is attributable to the ADDRESS and not to a scheme check one layer
	// up. A loopback endpoint spelled http:// would be refused for the wrong
	// reason and this test would advertise coverage it does not have.
	world.endpoint = "https://" + world.server.Listener.Addr().String()
	world.body[hostOnly(world.server.Listener.Addr().String())] = `{"active":false,"status":"deleted"}`

	resolver, err := NewHandleResolver(ResolverOptions{
		PLCDirectoryURL: world.server.URL,
		HTTPClient: &http.Client{Transport: &asSplitTransport{
			directoryHost: hostOnly(world.server.Listener.Addr().String()),
			directory:     &asRecordingTransport{world: world},
			// The production egress: ap.NewGuardedHTTPClient(false, …), whose
			// transport refuses loopback and RFC1918 at DIAL time.
			guarded: ap.NewGuardedHTTPClient(false, 5*time.Second).Transport,
		}},
		UserAgent: "tidepool-test/0.1",
		LookupTXT: func(context.Context, string) ([]string, error) { return nil, nil },
	})
	require.NoError(t, err)

	deleted, err := resolver.AccountStatus(context.Background(), asDID)
	require.Error(t, err,
		"a private or loopback PDS endpoint must not be fetched: the bridge would be "+
			"reading its own internal network on a stranger's instructions, and whatever "+
			"answered would decide whether that stranger's account is erased")
	assert.False(t, deleted, "and above all it must not produce the deleted verdict")
}

// asSplitTransport serves the DID document locally and sends everything else —
// the PDS leg, which is the one under test — through the production guard.
type asSplitTransport struct {
	directoryHost string
	directory     http.RoundTripper
	guarded       http.RoundTripper
}

func (rt *asSplitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !strings.HasPrefix(req.URL.Path, asStatusPath) {
		return rt.directory.RoundTrip(req)
	}
	return rt.guarded.RoundTrip(req)
}
