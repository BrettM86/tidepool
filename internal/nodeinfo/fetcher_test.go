package nodeinfo_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/nodeinfo"
)

const (
	testUserAgent = "tidepool-test/1.0 (+https://tidepool.test)"
	testHost      = "instance.test"

	rel20 = "http://nodeinfo.diaspora.software/ns/schema/2.0"
	rel21 = "http://nodeinfo.diaspora.software/ns/schema/2.1"
	rel10 = "http://nodeinfo.diaspora.software/ns/schema/1.0"
)

// oversizePadding is far above any sane per-document cap (the design names
// 1 MiB) while keeping the fixture cheap to serve.
var oversizePadding = strings.Repeat("p", 8<<20)

// originalSchemeHeader carries the scheme a request was made with across
// rewriteTransport's rewrite to plain HTTP, so the fixture can record it.
const originalSchemeHeader = "X-Test-Original-Scheme"

// rewriteTransport routes every request, whatever its host, to the local
// fixture server over plain HTTP, keeping the original host in r.Host and
// the original scheme in originalSchemeHeader. Its base is the round tripper
// that actually dials, so a guarded client keeps its dial-time egress guard
// underneath the rewrite.
type rewriteTransport struct {
	base   http.RoundTripper
	target string
}

func (rt rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Host = req.URL.Host
	clone.Header.Set(originalSchemeHeader, req.URL.Scheme)
	clone.URL.Scheme = "http"
	clone.URL.Host = rt.target
	return rt.base.RoundTrip(clone)
}

// seenRequest is one request the fixture server received.
type seenRequest struct {
	Scheme    string
	Host      string
	Path      string
	UserAgent string
}

// fixtureDocument is one response the fixture instance serves at a path.
type fixtureDocument struct {
	status int // 0 = 200
	body   string
}

// fixtureInstance is a local httptest stand-in for a fediverse instance. It
// serves fixed documents by path and records every request it receives,
// including ones for paths it has no document for (answered 404).
type fixtureInstance struct {
	server *httptest.Server
	mu     sync.Mutex
	seen   []seenRequest
}

func newFixtureInstance(t *testing.T, docs map[string]fixtureDocument) *fixtureInstance {
	t.Helper()
	f := &fixtureInstance{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.seen = append(f.seen, seenRequest{Scheme: r.Header.Get(originalSchemeHeader), Host: r.Host, Path: r.URL.Path, UserAgent: r.UserAgent()})
		f.mu.Unlock()
		doc, ok := docs[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if doc.status != 0 {
			w.WriteHeader(doc.status)
		}
		_, _ = w.Write([]byte(doc.body))
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fixtureInstance) requests() []seenRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seenRequest{}, f.seen...)
}

func (f *fixtureInstance) addr() string { return f.server.Listener.Addr().String() }

// testClient is an unguarded client that can only reach the fixture server.
func (f *fixtureInstance) testClient() *http.Client {
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: rewriteTransport{base: &http.Transport{}, target: f.addr()},
	}
}

// wellKnown renders a /.well-known/nodeinfo listing with the given
// (rel, href) pairs in order.
func wellKnown(links ...[2]string) string {
	var b strings.Builder
	b.WriteString(`{"links":[`)
	for i, l := range links {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"rel":"` + l[0] + `","href":"` + l[1] + `"}`)
	}
	b.WriteString(`]}`)
	return b.String()
}

// nodeinfoDoc renders a nodeinfo document whose software.name is the raw
// JSON value given (already quoted, or omitted when empty).
func nodeinfoDoc(version, rawName string) string {
	software := `{"version":"0.19.5"}`
	if rawName != "" {
		software = `{"name":` + rawName + `,"version":"0.19.5"}`
	}
	return `{"version":"` + version + `","software":` + software +
		`,"protocols":["activitypub"],"usage":{"users":{}},"openRegistrations":true}`
}

func TestFetcherSoftwareName(t *testing.T) {
	link20 := [2]string{rel20, "https://" + testHost + "/nodeinfo/2.0"}
	link10 := [2]string{rel10, "https://" + testHost + "/nodeinfo/1.0"}
	wellKnownPath := "/.well-known/nodeinfo"
	// request is one request the fixture should see from the fetcher.
	request := func(scheme, path string) seenRequest {
		return seenRequest{Scheme: scheme, Host: testHost, Path: path, UserAgent: testUserAgent}
	}

	tests := []struct {
		name string
		// scheme is the instance's scheme passed to the fetcher ("" = https).
		scheme string
		docs   map[string]fixtureDocument
		// want is the expected software name; "" means no value and an error.
		want string
		// wantRequests, when set, are exactly the requests the fixture must
		// see. Unset on a success row means the https well-known request
		// then the https 2.0 document request.
		wantRequests []seenRequest
	}{
		{
			name: "2.0 link, Lemmy is lowercased",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link20)},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"Lemmy"`)},
			},
			want: "lemmy",
		},
		{
			name: "padded PieFed is trimmed and lowercased",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link20)},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"  PieFed "`)},
			},
			want: "piefed",
		},
		{
			name: "64-byte ASCII name accepted verbatim",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link20)},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"`+strings.Repeat("x", 64)+`"`)},
			},
			want: strings.Repeat("x", 64),
		},
		{
			name: "1.0 link listed before 2.0 link follows 2.0",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link10, link20)},
				"/nodeinfo/1.0": {body: nodeinfoDoc("1.0", `"wrong"`)},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"Lemmy"`)},
			},
			want: "lemmy",
		},
		{
			name:   "http instance follows an http link",
			scheme: "http",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown([2]string{rel20, "http://" + testHost + "/nodeinfo/2.0"})},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"Lemmy"`)},
			},
			want:         "lemmy",
			wantRequests: []seenRequest{request("http", wellKnownPath), request("http", "/nodeinfo/2.0")},
		},
		{
			name:   "http instance follows an https link",
			scheme: "http",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link20)},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"Lemmy"`)},
			},
			want:         "lemmy",
			wantRequests: []seenRequest{request("http", wellKnownPath), request("https", "/nodeinfo/2.0")},
		},
		{
			name: "https instance refuses an http link (downgrade)",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown([2]string{rel20, "http://" + testHost + "/nodeinfo/2.0"})},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"Lemmy"`)},
			},
			wantRequests: []seenRequest{request("https", wellKnownPath)},
		},
		{
			name: "scheme-relative link refused",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown([2]string{rel20, "//" + testHost + "/nodeinfo/2.0"})},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"Lemmy"`)},
			},
			wantRequests: []seenRequest{request("https", wellKnownPath)},
		},
		{
			name:   "unsupported instance scheme makes no request",
			scheme: "ftp",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link20)},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"Lemmy"`)},
			},
			wantRequests: []seenRequest{},
		},
		{
			name: "well-known 404",
			docs: map[string]fixtureDocument{
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"Lemmy"`)},
			},
		},
		{
			name: "well-known malformed JSON",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: `{"links":[{"rel":"` + rel20 + `","href":"https://` + testHost + `/nodeinfo/2.0"`},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"Lemmy"`)},
			},
		},
		{
			name: "well-known lists only a 1.0 link",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link10)},
				"/nodeinfo/1.0": {body: nodeinfoDoc("1.0", `"wrong"`)},
			},
		},
		{
			name: "linked document 500",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link20)},
				"/nodeinfo/2.0": {status: http.StatusInternalServerError, body: nodeinfoDoc("2.0", `"Lemmy"`)},
			},
		},
		{
			name: "linked document without software.name",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link20)},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", "")},
			},
		},
		{
			name: "empty name",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link20)},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `""`)},
			},
		},
		{
			name: "whitespace-only name",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link20)},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"   \t "`)},
			},
		},
		{
			name: "65-byte ASCII name",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link20)},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"`+strings.Repeat("x", 65)+`"`)},
			},
		},
		{
			// 33 characters, 66 UTF-8 bytes.
			name: "multibyte name under 64 characters but over 64 bytes",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link20)},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"`+strings.Repeat("é", 33)+`"`)},
			},
		},
		{
			name: "name with a control character",
			docs: map[string]fixtureDocument{
				wellKnownPath:   {body: wellKnown(link20)},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"lem\u0007my"`)},
			},
		},
		{
			name: "linked document larger than the size cap",
			docs: map[string]fixtureDocument{
				wellKnownPath: {body: wellKnown(link20)},
				"/nodeinfo/2.0": {body: `{"version":"2.0","software":{"name":"Lemmy","version":"0.19.5"},` +
					`"protocols":["activitypub"],"padding":"` + oversizePadding + `"}`},
			},
		},
		{
			name: "well-known document larger than the size cap",
			docs: map[string]fixtureDocument{
				wellKnownPath: {body: `{"links":[{"rel":"` + rel20 + `","href":"https://` + testHost +
					`/nodeinfo/2.0"}],"padding":"` + oversizePadding + `"}`},
				"/nodeinfo/2.0": {body: nodeinfoDoc("2.0", `"Lemmy"`)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := newFixtureInstance(t, tt.docs)
			fetcher, err := nodeinfo.NewFetcher(instance.testClient(), testUserAgent)
			require.NoError(t, err)

			scheme := tt.scheme
			if scheme == "" {
				scheme = "https"
			}
			got, err := fetcher.SoftwareName(context.Background(), scheme, testHost)

			if tt.wantRequests != nil {
				assert.Equal(t, tt.wantRequests, instance.requests())
			}
			if tt.want == "" {
				require.Error(t, err)
				assert.Equal(t, "", got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			if tt.wantRequests == nil {
				assert.Equal(t, []seenRequest{
					request("https", wellKnownPath),
					request("https", "/nodeinfo/2.0"),
				}, instance.requests())
			}
		})
	}
}

func TestFetcherSoftwareNameEgressGuard(t *testing.T) {
	tests := []struct {
		name         string
		allowPrivate bool
		want         string // "" means no value and an error
		wantRequests int
	}{
		{name: "guard refuses the loopback instance", allowPrivate: false, want: "", wantRequests: 0},
		{name: "control: private addresses allowed", allowPrivate: true, want: "lemmy", wantRequests: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := newFixtureInstance(t, map[string]fixtureDocument{
				"/.well-known/nodeinfo": {body: wellKnown([2]string{rel20, "https://" + testHost + "/nodeinfo/2.0"})},
				"/nodeinfo/2.0":         {body: nodeinfoDoc("2.0", `"Lemmy"`)},
			})
			client := ap.NewGuardedHTTPClient(tt.allowPrivate, 10*time.Second)
			// Keep the guarded transport underneath: only the destination is
			// rewritten, the dial still goes through the egress guard.
			client.Transport = rewriteTransport{base: client.Transport, target: instance.addr()}

			fetcher, err := nodeinfo.NewFetcher(client, testUserAgent)
			require.NoError(t, err)

			got, err := fetcher.SoftwareName(context.Background(), "https", testHost)

			if tt.want == "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
			assert.Len(t, instance.requests(), tt.wantRequests)
		})
	}
}
