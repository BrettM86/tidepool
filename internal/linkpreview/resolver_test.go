package linkpreview_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/linkpreview"
)

const testUserAgent = "tidepool-test/1.0"

// seenRequest is one request as the fixture server received it. Host and
// Scheme are the original host and scheme the resolver addressed (preserved
// by rewriteTransport; Scheme is empty for requests that bypass it).
type seenRequest struct {
	Scheme   string
	Host     string
	Path     string
	RawQuery string
	Query    map[string][]string
}

// fixtureServer is an httptest server that logs every request it receives.
type fixtureServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []seenRequest
}

func newFixtureServer(t *testing.T, handler http.HandlerFunc) *fixtureServer {
	t.Helper()
	fs := &fixtureServer{}
	fs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fs.mu.Lock()
		fs.requests = append(fs.requests, seenRequest{
			Scheme:   r.Header.Get(originalSchemeHeader),
			Host:     r.Host,
			Path:     r.URL.Path,
			RawQuery: r.URL.RawQuery,
			Query:    r.URL.Query(),
		})
		fs.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(fs.Close)
	return fs
}

func (fs *fixtureServer) seen() []seenRequest {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]seenRequest(nil), fs.requests...)
}

func (fs *fixtureServer) oembedRequests() []seenRequest {
	var out []seenRequest
	for _, r := range fs.seen() {
		if r.Path == "/oembed" {
			out = append(out, r)
		}
	}
	return out
}

// originalSchemeHeader carries the scheme the resolver addressed through
// rewriteTransport, which downgrades every request to the plain-http fixture.
const originalSchemeHeader = "X-Test-Original-Scheme"

// rewriteTransport routes every outbound request, whatever its host, to the
// local fixture server, keeping the original host in the Host header (and
// the original scheme in originalSchemeHeader) so handlers can dispatch on
// it. Modeled on internal/materialize's harness.
type rewriteTransport struct{ target string }

func (rt rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Set(originalSchemeHeader, req.URL.Scheme)
	clone.Host = req.URL.Host
	clone.URL.Scheme = "http"
	clone.URL.Host = rt.target
	return http.DefaultTransport.RoundTrip(clone)
}

func rewriteClient(fs *fixtureServer) *http.Client {
	return &http.Client{Transport: rewriteTransport{target: fs.Listener.Addr().String()}}
}

// countingTransport wraps each response body so the test can observe how many
// body bytes the resolver actually consumed.
type countingTransport struct {
	base http.RoundTripper
	read *atomic.Int64
}

func (ct countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := ct.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &countingBody{ReadCloser: resp.Body, read: ct.read}
	return resp, nil
}

type countingBody struct {
	io.ReadCloser
	read *atomic.Int64
}

func (cb *countingBody) Read(p []byte) (int, error) {
	n, err := cb.ReadCloser.Read(p)
	cb.read.Add(int64(n))
	return n, err
}

func ogPage(imageContent string) string {
	return `<!doctype html><html><head><title>A page</title>` +
		`<meta property="og:title" content="A page">` +
		`<meta property="og:image" content="` + imageContent + `">` +
		`</head><body><p>hello</p></body></html>`
}

func serveHTML(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, body)
}

func serveJSON(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, body)
}

// B1: YouTube links use oEmbed with the exact link; look-alikes do not.
func TestThumbnailURL_YouTubeUsesOEmbed(t *testing.T) {
	links := []string{
		"https://www.youtube.com/watch?v=abc",
		"https://youtube.com/watch?v=abc",
		"https://m.youtube.com/watch?v=abc",
		"https://music.youtube.com/watch?v=abc",
		"https://youtu.be/abc",
		"https://www.youtube.com/shorts/abc",
		"https://WWW.YouTube.com/watch?v=abc", // host match is case-insensitive
	}
	for _, link := range links {
		t.Run(link, func(t *testing.T) {
			fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Host == "www.youtube.com" && r.URL.Path == "/oembed" {
					serveJSON(w, `{"type":"video","title":"A video","thumbnail_url":"https://i.ytimg.com/vi/abc/hqdefault.jpg","thumbnail_width":480,"thumbnail_height":360}`)
					return
				}
				serveHTML(w, ogPage("https://img.example/not-oembed.jpg"))
			})
			resolver := linkpreview.NewResolver(rewriteClient(fs), testUserAgent)

			thumb, err := resolver.ThumbnailURL(context.Background(), link)

			require.NoError(t, err)
			assert.Equal(t, "https://i.ytimg.com/vi/abc/hqdefault.jpg", thumb)
			oembed := fs.oembedRequests()
			require.Len(t, oembed, 1, "exactly one oEmbed request")
			assert.Equal(t, "https", oembed[0].Scheme, "oEmbed is requested over https")
			assert.Equal(t, "www.youtube.com", oembed[0].Host)
			assert.Equal(t, []string{link}, oembed[0].Query["url"])
			assert.Equal(t, []string{"json"}, oembed[0].Query["format"])
		})
	}
}

func TestThumbnailURL_YouTubeLookAlikesUseOpenGraph(t *testing.T) {
	links := []string{
		"https://notyoutube.com/watch?v=abc",
		"https://youtube.com.evil.example/watch?v=abc",
	}
	for _, link := range links {
		t.Run(link, func(t *testing.T) {
			fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/oembed" {
					serveJSON(w, `{"thumbnail_url":"https://i.ytimg.com/vi/abc/hqdefault.jpg"}`)
					return
				}
				serveHTML(w, ogPage("https://img.example/lookalike.jpg"))
			})
			resolver := linkpreview.NewResolver(rewriteClient(fs), testUserAgent)

			thumb, err := resolver.ThumbnailURL(context.Background(), link)

			require.NoError(t, err)
			assert.Equal(t, "https://img.example/lookalike.jpg", thumb)
			assert.Empty(t, fs.oembedRequests(), "look-alike host must not use oEmbed")
		})
	}
}

// B2: OpenGraph og:image extraction and resolution.
func TestThumbnailURL_OpenGraphAbsoluteImage(t *testing.T) {
	fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		serveHTML(w, ogPage("https://cdn.example/og.jpg"))
	})
	resolver := linkpreview.NewResolver(&http.Client{}, testUserAgent)

	thumb, err := resolver.ThumbnailURL(context.Background(), fs.URL+"/article")

	require.NoError(t, err)
	assert.Equal(t, "https://cdn.example/og.jpg", thumb)
}

func TestThumbnailURL_OpenGraphNameAttribute(t *testing.T) {
	fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		serveHTML(w, `<html><head><meta name="og:image" content="https://cdn.example/by-name.jpg"></head><body></body></html>`)
	})
	resolver := linkpreview.NewResolver(&http.Client{}, testUserAgent)

	thumb, err := resolver.ThumbnailURL(context.Background(), fs.URL+"/article")

	require.NoError(t, err)
	assert.Equal(t, "https://cdn.example/by-name.jpg", thumb)
}

func TestThumbnailURL_OpenGraphRelativeResolvedAgainstFinalURL(t *testing.T) {
	cases := []struct {
		name    string
		ogImage string
		path    string // expected path on the fixture server
	}{
		{name: "root-relative", ogImage: "/img/a.jpg", path: "/img/a.jpg"},
		{name: "document-relative", ogImage: "a.jpg", path: "/posts/2026/a.jpg"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/start":
					http.Redirect(w, r, "/posts/2026/page.html", http.StatusFound)
				case "/posts/2026/page.html":
					serveHTML(w, ogPage(tc.ogImage))
				default:
					http.NotFound(w, r)
				}
			})
			resolver := linkpreview.NewResolver(&http.Client{}, testUserAgent)

			thumb, err := resolver.ThumbnailURL(context.Background(), fs.URL+"/start")

			require.NoError(t, err)
			assert.Equal(t, fs.URL+tc.path, thumb)
		})
	}
}

func TestThumbnailURL_OpenGraphNoUsableImage(t *testing.T) {
	cases := []struct {
		name      string
		page      string
		wantError bool
	}{
		{name: "javascript scheme", page: ogPage("javascript:alert(1)"), wantError: true},
		{name: "data scheme", page: ogPage("data:image/png;base64,iVBORw0KGgo="), wantError: true},
		{name: "opaque https without host", page: ogPage("https:foo"), wantError: true},
		{name: "http with empty host", page: ogPage("http:///x"), wantError: true},
		{name: "https with port-only authority", page: ogPage("https://:443/thumb.jpg"), wantError: true},
		{name: "no og:image", page: `<html><head><title>x</title><meta property="og:title" content="x"></head><body></body></html>`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				serveHTML(w, tc.page)
			})
			resolver := linkpreview.NewResolver(&http.Client{}, testUserAgent)

			thumb, err := resolver.ThumbnailURL(context.Background(), fs.URL+"/article")

			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, "", thumb)
		})
	}
}

// Nothing to preview is not an error: a 2xx page that is not HTML (or says
// nothing about its type), and an oEmbed endpoint that refuses the video
// (private, removed, embedding disabled), return ("", nil).
func TestThumbnailURL_NothingToPreview(t *testing.T) {
	const youTubeLink = "https://www.youtube.com/watch?v=abc"
	const pageLink = "https://example.com/article"
	serveAs := func(contentType string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", contentType)
			_, _ = io.WriteString(w, ogPage("https://cdn.example/og.jpg"))
		}
	}
	oEmbedStatus := func(status int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/oembed" {
				serveHTML(w, ogPage("https://img.example/not-oembed.jpg"))
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, http.StatusText(status))
		}
	}
	cases := []struct {
		name    string
		link    string
		handler http.HandlerFunc
	}{
		{name: "image/jpeg page", link: pageLink, handler: serveAs("image/jpeg")},
		{name: "application/pdf page", link: pageLink, handler: serveAs("application/pdf")},
		{name: "application/json page", link: pageLink, handler: serveAs("application/json")},
		{name: "page without content type", link: pageLink, handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header()["Content-Type"] = nil // suppress net/http's sniffed default
			_, _ = io.WriteString(w, ogPage("https://cdn.example/og.jpg"))
		}},
		{name: "oEmbed 401", link: youTubeLink, handler: oEmbedStatus(http.StatusUnauthorized)},
		{name: "oEmbed 403", link: youTubeLink, handler: oEmbedStatus(http.StatusForbidden)},
		{name: "oEmbed 404", link: youTubeLink, handler: oEmbedStatus(http.StatusNotFound)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFixtureServer(t, tc.handler)
			resolver := linkpreview.NewResolver(rewriteClient(fs), testUserAgent)

			thumb, err := resolver.ThumbnailURL(context.Background(), tc.link)

			require.NoError(t, err)
			assert.Equal(t, "", thumb)
		})
	}
}

// B3: failure classes.
func TestThumbnailURL_PageFailuresReturnError(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{name: "non-2xx", handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, ogPage("https://cdn.example/og.jpg"))
		}},
		{name: "5xx", handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, ogPage("https://cdn.example/og.jpg"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFixtureServer(t, tc.handler)
			resolver := linkpreview.NewResolver(&http.Client{}, testUserAgent)

			thumb, err := resolver.ThumbnailURL(context.Background(), fs.URL+"/article")

			require.Error(t, err)
			assert.Equal(t, "", thumb)
		})
	}
}

func TestThumbnailURL_OEmbedFailures(t *testing.T) {
	cases := []struct {
		name      string
		status    int // 0 means 200
		body      string
		wantError bool
	}{
		{name: "oEmbed 500", status: http.StatusInternalServerError, body: `{"thumbnail_url":"https://i.ytimg.com/vi/abc/hqdefault.jpg"}`, wantError: true},
		{name: "malformed json", body: `{"thumbnail_url": "https://i.ytimg.com/vi/abc/hq`, wantError: true},
		{name: "thumbnail_url javascript scheme", body: `{"thumbnail_url":"javascript:x"}`, wantError: true},
		{name: "thumbnail_url relative", body: `{"thumbnail_url":"/relative.jpg"}`, wantError: true},
		{name: "thumbnail_url ftp scheme", body: `{"thumbnail_url":"ftp://x/y.jpg"}`, wantError: true},
		{name: "thumbnail_url opaque https without host", body: `{"thumbnail_url":"https:foo"}`, wantError: true},
		{name: "thumbnail_url https with port-only authority", body: `{"thumbnail_url":"https://:443/thumb.jpg"}`, wantError: true},
		{name: "missing thumbnail_url", body: `{"type":"video","title":"A video"}`, wantError: false},
		{
			name:      "body over 64 KiB",
			body:      `{"thumbnail_url":"https://i.ytimg.com/vi/abc/hqdefault.jpg","pad":"` + strings.Repeat("a", 70*1024) + `"}`,
			wantError: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if tc.status != 0 {
					w.WriteHeader(tc.status)
				}
				_, _ = io.WriteString(w, tc.body)
			})
			resolver := linkpreview.NewResolver(rewriteClient(fs), testUserAgent)

			thumb, err := resolver.ThumbnailURL(context.Background(), "https://www.youtube.com/watch?v=abc")

			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, "", thumb)
			require.Len(t, fs.oembedRequests(), 1, "oEmbed endpoint was contacted")
		})
	}
}

func TestThumbnailURL_LargeHTMLReadIsBounded(t *testing.T) {
	head := `<!doctype html><html><head><meta property="og:image" content="https://cdn.example/big.jpg"></head><body>`
	padding := strings.Repeat("<p>filler filler filler filler filler filler filler</p>\n", (5<<20)/56)
	fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		serveHTML(w, head+padding+`</body></html>`)
	})
	var read atomic.Int64
	client := &http.Client{Transport: countingTransport{base: http.DefaultTransport, read: &read}}
	resolver := linkpreview.NewResolver(client, testUserAgent)

	thumb, err := resolver.ThumbnailURL(context.Background(), fs.URL+"/huge")

	require.NoError(t, err)
	assert.Equal(t, "https://cdn.example/big.jpg", thumb)
	assert.Positive(t, read.Load(), "resolver read the body")
	assert.LessOrEqual(t, read.Load(), int64(1<<20+64<<10), "resolver read at most the 1 MiB cap plus slack")
}

// TestThumbnailURL_OGImageBeyondReadCapIsNotFound keeps the parser from
// stopping early: the head carries ~3 MiB of filler meta tags before the
// og:image and ~2 MiB after it, so only the 1 MiB read cap prevents the
// resolver from reaching (and returning) the late og:image.
func TestThumbnailURL_OGImageBeyondReadCapIsNotFound(t *testing.T) {
	filler := `<meta name="filler" content="filler filler filler filler filler">` + "\n"
	page := `<!doctype html><html><head>` +
		strings.Repeat(filler, (3<<20)/len(filler)) +
		`<meta property="og:image" content="https://cdn.example/late.jpg">` +
		strings.Repeat(filler, (2<<20)/len(filler)) +
		`</head><body></body></html>`
	fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		serveHTML(w, page)
	})
	var read atomic.Int64
	client := &http.Client{Transport: countingTransport{base: http.DefaultTransport, read: &read}}
	resolver := linkpreview.NewResolver(client, testUserAgent)

	thumb, err := resolver.ThumbnailURL(context.Background(), fs.URL+"/late-og")

	require.NoError(t, err)
	assert.Equal(t, "", thumb)
	assert.Positive(t, read.Load(), "resolver read the body")
	assert.LessOrEqual(t, read.Load(), int64(1<<20+64<<10), "resolver read at most the 1 MiB cap plus slack")
}

func TestThumbnailURL_StalledResponseHonorsContextDeadline(t *testing.T) {
	release := make(chan struct{})
	fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		<-release
	})
	t.Cleanup(func() { close(release) }) // runs before fs.Close (LIFO)
	resolver := linkpreview.NewResolver(&http.Client{}, testUserAgent)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	thumb, err := resolver.ThumbnailURL(ctx, fs.URL+"/stall")
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Equal(t, "", thumb)
	assert.Less(t, elapsed, 2*time.Second)
}

// A body read that fails before og:image is found (here the context deadline
// hits mid-body) is an error, not "page has no image".
func TestThumbnailURL_MidBodyReadFailureReturnsError(t *testing.T) {
	release := make(chan struct{})
	fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, `<!doctype html><html><head><title>A page</title>`)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(release) }) // runs before fs.Close (LIFO)
	resolver := linkpreview.NewResolver(&http.Client{}, testUserAgent)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	thumb, err := resolver.ThumbnailURL(ctx, fs.URL+"/partial")

	require.Error(t, err)
	assert.Equal(t, "", thumb)
}

func TestThumbnailURL_NonHTTPLinkRejectedWithoutRequest(t *testing.T) {
	fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		serveHTML(w, ogPage("https://cdn.example/og.jpg"))
	})
	resolver := linkpreview.NewResolver(rewriteClient(fs), testUserAgent)

	thumb, err := resolver.ThumbnailURL(context.Background(), "ftp://example.com/x")

	require.Error(t, err)
	assert.Equal(t, "", thumb)
	assert.Empty(t, fs.seen(), "no request for a non-http(s) link")
}

// B4: the SSRF-guarded client refuses a loopback page.
func TestThumbnailURL_GuardedClientRefusesLoopback(t *testing.T) {
	fs := newFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		serveHTML(w, ogPage("https://cdn.example/og.jpg"))
	})
	require.True(t, strings.HasPrefix(fs.URL, "http://127.0.0.1:"), "fixture must be on loopback")
	resolver := linkpreview.NewResolver(ap.NewGuardedHTTPClient(false, 5*time.Second), testUserAgent)

	thumb, err := resolver.ThumbnailURL(context.Background(), fs.URL+"/article")

	require.Error(t, err)
	assert.Equal(t, "", thumb)
	assert.Empty(t, fs.seen(), "guarded client must not reach a loopback server")
}
