package ap

import (
	"context"
	"crypto/tls"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// newTestClient builds a client with instant retries pointed at a test
// server, signing with the shared test key.
func newTestClient(t *testing.T, opts ClientOptions) *Client {
	t.Helper()
	if opts.Signer == nil {
		opts.Signer = NewSigner(testKeyID, testRSAKey(t))
	}
	if opts.UserAgent == "" {
		opts.UserAgent = "tidepool-test/0"
	}
	// Tests hit httptest servers on 127.0.0.1; relax the SSRF egress guard the
	// way local dev does (config.AllowPrivateAddresses). Egress-guard behavior
	// itself is covered by the dedicated tests in egress_test.go.
	opts.AllowPrivateAddresses = true
	c := NewClient(opts)
	c.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return c
}

func TestFetchObject_SignedGET(t *testing.T) {
	key := testRSAKey(t)
	verifier := NewVerifier(staticResolver(&key.PublicKey))

	var sawRequest *http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRequest = r.Clone(context.Background())
		w.Header().Set("Content-Type", ContentTypeActivityJSON)
		_, _ = w.Write(loadFixture(t, "page_lemmy_world.json"))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{})
	obj, err := client.FetchObject(context.Background(), server.URL+"/post/49131386")
	require.NoError(t, err)
	assert.Equal(t, TypePage, obj.Type)
	assert.Equal(t, "https://lemmy.world/post/49131386", obj.ID)

	require.NotNil(t, sawRequest)
	// Pinned to the bare type, not a compound list: at least one Lemmy
	// deployment's proxy (startrek.website) exact-matches Accept and serves
	// its HTML frontend to anything else. See acceptActivityJSON.
	assert.Equal(t, ContentTypeActivityJSON, sawRequest.Header.Get("Accept"))
	assert.Equal(t, "tidepool-test/0", sawRequest.Header.Get("User-Agent"))

	// The GET must carry a signature Lemmy would accept: verify it
	// server-side, empty body.
	_, err = verifier.Verify(context.Background(), sawRequest, nil)
	require.NoError(t, err, "signed GET must verify against the signer's public key")
	fields := parseSignatureHeader(sawRequest.Header.Get("Signature"))
	assert.Equal(t, "(request-target) host date digest", fields["headers"])
}

// A 200 whose body is the instance's HTML frontend (content negotiation
// missed) must be reported as such — naming the media type — rather than as
// the opaque "payload is not a JSON object". Conversely, valid JSON served
// under a wrong Content-Type must still parse: the check is a diagnostic,
// not a strictness gate.
func TestFetchObject_NonJSONResponseNamesContentType(t *testing.T) {
	t.Run("html with 200 is diagnosed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte("\n    <!DOCTYPE html>\n    <html lang=\"en\"><head></head><body>spa shell</body></html>"))
		}))
		defer server.Close()

		client := newTestClient(t, ClientOptions{})
		_, err := client.FetchObject(context.Background(), server.URL+"/c/startrek")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "text/html")
		assert.Contains(t, err.Error(), "not JSON")
		assert.NotContains(t, err.Error(), "payload is not a JSON object")
	})

	t.Run("json under a non-json content-type still parses", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write(loadFixture(t, "page_lemmy_world.json"))
		}))
		defer server.Close()

		client := newTestClient(t, ClientOptions{})
		obj, err := client.FetchObject(context.Background(), server.URL+"/post/49131386")
		require.NoError(t, err)
		assert.Equal(t, TypePage, obj.Type)
	})

	t.Run("json content-type with a non-json body still reaches the parser", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", ContentTypeActivityJSON)
			_, _ = w.Write([]byte(`"just a string"`))
		}))
		defer server.Close()

		client := newTestClient(t, ClientOptions{})
		_, err := client.FetchObject(context.Background(), server.URL+"/x")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "payload is not a JSON object")
	})
}

func TestFetchObject_StatusMapping(t *testing.T) {
	cases := []struct {
		status int
		check  func(t *testing.T, err error)
	}{
		{http.StatusNotFound, func(t *testing.T, err error) {
			assert.True(t, errors.IsNotFound(err), "404 → IsNotFound, got %v", err)
			assert.False(t, errors.IsTombstoned(err))
		}},
		{http.StatusGone, func(t *testing.T, err error) {
			assert.True(t, errors.IsTombstoned(err), "410 → IsTombstoned, got %v", err)
			assert.False(t, errors.IsNotFound(err), "tombstoned must NOT satisfy IsNotFound")
		}},
		{http.StatusForbidden, func(t *testing.T, err error) {
			assert.True(t, errors.IsNotFound(err), "403 (authorized fetch) → IsNotFound, got %v", err)
		}},
		{http.StatusUnauthorized, func(t *testing.T, err error) {
			assert.True(t, errors.IsNotFound(err), "401 → IsNotFound, got %v", err)
		}},
		{http.StatusTeapot, func(t *testing.T, err error) {
			var httpErr HTTPError
			require.ErrorAs(t, err, &httpErr)
			assert.Equal(t, http.StatusTeapot, httpErr.StatusCode)
		}},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer server.Close()

			client := newTestClient(t, ClientOptions{})
			_, err := client.FetchObject(context.Background(), server.URL+"/x")
			require.Error(t, err)
			tc.check(t, err)
		})
	}
}

func TestFetchObject_TombstoneBodyIsTombstoned(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"type":"Tombstone","id":"https://x.example/post/1","formerType":"Page"}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{})
	_, err := client.FetchObject(context.Background(), server.URL+"/post/1")
	require.Error(t, err)
	assert.True(t, errors.IsTombstoned(err),
		"a 200 Tombstone body must surface tombstone semantics, got %v", err)
}

func TestFetchObject_RetriesTransientFailures(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(`{"type":"Note","id":"https://x.example/1"}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{MaxAttempts: 3})
	obj, err := client.FetchObject(context.Background(), server.URL+"/1")
	require.NoError(t, err)
	assert.Equal(t, TypeNote, obj.Type)
	assert.Equal(t, int32(3), hits.Load())
}

func TestFetchObject_DoesNotRetryHardFailures(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{MaxAttempts: 3})
	_, err := client.FetchObject(context.Background(), server.URL+"/1")
	require.Error(t, err)
	assert.Equal(t, int32(1), hits.Load(), "404 must not be retried")
}

func TestFetchObject_ResponseSizeCap(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"type":"Note","content":"` + strings.Repeat("x", 4096) + `"}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{MaxResponseBytes: 1024})
	_, err := client.FetchObject(context.Background(), server.URL+"/big")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cap")
}

func TestFetchObject_RejectsBadURLs(t *testing.T) {
	client := newTestClient(t, ClientOptions{})
	for _, iri := range []string{"ftp://example.com/x", "not-a-url", "https:///nohost"} {
		_, err := client.FetchObject(context.Background(), iri)
		assert.Error(t, err, "iri %q", iri)
	}
}

func TestFetchObject_ContextCancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := newTestClient(t, ClientOptions{})
	_, err := client.FetchObject(ctx, server.URL+"/x")
	require.ErrorIs(t, err, context.Canceled)
}

func TestFetchObject_FollowsRedirectsWithFreshSignatures(t *testing.T) {
	key := testRSAKey(t)
	verifier := NewVerifier(staticResolver(&key.PublicKey))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/old", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/new", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/new", func(w http.ResponseWriter, r *http.Request) {
		// The redirected request must carry a signature valid for /new —
		// a naively replayed signature would still say (request-target) /old.
		if _, err := verifier.Verify(context.Background(), r, nil); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"type":"Note","id":"https://x.example/new"}`))
	})

	client := newTestClient(t, ClientOptions{})
	obj, err := client.FetchObject(context.Background(), server.URL+"/old")
	require.NoError(t, err)
	assert.Equal(t, "https://x.example/new", obj.ID)
}

// TestFetchObjectSameAuthority_NoRedirects: with NoRedirects set, even a
// same-host redirect is refused before its target is requested; without it
// the same-host hop is followed.
func TestFetchObjectSameAuthority_NoRedirects(t *testing.T) {
	tests := []struct {
		name            string
		noRedirects     bool
		wantRedirectErr bool
		wantTargetHits  int64
	}{
		{name: "redirects refused", noRedirects: true, wantRedirectErr: true, wantTargetHits: 0},
		{name: "redirects followed", noRedirects: false, wantRedirectErr: false, wantTargetHits: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var targetHits atomic.Int64
			mux := http.NewServeMux()
			server := httptest.NewServer(mux)
			defer server.Close()
			mux.HandleFunc("/activities/like/1", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "/files/payload.json", http.StatusFound)
			})
			mux.HandleFunc("/files/payload.json", func(w http.ResponseWriter, r *http.Request) {
				targetHits.Add(1)
				_, _ = w.Write([]byte(`{"type":"Like","id":"https://x.example/activities/like/1"}`))
			})

			client := newTestClient(t, ClientOptions{NoRedirects: tt.noRedirects})
			obj, err := client.FetchObjectSameAuthority(context.Background(), server.URL+"/activities/like/1")
			if tt.wantRedirectErr {
				require.Error(t, err)
				assert.True(t, errors.IsValidation(err), "want a validation error, got %v", err)
				var validation errors.ValidationError
				require.True(t, stderrors.As(err, &validation), "want an errors.ValidationError, got %T", err)
				assert.Equal(t, "redirect", validation.Field)
				assert.Nil(t, obj)
			} else {
				require.NoError(t, err)
				assert.Equal(t, "https://x.example/activities/like/1", obj.ID)
			}
			assert.Equal(t, tt.wantTargetHits, targetHits.Load(), "redirect target requests")
		})
	}
}

// RefusesRedirects reports the client's NoRedirects option (the vote origin
// verifier refuses a client that follows redirects).
func TestClientRefusesRedirectsReportsNoRedirects(t *testing.T) {
	tests := []struct {
		name        string
		noRedirects bool
		want        bool
	}{
		{name: "NoRedirects set", noRedirects: true, want: true},
		{name: "NoRedirects unset", noRedirects: false, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestClient(t, ClientOptions{NoRedirects: tt.noRedirects})
			assert.Equal(t, tt.want, client.RefusesRedirects())
		})
	}
}

// scriptedTransport is a RoundTripper answering from a script instead of the
// network: no dial ever happens, so egress-guard-ON behavior can be tested
// against fake public hostnames (guard-on clients cannot hit 127.0.0.1
// httptest servers). Every request URL is recorded.
type scriptedTransport struct {
	handler func(req *http.Request) (*http.Response, error)

	mu       sync.Mutex
	requests []string
}

func (s *scriptedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	s.requests = append(s.requests, req.URL.String())
	s.mu.Unlock()
	return s.handler(req)
}

// seen returns the URLs of all requests issued so far.
func (s *scriptedTransport) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

// scriptedResponse builds a minimal *http.Response for scriptedTransport.
func scriptedResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// TestRedirect_DowngradeRejectedWhenGuardOn: with the egress guard ON
// (production posture) an https fetch that 302s to an http:// URL must fail
// with the downgrade validation error BEFORE any plaintext request is
// issued — for both the object path (getOnce) and the media path
// (fetchMediaOnce).
func TestRedirect_DowngradeRejectedWhenGuardOn(t *testing.T) {
	newGuardedClient := func(transport *scriptedTransport) *Client {
		c := NewClient(ClientOptions{
			UserAgent:   "tidepool-test/0",
			HTTPClient:  &http.Client{Transport: transport},
			MaxAttempts: 1,
			// AllowPrivateAddresses deliberately false: guard on.
		})
		c.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
		return c
	}
	newDowngradeTransport := func() *scriptedTransport {
		return &scriptedTransport{handler: func(req *http.Request) (*http.Response, error) {
			if req.URL.Scheme != "https" {
				t.Errorf("plaintext request issued to %s", req.URL)
				return nil, fmt.Errorf("unexpected plaintext request")
			}
			resp := scriptedResponse(http.StatusFound, "")
			resp.Header.Set("Location", "http://remote.test/object")
			return resp, nil
		}}
	}

	t.Run("object", func(t *testing.T) {
		transport := newDowngradeTransport()
		_, _, err := newGuardedClient(transport).getOnce(
			context.Background(), "https://remote.test/object", fetchModeObject)
		require.Error(t, err)
		assert.True(t, errors.IsValidation(err), "downgrade must be a typed validation error, got %v", err)
		assert.Contains(t, err.Error(), "downgrades https to http")
		require.Len(t, transport.seen(), 1, "the http hop must never be requested")
		assert.True(t, strings.HasPrefix(transport.seen()[0], "https://"))
	})

	t.Run("media", func(t *testing.T) {
		transport := newDowngradeTransport()
		_, _, _, err := newGuardedClient(transport).fetchMediaOnce(
			context.Background(), "https://remote.test/img.png", 1<<20)
		require.Error(t, err)
		assert.True(t, errors.IsValidation(err), "downgrade must be a typed validation error, got %v", err)
		assert.Contains(t, err.Error(), "downgrades https to http")
		require.Len(t, transport.seen(), 1, "the http hop must never be requested")
		assert.True(t, strings.HasPrefix(transport.seen()[0], "https://"))
	})
}

// TestRedirect_DowngradeFollowedWhenGuardRelaxed: under the dev/e2e
// relaxation (AllowPrivateAddresses, ALLOW_PRIVATE_FETCH) plain-HTTP peers
// are expected, so an https→http redirect IS followed.
func TestRedirect_DowngradeFollowedWhenGuardRelaxed(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"type":"Note","id":"https://x.example/downgraded"}`))
	}))
	defer httpServer.Close()

	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, httpServer.URL+"/object", http.StatusFound)
	}))
	defer tlsServer.Close()

	client := newTestClient(t, ClientOptions{HTTPClient: tlsServer.Client()})
	obj, err := client.FetchObject(context.Background(), tlsServer.URL+"/old")
	require.NoError(t, err)
	assert.Equal(t, "https://x.example/downgraded", obj.ID,
		"with the guard relaxed the http redirect target must be fetched")
}

func TestFetchActor_RejectsNonActors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(loadFixture(t, "page_lemmy_world.json"))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{})
	_, err := client.FetchActor(context.Background(), server.URL+"/post/1")
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

func TestFetchActor_Group(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(loadFixture(t, "group_lemmy_world.json"))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{})
	actor, err := client.FetchActor(context.Background(), server.URL+"/c/technology")
	require.NoError(t, err)
	assert.Equal(t, TypeGroup, actor.Type)
}

func TestFetchCollection_InlineItems(t *testing.T) {
	// Lemmy outboxes: one OrderedCollection, all items inline, no paging.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(loadFixture(t, "outbox_lemmy_world.json"))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{})
	var types []string
	err := client.FetchCollection(context.Background(), server.URL+"/outbox", func(item *Object) error {
		types = append(types, item.Type)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{TypeAnnounce, TypeAnnounce}, types)
}

func TestFetchCollection_PagedWithCap(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	const totalPages = 5
	mux.HandleFunc("/collection", func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		if page == "" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "OrderedCollection", "id": server.URL + "/collection",
				"totalItems": totalPages, "first": server.URL + "/collection?page=1",
			})
			return
		}
		n := int(page[0] - '0')
		doc := map[string]any{
			"type": "OrderedCollectionPage",
			"id":   server.URL + "/collection?page=" + page,
			"orderedItems": []map[string]any{
				{"type": "Note", "id": fmt.Sprintf("https://x.example/%d", n)},
			},
		}
		if n < totalPages {
			doc["next"] = fmt.Sprintf("%s/collection?page=%d", server.URL, n+1)
		}
		_ = json.NewEncoder(w).Encode(doc)
	})

	// Uncapped walk sees every page.
	client := newTestClient(t, ClientOptions{})
	var ids []string
	err := client.FetchCollection(context.Background(), server.URL+"/collection", func(item *Object) error {
		ids = append(ids, item.ID)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, ids, totalPages)

	// Capped walk stops at MaxCollectionPages pages (header + first page +
	// one next = 3 fetches, 2 item-bearing pages) and reports truncation so
	// the caller can resume rather than mistaking it for a complete walk.
	client = newTestClient(t, ClientOptions{MaxCollectionPages: 3})
	ids = nil
	err = client.FetchCollection(context.Background(), server.URL+"/collection", func(item *Object) error {
		ids = append(ids, item.ID)
		return nil
	})
	require.ErrorIs(t, err, ErrCollectionTruncated, "page cap must signal truncation, not clean completion")
	var truncated *CollectionTruncatedError
	require.ErrorAs(t, err, &truncated)
	assert.Equal(t, 3, truncated.Pages)
	assert.NotEmpty(t, truncated.Next, "truncation must carry the resume pointer")
	assert.Len(t, ids, 2, "page cap must stop the walk early")

	// ErrStop halts silently.
	client = newTestClient(t, ClientOptions{})
	ids = nil
	err = client.FetchCollection(context.Background(), server.URL+"/collection", func(item *Object) error {
		ids = append(ids, item.ID)
		return ErrStop
	})
	require.NoError(t, err)
	assert.Len(t, ids, 1)

	// A visit error propagates.
	client = newTestClient(t, ClientOptions{})
	wantErr := fmt.Errorf("translation exploded")
	err = client.FetchCollection(context.Background(), server.URL+"/collection", func(item *Object) error {
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
}

func TestFetchCollection_NextLoopTerminates(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/collection", func(w http.ResponseWriter, _ *http.Request) {
		// Malicious/broken server: next points back at itself.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "OrderedCollectionPage", "id": server.URL + "/collection",
			"orderedItems": []map[string]any{{"type": "Note", "id": "https://x.example/1"}},
			"next":         server.URL + "/collection",
		})
	})

	client := newTestClient(t, ClientOptions{})
	count := 0
	err := client.FetchCollection(context.Background(), server.URL+"/collection", func(*Object) error {
		count++
		return nil
	})
	require.ErrorIs(t, err, ErrCollectionTruncated,
		"a next-pointer loop is truncation, not a clean end-of-collection")
	assert.Equal(t, 1, count, "a next-pointer loop must terminate after one visit")
}

// TestFetchCollection_NonPageNextRejected: a next pointer that resolves to
// something that is not a collection page (a Note, an actor, an HTML error
// page served with 200) must be a hard error, not a silent clean finish that
// would make backfill think it saw the whole collection.
func TestFetchCollection_NonPageNextRejected(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/collection", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "OrderedCollectionPage", "id": server.URL + "/collection",
			"orderedItems": []map[string]any{{"type": "Note", "id": "https://x.example/1"}},
			"next":         server.URL + "/notapage",
		})
	})
	mux.HandleFunc("/notapage", func(w http.ResponseWriter, _ *http.Request) {
		// A 200 that is a Note, not a collection page.
		_ = json.NewEncoder(w).Encode(map[string]any{"type": "Note", "id": "https://x.example/2"})
	})

	client := newTestClient(t, ClientOptions{})
	err := client.FetchCollection(context.Background(), server.URL+"/collection", func(*Object) error { return nil })
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err),
		"a next pointer resolving to a non-collection must be a validation error, not a clean finish")
}

func TestFetchCollection_RejectsNonCollections(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(loadFixture(t, "person_lemmy_world.json"))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{})
	err := client.FetchCollection(context.Background(), server.URL+"/u/x", func(*Object) error { return nil })
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

func TestGetDeduped_CollapsesConcurrentFetches(t *testing.T) {
	var hits atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		<-release
		_, _ = w.Write([]byte(`{"type":"Note","id":"https://x.example/1"}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{})
	const workers = 8
	var wg sync.WaitGroup
	objErrs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, objErrs[i] = client.FetchObject(context.Background(), server.URL+"/1")
		}(i)
	}
	// Give every worker time to join the in-flight group, then release.
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, err := range objErrs {
		require.NoError(t, err, "worker %d", i)
	}
	assert.Equal(t, int32(1), hits.Load(), "concurrent fetches of one IRI must hit the server once")
}

// TestGetDeduped_InitiatorCancelDoesNotPoisonWaiters proves the shared fetch
// runs on a context detached from the initiating caller: if caller A (whose
// ctx started the request) cancels, caller B — deduped onto the same in-flight
// request — must still succeed, not inherit A's cancellation (finding 4).
func TestGetDeduped_InitiatorCancelDoesNotPoisonWaiters(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		_, _ = w.Write([]byte(`{"type":"Note","id":"https://x.example/1"}`))
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{})
	iri := server.URL + "/1"

	ctxA, cancelA := context.WithCancel(context.Background())
	var aErr, bErr error
	var aObj, bObj *Object
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		aObj, aErr = client.FetchObject(ctxA, iri) // initiator
	}()
	// Wait until A's request is actually in flight (server handler entered) so
	// B joins the same singleflight call.
	<-entered
	wg.Add(1)
	go func() {
		defer wg.Done()
		bObj, bErr = client.FetchObject(context.Background(), iri) // waiter
	}()
	// Give B a moment to join the in-flight group, then cancel the initiator.
	time.Sleep(50 * time.Millisecond)
	cancelA()
	// Let A observe cancellation before the shared request completes.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	require.ErrorIs(t, aErr, context.Canceled, "the cancelling initiator stops waiting")
	require.NoError(t, bErr, "the waiter must not be poisoned by the initiator's cancel")
	require.NotNil(t, bObj)
	assert.Equal(t, "https://x.example/1", bObj.ID)
	_ = aObj
}

func TestPerHostRateLimiting(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"type":"Note","id":"https://x.example/1"}`))
	}))
	defer server.Close()

	// Burst 1 at 20 rps: the second request must wait ~50ms.
	client := newTestClient(t, ClientOptions{PerHostRPS: 20, PerHostBurst: 1})
	ctx := context.Background()

	start := time.Now()
	_, err := client.FetchObject(ctx, server.URL+"/a")
	require.NoError(t, err)
	_, err = client.FetchObject(ctx, server.URL+"/b")
	require.NoError(t, err)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond,
		"second request to the same host must be rate limited")
}

func TestSendActivity_SignedPOST(t *testing.T) {
	key := testRSAKey(t)
	verifier := NewVerifier(staticResolver(&key.PublicKey))

	var receivedBody []byte
	var verifyErr error
	var sawContentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		receivedBody = body
		sawContentType = r.Header.Get("Content-Type")
		_, verifyErr = verifier.Verify(context.Background(), r, body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{})
	follow := map[string]any{
		"@context": "https://www.w3.org/ns/activitystreams",
		"type":     TypeFollow,
		"id":       "https://bridge.example/activities/follow/1",
		"actor":    testActorID,
		"object":   "https://lemmy.world/c/technology",
	}
	require.NoError(t, client.SendActivity(context.Background(), server.URL+"/inbox", follow))

	require.NoError(t, verifyErr, "inbox POST must carry a valid signature over the body")
	assert.Equal(t, ContentTypeActivityJSON, sawContentType)
	assert.Contains(t, string(receivedBody), `"Follow"`)
}

func TestSendActivity_RetriesAndFails(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{MaxAttempts: 2})
	err := client.SendActivity(context.Background(), server.URL+"/inbox", map[string]any{"type": "Follow"})
	require.Error(t, err)
	assert.Equal(t, int32(2), hits.Load())

	var httpErr HTTPError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, http.StatusServiceUnavailable, httpErr.StatusCode)
}

// A peer's rejection reason is in the response body (Lemmy answers a refused
// Follow with a 400 and a JSON message), so the error must carry it and show
// it in its message, where the callers' logs pick it up.
func TestSendActivity_RejectionCarriesBody(t *testing.T) {
	const rejection = `{"error":"unknown","message":"Incoming activity has invalid digest for body"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, rejection)
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{MaxAttempts: 2})
	err := client.SendActivity(context.Background(), server.URL+"/inbox", map[string]any{"type": "Follow"})
	require.Error(t, err)

	var httpErr HTTPError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, http.StatusBadRequest, httpErr.StatusCode)
	assert.Equal(t, rejection, httpErr.Body)
	assert.Equal(t, "ap: POST "+server.URL+"/inbox: unexpected status 400: "+rejection, err.Error())
}

// A long body is cut at httpErrorBodyLimit without splitting a rune.
func TestHTTPError_TruncatesLongBody(t *testing.T) {
	body := strings.Repeat("a", httpErrorBodyLimit-1) + "é" + strings.Repeat("b", 100)
	err := HTTPError{URL: "https://peer.example/inbox", StatusCode: 400, Body: body, Method: http.MethodPost}
	assert.Equal(t, "ap: POST https://peer.example/inbox: unexpected status 400: "+
		strings.Repeat("a", httpErrorBodyLimit-1)+"…", err.Error())
	assert.Equal(t, "ap: GET https://peer.example/x: unexpected status 404",
		HTTPError{URL: "https://peer.example/x", StatusCode: 404, Body: "  "}.Error())
}

func TestSendActivity_RequiresSigner(t *testing.T) {
	client := NewClient(ClientOptions{})
	err := client.SendActivity(context.Background(), "https://lemmy.world/inbox", map[string]any{})
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

// Requests made as a Coves user (SendActivityAs) identify as Coves; every
// other request from the same client identifies as the bridge. Sending as a
// user first proves the user agent does not stick to the client afterwards.
func TestClient_UserAgentPerRequestOrigin(t *testing.T) {
	var mu sync.Mutex
	userAgentByPath := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		userAgentByPath[r.URL.Path] = r.Header.Get("User-Agent")
		mu.Unlock()
		switch r.URL.Path {
		case "/user-inbox", "/bridge-inbox":
			w.WriteHeader(http.StatusAccepted)
		case "/post/49131386":
			w.Header().Set("Content-Type", ContentTypeActivityJSON)
			_, _ = w.Write(loadFixture(t, "page_lemmy_world.json"))
		case "/avatar.png":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write([]byte("\x89PNG\r\n\x1a\nimage-bytes"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newTestClient(t, ClientOptions{
		UserAgent:           "bridge-agent/1",
		UserOriginUserAgent: "user-agent/1",
	})
	userAgentFor := func(path string) string {
		mu.Lock()
		defer mu.Unlock()
		return userAgentByPath[path]
	}
	activity := map[string]any{"type": TypeFollow, "actor": testActorID}

	userSigner := NewSigner(testKeyID, testRSAKey(t))
	require.NoError(t, client.SendActivityAs(context.Background(), userSigner, server.URL+"/user-inbox", activity))
	assert.Equal(t, "user-agent/1", userAgentFor("/user-inbox"),
		"a delivery signed as a Coves user carries the Coves user agent")

	require.NoError(t, client.SendActivity(context.Background(), server.URL+"/bridge-inbox", activity))
	assert.Equal(t, "bridge-agent/1", userAgentFor("/bridge-inbox"),
		"a bridge-signed delivery carries the bridge user agent")

	_, err := client.FetchObject(context.Background(), server.URL+"/post/49131386")
	require.NoError(t, err)
	assert.Equal(t, "bridge-agent/1", userAgentFor("/post/49131386"),
		"a signed GET carries the bridge user agent")

	_, _, err = client.FetchMedia(context.Background(), server.URL+"/avatar.png", 0)
	require.NoError(t, err)
	assert.Equal(t, "bridge-agent/1", userAgentFor("/avatar.png"),
		"a media fetch carries the bridge user agent")
}

func TestResolveKey_FetchesAndCaches(t *testing.T) {
	// Serve an actor document that publishes our test public key.
	key := testRSAKey(t)
	publicPEM, err := EncodePublicKeyPEM(&key.PublicKey)
	require.NoError(t, err)

	var hits atomic.Int32
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()
	actorID := server.URL + "/u/alice"
	keyID := actorID + "#main-key"
	mux.HandleFunc("/u/alice", func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "Person", "id": actorID, "preferredUsername": "alice",
			"publicKey": map[string]string{
				"id": keyID, "owner": actorID, "publicKeyPem": string(publicPEM),
			},
		})
	})

	client := newTestClient(t, ClientOptions{})
	resolved, ownerID, err := client.ResolveKey(context.Background(), keyID)
	require.NoError(t, err)
	assert.True(t, key.PublicKey.Equal(resolved))
	assert.Equal(t, actorID, ownerID)

	// Second resolve is served from cache.
	_, _, err = client.ResolveKey(context.Background(), keyID)
	require.NoError(t, err)
	assert.Equal(t, int32(1), hits.Load(), "resolved keys must be cached")

	// Expired cache re-fetches.
	client.now = func() time.Time { return time.Now().Add(2 * DefaultKeyCacheTTL) }
	_, _, err = client.ResolveKey(context.Background(), keyID)
	require.NoError(t, err)
	assert.Equal(t, int32(2), hits.Load(), "expired cache entries must be re-fetched")
}

// TestVerify_RefetchesRotatedKey proves the key-rotation recovery path
// (finding 5): a key is cached, the remote rotates it, and a signature made
// with the NEW key initially fails against the stale cached key — but the
// Verifier's one-shot ResolveKeyFresh retry re-fetches and verifies. Without
// it the actor's deliveries would blackhole for the full cache TTL.
func TestVerify_RefetchesRotatedKey(t *testing.T) {
	oldKey := testRSAKey(t)
	newKey, err := GenerateRSAKey()
	require.NoError(t, err)

	oldPEM, err := EncodePublicKeyPEM(&oldKey.PublicKey)
	require.NoError(t, err)
	newPEM, err := EncodePublicKeyPEM(&newKey.PublicKey)
	require.NoError(t, err)

	var (
		mu         sync.Mutex
		servedPEM  = string(oldPEM)
		fetchCount int
	)
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()
	actorID := server.URL + "/u/rotator"
	keyID := actorID + "#main-key"
	mux.HandleFunc("/u/rotator", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		pem := servedPEM
		fetchCount++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "Person", "id": actorID, "preferredUsername": "rotator",
			"publicKey": map[string]string{"id": keyID, "owner": actorID, "publicKeyPem": pem},
		})
	})

	client := newTestClient(t, ClientOptions{})
	// Warm the cache with the OLD key.
	_, _, err = client.ResolveKey(context.Background(), keyID)
	require.NoError(t, err)

	// Remote rotates to the NEW key.
	mu.Lock()
	servedPEM = string(newPEM)
	mu.Unlock()

	// A request signed with the new key arrives.
	signer := NewSigner(keyID, newKey)
	req := httptest.NewRequest(http.MethodGet, actorID, nil)
	require.NoError(t, signer.SignRequest(req, nil))

	verifier := NewVerifier(client)
	ownerID, err := verifier.Verify(context.Background(), req, nil)
	require.NoError(t, err, "verification must recover by re-fetching the rotated key")
	assert.Equal(t, actorID, ownerID)

	mu.Lock()
	assert.GreaterOrEqual(t, fetchCount, 2, "the stale cached key must trigger exactly one refetch")
	mu.Unlock()
}

// TestKeyCacheEviction proves the resolved-key cache is bounded (finding 8):
// an attacker minting unique keyIds cannot grow it without limit. Expired
// entries are swept first; if still at the cap the nearest-to-expiry entry is
// evicted.
func TestKeyCacheEviction(t *testing.T) {
	c := newTestClient(t, ClientOptions{})
	base := time.Now()
	c.now = func() time.Time { return base }
	c.maxKeyCacheEntries = 4

	// Insert past the cap through the same locked path resolveKeyUncached uses.
	for i := 0; i < 20; i++ {
		c.mu.Lock()
		c.evictKeyCacheLocked()
		c.keyCache[fmt.Sprintf("https://h%d.example/u/x#main-key", i)] =
			cachedKey{expiresAt: base.Add(time.Hour)}
		c.mu.Unlock()
	}
	c.mu.Lock()
	size := len(c.keyCache)
	c.mu.Unlock()
	assert.LessOrEqual(t, size, c.maxKeyCacheEntries, "key cache must stay within its cap")

	// Expired entries are swept when the cache is under pressure.
	c.mu.Lock()
	c.keyCache = map[string]cachedKey{
		"expired1": {expiresAt: base.Add(-time.Hour)},
		"expired2": {expiresAt: base.Add(-time.Minute)},
		"live1":    {expiresAt: base.Add(time.Hour)},
		"live2":    {expiresAt: base.Add(time.Hour)},
	}
	c.evictKeyCacheLocked() // len==cap(4) triggers the expired sweep
	_, e1 := c.keyCache["expired1"]
	_, e2 := c.keyCache["expired2"]
	_, l1 := c.keyCache["live1"]
	c.mu.Unlock()
	assert.False(t, e1, "expired entries must be swept")
	assert.False(t, e2)
	assert.True(t, l1, "live entries survive the sweep")
}

// TestLimiterEviction proves the per-host limiter map is bounded (finding 8).
func TestLimiterEviction(t *testing.T) {
	c := newTestClient(t, ClientOptions{})
	c.maxLimiters = 4
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		require.NoError(t, c.waitForHost(ctx, fmt.Sprintf("https://h%d.example/x", i)))
	}
	c.mu.Lock()
	size := len(c.limiters)
	c.mu.Unlock()
	assert.LessOrEqual(t, size, c.maxLimiters, "limiter map must stay within its cap")
}

func TestResolveKey_RejectsKeyIDMismatch(t *testing.T) {
	key := testRSAKey(t)
	publicPEM, err := EncodePublicKeyPEM(&key.PublicKey)
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()
	actorID := server.URL + "/u/mallory"
	mux.HandleFunc("/u/mallory", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "Person", "id": actorID,
			"publicKey": map[string]string{
				"id":           actorID + "#other-key",
				"owner":        actorID,
				"publicKeyPem": string(publicPEM),
			},
		})
	})

	client := newTestClient(t, ClientOptions{})
	_, _, err = client.ResolveKey(context.Background(), actorID+"#main-key")
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err),
		"an actor publishing a different keyId must not satisfy the requested one")
}

// TestResolveKey_RejectsCrossHostIDSpoof is the core actor-identity-binding
// test: a malicious host serves an actor document whose self-asserted id
// belongs to a DIFFERENT authority (lemmy.world) while signing with a keyId on
// its own host. ResolveKey must refuse to attribute the key to lemmy.world's
// actor, and must not cache the spoof as a positive result.
func TestResolveKey_RejectsCrossHostIDSpoof(t *testing.T) {
	key := testRSAKey(t)
	publicPEM, err := EncodePublicKeyPEM(&key.PublicKey)
	require.NoError(t, err)

	var hits atomic.Int32
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()
	keyID := server.URL + "/spoof#main-key"
	spoofedID := "https://lemmy.world/u/alice"
	mux.HandleFunc("/spoof", func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			// Claims to be lemmy.world's alice, but is served from the test host.
			"type": "Person", "id": spoofedID, "preferredUsername": "alice",
			"publicKey": map[string]string{
				"id": keyID, "owner": spoofedID, "publicKeyPem": string(publicPEM),
			},
		})
	})

	client := newTestClient(t, ClientOptions{})
	_, _, err = client.ResolveKey(context.Background(), keyID)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrInvalidInput,
		"a cross-authority id must be rejected (signature error)")

	// Must not have been cached as a positive key: a second resolve re-fetches.
	_, _, err = client.ResolveKey(context.Background(), keyID)
	require.Error(t, err)
	assert.Equal(t, int32(2), hits.Load(), "a rejected/spoofed resolution must not be cached")
}

// TestResolveKey_RejectsOwnerMismatch: the actor lives on the right authority
// and publishes the right keyId, but the key names a different owner than the
// actor that published it. Reject — otherwise a key could be laundered through
// an unrelated actor document.
func TestResolveKey_RejectsOwnerMismatch(t *testing.T) {
	key := testRSAKey(t)
	publicPEM, err := EncodePublicKeyPEM(&key.PublicKey)
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()
	actorID := server.URL + "/u/bob"
	keyID := actorID + "#main-key"
	mux.HandleFunc("/u/bob", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "Person", "id": actorID, "preferredUsername": "bob",
			"publicKey": map[string]string{
				"id": keyID, "owner": server.URL + "/u/eve", "publicKeyPem": string(publicPEM),
			},
		})
	})

	client := newTestClient(t, ClientOptions{})
	_, _, err = client.ResolveKey(context.Background(), keyID)
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err),
		"a key whose owner is not the publishing actor must be rejected")
}

// TestFetchObject_GoneHost pins which dead-host failures FetchObject reports
// as errors.IsHostGone (and not IsNotFound), so backfill can skip an item whose
// host is gone while live inbox callers keep retrying. All hosts are stubbed:
// URL hostnames are labels mapped to local httptest listeners (or to a forged
// DNS error) by the dialer below; no real DNS lookup is made.
func TestFetchObject_GoneHost(t *testing.T) {
	const (
		originHost    = "origin.example"
		elsewhereHost = "elsewhere.example"
		goneHost      = "gone-host.invalid"
		goneTarget    = "gone-target.invalid"
	)

	htmlHandler := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("<html><body>domain for sale</body></html>"))
	}
	noteHandler := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", ContentTypeActivityJSON)
		_, _ = w.Write(loadFixture(t, "note_lemmy_zip.json"))
	}

	type goneHostCase struct {
		name string
		// dnsError, when set, is what the dialer returns for dnsErrorHost.
		dnsError *net.DNSError
		// dnsErrorHost is the dialed host that gets dnsError; empty means
		// goneHost. It differs from dnsError.Name to model a proxy whose own
		// name failed to resolve.
		dnsErrorHost string
		// fetch is the entry point under test; nil means FetchObject.
		fetch func(*Client, context.Context, string) (*Object, error)
		// origin and elsewhere serve origin.example and elsewhere.example.
		origin    http.HandlerFunc
		elsewhere http.HandlerFunc
		iri       string
		// wantErr false means the fetch must succeed.
		wantErr      bool
		wantNotFound bool
		wantHostGone bool
		// wantGoneHost is what errors.HostGoneHost must return when
		// wantHostGone: the IRI's own (origin) host, never a redirect's
		// landing host.
		wantGoneHost string
		// wantDials, when positive, is how many times the dialer must have
		// been asked for dnsErrorHost (MaxAttempts is 2).
		wantDials int32
		// hosts serves further stub hosts (x.example, z.example) by name,
		// alongside origin and elsewhere.
		hosts map[string]http.HandlerFunc
		// tlsHost, when set, is served from hosts over TLS: tlsMode
		// "untrusted" presents the httptest certificate the client does not
		// trust; "certificate-error" fails GetCertificate so the server
		// answers the handshake with an alert.
		tlsHost string
		tlsMode string
		// dialErrors maps a dialed host to the error the dialer returns for
		// it; dialFailures, when positive, limits that to the host's first N
		// dials (later dials reach its listener).
		dialErrors   map[string]error
		dialFailures int32
		// clientTimeout sets the http.Client Timeout (zero: the default).
		clientTimeout time.Duration
		// wantReason is the errors.HostGoneReason code when wantHostGone.
		wantReason string
		// wantRequests maps a host to the requests its handler must have
		// received; wantDialsTo maps a host to the dials it must have seen.
		// Both prove the failing phase was reached.
		wantRequests map[string]int32
		wantDialsTo  map[string]int32
	}

	// edgeStatusRow: a CDN edge status from the IRI's own host (its origin
	// is down behind the edge) is edge_status, classified only after the
	// last attempt.
	edgeStatusRow := func(status int) goneHostCase {
		return goneHostCase{
			name: fmt.Sprintf("edge status %d from the iri host is edge_status", status),
			hosts: map[string]http.HandlerFunc{"x.example": func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantHostGone: true,
			wantGoneHost: "x.example",
			wantReason:   "edge_status",
			wantRequests: map[string]int32{"x.example": 2},
		}
	}
	// notCountedStatusRow: a status that says nothing about the host being
	// unreachable. 401/403 stay not found (AccessRefusedError).
	notCountedStatusRow := func(status int, wantNotFound bool) goneHostCase {
		return goneHostCase{
			name: fmt.Sprintf("status %d from the iri host is not host gone", status),
			hosts: map[string]http.HandlerFunc{"x.example": func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantNotFound: wantNotFound,
		}
	}

	tests := []goneHostCase{

		{
			name:         "dns nxdomain",
			dnsError:     &net.DNSError{Err: "no such host", Name: goneHost, IsNotFound: true},
			iri:          "http://" + goneHost + "/post/1",
			wantErr:      true,
			wantNotFound: false,
			wantHostGone: true,
			wantGoneHost: "gone-host.invalid",
			wantReason:   "dns_not_found",
			wantDials:    1,
		},
		{
			name:         "temporary dns failure stays retryable",
			dnsError:     &net.DNSError{Err: "server misbehaving", Name: goneHost, IsTemporary: true},
			iri:          "http://" + goneHost + "/post/1",
			wantErr:      true,
			wantNotFound: false,
			wantDials:    2,
		},
		{
			// A resolver timeout surfaces as a dial OpError whose Timeout()
			// is true, but no connection to the host was attempted: it is
			// not connect_timeout.
			name:         "dns lookup timeout at dial is not connect_timeout",
			dnsError:     &net.DNSError{Err: "i/o timeout", Name: goneHost, IsTimeout: true},
			iri:          "http://" + goneHost + "/post/1",
			wantErr:      true,
			wantNotFound: false,
			wantDials:    2,
		},
		{
			name: "off-host redirect to html",
			origin: func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "http://"+elsewhereHost+"/parked", http.StatusFound)
			},
			elsewhere:    htmlHandler,
			iri:          "http://" + originHost + "/post/1",
			wantErr:      true,
			wantNotFound: false,
			wantHostGone: true,
			wantGoneHost: "origin.example",
			wantReason:   "parked_redirect",
		},
		{
			name: "same-host redirect to html keeps ordinary error",
			origin: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/post/1" {
					http.Redirect(w, r, "/landing", http.StatusFound)
					return
				}
				htmlHandler(w, r)
			},
			iri:          "http://" + originHost + "/post/1",
			wantErr:      true,
			wantNotFound: false,
		},
		{
			name: "off-host redirect to activity json succeeds",
			origin: func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "http://"+elsewhereHost+"/comment/1", http.StatusFound)
			},
			elsewhere: noteHandler,
			iri:       "http://" + originHost + "/post/1",
			wantErr:   false,
		},
		{
			// The inbox self-delete confirmation (ingest.tombstonedSelfDelete)
			// must defer on a DNS failure rather than read it as "gone".
			name:         "pinned-authority actor fetch keeps nxdomain retryable",
			dnsError:     &net.DNSError{Err: "no such host", Name: goneHost, IsNotFound: true},
			fetch:        (*Client).FetchActorSameAuthority,
			iri:          "http://" + goneHost + "/u/alice",
			wantErr:      true,
			wantNotFound: false,
			wantDials:    2,
		},
		{
			// A proxy whose own name does not resolve says nothing about the
			// fetched host.
			name:         "nxdomain for a different host than the iri stays retryable",
			dnsError:     &net.DNSError{Err: "no such host", Name: "proxy.invalid", IsNotFound: true},
			dnsErrorHost: originHost,
			iri:          "http://" + originHost + "/post/1",
			wantErr:      true,
			wantNotFound: false,
			wantDials:    2,
		},
		{
			// NXDOMAIN counts only for the IRI's own host on the first hop: a
			// redirect target that fails to resolve stays an ordinary error.
			name:         "nxdomain of a redirect target stays an ordinary error",
			dnsError:     &net.DNSError{Err: "no such host", Name: goneTarget, IsNotFound: true},
			dnsErrorHost: goneTarget,
			origin: func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "http://"+goneTarget+"/x", http.StatusFound)
			},
			iri:          "http://" + originHost + "/post/1",
			wantErr:      true,
			wantNotFound: false,
			wantDials:    2,
		},
		{
			name:         "dns nxdomain through FetchActor",
			dnsError:     &net.DNSError{Err: "no such host", Name: goneHost, IsNotFound: true},
			fetch:        (*Client).FetchActor,
			iri:          "http://" + goneHost + "/u/alice",
			wantErr:      true,
			wantNotFound: false,
			wantHostGone: true,
			wantGoneHost: "gone-host.invalid",
			wantDials:    1,
		},
		{
			// The dead host is reported lowercased and without the port, the
			// form the instances table keys on.
			name:         "dns nxdomain of a mixed-case host with a port",
			dnsError:     &net.DNSError{Err: "no such host", Name: goneHost, IsNotFound: true},
			iri:          "http://Gone-Host.invalid:8080/post/1",
			wantErr:      true,
			wantNotFound: false,
			wantHostGone: true,
			wantGoneHost: "gone-host.invalid",
			wantDials:    1,
		},
		{
			name: "redirect dropping explicit default port is same host",
			origin: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/post/1" {
					http.Redirect(w, r, "http://"+originHost+"/landing", http.StatusFound)
					return
				}
				htmlHandler(w, r)
			},
			iri:          "http://" + originHost + ":80/post/1",
			wantErr:      true,
			wantNotFound: false,
		},
		// Counted unreachable-host classes: each is classified only after the
		// final attempt (MaxAttempts 2), names the IRI's own host, and carries
		// its reason code.
		{
			name:         "dial timeout to the iri host is connect_timeout",
			dialErrors:   map[string]error{"x.example": &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantHostGone: true,
			wantGoneHost: "x.example",
			wantReason:   "connect_timeout",
			wantDialsTo:  map[string]int32{"x.example": 2},
		},
		{
			name:         "connection refused by the iri host is connection_refused",
			dialErrors:   map[string]error{"x.example": &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantHostGone: true,
			wantGoneHost: "x.example",
			wantReason:   "connection_refused",
			wantDialsTo:  map[string]int32{"x.example": 2},
		},
		{
			name:         "connection reset at dial is connection_reset",
			dialErrors:   map[string]error{"x.example": &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNRESET}}},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantHostGone: true,
			wantGoneHost: "x.example",
			wantReason:   "connection_reset",
			wantDialsTo:  map[string]int32{"x.example": 2},
		},
		{
			name:         "host unreachable is no_route",
			dialErrors:   map[string]error{"x.example": &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.EHOSTUNREACH}}},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantHostGone: true,
			wantGoneHost: "x.example",
			wantReason:   "no_route",
			wantDialsTo:  map[string]int32{"x.example": 2},
		},
		{
			name:         "network unreachable is no_route",
			dialErrors:   map[string]error{"x.example": &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ENETUNREACH}}},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantHostGone: true,
			wantGoneHost: "x.example",
			wantReason:   "no_route",
			wantDialsTo:  map[string]int32{"x.example": 2},
		},
		{
			name:         "untrusted tls certificate is tls_failure",
			hosts:        map[string]http.HandlerFunc{"x.example": noteHandler},
			tlsHost:      "x.example",
			tlsMode:      "untrusted",
			iri:          "https://x.example/post/1",
			wantErr:      true,
			wantHostGone: true,
			wantGoneHost: "x.example",
			wantReason:   "tls_failure",
			wantDialsTo:  map[string]int32{"x.example": 2},
			wantRequests: map[string]int32{"x.example": 0},
		},
		{
			name:         "tls handshake alert is tls_failure",
			hosts:        map[string]http.HandlerFunc{"x.example": noteHandler},
			tlsHost:      "x.example",
			tlsMode:      "certificate-error",
			iri:          "https://x.example/post/1",
			wantErr:      true,
			wantHostGone: true,
			wantGoneHost: "x.example",
			wantReason:   "tls_failure",
			wantDialsTo:  map[string]int32{"x.example": 2},
			wantRequests: map[string]int32{"x.example": 0},
		},
		{
			name: "same-host redirect loop is too_many_redirects",
			hosts: map[string]http.HandlerFunc{"x.example": func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "/post/1", http.StatusFound)
			}},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantHostGone: true,
			wantGoneHost: "x.example",
			wantReason:   "too_many_redirects",
		},
		edgeStatusRow(521),
		edgeStatusRow(522),
		edgeStatusRow(523),
		edgeStatusRow(525),
		edgeStatusRow(526),
		edgeStatusRow(530),
		{
			name: "same-host redirect to the root html page is redirect_to_root",
			hosts: map[string]http.HandlerFunc{"x.example": func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/" {
					http.Redirect(w, r, "/", http.StatusFound)
					return
				}
				htmlHandler(w, r)
			}},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantHostGone: true,
			wantGoneHost: "x.example",
			wantReason:   "redirect_to_root",
		},
		{
			name:         "connection refused then served on the retry returns the object",
			hosts:        map[string]http.HandlerFunc{"x.example": noteHandler},
			dialErrors:   map[string]error{"x.example": &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}},
			dialFailures: 1,
			iri:          "http://x.example/post/1",
			wantErr:      false,
			wantDialsTo:  map[string]int32{"x.example": 2},
			wantRequests: map[string]int32{"x.example": 1},
		},
		// Not counted: transient or caller-side failures, and failures that
		// are not the IRI host's own.
		notCountedStatusRow(http.StatusBadGateway, false),
		notCountedStatusRow(http.StatusServiceUnavailable, false),
		notCountedStatusRow(http.StatusGatewayTimeout, false),
		notCountedStatusRow(520, false),
		notCountedStatusRow(524, false),
		notCountedStatusRow(http.StatusUnauthorized, true),
		notCountedStatusRow(http.StatusForbidden, true),
		notCountedStatusRow(http.StatusTooManyRequests, false),
		{
			name: "response header timeout after connect is not host gone",
			hosts: map[string]http.HandlerFunc{"x.example": func(_ http.ResponseWriter, r *http.Request) {
				<-r.Context().Done()
			}},
			clientTimeout: 100 * time.Millisecond,
			iri:           "http://x.example/post/1",
			wantErr:       true,
			wantRequests:  map[string]int32{"x.example": 2},
		},
		{
			name: "connection refused on an off-host redirect hop is not host gone",
			hosts: map[string]http.HandlerFunc{"x.example": func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "http://z.example/post/1", http.StatusFound)
			}},
			dialErrors:   map[string]error{"z.example": &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantDialsTo:  map[string]int32{"z.example": 2},
			wantRequests: map[string]int32{"x.example": 2},
		},
		{
			name: "edge status from an off-host redirect target is not host gone",
			hosts: map[string]http.HandlerFunc{
				"x.example": func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, "http://z.example/post/1", http.StatusFound)
				},
				"z.example": func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(522) },
			},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantRequests: map[string]int32{"x.example": 2, "z.example": 2},
		},
		{
			name: "root html reached through another host is not redirect_to_root",
			hosts: map[string]http.HandlerFunc{
				"x.example": func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/" {
						http.Redirect(w, r, "http://z.example/hop", http.StatusFound)
						return
					}
					htmlHandler(w, r)
				},
				"z.example": func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, "http://x.example/", http.StatusFound)
				},
			},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantRequests: map[string]int32{"x.example": 2, "z.example": 1},
		},
		{
			name: "redirect loop bouncing between two hosts is not too_many_redirects",
			hosts: map[string]http.HandlerFunc{
				"x.example": func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, "http://z.example/bounce", http.StatusFound)
				},
				"z.example": func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, "http://x.example/post/1", http.StatusFound)
				},
			},
			iri:          "http://x.example/post/1",
			wantErr:      true,
			wantRequests: map[string]int32{"x.example": 3, "z.example": 3},
		},
		{
			name: "503 then 200 returns the object",
			hosts: map[string]http.HandlerFunc{"x.example": func() http.HandlerFunc {
				var calls atomic.Int32
				return func(w http.ResponseWriter, r *http.Request) {
					if calls.Add(1) == 1 {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					noteHandler(w, r)
				}
			}()},
			iri:          "http://x.example/post/1",
			wantErr:      false,
			wantRequests: map[string]int32{"x.example": 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlers := map[string]http.HandlerFunc{originHost: tt.origin, elsewhereHost: tt.elsewhere}
			for host, handler := range tt.hosts {
				handlers[host] = handler
			}
			addrs := map[string]string{}
			var countsMu sync.Mutex
			requests := map[string]int32{}
			dials := map[string]int32{}
			for host, handler := range handlers {
				if handler == nil {
					continue
				}
				counted := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					countsMu.Lock()
					requests[host]++
					countsMu.Unlock()
					handler(w, r)
				})
				var server *httptest.Server
				switch {
				case host == tt.tlsHost && tt.tlsMode == "untrusted":
					server = httptest.NewTLSServer(counted)
				case host == tt.tlsHost && tt.tlsMode == "certificate-error":
					server = httptest.NewUnstartedServer(counted)
					server.TLS = &tls.Config{GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
						return nil, stderrors.New("test server: no certificate for this name")
					}}
					server.StartTLS()
				default:
					server = httptest.NewServer(counted)
				}
				t.Cleanup(server.Close)
				addrs[host] = server.Listener.Addr().String()
			}

			var goneDials atomic.Int32
			var dialer net.Dialer
			transport := &http.Transport{
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					host, _, err := net.SplitHostPort(addr)
					if err != nil {
						return nil, err
					}
					countsMu.Lock()
					dials[strings.ToLower(host)]++
					dialCount := dials[strings.ToLower(host)]
					countsMu.Unlock()
					if dialErr, ok := tt.dialErrors[strings.ToLower(host)]; ok &&
						(tt.dialFailures == 0 || dialCount <= tt.dialFailures) {
						return nil, dialErr
					}
					dnsErrorHost := tt.dnsErrorHost
					if dnsErrorHost == "" {
						dnsErrorHost = goneHost
					}
					// The port is ignored, so origin.example:80 reaches the
					// origin listener too, and the host matches case-insensitively
					// as DNS does.
					if strings.EqualFold(host, dnsErrorHost) && tt.dnsError != nil {
						goneDials.Add(1)
						dnsErr := *tt.dnsError
						return nil, &net.OpError{Op: "dial", Net: network, Err: &dnsErr}
					}
					if local, ok := addrs[host]; ok {
						return dialer.DialContext(ctx, network, local)
					}
					return nil, fmt.Errorf("test dialer: unexpected host %q", host)
				},
			}
			t.Cleanup(transport.CloseIdleConnections)

			client := newTestClient(t, ClientOptions{
				HTTPClient:     &http.Client{Transport: transport, Timeout: tt.clientTimeout},
				MaxAttempts:    2,
				RetryBaseDelay: time.Millisecond,
			})

			fetch := tt.fetch
			if fetch == nil {
				fetch = (*Client).FetchObject
			}
			obj, err := fetch(client, context.Background(), tt.iri)
			if tt.dnsError != nil {
				assert.Positive(t, goneDials.Load(), "the stub dialer must have been reached")
			}
			if tt.wantDials > 0 {
				assert.Equal(t, tt.wantDials, goneDials.Load(), "dials of the failing host")
			}
			countsMu.Lock()
			for host, want := range tt.wantRequests {
				assert.Equal(t, want, requests[host], "requests served by %s", host)
			}
			for host, want := range tt.wantDialsTo {
				assert.Equal(t, want, dials[host], "dials of %s", host)
			}
			countsMu.Unlock()
			if !tt.wantErr {
				require.NoError(t, err)
				require.NotNil(t, obj)
				assert.Equal(t, "Note", obj.Type)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tt.wantNotFound, errors.IsNotFound(err), "errors.IsNotFound(%v)", err)
			assert.Equal(t, tt.wantHostGone, errors.IsHostGone(err), "errors.IsHostGone(%v)", err)
			if tt.wantHostGone {
				host, ok := errors.HostGoneHost(err)
				assert.True(t, ok, "errors.HostGoneHost(%v) must find the dead host", err)
				assert.Equal(t, tt.wantGoneHost, host, "errors.HostGoneHost(%v)", err)
				if tt.wantReason != "" {
					reason, ok := errors.HostGoneReason(err)
					assert.True(t, ok, "errors.HostGoneReason(%v) must find a reason", err)
					assert.Equal(t, tt.wantReason, reason, "errors.HostGoneReason(%v)", err)
				}
			}
		})
	}
}

// TestFetchObject_CallerSideFailuresAreNotHostGone pins the failures that
// happen before or around a response but say nothing about the IRI host being
// unreachable: the per-host limiter wait, the caller's own context, the egress
// guard, and the detached request budget expiring. None may be classified as
// host gone, and each row proves the intended phase was reached.
func TestFetchObject_CallerSideFailuresAreNotHostGone(t *testing.T) {
	noteResponse := func(*http.Request) (*http.Response, error) {
		return scriptedResponse(http.StatusOK, `{"type":"Note","id":"https://x.example/post/0"}`), nil
	}

	tests := []struct {
		name string
		run  func(t *testing.T) error
	}{
		{
			name: "per-host limiter wait failing on the caller deadline",
			run: func(t *testing.T) error {
				transport := &scriptedTransport{handler: noteResponse}
				client := newTestClient(t, ClientOptions{
					HTTPClient:   &http.Client{Transport: transport},
					PerHostRPS:   1,
					PerHostBurst: 1,
				})
				_, err := client.FetchObject(context.Background(), "https://x.example/post/0")
				require.NoError(t, err, "the first fetch spends the host's only token")

				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
				_, err = client.FetchObject(ctx, "https://x.example/post/1")
				assert.ErrorIs(t, err, context.DeadlineExceeded, "the caller's deadline ends the wait")
				assert.Equal(t, []string{"https://x.example/post/0"}, transport.seen(),
					"the second fetch must still be waiting on the limiter")
				return err
			},
		},
		{
			name: "caller context canceled",
			run: func(t *testing.T) error {
				transport := &scriptedTransport{handler: noteResponse}
				client := newTestClient(t, ClientOptions{HTTPClient: &http.Client{Transport: transport}})
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := client.FetchObject(ctx, "https://x.example/post/1")
				assert.ErrorIs(t, err, context.Canceled)
				assert.Empty(t, transport.seen())
				return err
			},
		},
		{
			name: "egress guard refusal",
			run: func(t *testing.T) error {
				client := NewClient(ClientOptions{UserAgent: "tidepool-test/0", Signer: NewSigner(testKeyID, testRSAKey(t))})
				_, err := client.FetchObject(context.Background(), "http://127.0.0.1:1/post/1")
				assert.True(t, errors.IsValidation(err), "the guard refuses loopback, got %v", err)
				return err
			},
		},
		{
			name: "limiter wait failing inside the detached request budget",
			run: func(t *testing.T) error {
				transport := &scriptedTransport{handler: noteResponse}
				client := newTestClient(t, ClientOptions{
					HTTPClient:   &http.Client{Transport: transport},
					PerHostRPS:   0.001,
					PerHostBurst: 1,
				})
				_, err := client.FetchObject(context.Background(), "https://x.example/post/0")
				require.NoError(t, err, "the first fetch spends the host's only token")

				client.requestBudget = 50 * time.Millisecond
				callerCtx := context.Background()
				_, err = client.FetchObject(callerCtx, "https://x.example/post/1")
				require.Error(t, err)
				assert.NoError(t, callerCtx.Err(), "the caller's context is still live")
				assert.Equal(t, []string{"https://x.example/post/0"}, transport.seen(),
					"the second fetch must fail in the limiter wait, before any request")
				return err
			},
		},
		{
			name: "request budget expiring mid-dial",
			run: func(t *testing.T) error {
				// The round trip blocks until the request budget ends, then
				// reports a dial timeout, the error a dial cut off by the
				// budget's deadline gives. The budget ran out, not the host.
				var dialsEntered atomic.Int32
				transport := &scriptedTransport{handler: func(req *http.Request) (*http.Response, error) {
					dialsEntered.Add(1)
					<-req.Context().Done()
					return nil, &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
				}}
				client := newTestClient(t, ClientOptions{
					HTTPClient:  &http.Client{Transport: transport},
					MaxAttempts: 1,
				})
				client.requestBudget = 50 * time.Millisecond
				callerCtx := context.Background()
				_, err := client.FetchObject(callerCtx, "http://x.example/post/1")
				require.Error(t, err)
				assert.NoError(t, callerCtx.Err(), "the caller's context is still live")
				assert.Equal(t, int32(1), dialsEntered.Load(), "the dial must have been entered")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run(t)
			require.Error(t, err)
			assert.False(t, errors.IsHostGone(err), "errors.IsHostGone(%v)", err)
		})
	}
}

// TestProbeNodeInfo pins the reachability probe: only a 2xx JSON object at
// https://<host>/.well-known/nodeinfo counts as the host answering.
func TestProbeNodeInfo(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
		wantErr     bool
		host        string // "" means x.example
		wantURL     string // "" means https://x.example/.well-known/nodeinfo
	}{
		{name: "json object", status: http.StatusOK, contentType: "application/json", body: `{"links":[]}`, wantErr: false},
		{
			name: "ipv6 literal host", status: http.StatusOK, contentType: "application/json", body: `{"links":[]}`, wantErr: false,
			host: "::1", wantURL: "https://[::1]/.well-known/nodeinfo",
		},
		{name: "html page", status: http.StatusOK, contentType: "text/html", body: "<html><body>parked</body></html>", wantErr: true},
		{name: "html labelled json", status: http.StatusOK, contentType: "application/json", body: "<html><body>parked</body></html>", wantErr: true},
		{name: "malformed json", status: http.StatusOK, contentType: "application/json", body: `{"links":`, wantErr: true},
		{name: "not found", status: http.StatusNotFound, contentType: "application/json", body: `{"error":"not found"}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &scriptedTransport{handler: func(*http.Request) (*http.Response, error) {
				resp := scriptedResponse(tt.status, tt.body)
				resp.Header.Set("Content-Type", tt.contentType)
				return resp, nil
			}}
			client := newTestClient(t, ClientOptions{HTTPClient: &http.Client{Transport: transport}, MaxAttempts: 1})

			host, wantURL := tt.host, tt.wantURL
			if host == "" {
				host, wantURL = "x.example", "https://x.example/.well-known/nodeinfo"
			}
			err := client.ProbeNodeInfo(context.Background(), host)
			assert.Equal(t, []string{wantURL}, transport.seen())
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

// hostScript answers scripted requests per host; a host's responder can be
// swapped mid-test (a dead host coming back).
type hostScript struct {
	mu         sync.Mutex
	responders map[string]func(*http.Request) (*http.Response, error)
}

func (h *hostScript) set(host string, responder func(*http.Request) (*http.Response, error)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.responders[host] = responder
}

func (h *hostScript) roundTrip(req *http.Request) (*http.Response, error) {
	h.mu.Lock()
	responder, ok := h.responders[req.URL.Hostname()]
	h.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("host script: unexpected host %q", req.URL.Hostname())
	}
	return responder(req)
}

// requestsTo counts the transport's requests to host.
func requestsTo(transport *scriptedTransport, host string) int {
	count := 0
	for _, raw := range transport.seen() {
		if parsed, err := url.Parse(raw); err == nil && parsed.Hostname() == host {
			count++
		}
	}
	return count
}

func refusedDial(*http.Request) (*http.Response, error) {
	return nil, &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}
}

func jsonResponder(body string) func(*http.Request) (*http.Response, error) {
	return func(*http.Request) (*http.Response, error) {
		resp := scriptedResponse(http.StatusOK, body)
		resp.Header.Set("Content-Type", ContentTypeActivityJSON)
		return resp, nil
	}
}

func htmlResponse() *http.Response {
	resp := scriptedResponse(http.StatusOK, "<html><body>domain for sale</body></html>")
	resp.Header.Set("Content-Type", "text/html")
	return resp
}

func redirectResponse(location string) *http.Response {
	resp := scriptedResponse(http.StatusFound, "")
	resp.Header.Set("Location", location)
	return resp
}

// newHostScriptClient builds a client (MaxAttempts 2) over a per-host script.
func newHostScriptClient(t *testing.T) (*Client, *scriptedTransport, *hostScript) {
	t.Helper()
	script := &hostScript{responders: map[string]func(*http.Request) (*http.Response, error){}}
	transport := &scriptedTransport{handler: script.roundTrip}
	client := newTestClient(t, ClientOptions{
		HTTPClient:     &http.Client{Transport: transport},
		MaxAttempts:    2,
		RetryBaseDelay: time.Millisecond,
		PerHostRPS:     1000,
		PerHostBurst:   100,
	})
	return client, transport, script
}

// assertCachedVerdict checks err is a host-gone verdict for x.example served
// from the per-walk cache.
func assertCachedVerdict(t *testing.T, err error, wantReason string) {
	t.Helper()
	require.Error(t, err)
	assert.True(t, errors.IsHostGone(err), "errors.IsHostGone(%v)", err)
	host, _ := errors.HostGoneHost(err)
	assert.Equal(t, "x.example", host, "errors.HostGoneHost(%v)", err)
	reason, _ := errors.HostGoneReason(err)
	assert.Equal(t, wantReason, reason, "errors.HostGoneReason(%v)", err)
	assert.ErrorIs(t, err, ErrCachedHostVerdict)
}

// seedRefused fails one FetchObject on x.example with connection_refused in
// ctx, after its two attempts.
func seedRefused(t *testing.T, ctx context.Context, client *Client, transport *scriptedTransport) {
	t.Helper()
	seedVerdict(t, ctx, client, transport, "connection_refused", 2)
}

// seedVerdict fails one FetchObject on x.example with wantReason in ctx,
// after wantRequests requests.
func seedVerdict(t *testing.T, ctx context.Context, client *Client, transport *scriptedTransport, wantReason string, wantRequests int) {
	t.Helper()
	before := requestsTo(transport, "x.example")
	_, err := client.FetchObject(ctx, "https://x.example/post/1")
	assert.True(t, errors.IsHostGone(err), "seed: errors.IsHostGone(%v)", err)
	reason, _ := errors.HostGoneReason(err)
	assert.Equal(t, wantReason, reason, "seed: errors.HostGoneReason(%v)", err)
	assert.False(t, stderrors.Is(err, ErrCachedHostVerdict), "the seeding fetch is a fresh observation")
	assert.Equal(t, wantRequests, requestsTo(transport, "x.example")-before, "requests of the seeding fetch")
}

// TestFetchObject_ConnectionAndEdgeAttribution pins which connection- and
// edge-class failures name the IRI's host: only a failure on the first hop,
// and only when the IRI uses no port or its scheme's default port. A failure
// on a later same-host hop, or on another port of the host, says nothing
// about the host the instances table tracks, so it is neither classified nor
// cached.
func TestFetchObject_ConnectionAndEdgeAttribution(t *testing.T) {
	dialTimeout := func(*http.Request) (*http.Response, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
	}
	edge522 := func(*http.Request) (*http.Response, error) { return scriptedResponse(522, ""), nil }
	// redirectThen redirects /post/1 to /next on the same host and answers
	// every later hop with final.
	redirectThen := func(final func(*http.Request) (*http.Response, error)) func(*http.Request) (*http.Response, error) {
		return func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/post/1" {
				return redirectResponse("/next"), nil
			}
			return final(req)
		}
	}

	tests := []struct {
		name       string
		iri        string
		x          func(*http.Request) (*http.Response, error)
		wantReason string // "" means not host gone and not cached
	}{
		{name: "refused after a same-host redirect", iri: "https://x.example/post/1", x: redirectThen(refusedDial)},
		{name: "dial timeout after a same-host redirect", iri: "https://x.example/post/1", x: redirectThen(dialTimeout)},
		{name: "edge status after a same-host redirect", iri: "https://x.example/post/1", x: redirectThen(edge522)},
		{name: "refused on a non-default https port", iri: "https://x.example:8443/post/1", x: refusedDial},
		{name: "dial timeout on a non-default http port", iri: "http://x.example:8080/post/1", x: dialTimeout},
		{name: "edge status on a non-default port", iri: "https://x.example:8443/post/1", x: edge522},
		{name: "refused on the explicit https default port", iri: "https://x.example:443/post/1", x: refusedDial, wantReason: "connection_refused"},
		{name: "refused on the explicit http default port", iri: "http://x.example:80/post/1", x: refusedDial, wantReason: "connection_refused"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, transport, script := newHostScriptClient(t)
			script.set("x.example", tt.x)
			ctx := WithUnreachableHostCache(context.Background())

			_, err := client.FetchObject(ctx, tt.iri)
			require.Error(t, err)
			assert.Equal(t, tt.wantReason != "", errors.IsHostGone(err), "errors.IsHostGone(%v)", err)
			reason, _ := errors.HostGoneReason(err)
			assert.Equal(t, tt.wantReason, reason, "errors.HostGoneReason(%v)", err)

			script.set("x.example", jsonResponder(`{"type":"Note","id":"https://x.example/post/2"}`))
			before := requestsTo(transport, "x.example")
			obj, err := client.FetchObject(ctx, "https://x.example/post/2")
			if tt.wantReason != "" {
				assertCachedVerdict(t, err, tt.wantReason)
				assert.Equal(t, before, requestsTo(transport, "x.example"), "the cached verdict makes no request")
				return
			}
			require.NoError(t, err, "x.example must not be cached")
			assert.Equal(t, "https://x.example/post/2", obj.ID)
			assert.Equal(t, before+1, requestsTo(transport, "x.example"))
		})
	}
}

// TestUnreachableHostCache pins the per-walk unreachable-host cache carried in
// ctx: a connection-level verdict for a host short-circuits later object-mode
// fetches to it within the same ctx, and nothing else.
func TestUnreachableHostCache(t *testing.T) {
	t.Run("no cache in ctx keeps hitting the network", func(t *testing.T) {
		client, transport, script := newHostScriptClient(t)
		script.set("x.example", refusedDial)

		for _, iri := range []string{"https://x.example/post/1", "https://x.example/post/2"} {
			_, err := client.FetchObject(context.Background(), iri)
			assert.True(t, errors.IsHostGone(err), "errors.IsHostGone(%v)", err)
			reason, _ := errors.HostGoneReason(err)
			assert.Equal(t, "connection_refused", reason, "errors.HostGoneReason(%v)", err)
			assert.False(t, stderrors.Is(err, ErrCachedHostVerdict), "no cache, no cached verdict")
		}
		assert.Equal(t, 4, requestsTo(transport, "x.example"), "both fetches make both attempts")
	})

	t.Run("a connection-level verdict short-circuits later object fetches to the host", func(t *testing.T) {
		dialFailure := func(dialErr error) func(*http.Request) (*http.Response, error) {
			return func(*http.Request) (*http.Response, error) {
				return nil, &net.OpError{Op: "dial", Net: "tcp", Err: dialErr}
			}
		}
		tests := []struct {
			name         string
			x            func(*http.Request) (*http.Response, error)
			wantRequests int
		}{
			{
				name:         "dns_not_found",
				x:            dialFailure(&net.DNSError{Err: "no such host", Name: "x.example", IsNotFound: true}),
				wantRequests: 1,
			},
			{name: "connect_timeout", x: dialFailure(os.ErrDeadlineExceeded), wantRequests: 2},
			{name: "connection_refused", x: refusedDial, wantRequests: 2},
			{
				name:         "connection_reset",
				x:            dialFailure(&os.SyscallError{Syscall: "connect", Err: syscall.ECONNRESET}),
				wantRequests: 2,
			},
			{
				name:         "no_route",
				x:            dialFailure(&os.SyscallError{Syscall: "connect", Err: syscall.EHOSTUNREACH}),
				wantRequests: 2,
			},
			{
				name: "tls_failure",
				x: func(*http.Request) (*http.Response, error) {
					return nil, tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}
				},
				wantRequests: 2,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				client, transport, script := newHostScriptClient(t)
				script.set("x.example", tt.x)
				script.set("y.example", func(req *http.Request) (*http.Response, error) {
					if req.URL.Path == "/outbox" {
						return jsonResponder(`{"type":"OrderedCollectionPage","id":"https://y.example/outbox","orderedItems":[],"next":"https://x.example/outbox?page=2"}`)(req)
					}
					return jsonResponder(`{"type":"Note","id":"https://y.example/note/1"}`)(req)
				})
				ctx := WithUnreachableHostCache(context.Background())
				seedVerdict(t, ctx, client, transport, tt.name, tt.wantRequests)
				before := requestsTo(transport, "x.example")

				_, err := client.FetchObject(ctx, "https://x.example/post/2")
				assertCachedVerdict(t, err, tt.name)
				_, err = client.FetchActor(ctx, "https://x.example/u/alice")
				assertCachedVerdict(t, err, tt.name)
				err = client.FetchCollection(ctx, "https://x.example/c/outbox", func(*Object) error { return nil })
				assertCachedVerdict(t, err, tt.name)
				assert.Equal(t, before, requestsTo(transport, "x.example"), "cached fetches make no request to x.example")

				obj, err := client.FetchObject(ctx, "https://y.example/note/1")
				require.NoError(t, err, "another host is unaffected")
				assert.Equal(t, "https://y.example/note/1", obj.ID)
				assert.Equal(t, 1, requestsTo(transport, "y.example"))

				err = client.FetchCollection(ctx, "https://y.example/outbox", func(*Object) error { return nil })
				assertCachedVerdict(t, err, tt.name)
				assert.Equal(t, before, requestsTo(transport, "x.example"),
					"a collection page on the cached host makes no request to it")
			})
		}
	})

	t.Run("non-connection reasons do not seed", func(t *testing.T) {
		tests := []struct {
			name       string
			x          func(*http.Request) (*http.Response, error)
			wantReason string
		}{
			{
				name:       "edge_status",
				x:          func(*http.Request) (*http.Response, error) { return scriptedResponse(522, ""), nil },
				wantReason: "edge_status",
			},
			{
				name:       "too_many_redirects",
				x:          func(req *http.Request) (*http.Response, error) { return redirectResponse(req.URL.String()), nil },
				wantReason: "too_many_redirects",
			},
			{
				name: "redirect_to_root",
				x: func(req *http.Request) (*http.Response, error) {
					if req.URL.Path != "/" {
						return redirectResponse("/"), nil
					}
					return htmlResponse(), nil
				},
				wantReason: "redirect_to_root",
			},
			{
				name:       "parked_redirect",
				x:          func(*http.Request) (*http.Response, error) { return redirectResponse("https://z.example/parked"), nil },
				wantReason: "parked_redirect",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				client, transport, script := newHostScriptClient(t)
				script.set("x.example", tt.x)
				script.set("z.example", func(*http.Request) (*http.Response, error) { return htmlResponse(), nil })
				ctx := WithUnreachableHostCache(context.Background())

				_, err := client.FetchObject(ctx, "https://x.example/post/1")
				assert.True(t, errors.IsHostGone(err), "errors.IsHostGone(%v)", err)
				reason, _ := errors.HostGoneReason(err)
				assert.Equal(t, tt.wantReason, reason, "errors.HostGoneReason(%v)", err)

				before := requestsTo(transport, "x.example")
				_, err = client.FetchObject(ctx, "https://x.example/post/2")
				assert.Greater(t, requestsTo(transport, "x.example"), before, "the next fetch to x.example hits the network")
				assert.False(t, stderrors.Is(err, ErrCachedHostVerdict), "no cached verdict for %s", tt.wantReason)
			})
		}
	})

	t.Run("a successful media fetch drops the entry", func(t *testing.T) {
		client, transport, script := newHostScriptClient(t)
		script.set("x.example", refusedDial)
		ctx := WithUnreachableHostCache(context.Background())
		seedRefused(t, ctx, client, transport)

		script.set("x.example", func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/image.png" {
				resp := scriptedResponse(http.StatusOK, "\x89PNG")
				resp.Header.Set("Content-Type", "image/png")
				return resp, nil
			}
			return jsonResponder(`{"type":"Note","id":"https://x.example/post/2"}`)(req)
		})
		before := requestsTo(transport, "x.example")
		_, _, err := client.FetchMedia(ctx, "https://x.example/image.png", 0)
		require.NoError(t, err)
		assert.Equal(t, before+1, requestsTo(transport, "x.example"), "media is never served from the cache")

		obj, err := client.FetchObject(ctx, "https://x.example/post/2")
		require.NoError(t, err, "the entry was dropped, so the fetch reaches the recovered host")
		assert.Equal(t, "https://x.example/post/2", obj.ID)
		assert.Equal(t, before+2, requestsTo(transport, "x.example"))
	})

	t.Run("a successful nodeinfo probe drops the entry", func(t *testing.T) {
		client, transport, script := newHostScriptClient(t)
		script.set("x.example", refusedDial)
		ctx := WithUnreachableHostCache(context.Background())
		seedRefused(t, ctx, client, transport)

		script.set("x.example", func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/.well-known/nodeinfo" {
				resp := scriptedResponse(http.StatusOK, `{"links":[]}`)
				resp.Header.Set("Content-Type", "application/json")
				return resp, nil
			}
			return jsonResponder(`{"type":"Note","id":"https://x.example/post/2"}`)(req)
		})
		before := requestsTo(transport, "x.example")
		require.NoError(t, client.ProbeNodeInfo(ctx, "x.example"))
		assert.Equal(t, before+1, requestsTo(transport, "x.example"), "the probe reaches the host")

		obj, err := client.FetchObject(ctx, "https://x.example/post/2")
		require.NoError(t, err, "the entry was dropped, so the fetch reaches the recovered host")
		assert.Equal(t, "https://x.example/post/2", obj.ID)
		assert.Equal(t, before+2, requestsTo(transport, "x.example"))
	})

	t.Run("a cached verdict covers only the default port", func(t *testing.T) {
		tests := []struct {
			name       string
			iri        string
			wantCached bool
		}{
			{name: "non-default https port reaches the network", iri: "https://x.example:8443/obj"},
			{name: "non-default http port reaches the network", iri: "http://x.example:8080/obj"},
			{name: "explicit https default port is cached", iri: "https://x.example:443/obj", wantCached: true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				client, transport, script := newHostScriptClient(t)
				script.set("x.example", refusedDial)
				ctx := WithUnreachableHostCache(context.Background())
				seedRefused(t, ctx, client, transport)

				script.set("x.example", jsonResponder(`{"type":"Note","id":"`+tt.iri+`"}`))
				before := requestsTo(transport, "x.example")
				obj, err := client.FetchObject(ctx, tt.iri)
				if tt.wantCached {
					assertCachedVerdict(t, err, "connection_refused")
					assert.Equal(t, before, requestsTo(transport, "x.example"), "the cached verdict makes no request")
					return
				}
				require.NoError(t, err, "another port of x.example is not covered by the default-port verdict")
				assert.Equal(t, tt.iri, obj.ID)
				assert.Equal(t, before+1, requestsTo(transport, "x.example"))
			})
		}
	})

	t.Run("separate caches are independent", func(t *testing.T) {
		client, transport, script := newHostScriptClient(t)
		script.set("x.example", refusedDial)
		first := WithUnreachableHostCache(context.Background())
		second := WithUnreachableHostCache(context.Background())
		seedRefused(t, first, client, transport)

		before := requestsTo(transport, "x.example")
		_, err := client.FetchObject(second, "https://x.example/post/2")
		assert.Equal(t, before+2, requestsTo(transport, "x.example"), "the second cache has no verdict for x.example")
		assert.False(t, stderrors.Is(err, ErrCachedHostVerdict))

		before = requestsTo(transport, "x.example")
		_, err = client.FetchObject(first, "https://x.example/post/3")
		assertCachedVerdict(t, err, "connection_refused")
		assert.Equal(t, before, requestsTo(transport, "x.example"), "the first cache still holds its verdict")
	})
}
