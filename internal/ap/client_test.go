package ap

import (
	"context"
	"encoding/json"
	"fmt"
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
	assert.Contains(t, sawRequest.Header.Get("Accept"), "application/activity+json")
	assert.Equal(t, "tidepool-test/0", sawRequest.Header.Get("User-Agent"))

	// The GET must carry a signature Lemmy would accept: verify it
	// server-side, empty body.
	_, err = verifier.Verify(context.Background(), sawRequest, nil)
	require.NoError(t, err, "signed GET must verify against the signer's public key")
	fields := parseSignatureHeader(sawRequest.Header.Get("Signature"))
	assert.Equal(t, "(request-target) host date digest", fields["headers"])
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

func TestSendActivity_RequiresSigner(t *testing.T) {
	client := NewClient(ClientOptions{})
	err := client.SendActivity(context.Background(), "https://lemmy.world/inbox", map[string]any{})
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
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
