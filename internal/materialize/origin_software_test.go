package materialize

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

const wellKnownNodeInfoPath = "/.well-known/nodeinfo"

// instanceRow is an instances row as stored.
type instanceRow struct {
	software  sql.NullString
	fetchedAt time.Time
}

// seedInstance writes an instances row directly; software "" stores NULL.
func seedInstance(t *testing.T, host, software string, fetchedAt time.Time) {
	t.Helper()
	_, err := testutil.DB(t).ExecContext(context.Background(),
		`INSERT INTO instances (host, software_name, fetched_at) VALUES ($1, $2, $3)`,
		host, sql.NullString{String: software, Valid: software != ""}, fetchedAt)
	require.NoError(t, err)
}

// storedInstance reads host's instances row; ok is false when there is none.
func storedInstance(t *testing.T, host string) (row instanceRow, ok bool) {
	t.Helper()
	err := testutil.DB(t).QueryRowContext(context.Background(),
		`SELECT software_name, fetched_at FROM instances WHERE host = $1`, host).
		Scan(&row.software, &row.fetchedAt)
	if err == sql.ErrNoRows {
		return instanceRow{}, false
	}
	require.NoError(t, err)
	return row, true
}

// servePerson serves a synthetic Person at https://<host>/u/<username> and
// returns its id. Fixture paths are host-blind, so usernames must be
// unique within a harness.
func servePerson(h *harness, host, username string) string {
	id := "https://" + host + "/u/" + username
	h.serveObject("/u/"+username, person(id, username, nil))
	return id
}

// storedProfile reads the profile record committed in did's repo.
func storedProfile(t *testing.T, h *harness, did, collection string) map[string]any {
	t.Helper()
	record, _, err := h.manager.GetRecord(context.Background(), did, collection, ProfileRKey)
	require.NoError(t, err)
	return record
}

// respondStatus answers every request with status.
func respondStatus(status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	})
}

// TestOriginSoftwareFailedRefreshKeepsStoredValue (B5, DoD 5): a failed
// discovery is cached — the stored name survives, fetched_at advances, and
// the next actor on the host triggers no discovery.
func TestOriginSoftwareFailedRefreshKeepsStoredValue(t *testing.T) {
	const host = "lemmy.world"
	for _, tc := range []struct {
		name         string
		seedSoftware string // "" with seed=false: no row at all
		seed         bool
		wantSoftware sql.NullString
		wantProfile  string // "" means the field is omitted
	}{
		{
			name: "stale row keeps its name", seed: true, seedSoftware: "lemmy",
			wantSoftware: sql.NullString{String: "lemmy", Valid: true}, wantProfile: "lemmy",
		},
		{
			name:         "no row records a failure",
			wantSoftware: sql.NullString{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			seededAt := time.Now().Add(-25 * time.Hour)
			if tc.seed {
				seedInstance(t, host, tc.seedSoftware, seededAt)
			}
			h.serveNodeInfoHandler(host, respondStatus(http.StatusInternalServerError))
			alice := servePerson(h, host, "alice")
			bob := servePerson(h, host, "bob")

			start := time.Now()
			actor, err := h.m.EnsureActor(ctx, &ap.Object{ID: alice})
			require.NoError(t, err, "a nodeinfo failure must not fail materialization")
			end := time.Now()

			assert.Equal(t, 1, h.requestCount(host, wellKnownNodeInfoPath), "discovery must have been attempted")
			row, ok := storedInstance(t, host)
			require.True(t, ok, "the failed discovery must be recorded")
			assert.Equal(t, tc.wantSoftware, row.software)
			assert.WithinRange(t, row.fetchedAt, start.Add(-time.Millisecond), end.Add(time.Millisecond),
				"fetched_at must advance to the failed attempt")

			profile := storedProfile(t, h, actor.DID, CollectionActorProfile)
			if tc.wantProfile == "" {
				assert.NotContains(t, profile, "originSoftware")
			} else {
				assert.Equal(t, tc.wantProfile, profile["originSoftware"])
			}

			before := h.nodeInfoRequestCount(host)
			_, err = h.m.EnsureActor(ctx, &ap.Object{ID: bob})
			require.NoError(t, err)
			assert.Equal(t, before, h.nodeInfoRequestCount(host),
				"the cached failure must stop a second actor's discovery")
		})
	}
}

// TestOriginSoftwareRefreshInterval (B6, DoD 6): a fresh row is served
// without discovery; a stale row is refreshed once; an existing profile
// picks the new value up on its own next refresh, not by a forced rewrite.
func TestOriginSoftwareRefreshInterval(t *testing.T) {
	const host = "lemmy.world"

	t.Run("fresh row is served without discovery", func(t *testing.T) {
		h := newHarness(t)
		ctx := context.Background()
		// 23 h with the 25 h stale seed below pins RefreshInterval to 23-25 h.
		seedInstance(t, host, "lemmy", time.Now().Add(-23*time.Hour))
		h.serveNodeInfo(host, "mbin")

		for _, username := range []string{"alice", "bob"} {
			actor, err := h.m.EnsureActor(ctx, &ap.Object{ID: servePerson(h, host, username)})
			require.NoError(t, err)
			assert.Equal(t, "lemmy", storedProfile(t, h, actor.DID, CollectionActorProfile)["originSoftware"],
				"%s must carry the stored name", username)
		}
		assert.Zero(t, h.nodeInfoRequestCount(host))
	})

	t.Run("stale row is refreshed once", func(t *testing.T) {
		h := newHarness(t)
		ctx := context.Background()
		seedInstance(t, host, "lemmy", time.Now().Add(-25*time.Hour))
		h.serveNodeInfo(host, "mbin")

		actor, err := h.m.EnsureActor(ctx, &ap.Object{ID: servePerson(h, host, "alice")})
		require.NoError(t, err)
		assert.Equal(t, 1, h.requestCount(host, wellKnownNodeInfoPath))
		assert.Equal(t, 1, h.requestCount(host, nodeInfoDocumentPath))
		assert.Equal(t, "mbin", storedProfile(t, h, actor.DID, CollectionActorProfile)["originSoftware"])

		_, err = h.m.EnsureActor(ctx, &ap.Object{ID: servePerson(h, host, "carol")})
		require.NoError(t, err)
		assert.Equal(t, 2, h.nodeInfoRequestCount(host), "a third actor must not trigger another discovery")
	})

	t.Run("existing profile updates on its next refresh", func(t *testing.T) {
		h := newHarness(t)
		ctx := context.Background()
		seedInstance(t, host, "lemmy", time.Now().Add(-time.Hour))
		h.serveNodeInfo(host, "mbin")
		aliceID := servePerson(h, host, "alice")

		alice, err := h.m.EnsureActor(ctx, &ap.Object{ID: aliceID})
		require.NoError(t, err)
		require.Equal(t, "lemmy", storedProfile(t, h, alice.DID, CollectionActorProfile)["originSoftware"])

		_, err = testutil.DB(t).ExecContext(ctx,
			`UPDATE instances SET fetched_at = $1 WHERE host = $2`, time.Now().Add(-25*time.Hour), host)
		require.NoError(t, err)

		bob, err := h.m.EnsureActor(ctx, &ap.Object{ID: servePerson(h, host, "bob")})
		require.NoError(t, err)
		assert.Equal(t, "mbin", storedProfile(t, h, bob.DID, CollectionActorProfile)["originSoftware"])
		assert.Equal(t, "lemmy", storedProfile(t, h, alice.DID, CollectionActorProfile)["originSoftware"],
			"a new value must not force a rewrite of existing profiles")

		h.m.now = func() time.Time { return time.Now().Add(25 * time.Hour) } // past ProfileRefreshTTL
		_, err = h.m.EnsureActor(ctx, &ap.Object{ID: aliceID})
		require.NoError(t, err)
		assert.Equal(t, "mbin", storedProfile(t, h, alice.DID, CollectionActorProfile)["originSoftware"],
			"the profile's own refresh must pick the new value up")
	})
}

// TestOriginSoftwareOnBothProfiles (B7, DoD 7): user and community
// profiles on a host carry its software name and still validate; with no
// value both omit the field.
func TestOriginSoftwareOnBothProfiles(t *testing.T) {
	const host = "lemmy.world"
	for _, tc := range []struct {
		name string
		seed bool
		want string // "" means the field is omitted
	}{
		{name: "fresh row", seed: true, want: "lemmy"},
		{name: "no value (nodeinfo 404)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			if tc.seed {
				seedInstance(t, host, "lemmy", time.Now().Add(-time.Hour))
			}
			const communityID = "https://" + host + "/c/rust"
			h.serveObject("/c/rust", group(communityID, "rust", nil))

			actor, err := h.m.EnsureActor(ctx, &ap.Object{ID: servePerson(h, host, "alice")})
			require.NoError(t, err)
			community, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: communityID})
			require.NoError(t, err)

			for _, profile := range []struct {
				did, collection string
			}{
				{actor.DID, CollectionActorProfile},
				{community.DID, CollectionCommunityProfile},
			} {
				record := storedProfile(t, h, profile.did, profile.collection)
				if tc.want == "" {
					assert.NotContains(t, record, "originSoftware", profile.collection)
				} else {
					assert.Equal(t, tc.want, record["originSoftware"], profile.collection)
				}
				requireValidRecord(t, record, profile.collection)
			}
		})
	}
}

// TestOriginSoftwareDiscoveryDeadline (B8, DoD 8): one overall deadline
// bounds a host's discovery cycle; when it expires materialization still
// completes in time, without the field, and the failure is cached.
func TestOriginSoftwareDiscoveryDeadline(t *testing.T) {
	const (
		host     = "lemmy.world"
		deadline = 300 * time.Millisecond
	)
	// blockUntilReleased holds the well-known response until the test ends
	// (or the client gives up).
	blockUntilReleased := func(release <-chan struct{}) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		})
	}
	// delayEach delays every nodeinfo response by 200ms: each request fits
	// the deadline, the two together do not.
	delayEach := func(release <-chan struct{}) http.Handler {
		fixture := nodeInfoFixture("lemmy")
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			timer := time.NewTimer(200 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-release:
			case <-r.Context().Done():
			}
			fixture.ServeHTTP(w, r)
		})
	}

	for _, tc := range []struct {
		name          string
		handler       func(release <-chan struct{}) http.Handler
		actors        int
		wantWellKnown int // 0: not asserted
	}{
		{name: "well-known hangs", handler: blockUntilReleased, actors: 1},
		{name: "each response delayed under the deadline", handler: delayEach, actors: 1},
		{name: "well-known hangs under four concurrent actors", handler: blockUntilReleased, actors: 4, wantWellKnown: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, withNodeInfoDiscoveryDeadline(deadline))
			ctx := context.Background()
			release := make(chan struct{})
			// Registered after newHarness, so it runs before the fixture
			// server's Close waits on the held handlers.
			t.Cleanup(func() { close(release) })
			h.serveNodeInfoHandler(host, tc.handler(release))

			ids := make([]string, tc.actors)
			for i := range ids {
				ids[i] = servePerson(h, host, fmt.Sprintf("user%d", i))
			}
			type outcome struct {
				actor   *store.BridgedActor
				err     error
				elapsed time.Duration
			}
			outcomes := make([]outcome, tc.actors)
			var wg sync.WaitGroup
			for i, id := range ids {
				wg.Add(1)
				go func() {
					defer wg.Done()
					start := time.Now()
					actor, err := h.m.EnsureActor(ctx, &ap.Object{ID: id})
					outcomes[i] = outcome{actor: actor, err: err, elapsed: time.Since(start)}
				}()
			}
			wg.Wait()

			_, recorded := storedInstance(t, host)
			assert.True(t, recorded, "the expired cycle's failure must be recorded by the time the calls return")
			for i, got := range outcomes {
				require.NoError(t, got.err, ids[i])
				assert.LessOrEqual(t, got.elapsed, deadline+time.Second, ids[i])
				assert.NotContains(t, storedProfile(t, h, got.actor.DID, CollectionActorProfile), "originSoftware", ids[i])
			}
			if tc.wantWellKnown != 0 {
				assert.Equal(t, tc.wantWellKnown, h.requestCount(host, wellKnownNodeInfoPath))
			}

			before := h.nodeInfoRequestCount(host)
			_, err := h.m.EnsureActor(ctx, &ap.Object{ID: servePerson(h, host, "later")})
			require.NoError(t, err)
			assert.Equal(t, before, h.nodeInfoRequestCount(host),
				"the cached failure must stop a further actor's discovery")
		})
	}
}

// TestOriginSoftwareConcurrentDiscoveryCoordinated (B9, DoD 9): eight
// actors on one host materialized at once share one discovery cycle.
func TestOriginSoftwareConcurrentDiscoveryCoordinated(t *testing.T) {
	const (
		host   = "lemmy.world"
		actors = 8
	)
	for _, tc := range []struct {
		name string
		seed bool
	}{
		{name: "no row"},
		{name: "stale row", seed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			if tc.seed {
				seedInstance(t, host, "lemmy", time.Now().Add(-25*time.Hour))
			}
			// The well-known response is held until all eight requests an
			// uncoordinated implementation would send have arrived, or 2s,
			// so their requests overlap whatever the scheduling.
			fixture := nodeInfoFixture("mbin")
			var mu sync.Mutex
			arrivals := 0
			allArrived := make(chan struct{})
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			h.serveNodeInfoHandler(host, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == wellKnownNodeInfoPath {
					mu.Lock()
					arrivals++
					if arrivals == actors {
						close(allArrived)
					}
					mu.Unlock()
					timer := time.NewTimer(2 * time.Second)
					defer timer.Stop()
					select {
					case <-allArrived:
					case <-timer.C:
					case <-release:
					case <-r.Context().Done():
					}
				}
				fixture.ServeHTTP(w, r)
			}))

			ids := make([]string, actors)
			for i := range ids {
				ids[i] = servePerson(h, host, fmt.Sprintf("user%d", i))
			}
			bridged := make([]*store.BridgedActor, actors)
			errs := make([]error, actors)
			var wg sync.WaitGroup
			for i, id := range ids {
				wg.Add(1)
				go func() {
					defer wg.Done()
					bridged[i], errs[i] = h.m.EnsureActor(ctx, &ap.Object{ID: id})
				}()
			}
			wg.Wait()

			assert.Equal(t, 1, h.requestCount(host, wellKnownNodeInfoPath))
			assert.Equal(t, 1, h.requestCount(host, nodeInfoDocumentPath))
			for i := range ids {
				require.NoError(t, errs[i], ids[i])
				assert.Equal(t, "mbin", storedProfile(t, h, bridged[i].DID, CollectionActorProfile)["originSoftware"], ids[i])
			}
		})
	}
}

// TestOriginSoftwareDiscoveryStaysLocal (B10, DoD 10): the default harness
// sends nodeinfo discovery to the local fixture server for every host —
// one with a fixture and one without.
func TestOriginSoftwareDiscoveryStaysLocal(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	h.serveNodeInfo("lemmy.world", "lemmy")
	ctx := context.Background()

	actor, err := h.m.EnsureActor(ctx, &ap.Object{ID: personID})
	require.NoError(t, err)
	assert.Equal(t, "lemmy", storedProfile(t, h, actor.DID, CollectionActorProfile)["originSoftware"])
	requests := h.fixtureRequests()
	assert.Contains(t, requests, fixtureRequest{Host: "lemmy.world", Path: wellKnownNodeInfoPath, Status: http.StatusOK})
	assert.Contains(t, requests, fixtureRequest{Host: "lemmy.world", Path: nodeInfoDocumentPath, Status: http.StatusOK})

	other, err := h.m.EnsureActor(ctx, &ap.Object{ID: servePerson(h, "piefed.social", "rimu")})
	require.NoError(t, err)
	assert.NotContains(t, storedProfile(t, h, other.DID, CollectionActorProfile), "originSoftware")
	assert.Contains(t, h.fixtureRequests(),
		fixtureRequest{Host: "piefed.social", Path: wellKnownNodeInfoPath, Status: http.StatusNotFound})
}

// schemeRecordingTransport records each outbound request's URL as the
// client asked for it, before the wrapped transport (rewriteTransport,
// which forces http to reach the fixture server) rewrites it.
type schemeRecordingTransport struct {
	next     http.RoundTripper
	mu       sync.Mutex
	requests []string
}

func (s *schemeRecordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	s.requests = append(s.requests, req.URL.String())
	s.mu.Unlock()
	return s.next.RoundTrip(req)
}

func (s *schemeRecordingTransport) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

// withNodeInfoSchemeRecorder wraps the harness's nodeinfo client transport
// in recorder.
func withNodeInfoSchemeRecorder(recorder *schemeRecordingTransport) func(*Options) {
	return func(o *Options) {
		recorder.next = o.NodeInfoClient.Transport
		o.NodeInfoClient = &http.Client{Transport: recorder}
	}
}

// TestOriginSoftwareHTTPActorDiscoversOverHTTP (R1-2): an http:// actor id
// runs nodeinfo discovery over http, following an http link, and its
// profile carries the discovered name.
func TestOriginSoftwareHTTPActorDiscoversOverHTTP(t *testing.T) {
	const host = "lemmy"
	recorder := &schemeRecordingTransport{}
	h := newHarness(t, withNodeInfoSchemeRecorder(recorder))
	ctx := context.Background()
	h.serveNodeInfoHandler(host, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == wellKnownNodeInfoPath {
			_ = json.NewEncoder(w).Encode(map[string]any{"links": []any{map[string]any{
				"rel":  "http://nodeinfo.diaspora.software/ns/schema/2.0",
				"href": "http://" + r.Host + nodeInfoDocumentPath,
			}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"version":  "2.0",
			"software": map[string]any{"name": "lemmy", "version": "0.19.0"},
		})
	}))
	const actorID = "http://" + host + "/u/alice"
	h.serveObject("/u/alice", person(actorID, "alice", nil))

	actor, err := h.m.EnsureActor(ctx, &ap.Object{ID: actorID})
	require.NoError(t, err)

	assert.Equal(t, "lemmy", storedProfile(t, h, actor.DID, CollectionActorProfile)["originSoftware"])
	assert.Equal(t, []string{
		"http://" + host + wellKnownNodeInfoPath,
		"http://" + host + nodeInfoDocumentPath,
	}, recorder.recorded(), "discovery must use the actor id's http scheme")
}

// TestOriginSoftwareUnsupportedSchemeSkipsDiscovery: an actor id whose
// scheme is neither http nor https gets no discovery attempt, no instances
// row, and no originSoftware on its committed profile. The fetcher refuses
// ftp, so the actor arrives as an embedded document (the Update{Person}
// path).
func TestOriginSoftwareUnsupportedSchemeSkipsDiscovery(t *testing.T) {
	const host = "lemmy.world"
	h := newHarness(t)
	ctx := context.Background()
	h.serveNodeInfo(host, "lemmy")
	const actorID = "ftp://" + host + "/u/alice"
	body, err := json.Marshal(person(actorID, "alice", nil))
	require.NoError(t, err)
	doc, err := ap.ParseObject(body)
	require.NoError(t, err)

	actor, err := h.m.RefreshActor(ctx, doc)
	require.NoError(t, err)

	assert.NotContains(t, storedProfile(t, h, actor.DID, CollectionActorProfile), "originSoftware")
	assert.Zero(t, h.nodeInfoRequestCount(host), "no discovery request may be sent")
	_, recorded := storedInstance(t, host)
	assert.False(t, recorded, "an unsupported scheme must not reach discovery at all")
}
