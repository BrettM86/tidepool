package materialize

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// serveSwitchableGroup serves the Group at path from a document the test can
// replace between calls, the way an upstream community is edited.
func serveSwitchableGroup(h *harness, path string, initial map[string]any) func(map[string]any) {
	var mu sync.Mutex
	doc := initial
	h.mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
		_ = json.NewEncoder(w).Encode(doc)
	})
	return func(next map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		doc = next
	}
}

// TestEnsureCommunityStoresSensitiveOnInsert: a community first reached by an
// ID-only reference is fetched, and the fetched Group's sensitive flag is what
// the inserted row holds.
func TestEnsureCommunityStoresSensitiveOnInsert(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const groupIRI = "https://lemmy.world/c/afterdark"
	h.serveObject("/c/afterdark", group(groupIRI, "afterdark", map[string]any{"sensitive": true}))

	_, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: groupIRI})
	require.NoError(t, err)

	stored, err := h.communities.GetByAPGroupID(ctx, groupIRI)
	require.NoError(t, err)
	assert.True(t, stored.Sensitive)
}

// TestCommunitySensitiveFollowsTheGroupDocument walks one materialized
// community through every way a Group document reaches ensureCommunity, reading
// the persisted row after each step. A stale-fetch step expires the profile
// first, because each refresh makes it fresh again.
func TestCommunitySensitiveFollowsTheGroupDocument(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const (
		groupIRI = "https://lemmy.world/c/switching"
		aliasIRI = "https://lemmy.world/c/elsewhere"
	)
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.m.now = func() time.Time { return clock }

	flagged := group(groupIRI, "switching", map[string]any{"sensitive": true})
	unflagged := group(groupIRI, "switching", map[string]any{"sensitive": false})
	omitted := group(groupIRI, "switching", nil)
	serve := serveSwitchableGroup(h, "/c/switching", unflagged)

	_, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: groupIRI})
	require.NoError(t, err)

	type step struct {
		name string
		// expire moves the clock past the profile TTL before the call.
		expire bool
		// served is what the Group's IRI serves from this step on; nil keeps
		// the previous document.
		served map[string]any
		call   func() error
		// wantSkip is set when the call must be refused as a SkipError.
		wantSkip      bool
		wantSensitive bool
	}
	refresh := func(doc map[string]any) func() error {
		return func() error {
			_, err := h.m.RefreshCommunity(ctx, mustObject(t, doc))
			return err
		}
	}
	ensure := func() error {
		_, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: groupIRI})
		return err
	}
	withDocument := func(doc map[string]any) func() error {
		return func() error {
			_, err := h.m.EnsureCommunityWithDocument(ctx, groupIRI, mustObject(t, doc))
			return err
		}
	}
	for _, s := range []step{
		{name: "Update{Group} with sensitive true", call: refresh(flagged), wantSensitive: true},
		{name: "Update{Group} with sensitive false", call: refresh(unflagged), wantSensitive: false},
		{name: "Update{Group} with sensitive true again", call: refresh(flagged), wantSensitive: true},
		{name: "Update{Group} with sensitive omitted", call: refresh(omitted), wantSensitive: false},
		{name: "stale fetch with sensitive true", expire: true, served: flagged, call: ensure, wantSensitive: true},
		{name: "stale fetch with sensitive false", expire: true, served: unflagged, call: ensure, wantSensitive: false},
		{name: "stale fetch with sensitive true again", expire: true, served: flagged, call: ensure, wantSensitive: true},
		{name: "stale fetch with sensitive omitted", expire: true, served: omitted, call: ensure, wantSensitive: false},
		{name: "stale fetch with sensitive true before a fresh call", expire: true, served: flagged, call: ensure, wantSensitive: true},
		// The IRI now serves false, so a value of false here would mean a
		// fresh profile was re-fetched or its flag reset without a document.
		{name: "fresh profile with no document keeps the stored flag", served: unflagged, call: ensure, wantSensitive: true},
		{name: "fresh profile with a supplied sensitive false document", call: withDocument(unflagged), wantSensitive: false},
		{name: "stale fetch with sensitive true before an alias document", expire: true, served: flagged, call: ensure, wantSensitive: true},
		{
			name:     "supplied document naming another id is refused",
			call:     withDocument(group(aliasIRI, "elsewhere", map[string]any{"sensitive": false})),
			wantSkip: true, wantSensitive: true,
		},
	} {
		if s.expire {
			clock = clock.Add(h.m.profileTTL + time.Minute)
		}
		if s.served != nil {
			serve(s.served)
		}
		err := s.call()
		if s.wantSkip {
			assert.True(t, IsSkip(err), "%s: want a SkipError, got %v", s.name, err)
		} else {
			require.NoError(t, err, s.name)
		}
		stored, err := h.communities.GetByAPGroupID(ctx, groupIRI)
		require.NoError(t, err, s.name)
		assert.Equal(t, s.wantSensitive, stored.Sensitive, s.name)
	}

	_, err = h.communities.GetByAPGroupID(ctx, aliasIRI)
	assert.True(t, errors.IsNotFound(err), "the alias document must not create a row, got %v", err)
}

// TestGroupAuthorRefreshStoresSensitive: a community whose Group is refreshed
// as a post's author (EnsureActor, attributedTo naming the Group) stores the
// re-fetched sensitive flag. That refresh makes the profile fresh, so the next
// EnsureCommunity reads no document and would otherwise keep the old flag.
func TestGroupAuthorRefreshStoresSensitive(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const groupIRI = "https://lemmy.world/c/turning"
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.m.now = func() time.Time { return clock }

	serve := serveSwitchableGroup(h, "/c/turning", group(groupIRI, "turning", map[string]any{"sensitive": false}))
	_, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: groupIRI})
	require.NoError(t, err)

	clock = clock.Add(h.m.profileTTL + time.Minute)
	serve(group(groupIRI, "turning", map[string]any{"sensitive": true}))
	_, err = h.m.EnsureActor(ctx, &ap.Object{ID: groupIRI})
	require.NoError(t, err)

	stored, err := h.communities.GetByAPGroupID(ctx, groupIRI)
	require.NoError(t, err)
	assert.True(t, stored.Sensitive, "the author-path refresh must store the Group's sensitive flag")

	_, err = h.m.EnsureCommunity(ctx, &ap.Object{ID: groupIRI})
	require.NoError(t, err)
	stored, err = h.communities.GetByAPGroupID(ctx, groupIRI)
	require.NoError(t, err)
	assert.True(t, stored.Sensitive, "the following EnsureCommunity must keep the flag")
}

// TestGroupAuthorOnlyGainsNoCommunityRow: a Group known only as an author is
// bridged and refreshed through EnsureActor without a communities row.
func TestGroupAuthorOnlyGainsNoCommunityRow(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const groupIRI = "https://lemmy.world/c/authoronly"
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.m.now = func() time.Time { return clock }

	serve := serveSwitchableGroup(h, "/c/authoronly", group(groupIRI, "authoronly", map[string]any{"sensitive": false}))
	_, err := h.m.EnsureActor(ctx, &ap.Object{ID: groupIRI})
	require.NoError(t, err)

	clock = clock.Add(h.m.profileTTL + time.Minute)
	serve(group(groupIRI, "authoronly", map[string]any{"sensitive": true}))
	_, err = h.m.EnsureActor(ctx, &ap.Object{ID: groupIRI})
	require.NoError(t, err)

	_, err = h.communities.GetByAPGroupID(ctx, groupIRI)
	assert.True(t, errors.IsNotFound(err), "an author-only Group must not gain a communities row, got %v", err)
}

// failingSensitiveCommunities fails SetSensitive while fail is set.
type failingSensitiveCommunities struct {
	store.Communities
	fail bool
}

func (c *failingSensitiveCommunities) SetSensitive(ctx context.Context, apGroupID string, sensitive bool) error {
	if c.fail {
		return stderrors.New("injected sensitive write failure")
	}
	return c.Communities.SetSensitive(ctx, apGroupID, sensitive)
}

// TestGroupAuthorRefreshRetriesFailedSensitiveWrite: when the sensitive write
// of a Group refresh fails, the profile is not stamped fresh, so the next
// EnsureActor refreshes again and stores the flag.
func TestGroupAuthorRefreshRetriesFailedSensitiveWrite(t *testing.T) {
	var communities *failingSensitiveCommunities
	h := newHarness(t, func(o *Options) {
		communities = &failingSensitiveCommunities{Communities: o.Communities}
		o.Communities = communities
	})
	ctx := context.Background()
	const groupIRI = "https://lemmy.world/c/retrying"
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.m.now = func() time.Time { return clock }

	serve := serveSwitchableGroup(h, "/c/retrying", group(groupIRI, "retrying", map[string]any{"sensitive": false}))
	_, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: groupIRI})
	require.NoError(t, err)

	clock = clock.Add(h.m.profileTTL + time.Minute)
	serve(group(groupIRI, "retrying", map[string]any{"sensitive": true}))
	communities.fail = true
	_, err = h.m.EnsureActor(ctx, &ap.Object{ID: groupIRI})
	require.Error(t, err, "the failed sensitive write must fail the refresh")

	communities.fail = false
	_, err = h.m.EnsureActor(ctx, &ap.Object{ID: groupIRI})
	require.NoError(t, err)
	stored, err := h.communities.GetByAPGroupID(ctx, groupIRI)
	require.NoError(t, err)
	assert.True(t, stored.Sensitive, "the retried refresh must store the Group's sensitive flag")
}

// TestCommunityPromotedFromGroupAuthorStoresSensitive: a sensitive Group first
// bridged as an author, then reached as a community within the profile TTL
// with no document in hand, gets a communities row holding its flag.
func TestCommunityPromotedFromGroupAuthorStoresSensitive(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const groupIRI = "https://lemmy.world/c/promoted"
	h.serveObject("/c/promoted", group(groupIRI, "promoted", map[string]any{"sensitive": true}))

	_, err := h.m.EnsureActor(ctx, &ap.Object{ID: groupIRI})
	require.NoError(t, err)
	_, err = h.communities.GetByAPGroupID(ctx, groupIRI)
	require.True(t, errors.IsNotFound(err), "EnsureActor alone must not create a communities row, got %v", err)

	_, err = h.m.EnsureCommunity(ctx, &ap.Object{ID: groupIRI})
	require.NoError(t, err)
	stored, err := h.communities.GetByAPGroupID(ctx, groupIRI)
	require.NoError(t, err)
	assert.True(t, stored.Sensitive, "the first communities row must hold the Group's sensitive flag")
}
