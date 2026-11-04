package materialize

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/store"
)

// heldWaitMinter models a mint held on the PLC budget: during runs where the
// wait falls, after the actor check and the document fetch, and Recheck runs
// once it ends, in the real Minter's order. Later mints pass straight through.
type heldWaitMinter struct {
	ActorMinter
	during func()
	held   bool
}

func (m *heldWaitMinter) MintActor(ctx context.Context, request identity.MintRequest) (*identity.Identity, error) {
	if !m.held {
		m.held = true
		m.during()
		if err := request.Recheck(ctx, true); err != nil {
			return nil, err
		}
	}
	return m.ActorMinter.MintActor(ctx, request)
}

// serveSwappable serves doc at path and returns a function replacing what later
// fetches get, for a document that changes mid-test.
func (h *harness) serveSwappable(path string, doc map[string]any) func(map[string]any) {
	h.t.Helper()
	var current atomic.Pointer[[]byte]
	swap := func(next map[string]any) {
		h.t.Helper()
		body, err := json.Marshal(next)
		require.NoError(h.t, err)
		current.Store(&body)
	}
	swap(doc)
	h.mux.HandleFunc("GET "+path, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
		_, _ = w.Write(*current.Load())
	})
	return swap
}

// insertUnprofiledRow writes a bridged_actors row with no profile committed:
// what a first bridge leaves when its profile commit failed.
func (h *harness) insertUnprofiledRow(ctx context.Context, apActorID string, actorType store.ActorType, username string) *store.BridgedActor {
	h.t.Helper()
	minted, err := h.minter.MintActor(ctx, identity.MintRequest{
		ActorType: actorType, PreferredUsername: username, Instance: "lemmy.world",
	})
	require.NoError(h.t, err)
	row, err := h.actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:           apActorID,
		ActorType:           actorType,
		DID:                 minted.DID,
		Handle:              minted.Handle,
		PreferredUsername:   username,
		SigningKeyEncrypted: minted.SigningKeyEncrypted,
		ConsentState:        store.ConsentStateOK,
	})
	require.NoError(h.t, err)
	return row
}

// requireNoProfile asserts nothing was committed for the actor.
func (h *harness) requireNoProfile(ctx context.Context, row *store.BridgedActor, collection string) {
	h.t.Helper()
	_, err := h.objects.GetByAPID(ctx, row.APActorID)
	require.True(h.t, errors.IsNotFound(err), "no profile mapping may exist, got %v", err)
	_, _, err = h.manager.GetRecord(ctx, row.DID, collection, ProfileRKey)
	require.True(h.t, errors.IsNotFound(err), "no profile record may exist, got %v", err)
}

// TestMintWait_PersonRowThatOptedOutDuringTheWaitIsSkipped: the row the wait
// turned up goes through consent policy, not straight to a profile commit from
// the document read before the wait. The row has no profile mapping, so nothing
// else would refuse the commit.
func TestMintWait_PersonRowThatOptedOutDuringTheWaitIsSkipped(t *testing.T) {
	const actorID = "https://lemmy.world/u/waiter"
	h := newHarness(t)
	ctx := t.Context()
	swap := h.serveSwappable("/u/waiter", person(actorID, "waiter", nil))
	var row *store.BridgedActor
	h.m.minter = &heldWaitMinter{ActorMinter: h.m.minter, during: func() {
		swap(person(actorID, "waiter_after_wait", map[string]any{"summary": "<p>#nobridge</p>"}))
		row = h.insertUnprofiledRow(ctx, actorID, store.ActorTypePerson, "waiter")
		require.NoError(t, h.actors.SetConsentState(ctx, actorID, store.ConsentStateNoBridge))
	}}

	_, err := h.m.EnsureActor(ctx, &ap.Object{ID: actorID})

	require.True(t, IsSkip(err), "want a skip, got %v", err)
	h.requireNoProfile(ctx, row, CollectionActorProfile)
	after, err := h.actors.GetByAPActorID(ctx, actorID)
	require.NoError(t, err)
	assert.Equal(t, store.ConsentStateNoBridge, after.ConsentState)
	assert.Equal(t, "waiter_after_wait", after.PreferredUsername,
		"the stamp comes from the document read after the wait")
}

// TestMintWait_FreshRowKeepsItsProfile: another worker bridged the actor during
// the wait. Its profile is newer than the document this caller read before the
// wait, which must not overwrite it.
func TestMintWait_FreshRowKeepsItsProfile(t *testing.T) {
	const actorID = "https://lemmy.world/u/cleaner"
	h := newHarness(t)
	ctx := t.Context()
	ref := &ap.Object{ID: actorID}
	swap := h.serveSwappable("/u/cleaner", person(actorID, "cleaner", map[string]any{"summary": "<p>old bio</p>"}))
	h.m.minter = &heldWaitMinter{ActorMinter: h.m.minter, during: func() {
		swap(person(actorID, "cleaner", map[string]any{"summary": "<p>cleaned bio</p>"}))
		_, err := h.m.EnsureActor(ctx, ref)
		require.NoError(t, err)
	}}

	stored, err := h.m.EnsureActor(ctx, ref)

	require.NoError(t, err)
	assert.Equal(t, actorID, stored.APActorID)
	description, ok := h.recordFor(t, actorID)["description"].(string)
	require.True(t, ok, "the profile has a description")
	assert.Contains(t, description, "cleaned bio")
	assert.NotContains(t, description, "old bio")
}

// TestMintWait_GroupRowThatOptedOutDuringTheWaitStaysOptedOut: ensureCommunity
// hands bridgeNewActor its own unforced document with allowEmbedded set, so that
// document must not count as a fresh, trusted one for the row. Read as one, it
// has no marker and restores the row to OK.
func TestMintWait_GroupRowThatOptedOutDuringTheWaitStaysOptedOut(t *testing.T) {
	const communityID = "https://lemmy.world/c/waiting"
	h := newHarness(t)
	ctx := t.Context()
	groupDoc := func(extra map[string]any) map[string]any {
		doc := person(communityID, "waiting", extra)
		doc["type"] = "Group"
		return doc
	}
	swap := h.serveSwappable("/c/waiting", groupDoc(nil))
	var row *store.BridgedActor
	h.m.minter = &heldWaitMinter{ActorMinter: h.m.minter, during: func() {
		swap(groupDoc(map[string]any{"summary": "<p>#nobridge</p>"}))
		row = h.insertUnprofiledRow(ctx, communityID, store.ActorTypeGroup, "waiting")
		require.NoError(t, h.actors.SetConsentState(ctx, communityID, store.ConsentStateNoBridge))
	}}

	_, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: communityID})

	require.True(t, IsSkip(err), "want a skip, got %v", err)
	h.requireNoProfile(ctx, row, CollectionCommunityProfile)
	after, err := h.actors.GetByAPActorID(ctx, communityID)
	require.NoError(t, err)
	assert.Equal(t, store.ConsentStateNoBridge, after.ConsentState)
	_, err = h.communities.GetByAPGroupID(ctx, communityID)
	assert.True(t, errors.IsNotFound(err), "no communities row may exist, got %v", err)
}
