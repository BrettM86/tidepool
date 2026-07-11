package materialize

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
)

// Task 11 hardening tests: the #account frame + vote scrub + blob scrub on
// Delete(Actor), media carry-forward on transient refresh failure, and the
// validation-failure metric.

// TestDeleteActor_AccountEventVoteAndBlobScrub drives the full terminal
// scrub on the captured lemmy.world fixtures: record deletes, the voter's
// vote_events scrub, blob deletion under BOTH the author's and the
// community's DID, and the trailing #account{active:false,status:deleted}
// firehose event.
func TestDeleteActor_AccountEventVoteAndBlobScrub(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)
	communityDID := testDIDFor("technology", "lemmy.world")
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")

	// The post's external embed carries a thumbnail blob stored under the
	// COMMUNITY's DID — the blob nothing but this scrub would ever clean up.
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	record, _, err := h.manager.GetRecord(ctx, communityDID, CollectionPost, mapping.RKey)
	require.NoError(t, err)
	blobs := atdata.ExtractBlobs(record)
	require.NotEmpty(t, blobs, "the fixture post must embed a thumbnail blob")
	thumbCID := cid.Cid(blobs[0].Ref).String()
	_, _, err = h.manager.GetBlob(ctx, communityDID, thumbCID)
	require.NoError(t, err, "thumbnail blob stored under the community DID before the scrub")

	require.NoError(t, h.m.DeleteActor(ctx, personID))

	// Vote scrub hook called with the actor's AP id.
	assert.Equal(t, []string{personID}, h.scrubbed.calls())

	// The community-DID thumbnail blob is gone; the community's own profile
	// media (if any) would survive because only THIS actor's records were
	// walked.
	_, _, err = h.manager.GetBlob(ctx, communityDID, thumbCID)
	assert.True(t, errors.IsNotFound(err), "the deleted author's post thumbnail must be scrubbed from the community repo")

	// Every blob under the author's own (terminally frozen) DID is gone.
	n, err := h.manager.DeleteBlobsForDID(ctx, authorDID)
	require.NoError(t, err)
	assert.Zero(t, n, "DeleteActor must already have emptied the author's blob rows")

	// The LAST firehose event for the author repo is the #account frame:
	// active=false, status=deleted, sequenced after the scrub deletes.
	events := h.firehoseEvents()
	require.NotEmpty(t, events)
	last := events[len(events)-1]
	assert.Equal(t, repo.EventKindAccount, last.Kind)
	assert.Equal(t, authorDID, last.DID)
	assert.False(t, last.AccountActive)
	assert.Equal(t, repo.AccountStatusDeleted, last.AccountStatus)
	var accountEvents, deleteOps int
	for _, ev := range events {
		if ev.Kind == repo.EventKindAccount {
			accountEvents++
			continue
		}
		for _, op := range ev.Ops {
			if op.Action == repo.OpActionDelete {
				deleteOps++
			}
		}
	}
	assert.Equal(t, 1, accountEvents, "exactly one account event for one Delete(Actor)")
	assert.Equal(t, 2, deleteOps, "scrub deletes: the post and the actor profile")
}

// TestSuppressActor_ScrubsVotesToo: the reversible nobridge scrub erases
// vote_events rows with the same posture as the terminal delete — but emits
// NO #account frame (the repo stays active).
func TestSuppressActor_ScrubsVotesToo(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)

	require.NoError(t, h.m.SuppressActor(ctx, personID))
	assert.Equal(t, []string{personID}, h.scrubbed.calls())
	for _, ev := range h.firehoseEvents() {
		assert.NotEqual(t, repo.EventKindAccount, ev.Kind,
			"nobridge is reversible: no #account frame may be emitted")
	}
}

// TestProfileRefresh_MediaCarryForward: a profile refresh whose avatar
// fetch fails transiently keeps the previously stored blob; an actor that
// REMOVED its avatar loses it.
func TestProfileRefresh_MediaCarryForward(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const actorID = "https://lemmy.world/u/carrie"
	const avatarPath = "/pictrs/image/carrie-avatar.png"
	var failAvatar atomic.Bool
	h.mux.HandleFunc("GET "+avatarPath, func(w http.ResponseWriter, _ *http.Request) {
		if failAvatar.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngBytes)
	})
	// The actor document is mutable mid-test (icon removed in phase 3), so
	// it is served through a swappable pointer instead of serveObject
	// (ServeMux refuses duplicate patterns).
	var actorDoc atomic.Value
	actorDoc.Store(person(actorID, "carrie", map[string]any{
		"icon": map[string]any{"type": "Image", "url": "https://lemmy.world" + avatarPath},
	}))
	h.mux.HandleFunc("GET /u/carrie", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
		_ = json.NewEncoder(w).Encode(actorDoc.Load())
	})

	stored, err := h.m.EnsureActor(ctx, &ap.Object{ID: actorID})
	require.NoError(t, err)
	profile, _, err := h.manager.GetRecord(ctx, stored.DID, CollectionActorProfile, ProfileRKey)
	require.NoError(t, err)
	original, ok := profile["avatar"].(atdata.Blob)
	require.True(t, ok, "first materialization stores the avatar blob")

	// Transient origin failure: the refresh must carry the old blob forward.
	failAvatar.Store(true)
	_, err = h.m.RefreshActor(ctx, &ap.Object{ID: actorID})
	require.NoError(t, err)
	profile, _, err = h.manager.GetRecord(ctx, stored.DID, CollectionActorProfile, ProfileRKey)
	require.NoError(t, err)
	carried, ok := profile["avatar"].(atdata.Blob)
	require.True(t, ok, "a transient media failure must not strip the avatar")
	assert.Equal(t, cid.Cid(original.Ref).String(), cid.Cid(carried.Ref).String())

	// Deliberate removal: the actor no longer advertises an icon → no
	// carry-forward, the avatar drops.
	actorDoc.Store(person(actorID, "carrie", nil))
	_, err = h.m.RefreshActor(ctx, &ap.Object{ID: actorID})
	require.NoError(t, err)
	profile, _, err = h.manager.GetRecord(ctx, stored.DID, CollectionActorProfile, ProfileRKey)
	require.NoError(t, err)
	_, ok = profile["avatar"].(atdata.Blob)
	assert.False(t, ok, "a removed avatar must not be carried forward")
}

// TestProfileRefresh_PermanentRemovalDropsBlob: when the actor still
// advertises an image URL but the origin now answers 404 (the image was
// deleted at source), the stale blob is DROPPED — not carried forward
// forever like a transient failure would be.
func TestProfileRefresh_PermanentRemovalDropsBlob(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const actorID = "https://lemmy.world/u/perm"
	const avatarPath = "/pictrs/image/perm-avatar.png"
	var gone atomic.Bool
	h.mux.HandleFunc("GET "+avatarPath, func(w http.ResponseWriter, _ *http.Request) {
		if gone.Load() {
			// Permanent: image removed at origin (404 → IsNotFound).
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngBytes)
	})
	doc := person(actorID, "perm", map[string]any{
		"icon": map[string]any{"type": "Image", "url": "https://lemmy.world" + avatarPath},
	})
	h.mux.HandleFunc("GET /u/perm", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
		_ = json.NewEncoder(w).Encode(doc)
	})

	stored, err := h.m.EnsureActor(ctx, &ap.Object{ID: actorID})
	require.NoError(t, err)
	profile, _, err := h.manager.GetRecord(ctx, stored.DID, CollectionActorProfile, ProfileRKey)
	require.NoError(t, err)
	_, ok := profile["avatar"].(atdata.Blob)
	require.True(t, ok, "first materialization stores the avatar blob")

	// Image gone at origin, but the doc STILL advertises its URL: a permanent
	// removal drops the blob (a transient 5xx would have carried it forward).
	gone.Store(true)
	_, err = h.m.RefreshActor(ctx, &ap.Object{ID: actorID})
	require.NoError(t, err)
	profile, _, err = h.manager.GetRecord(ctx, stored.DID, CollectionActorProfile, ProfileRKey)
	require.NoError(t, err)
	_, ok = profile["avatar"].(atdata.Blob)
	assert.False(t, ok, "a permanent 404 removal must not be carried forward")
}

// TestDeleteActor_BlobScrubFailureIsRetryable: a failing blob delete makes
// DeleteActor return a retryable (non-skip) error so the queue redelivers it;
// a subsequent retry, with the failure cleared, completes idempotently — the
// orphan blob is scrubbed and the #account frame is emitted exactly once.
func TestDeleteActor_BlobScrubFailureIsRetryable(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)
	communityDID := testDIDFor("technology", "lemmy.world")
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	record, _, err := h.manager.GetRecord(ctx, communityDID, CollectionPost, mapping.RKey)
	require.NoError(t, err)
	blobs := atdata.ExtractBlobs(record)
	require.NotEmpty(t, blobs, "the fixture post must embed a thumbnail blob")
	thumbCID := cid.Cid(blobs[0].Ref).String()

	// Inject a transient failure on the community-DID thumbnail delete.
	realDelete := h.m.deleteBlob
	var fail atomic.Bool
	fail.Store(true)
	h.m.deleteBlob = func(ctx context.Context, did, c string) error {
		if fail.Load() && c == thumbCID {
			return fmt.Errorf("transient storage failure")
		}
		return realDelete(ctx, did, c)
	}

	// First attempt fails, and the failure is RETRYABLE (not a skip): the
	// queue must redeliver rather than drop the delete and orphan the blob.
	err = h.m.DeleteActor(ctx, personID)
	require.Error(t, err)
	assert.False(t, IsSkip(err), "a blob scrub failure must be retryable, not a skip")
	_, _, err = h.manager.GetBlob(ctx, communityDID, thumbCID)
	require.NoError(t, err, "the orphan blob survives the failed scrub")
	for _, ev := range h.firehoseEvents() {
		assert.NotEqual(t, repo.EventKindAccount, ev.Kind,
			"no #account frame until the scrub completes")
	}

	// Retry with the failure cleared: DeleteActor is idempotent and completes.
	fail.Store(false)
	require.NoError(t, h.m.DeleteActor(ctx, personID))
	_, _, err = h.manager.GetBlob(ctx, communityDID, thumbCID)
	assert.True(t, errors.IsNotFound(err), "the retry scrubs the orphaned blob")
	events := h.firehoseEvents()
	require.NotEmpty(t, events)
	assert.Equal(t, repo.EventKindAccount, events[len(events)-1].Kind,
		"the completing retry emits the #account frame")
	var accountEvents int
	for _, ev := range events {
		if ev.Kind == repo.EventKindAccount {
			accountEvents++
		}
	}
	assert.Equal(t, 1, accountEvents, "exactly one #account frame across the failed attempt and the retry")
}

// TestValidationFailureMetric: the expvar counter moves in both strict and
// log-and-write modes.
func TestValidationFailureMetric(t *testing.T) {
	h := newHarness(t)

	invalid := map[string]any{"$type": CollectionPost} // missing every required field
	before := ValidationFailures.Value()

	// Strict (the harness default): failure is an error AND counted.
	err := h.m.validateRecord(invalid)
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
	assert.Equal(t, before+1, ValidationFailures.Value())

	// Log-and-write (production): no error, still counted.
	h.m.strict = false
	require.NoError(t, h.m.validateRecord(invalid))
	assert.Equal(t, before+2, ValidationFailures.Value())

	// A valid record does not move the counter.
	valid := map[string]any{
		"$type":     CollectionActorProfile,
		"bio":       "hello",
		"createdAt": "2026-01-01T00:00:00.000Z",
	}
	require.NoError(t, h.m.validateRecord(valid))
	assert.Equal(t, before+2, ValidationFailures.Value())
}
