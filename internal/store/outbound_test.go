package store

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// Task 14 cycle A: the outbound state the consumer writes and tasks 15-17
// read back. These tables are the answer to a hard fact about Jetstream — a
// delete commit carries the DID, collection and rkey and NOTHING else — so
// every assertion here is really about "can a Delete still be built after the
// record is gone?".

// outboundTestDB returns a migrated connection with task 14's tables emptied.
func outboundTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "outbound_objects", "outbound_votes", "federation_prefs")
	return database
}

const (
	testCommentATURI   = "at://" + testDID + "/social.coves.community.comment/3lzcommentaaa"
	testCommunityDID   = "did:plc:44ybard66vv44zksje25o7dz"
	testCommunityAPID  = "https://lemmy.world/c/technology"
	testCommentAPID    = "https://coves.social/ap/object/3lzcommentaaa"
	testVoteATURI      = "at://" + testDID + "/social.coves.feed.vote/3lzvoteaaaaaa"
	testOtherVoteATURI = "at://" + testDID + "/social.coves.feed.vote/3lzvotebbbbbb"
	testSubjectATURI   = "at://" + testCommunityDID + "/social.coves.community.postv2/3lzpostaaaaaa"
	testSubjectAPID    = "https://lemmy.world/post/12345"
)

func testOutboundObject() OutboundObject {
	return OutboundObject{
		ATURI:              testCommentATURI,
		APObjectID:         testCommentAPID,
		LastCID:            testCID,
		LastRev:            "3lzrev0000001",
		CommunityDID:       testCommunityDID,
		CommunityAPID:      testCommunityAPID,
		TranslatedSnapshot: []byte(`{"type":"Note","content":"hello"}`),
		Depth:              1,
	}
}

func testOutboundVote() OutboundVote {
	return OutboundVote{
		VoteATURI:         testVoteATURI,
		ActorDID:          testDID,
		SubjectATURI:      testSubjectATURI,
		SubjectAPID:       testSubjectAPID,
		CommunityDID:      testCommunityDID,
		Direction:         "up",
		CurrentActivityID: "https://coves.social/ap/activity/" + repeatHex('a'),
	}
}

// repeatHex builds a 64-character hex-ish digest stand-in for fixtures.
func repeatHex(c byte) string {
	out := make([]byte, 64)
	for i := range out {
		out[i] = c
	}
	return string(out)
}

// ---------------------------------------------------------------------------
// outbound_objects
// ---------------------------------------------------------------------------

func TestOutboundObjects_UpsertStartsAtSeqZeroAndBumpsOnUpdate(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundObjects(database)
	ctx := context.Background()

	created, err := repo.Upsert(ctx, testOutboundObject())
	require.NoError(t, err, "first upsert of %s", testCommentATURI)
	require.NotNil(t, created, "Upsert must return the stored row")

	assert.Equal(t, testCommentATURI, created.ATURI)
	assert.Equal(t, testCommentAPID, created.APObjectID)
	assert.Equal(t, testCommunityDID, created.CommunityDID)
	assert.Equal(t, testCommunityAPID, created.CommunityAPID)
	assert.JSONEq(t, `{"type":"Note","content":"hello"}`, string(created.TranslatedSnapshot),
		"the translated snapshot is what task 15 serves the object from")
	assert.Equal(t, 1, created.Depth, "reply depth rides the row (Lemmy caps comments at 50)")
	assert.Equal(t, 0, created.LastActivitySeq,
		"a fresh outbound object starts at seq 0: its Create is activity 0")
	assert.Nil(t, created.TombstonedAt, "a created object is not tombstoned")
	assert.False(t, created.IsTombstoned())

	// An APPLIED update (the rev gate already let it through) is a second
	// activity and must not reuse the Create's id.
	updated := testOutboundObject()
	updated.LastRev = "3lzrev0000002"
	updated.LastCID = testUpdatedCID
	updated.TranslatedSnapshot = []byte(`{"type":"Note","content":"edited"}`)
	second, err := repo.Upsert(ctx, updated)
	require.NoError(t, err, "second upsert of the same at-uri")
	require.NotNil(t, second)

	assert.Equal(t, 1, second.LastActivitySeq,
		"every applied write bumps last_activity_seq so each operation gets its own activity id")
	assert.Equal(t, testUpdatedCID, second.LastCID, "provenance columns follow the newest commit")
	assert.Equal(t, "3lzrev0000002", second.LastRev,
		"last_rev is PROVENANCE ONLY — the ordering gate is jetstream_record_revs")
	assert.JSONEq(t, `{"type":"Note","content":"edited"}`, string(second.TranslatedSnapshot))
	assert.Equal(t, created.CreatedAt, second.CreatedAt, "created_at is preserved across upserts")
	assert.False(t, second.UpdatedAt.Before(created.UpdatedAt), "updated_at moves forward")
}

func TestOutboundObjects_GetByATURI(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundObjects(database)
	ctx := context.Background()

	stored, err := repo.Upsert(ctx, testOutboundObject())
	require.NoError(t, err)
	require.NotNil(t, stored)

	got, err := repo.GetByATURI(ctx, testCommentATURI)
	require.NoError(t, err, "GetByATURI for a stored object")
	require.NotNil(t, got)
	assert.Equal(t, stored.APObjectID, got.APObjectID)
	assert.Equal(t, stored.CommunityAPID, got.CommunityAPID)
	assert.Equal(t, stored.LastActivitySeq, got.LastActivitySeq)

	_, err = repo.GetByATURI(ctx, "at://did:plc:nobody/social.coves.community.comment/nope")
	require.Error(t, err, "an unknown at-uri must not silently return a zero row")
	assert.True(t, errors.IsNotFound(err), "want NotFound, got %v", err)
}

func TestOutboundObjects_TombstoneReturnsTheStateTheDeleteIsBuiltFrom(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundObjects(database)
	ctx := context.Background()

	created, err := repo.Upsert(ctx, testOutboundObject())
	require.NoError(t, err)
	require.NotNil(t, created)

	// The delete commit that triggers this carries no record and no CID, so
	// Tombstone has to hand back everything the Delete{Note} needs.
	dead, err := repo.Tombstone(ctx, testCommentATURI)
	require.NoError(t, err, "tombstone %s", testCommentATURI)
	require.NotNil(t, dead, "Tombstone must return the stored state, not just an error")

	require.NotNil(t, dead.TombstonedAt, "tombstoned_at must be stamped")
	assert.True(t, dead.IsTombstoned())
	assert.Equal(t, testCommentAPID, dead.APObjectID,
		"the AP id the Delete addresses comes from state — the delete frame has none")
	assert.Equal(t, testCommunityAPID, dead.CommunityAPID,
		"the community the Delete is addressed to comes from state")
	assert.JSONEq(t, `{"type":"Note","content":"hello"}`, string(dead.TranslatedSnapshot),
		"the snapshot survives the tombstone: task 17 restores from it")
	assert.Equal(t, 1, dead.LastActivitySeq,
		"the Delete is the next activity, so the seq bumps once")

	// The row SURVIVES: it is what a replayed create is rejected against.
	got, err := repo.GetByATURI(ctx, testCommentATURI)
	require.NoError(t, err, "a tombstoned row is still readable")
	require.NotNil(t, got)
	assert.True(t, got.IsTombstoned())
}

func TestOutboundObjects_TombstoneIsIdempotent(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundObjects(database)
	ctx := context.Background()

	_, err := repo.Upsert(ctx, testOutboundObject())
	require.NoError(t, err)

	first, err := repo.Tombstone(ctx, testCommentATURI)
	require.NoError(t, err)
	require.NotNil(t, first)

	second, err := repo.Tombstone(ctx, testCommentATURI)
	require.NoError(t, err, "re-tombstoning is a no-op success, not an error")
	require.NotNil(t, second)

	require.NotNil(t, second.TombstonedAt)
	assert.Equal(t, *first.TombstonedAt, *second.TombstonedAt,
		"the original tombstone time is preserved")
	assert.Equal(t, first.LastActivitySeq, second.LastActivitySeq,
		"a redelivered delete must reuse the activity id the first one sent, "+
			"so the seq must NOT bump again")
}

func TestOutboundObjects_TombstoneMissingIsNotFound(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundObjects(database)
	ctx := context.Background()

	_, err := repo.Tombstone(ctx, "at://did:plc:nobody/social.coves.community.comment/nope")
	require.Error(t, err,
		"a delete for a record we never federated must be distinguishable from a real tombstone")
	assert.True(t, errors.IsNotFound(err), "want NotFound, got %v", err)
}

func TestOutboundObjects_UpsertTxRidesTheTransaction(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundObjects(database)
	ctx := context.Background()

	// Rolled back: the outbound state must vanish with the rev-gate claim it
	// rode in with, or a replay would find state but no gate row.
	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	stored, err := repo.UpsertTx(ctx, tx, testOutboundObject())
	require.NoError(t, err, "UpsertTx inside a transaction")
	require.NotNil(t, stored)
	require.NoError(t, tx.Rollback())

	_, err = repo.GetByATURI(ctx, testCommentATURI)
	require.Error(t, err, "a rolled-back UpsertTx must leave no row")
	assert.True(t, errors.IsNotFound(err), "want NotFound after rollback, got %v", err)

	// Committed: the row lands.
	tx, err = database.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = repo.UpsertTx(ctx, tx, testOutboundObject())
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	got, err := repo.GetByATURI(ctx, testCommentATURI)
	require.NoError(t, err, "a committed UpsertTx must be visible")
	require.NotNil(t, got)
	assert.Equal(t, 0, got.LastActivitySeq,
		"the rolled-back attempt must not have consumed a seq")
}

func TestOutboundObjects_TxVariantsRejectNilTx(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundObjects(database)
	ctx := context.Background()

	_, err := repo.UpsertTx(ctx, nil, testOutboundObject())
	require.Error(t, err, "UpsertTx with a nil tx must not silently fall back to the pool")
	assert.True(t, errors.IsValidation(err), "want validation error, got %v", err)

	_, err = repo.TombstoneTx(ctx, nil, testCommentATURI)
	require.Error(t, err, "TombstoneTx with a nil tx must not silently fall back to the pool")
	assert.True(t, errors.IsValidation(err), "want validation error, got %v", err)
}

// ---------------------------------------------------------------------------
// outbound_votes
// ---------------------------------------------------------------------------

func TestOutboundVotes_UpsertAndBothLookups(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundVotes(database)
	ctx := context.Background()

	created, err := repo.Upsert(ctx, testOutboundVote())
	require.NoError(t, err, "upsert vote %s", testVoteATURI)
	require.NotNil(t, created, "Upsert must return the stored row")

	assert.Equal(t, testVoteATURI, created.VoteATURI)
	assert.Equal(t, "up", created.Direction)
	assert.Equal(t, 0, created.ActivitySeq)
	assert.Equal(t, DeliveredStatePending, created.DeliveredState,
		"the consumer records INTENT only: an unstated delivered_state is pending, "+
			"never delivered — task 15 claims delivery, and only on success")

	// The DELETE path's lookup: a vote delete commit carries the vote record
	// at-uri and nothing else.
	byURI, err := repo.GetByATURI(ctx, testVoteATURI)
	require.NoError(t, err, "GetByATURI is the delete path's only key")
	require.NotNil(t, byURI)
	assert.Equal(t, "up", byURI.Direction, "the Undo's direction is read back from here")
	assert.Equal(t, created.CurrentActivityID, byURI.CurrentActivityID,
		"the Undo must embed the id the Like went out under")

	// The CREATE path's lookup: has this actor already voted here?
	bySubject, err := repo.GetByActorSubject(ctx, testDID, testSubjectATURI)
	require.NoError(t, err, "GetByActorSubject must find the same row")
	require.NotNil(t, bySubject)
	assert.Equal(t, testVoteATURI, bySubject.VoteATURI)

	_, err = repo.GetByATURI(ctx, testOtherVoteATURI)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err), "unknown vote at-uri: want NotFound, got %v", err)

	_, err = repo.GetByActorSubject(ctx, testSecondDID, testSubjectATURI)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err), "unknown (actor, subject): want NotFound, got %v", err)
}

func TestOutboundVotes_ReUpsertBumpsSeq(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundVotes(database)
	ctx := context.Background()

	_, err := repo.Upsert(ctx, testOutboundVote())
	require.NoError(t, err)

	flipped := testOutboundVote()
	flipped.Direction = "down"
	second, err := repo.Upsert(ctx, flipped)
	require.NoError(t, err, "re-upserting the same vote record")
	require.NotNil(t, second)

	assert.Equal(t, "down", second.Direction)
	assert.Equal(t, 1, second.ActivitySeq,
		"a second applied write is a second activity and must not reuse the first id")
}

func TestOutboundVotes_OneLiveVotePerActorSubject(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundVotes(database)
	ctx := context.Background()

	_, err := repo.Upsert(ctx, testOutboundVote())
	require.NoError(t, err)

	// A second vote RECORD for the same subject by the same actor: the
	// unique constraint must refuse it rather than clobber the row whose
	// Undo is still owed.
	second := testOutboundVote()
	second.VoteATURI = testOtherVoteATURI
	_, err = repo.Upsert(ctx, second)
	require.Error(t, err,
		"a second vote record for the same (actor, subject) must not silently replace the first — "+
			"that would strand the first vote's Undo")
	assert.True(t, errors.IsAlreadyExists(err), "want AlreadyExists, got %v", err)

	// The original is untouched.
	got, err := repo.GetByATURI(ctx, testVoteATURI)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 0, got.ActivitySeq, "the refused write must not have bumped the seq")
}

func TestOutboundVotes_DeliveredStateTransitions(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundVotes(database)
	ctx := context.Background()

	_, err := repo.Upsert(ctx, testOutboundVote())
	require.NoError(t, err)

	require.NoError(t, repo.SetDeliveredState(ctx, testVoteATURI, DeliveredStateDelivered),
		"task 15 flips pending -> delivered on delivery success")
	got, err := repo.GetByATURI(ctx, testVoteATURI)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, DeliveredStateDelivered, got.DeliveredState)

	require.NoError(t, repo.SetDeliveredState(ctx, testVoteATURI, DeliveredStateUndone))
	got, err = repo.GetByATURI(ctx, testVoteATURI)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, DeliveredStateUndone, got.DeliveredState)

	err = repo.SetDeliveredState(ctx, testVoteATURI, DeliveredState("shipped"))
	require.Error(t, err, "an unknown delivered_state must be rejected, not stored")
	assert.True(t, errors.IsValidation(err), "want validation error, got %v", err)

	err = repo.SetDeliveredState(ctx, testOtherVoteATURI, DeliveredStateDelivered)
	require.Error(t, err, "delivering a vote we have no state for is a bug, not a no-op")
	assert.True(t, errors.IsNotFound(err), "want NotFound, got %v", err)
}

func TestOutboundVotes_Delete(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundVotes(database)
	ctx := context.Background()

	_, err := repo.Upsert(ctx, testOutboundVote())
	require.NoError(t, err)

	require.NoError(t, repo.Delete(ctx, testVoteATURI))
	_, err = repo.GetByATURI(ctx, testVoteATURI)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err), "want NotFound after delete, got %v", err)

	require.NoError(t, repo.Delete(ctx, testVoteATURI),
		"deleting an already-deleted vote is a no-op success (the callback may re-fire)")

	// With the row gone the (actor, subject) slot is free again.
	fresh := testOutboundVote()
	fresh.VoteATURI = testOtherVoteATURI
	_, err = repo.Upsert(ctx, fresh)
	require.NoError(t, err, "a re-vote after the Undo lands must be storable")
}

func TestOutboundVotes_UpsertTxRidesTheTransaction(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundVotes(database)
	ctx := context.Background()

	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = repo.UpsertTx(ctx, tx, testOutboundVote())
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	_, err = repo.GetByATURI(ctx, testVoteATURI)
	require.Error(t, err, "a rolled-back UpsertTx must leave no vote state")
	assert.True(t, errors.IsNotFound(err), "want NotFound after rollback, got %v", err)

	_, err = repo.UpsertTx(ctx, nil, testOutboundVote())
	require.Error(t, err, "UpsertTx with a nil tx must not silently fall back to the pool")
	assert.True(t, errors.IsValidation(err), "want validation error, got %v", err)
}

// ---------------------------------------------------------------------------
// federation_prefs
// ---------------------------------------------------------------------------

func TestFederationPrefs_AbsentRowIsNotFound(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewFederationPrefs(database)
	ctx := context.Background()

	_, err := repo.Get(ctx, testDID)
	require.Error(t, err,
		"federation is DEFAULT-ON and the record is an opt-out, so a user who never "+
			"spoke has no row — the store must say NotFound, never invent an enabled one")
	assert.True(t, errors.IsNotFound(err), "want NotFound, got %v", err)
}

func TestFederationPrefs_UpsertOverwritesEveryField(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewFederationPrefs(database)
	ctx := context.Background()

	optOut := FederationPref{
		DID:          testDID,
		Enabled:      false,
		DeleteRemote: true,
		Source:       FederationPrefSourceRecord,
	}
	stored, err := repo.Upsert(ctx, optOut)
	require.NoError(t, err, "upsert opt-out for %s", testDID)
	require.NotNil(t, stored)
	assert.False(t, stored.Enabled)
	assert.True(t, stored.DeleteRemote)
	assert.Equal(t, FederationPrefSourceRecord, stored.Source)

	got, err := repo.Get(ctx, testDID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.Enabled)
	assert.True(t, got.DeleteRemote)

	// Re-enabling must also clear the destructive flag: a stale deleteRemote
	// on an enabled row is a loaded gun pointed at task 17.
	reEnabled, err := repo.Upsert(ctx, FederationPref{
		DID:     testDID,
		Enabled: true,
		Source:  FederationPrefSourceProbe,
	})
	require.NoError(t, err)
	require.NotNil(t, reEnabled)
	assert.True(t, reEnabled.Enabled)
	assert.False(t, reEnabled.DeleteRemote,
		"re-enabling clears delete_remote: every field is overwritten")
	assert.Equal(t, FederationPrefSourceProbe, reEnabled.Source)
	assert.False(t, reEnabled.UpdatedAt.Before(stored.UpdatedAt), "updated_at moves forward")
}

func TestFederationPrefs_SourceMustBeStated(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewFederationPrefs(database)
	ctx := context.Background()

	_, err := repo.Upsert(ctx, FederationPref{DID: testDID, Enabled: false})
	require.Error(t, err,
		"the zero source is invalid: 'we read a record' and 'we went and asked' have "+
			"different staleness, and a defaulted source hides which one applied")
	assert.True(t, errors.IsValidation(err), "want validation error, got %v", err)

	_, err = repo.Upsert(ctx, FederationPref{
		DID: testDID, Enabled: false, Source: FederationPrefSource("guess"),
	})
	require.Error(t, err, "an unknown source must be rejected")
	assert.True(t, errors.IsValidation(err), "want validation error, got %v", err)
}

func TestFederationPrefs_DeleteRestoresDefaultOn(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewFederationPrefs(database)
	ctx := context.Background()

	_, err := repo.Upsert(ctx, FederationPref{
		DID: testDID, Enabled: false, Source: FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	require.NoError(t, repo.Delete(ctx, testDID),
		"deleting the opt-out record removes the row — absence IS the default-on state")

	_, err = repo.Get(ctx, testDID)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err), "want NotFound after delete, got %v", err)

	require.NoError(t, repo.Delete(ctx, testSecondDID),
		"deleting a preference that never existed is a no-op success: a record delete "+
			"for a user who never opted out is the common case")
}
