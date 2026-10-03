package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

const (
	qualificationAuthorDID     = "did:plc:000000000000000000000001"
	qualificationOtherDID      = "did:plc:000000000000000000000002"
	qualificationCommunityDID  = "did:plc:000000000000000000000003"
	qualificationUnfollowedDID = "did:plc:000000000000000000000004"
	qualificationRootAuthorDID = "did:plc:000000000000000000000005"
)

type qualificationFixture struct {
	t           *testing.T
	db          *sql.DB
	ctx         context.Context
	actors      BridgedActors
	communities Communities
	objects     APObjects
}

func newQualificationFixture(t *testing.T) *qualificationFixture {
	t.Helper()
	db := testDB(t) // Truncates ap_objects, bridged_actors and communities.
	return &qualificationFixture{t: t, db: db, ctx: context.Background(),
		actors: NewBridgedActors(db), communities: NewCommunities(db), objects: NewAPObjects(db)}
}

func qualificationTime(t *testing.T, value string) time.Time {
	t.Helper()
	when, err := time.Parse(time.RFC3339, value)
	require.NoError(t, err)
	return when
}

func (f *qualificationFixture) actor(handle, did string, consent ConsentState, createdAt time.Time, actorType ActorType) {
	f.t.Helper()
	if consent == "" {
		consent = ConsentStateOK
	}
	apID := "https://lemmy.example/actor/" + handle
	_, err := f.actors.UpsertActor(f.ctx, BridgedActor{
		APActorID: apID, ActorType: actorType, DID: did, Handle: handle,
		ConsentState: ConsentStateOK, SigningKeyEncrypted: []byte("fixture-key"),
	})
	require.NoError(f.t, err)
	if consent != ConsentStateOK {
		require.NoError(f.t, f.actors.SetConsentState(f.ctx, apID, consent))
	}
	_, err = f.db.ExecContext(f.ctx, `UPDATE bridged_actors SET created_at = $2 WHERE ap_actor_id = $1`, apID, createdAt)
	require.NoError(f.t, err)
}

func (f *qualificationFixture) community(did string, state FollowState) {
	f.t.Helper()
	apID := "https://lemmy.example/group/" + did
	_, err := f.communities.UpsertCommunity(f.ctx, Community{
		APGroupID: apID, DID: did, PreferredUsername: "fixture", Instance: "lemmy.example",
	})
	require.NoError(f.t, err)
	require.NoError(f.t, f.communities.SetFollowState(f.ctx, apID, state))
}

type qualificationMapping struct {
	name, authorDID, did, collection, communityDID, threadRoot string
	indexedAt                                                  time.Time
	deletedAt, publishedAt                                     *time.Time
	// notAnnounced leaves the row as it would be after a bare delivery or an
	// ancestor fetch; by default fixtures arrived inside the community's Announce.
	notAnnounced bool
}

func (f *qualificationFixture) mapping(row qualificationMapping) *APObjectMapping {
	f.t.Helper()
	inserted, err := f.objects.PutMapping(f.ctx, APObjectMapping{
		APID: "https://lemmy.example/object/" + row.name, APType: "Page", OriginInstance: "lemmy.example",
		AuthorDID: row.authorDID, DID: row.did, Collection: row.collection, RKey: row.name,
		CommunityDID: row.communityDID, ThreadRootATURI: row.threadRoot, CID: testCID,
	})
	require.NoError(f.t, err)
	arrival := "community_announced"
	if row.notAnnounced {
		arrival = "not_announced"
	}
	_, err = f.db.ExecContext(f.ctx, `UPDATE ap_objects SET indexed_at = $2, deleted_at = $3, ap_published_at = $4, arrival = $5 WHERE id = $1`,
		inserted.ID, row.indexedAt, row.deletedAt, row.publishedAt, arrival)
	require.NoError(f.t, err)
	return inserted
}

func assertQualificationContributions(t *testing.T, got, want []Contribution) {
	t.Helper()
	require.Len(t, got, len(want))
	for index, expected := range want {
		assert.Equal(t, expected.ATURI, got[index].ATURI)
		assert.Equal(t, expected.Collection, got[index].Collection)
		assert.Equal(t, expected.CommunityDID, got[index].CommunityDID)
		assert.True(t, got[index].IndexedAt.Equal(expected.IndexedAt), "row %d indexed_at: got %s, want %s", index, got[index].IndexedAt, expected.IndexedAt)
		assert.Equal(t, expected.ID, got[index].ID)
		assert.Equal(t, expected.RootATURI, got[index].RootATURI)
		assert.Equal(t, expected.RootCollection, got[index].RootCollection)
		assert.Equal(t, expected.RootCommunityDID, got[index].RootCommunityDID)
	}
}

func TestBridgedActors_UpsertIsIdempotent(t *testing.T) {
	database := testDB(t)
	repo := NewBridgedActors(database)
	ctx := context.Background()

	first, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)
	assert.Equal(t, ConsentStateOK, first.ConsentState)
	assert.NotZero(t, first.CreatedAt)

	// Re-upserting with new non-empty values updates the mutable fields
	// and nothing else.
	updated := testActor()
	updated.Handle = "alice-renamed.lemmy-world.tidepool.example"
	updated.SigningKeyEncrypted = []byte("rotated-key-bytes")
	second, err := repo.UpsertActor(ctx, updated)
	require.NoError(t, err)

	assert.Equal(t, first.ID, second.ID, "upsert must reuse the existing row")
	assert.Equal(t, "alice-renamed.lemmy-world.tidepool.example", second.Handle)
	assert.Equal(t, []byte("rotated-key-bytes"), second.SigningKeyEncrypted)
	assert.Equal(t, first.DID, second.DID)
	assert.Equal(t, first.CreatedAt, second.CreatedAt)

	var total int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM bridged_actors`).Scan(&total))
	assert.Equal(t, 1, total)
}

func TestBridgedActors_UpsertPreservesHandleAndKeyWhenOmitted(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	first, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)
	require.NotEmpty(t, first.Handle)
	require.NotEmpty(t, first.SigningKeyEncrypted)

	// A profile refresh built purely from AP data carries no handle and no
	// key material; upserting it must not clobber the escrowed values.
	refresh := testActor()
	refresh.Handle = ""
	refresh.SigningKeyEncrypted = nil
	refreshed, err := repo.UpsertActor(ctx, refresh)
	require.NoError(t, err)

	assert.Equal(t, first.Handle, refreshed.Handle, "empty handle must not clobber the stored handle")
	assert.Equal(t, first.SigningKeyEncrypted, refreshed.SigningKeyEncrypted,
		"nil signing key must not clobber the escrowed key")
}

func TestBridgedActors_UpsertOnDeletedActorIsFrozen(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	original, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)
	require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateDeleted))

	// A tombstoned actor is frozen: the upsert modifies nothing — not even
	// the normally-mutable handle and key — and returns the stored row.
	attempt := testActor()
	attempt.Handle = "necromancer.lemmy-world.tidepool.example"
	attempt.SigningKeyEncrypted = []byte("fresh-key-bytes")
	frozen, err := repo.UpsertActor(ctx, attempt)
	require.NoError(t, err)

	assert.Equal(t, original.ID, frozen.ID)
	assert.Equal(t, ConsentStateDeleted, frozen.ConsentState, "consent must stay deleted")
	assert.Equal(t, original.Handle, frozen.Handle, "handle must stay frozen on a deleted actor")
	assert.Equal(t, original.SigningKeyEncrypted, frozen.SigningKeyEncrypted,
		"signing key must stay frozen on a deleted actor")

	stored, err := repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	assert.Equal(t, ConsentStateDeleted, stored.ConsentState)
	assert.Equal(t, original.Handle, stored.Handle)
	assert.Equal(t, original.SigningKeyEncrypted, stored.SigningKeyEncrypted)
}

func TestBridgedActors_UpsertPreservesConsentState(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	_, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)
	require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateNoBridge))

	// A profile refresh (upsert) must not silently flip consent back,
	// even though the incoming actor states ConsentStateOK.
	refreshed, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)
	assert.Equal(t, ConsentStateNoBridge, refreshed.ConsentState)
}

func TestBridgedActors_UpsertRejectsIdentityDrift(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	original, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)

	// Same AP actor id arriving with a different DID must be a conflict,
	// not a silent success that keeps the old DID.
	drifted := testActor()
	drifted.DID = testSecondDID
	_, err = repo.UpsertActor(ctx, drifted)
	assert.True(t, errors.IsAlreadyExists(err), "changed DID must conflict, got %v", err)

	// Same for a changed actor type.
	retyped := testActor()
	retyped.ActorType = ActorTypeGroup
	_, err = repo.UpsertActor(ctx, retyped)
	assert.True(t, errors.IsAlreadyExists(err), "changed actor_type must conflict, got %v", err)

	// The stored row is untouched by the rejected upserts.
	stored, err := repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	assert.Equal(t, original.DID, stored.DID)
	assert.Equal(t, original.ActorType, stored.ActorType)
}

func TestBridgedActors_Get(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	stored, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)

	byAPID, err := repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	assert.Equal(t, stored.ID, byAPID.ID)

	byDID, err := repo.GetByDID(ctx, testDID)
	require.NoError(t, err)
	assert.Equal(t, stored.ID, byDID.ID)

	_, err = repo.GetByAPActorID(ctx, "https://lemmy.world/u/nobody")
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
	_, err = repo.GetByDID(ctx, testSecondDID)
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}

func TestBridgedActors_GetByHandle(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	stored, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)

	byHandle, err := repo.GetByHandle(ctx, stored.Handle)
	require.NoError(t, err)
	assert.Equal(t, stored.ID, byHandle.ID)

	_, err = repo.GetByHandle(ctx, "nobody.lemmy-world.tidepool.example")
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)

	// Actors without a handle yet (NULL) must not match anything.
	unhandled := testActor()
	unhandled.APActorID = "https://lemmy.world/u/bob"
	unhandled.DID = testSecondDID
	unhandled.Handle = ""
	_, err = repo.UpsertActor(ctx, unhandled)
	require.NoError(t, err)
	_, err = repo.GetByHandle(ctx, "")
	assert.True(t, errors.IsNotFound(err), "empty handle must not match NULL-handle rows")
}

func TestBridgedActors_ConsentStateTransitions(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	_, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)

	// ok -> nobridge (actor added #nobridge to their bio) and back
	// (they removed it).
	require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateNoBridge))
	actor, err := repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	assert.Equal(t, ConsentStateNoBridge, actor.ConsentState)

	require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateOK))
	actor, err = repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	assert.Equal(t, ConsentStateOK, actor.ConsentState)

	// Delete(Actor) tombstones; deleting twice is idempotent.
	require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateDeleted))
	require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateDeleted))

	// Deleted is terminal.
	err = repo.SetConsentState(ctx, testAPActorID, ConsentStateOK)
	assert.True(t, errors.IsValidation(err), "leaving deleted must fail validation, got %v", err)
	err = repo.SetConsentState(ctx, testAPActorID, ConsentStateNoBridge)
	assert.True(t, errors.IsValidation(err), "leaving deleted must fail validation, got %v", err)

	actor, err = repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	assert.Equal(t, ConsentStateDeleted, actor.ConsentState)

	// Unknown states and unknown actors are rejected.
	err = repo.SetConsentState(ctx, testAPActorID, ConsentState("banished"))
	assert.True(t, errors.IsValidation(err), "expected validation error, got %v", err)
	err = repo.SetConsentState(ctx, "https://lemmy.world/u/nobody", ConsentStateOK)
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}

func TestBridgedActors_MarkProfileSynced(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	stored, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)
	assert.Nil(t, stored.ProfileSyncedAt)

	syncedAt := time.Date(2026, 6, 1, 8, 30, 0, 0, time.UTC)
	require.NoError(t, repo.MarkProfileSynced(ctx, testAPActorID, syncedAt))

	actor, err := repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	require.NotNil(t, actor.ProfileSyncedAt)
	assert.True(t, actor.ProfileSyncedAt.Equal(syncedAt))

	err = repo.MarkProfileSynced(ctx, "https://lemmy.world/u/nobody", syncedAt)
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}

// TestBridgedActors_UpsertValidation is pure input validation: it never
// touches postgres, so it runs without TIDEPOOL_TEST_DATABASE_URL.
func TestBridgedActors_UpsertValidation(t *testing.T) {
	repo := NewBridgedActors(nil)
	ctx := context.Background()

	cases := []struct {
		name   string
		mutate func(*BridgedActor)
	}{
		{"empty ap_actor_id", func(a *BridgedActor) { a.APActorID = "" }},
		{"invalid actor_type", func(a *BridgedActor) { a.ActorType = "service" }},
		{"invalid did", func(a *BridgedActor) { a.DID = "not-a-did" }},
		{"invalid handle", func(a *BridgedActor) { a.Handle = "no spaces allowed" }},
		{"invalid consent_state", func(a *BridgedActor) { a.ConsentState = "banished" }},
		// Consent must never default: the zero value failing open to
		// "consented" would be a consent bug.
		{"empty consent_state", func(a *BridgedActor) { a.ConsentState = "" }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			actor := testActor()
			testCase.mutate(&actor)
			_, err := repo.UpsertActor(ctx, actor)
			assert.True(t, errors.IsValidation(err), "expected validation error, got %v", err)
		})
	}
}

func TestBridgedActors_DIDCollisionIsConflict(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	_, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)

	// A different AP actor must not claim the same DID.
	collision := testActor()
	collision.APActorID = "https://lemmy.world/u/mallory"
	collision.Handle = "mallory.lemmy-world.tidepool.example"
	_, err = repo.UpsertActor(ctx, collision)
	assert.True(t, errors.IsAlreadyExists(err), "expected IsAlreadyExists, got %v", err)
}

func TestBridgedActors_HandleCollisionIsConflict(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	_, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)

	// atproto handles are 1:1 with DIDs: a different actor (different AP
	// id, different DID) must not claim the same handle.
	collision := testActor()
	collision.APActorID = "https://lemmy.world/u/mallory"
	collision.DID = testSecondDID
	_, err = repo.UpsertActor(ctx, collision)
	assert.True(t, errors.IsAlreadyExists(err), "expected IsAlreadyExists, got %v", err)

	// Unassigned handles (NULL) may repeat freely.
	noHandle := testActor()
	noHandle.APActorID = "https://lemmy.world/u/newcomer"
	noHandle.DID = testSecondDID
	noHandle.Handle = ""
	_, err = repo.UpsertActor(ctx, noHandle)
	assert.NoError(t, err, "actors without handles must not collide")
}

func TestBridgedActors_ListInstanceLabels(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()
	for index, fixture := range []struct {
		handle  string
		consent ConsentState
	}{
		{"alice.a.tdpl.example", ConsentStateOK},
		{"bob.b.tdpl.example", ConsentStateDeleted},
		{"carol.b.tdpl.example", ConsentStateDeleted},
		{"dave.c.tdpl.example", ConsentStateDeleted},
		{"eve.c.tdpl.example", ConsentStateOK},
		{"frank.n.tdpl.example", ConsentStateNoBridge},
		{"user.foreign.nottdpl.example", ConsentStateOK},
		{"x.d.other.example", ConsentStateOK},
	} {
		actor := testActor()
		actor.APActorID = fmt.Sprintf("https://lemmy.example/u/instance-label-%d", index)
		actor.DID = fmt.Sprintf("did:plc:%024d", index+1)
		actor.Handle = fixture.handle
		actor.ConsentState = ConsentStateOK
		_, err := repo.UpsertActor(ctx, actor)
		require.NoError(t, err, "insert %s", fixture.handle)
		if fixture.consent != ConsentStateOK {
			require.NoError(t, repo.SetConsentState(ctx, actor.APActorID, fixture.consent))
		}
	}

	labels, err := repo.ListInstanceLabels(ctx, "tdpl.example")
	require.NoError(t, err)
	assert.ElementsMatch(t, []InstanceLabel{
		{Label: "a", HasLiveActor: true},
		{Label: "b", HasLiveActor: false},
		{Label: "c", HasLiveActor: true},
		{Label: "n", HasLiveActor: true},
	}, labels)
}

func TestBridgedActors_ListLabelContributionsFilters(t *testing.T) {
	cases := []struct {
		name, handle, indexedAt, collection, publishedAt string
		consent                                          ConsentState
		follow                                           FollowState
		deleted, rootDeleted, notAnnounced, wantRow      bool
	}{
		{name: "old comment in followed community", handle: "alice.a.tdpl.example", indexedAt: "2026-09-01T10:00:00Z", wantRow: true},
		{name: "indexed exactly at cutoff", handle: "alice.a.tdpl.example", indexedAt: "2026-09-01T10:40:00Z", wantRow: true},
		{name: "nobridge author is live", handle: "alice.a.tdpl.example", indexedAt: "2026-09-01T10:00:00Z", consent: ConsentStateNoBridge, wantRow: true},
		{name: "ten minutes old", handle: "alice.a.tdpl.example", indexedAt: "2026-09-01T10:50:00Z"},
		{name: "soft deleted mapping", handle: "alice.a.tdpl.example", indexedAt: "2026-09-01T10:00:00Z", deleted: true},
		{name: "community not followed", handle: "alice.a.tdpl.example", indexedAt: "2026-09-01T10:00:00Z", follow: FollowStateNone},
		{name: "deleted author with another live actor on label", handle: "alice.a.tdpl.example", indexedAt: "2026-09-01T10:00:00Z", consent: ConsentStateDeleted},
		{name: "other label", handle: "alice.b.tdpl.example", indexedAt: "2026-09-01T10:00:00Z"},
		{name: "unsupported collection", handle: "alice.a.tdpl.example", indexedAt: "2026-09-01T10:00:00Z", collection: "social.coves.feed.vote"},
		{name: "published before but indexed after cutoff", handle: "alice.a.tdpl.example", indexedAt: "2026-09-01T10:50:00Z", publishedAt: "2026-09-01T10:00:00Z"},
		{name: "different zone root", handle: "alice.a.foreign.example", indexedAt: "2026-09-01T10:00:00Z"},
		{name: "extra handle label depth", handle: "alice.a.deep.tdpl.example", indexedAt: "2026-09-01T10:00:00Z"},
		{name: "not announced by the community", handle: "alice.a.tdpl.example", indexedAt: "2026-09-01T10:00:00Z", notAnnounced: true},
		{name: "thread root deleted", handle: "alice.a.tdpl.example", indexedAt: "2026-09-01T10:00:00Z", rootDeleted: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newQualificationFixture(t)
			created := qualificationTime(t, "2026-09-01T09:00:00Z")
			f.actor(tc.handle, qualificationAuthorDID, tc.consent, created, ActorTypePerson)
			if tc.consent == ConsentStateDeleted {
				f.actor("bob.a.tdpl.example", qualificationOtherDID, ConsentStateOK, created, ActorTypePerson)
			}
			follow := tc.follow
			if follow == "" {
				follow = FollowStateAccepted
			}
			f.community(qualificationCommunityDID, follow)
			collection := tc.collection
			if collection == "" {
				collection = "social.coves.community.comment"
			}
			var deletedAt, publishedAt *time.Time
			if tc.deleted {
				deletedAt = &created
			}
			if tc.publishedAt != "" {
				published := qualificationTime(t, tc.publishedAt)
				publishedAt = &published
			}
			// A comment counts only while its thread root stands. The root's
			// author has no handle, so the root itself is never a contribution.
			var rootDeletedAt *time.Time
			if tc.rootDeleted {
				rootDeletedAt = &created
			}
			root := f.mapping(qualificationMapping{name: "filterroot", authorDID: qualificationRootAuthorDID,
				did: qualificationCommunityDID, collection: "social.coves.community.post",
				indexedAt: created, deletedAt: rootDeletedAt})
			row := f.mapping(qualificationMapping{name: "filtercase", authorDID: qualificationAuthorDID,
				did: qualificationAuthorDID, collection: collection, communityDID: qualificationCommunityDID, threadRoot: root.ATURI,
				indexedAt: qualificationTime(t, tc.indexedAt), deletedAt: deletedAt, publishedAt: publishedAt, notAnnounced: tc.notAnnounced})
			got, err := f.actors.ListLabelContributions(f.ctx, "tdpl.example", "a",
				qualificationTime(t, "2026-09-01T10:40:00Z"), ContributionCursor{}, 10)
			require.NoError(t, err)
			if !tc.wantRow {
				assert.Empty(t, got)
				return
			}
			assertQualificationContributions(t, got, []Contribution{{
				ATURI:      "at://did:plc:000000000000000000000001/social.coves.community.comment/filtercase",
				Collection: "social.coves.community.comment", CommunityDID: "did:plc:000000000000000000000003",
				IndexedAt: qualificationTime(t, tc.indexedAt), ID: row.ID,
				RootATURI:      "at://did:plc:000000000000000000000003/social.coves.community.post/filterroot",
				RootCollection: "social.coves.community.post", RootCommunityDID: "did:plc:000000000000000000000003",
			}})
		})
	}
}

func TestBridgedActors_ListLabelContributionsResolvesCommunities(t *testing.T) {
	f := newQualificationFixture(t)
	indexed := qualificationTime(t, "2026-09-01T10:00:00Z")
	f.actor("alice.a.tdpl.example", qualificationAuthorDID, ConsentStateOK, indexed, ActorTypePerson)
	f.community(qualificationCommunityDID, FollowStateAccepted)
	f.community(qualificationUnfollowedDID, FollowStateNone)
	postv2Root := f.mapping(qualificationMapping{name: "postv2root", authorDID: qualificationOtherDID,
		did: qualificationOtherDID, collection: "social.coves.community.postv2", communityDID: qualificationCommunityDID, indexedAt: indexed})
	legacyRoot := f.mapping(qualificationMapping{name: "legacyroot", authorDID: qualificationOtherDID,
		did: qualificationCommunityDID, collection: "social.coves.community.post", indexedAt: indexed})
	unfollowedRoot := f.mapping(qualificationMapping{name: "unfollowedroot", authorDID: qualificationOtherDID,
		did: qualificationOtherDID, collection: "social.coves.community.postv2", communityDID: qualificationUnfollowedDID, indexedAt: indexed})
	deletedRoot := f.mapping(qualificationMapping{name: "deletedroot", authorDID: qualificationOtherDID,
		did: qualificationOtherDID, collection: "social.coves.community.postv2", communityDID: qualificationCommunityDID,
		indexedAt: indexed, deletedAt: &indexed})
	cases := []struct {
		name, did, collection, communityDID, root, atURI string
		rootCollection, rootCommunityDID                 string
		want                                             bool
	}{
		{name: "postv2", did: qualificationAuthorDID, collection: "social.coves.community.postv2", communityDID: qualificationCommunityDID,
			atURI: "at://did:plc:000000000000000000000001/social.coves.community.postv2/postv2", want: true},
		{name: "legacy", did: qualificationCommunityDID, collection: "social.coves.community.post",
			atURI: "at://did:plc:000000000000000000000003/social.coves.community.post/legacy", want: true},
		{name: "postv2 root fallback", did: qualificationAuthorDID, collection: "social.coves.community.comment", root: postv2Root.ATURI,
			atURI:          "at://did:plc:000000000000000000000001/social.coves.community.comment/postv2rootfallback",
			rootCollection: "social.coves.community.postv2", rootCommunityDID: "did:plc:000000000000000000000003", want: true},
		{name: "legacy root fallback", did: qualificationAuthorDID, collection: "social.coves.community.comment", root: legacyRoot.ATURI,
			atURI:          "at://did:plc:000000000000000000000001/social.coves.community.comment/legacyrootfallback",
			rootCollection: "social.coves.community.post", rootCommunityDID: "did:plc:000000000000000000000003", want: true},
		{name: "own community takes precedence", did: qualificationAuthorDID, collection: "social.coves.community.comment", communityDID: qualificationCommunityDID, root: unfollowedRoot.ATURI,
			atURI:          "at://did:plc:000000000000000000000001/social.coves.community.comment/owncommunitytakesprecedence",
			rootCollection: "social.coves.community.postv2", rootCommunityDID: "did:plc:000000000000000000000004", want: true},
		{name: "missing root", did: qualificationAuthorDID, collection: "social.coves.community.comment", root: "at://did:plc:000000000000000000000002/social.coves.community.postv2/missing",
			atURI: "at://did:plc:000000000000000000000001/social.coves.community.comment/missingroot"},
		{name: "missing root with own community", did: qualificationAuthorDID, collection: "social.coves.community.comment", communityDID: qualificationCommunityDID,
			root:  "at://did:plc:000000000000000000000002/social.coves.community.postv2/missing",
			atURI: "at://did:plc:000000000000000000000001/social.coves.community.comment/missingrootowncommunity"},
		{name: "deleted root with own community", did: qualificationAuthorDID, collection: "social.coves.community.comment", communityDID: qualificationCommunityDID,
			root: deletedRoot.ATURI, atURI: "at://did:plc:000000000000000000000001/social.coves.community.comment/deletedrootowncommunity"},
	}
	var want []Contribution
	for _, tc := range cases {
		// Names are stable record keys; expected AT-URIs are independent literals.
		key := map[string]string{"postv2 root fallback": "postv2rootfallback", "legacy root fallback": "legacyrootfallback",
			"own community takes precedence": "owncommunitytakesprecedence", "missing root": "missingroot",
			"missing root with own community": "missingrootowncommunity", "deleted root with own community": "deletedrootowncommunity"}[tc.name]
		if key == "" {
			key = tc.name
		}
		row := f.mapping(qualificationMapping{name: key, authorDID: qualificationAuthorDID,
			did: tc.did, collection: tc.collection, communityDID: tc.communityDID, threadRoot: tc.root, indexedAt: indexed})
		if tc.want {
			want = append(want, Contribution{ATURI: tc.atURI, Collection: tc.collection,
				CommunityDID: "did:plc:000000000000000000000003", IndexedAt: indexed, ID: row.ID,
				RootATURI: tc.root, RootCollection: tc.rootCollection, RootCommunityDID: tc.rootCommunityDID})
		}
	}
	got, err := f.actors.ListLabelContributions(f.ctx, "tdpl.example", "a", qualificationTime(t, "2026-09-01T11:00:00Z"), ContributionCursor{}, 20)
	require.NoError(t, err)
	assertQualificationContributions(t, got, want)
}

func TestBridgedActors_ListLabelContributionsKeysetPages(t *testing.T) {
	f := newQualificationFixture(t)
	indexed := qualificationTime(t, "2026-09-01T10:00:00Z")
	f.actor("alice.a.tdpl.example", qualificationAuthorDID, ConsentStateOK, indexed, ActorTypePerson)
	f.community(qualificationCommunityDID, FollowStateAccepted)
	times := []string{"2026-09-01T10:00:00Z", "2026-09-01T10:01:00Z", "2026-09-01T10:01:00Z", "2026-09-01T10:02:00Z", "2026-09-01T10:03:00Z"}
	uri := []string{
		"at://did:plc:000000000000000000000001/social.coves.community.comment/pageone",
		"at://did:plc:000000000000000000000001/social.coves.community.comment/pagetwo",
		"at://did:plc:000000000000000000000001/social.coves.community.comment/pagethree",
		"at://did:plc:000000000000000000000001/social.coves.community.comment/pagefour",
		"at://did:plc:000000000000000000000001/social.coves.community.comment/pagefive",
	}
	keys := []string{"pageone", "pagetwo", "pagethree", "pagefour", "pagefive"}
	root := f.mapping(qualificationMapping{name: "pageroot", authorDID: qualificationRootAuthorDID, did: qualificationCommunityDID,
		collection: "social.coves.community.post", indexedAt: indexed})
	var want []Contribution
	for i, key := range keys {
		when := qualificationTime(t, times[i])
		row := f.mapping(qualificationMapping{name: key, authorDID: qualificationAuthorDID, did: qualificationAuthorDID,
			collection: "social.coves.community.comment", communityDID: qualificationCommunityDID, threadRoot: root.ATURI, indexedAt: when})
		want = append(want, Contribution{ATURI: uri[i], Collection: "social.coves.community.comment",
			CommunityDID: "did:plc:000000000000000000000003", IndexedAt: when, ID: row.ID,
			RootATURI:      "at://did:plc:000000000000000000000003/social.coves.community.post/pageroot",
			RootCollection: "social.coves.community.post", RootCommunityDID: "did:plc:000000000000000000000003"})
	}
	cursor := ContributionCursor{}
	for page, expected := range [][]Contribution{want[:2], want[2:4], want[4:]} {
		got, err := f.actors.ListLabelContributions(f.ctx, "tdpl.example", "a", qualificationTime(t, "2026-09-01T11:00:00Z"), cursor, 2)
		require.NoError(t, err, "page %d", page+1)
		assertQualificationContributions(t, got, expected)
		cursor = ContributionCursor{IndexedAt: got[len(got)-1].IndexedAt, ID: got[len(got)-1].ID}
	}
	got, err := f.actors.ListLabelContributions(f.ctx, "tdpl.example", "a", qualificationTime(t, "2026-09-01T11:00:00Z"), cursor, 2)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestBridgedActors_ListOutrightQualifiedLabelsFollowedCommunity(t *testing.T) {
	cases := []struct {
		name            string
		follow          FollowState
		consent         ConsentState
		requested, want bool
	}{
		{name: "accepted", follow: FollowStateAccepted, consent: ConsentStateOK, requested: true, want: true},
		{name: "pending", follow: FollowStatePending, consent: ConsentStateOK, requested: true},
		{name: "none", follow: FollowStateNone, consent: ConsentStateOK, requested: true},
		{name: "deleted community actor", follow: FollowStateAccepted, consent: ConsentStateDeleted, requested: true},
		{name: "unrequested label", follow: FollowStateAccepted, consent: ConsentStateOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newQualificationFixture(t)
			created := qualificationTime(t, "2026-10-04T10:00:00Z")
			f.actor("group.a.tdpl.example", qualificationCommunityDID, tc.consent, created, ActorTypeGroup)
			f.community(qualificationCommunityDID, tc.follow)
			labels := []string{"other"}
			if tc.requested {
				labels = []string{"a"}
			}
			got, err := f.actors.ListOutrightQualifiedLabels(f.ctx, "tdpl.example", labels,
				qualificationTime(t, "2026-10-03T00:00:00Z"))
			require.NoError(t, err)
			if !tc.want {
				assert.Empty(t, got)
				return
			}
			require.Len(t, got, 1)
			assert.Equal(t, "a", got[0].Label)
			assert.True(t, got[0].QualifiedAt.Equal(created), "qualified_at: got %s, want %s", got[0].QualifiedAt, created)
		})
	}
}

func TestBridgedActors_ListOutrightQualifiedLabelsGrandfather(t *testing.T) {
	cases := []struct {
		name                        string
		firstCreated, secondCreated string
		firstConsent                ConsentState
		secondFollow                FollowState
		wantAt                      string
	}{
		{name: "before cutoff", firstCreated: "2026-10-02T23:00:00Z", wantAt: "2026-10-02T23:00:00Z"},
		{name: "equal cutoff", firstCreated: "2026-10-03T00:00:00Z"},
		{name: "after cutoff", firstCreated: "2026-10-03T01:00:00Z"},
		{name: "deleted old actor with live post-cutoff actor", firstCreated: "2026-10-02T23:00:00Z", firstConsent: ConsentStateDeleted,
			secondCreated: "2026-10-04T10:00:00Z"},
		{name: "earlier grandfather and later followed community", firstCreated: "2026-10-02T23:00:00Z",
			secondCreated: "2026-10-04T10:00:00Z", secondFollow: FollowStateAccepted, wantAt: "2026-10-02T23:00:00Z"},
		{name: "earliest unqualified actor before later followed community", firstCreated: "2026-10-03T01:00:00Z",
			secondCreated: "2026-10-04T10:00:00Z", secondFollow: FollowStateAccepted, wantAt: "2026-10-04T10:00:00Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newQualificationFixture(t)
			f.actor("alice.a.tdpl.example", qualificationAuthorDID, tc.firstConsent,
				qualificationTime(t, tc.firstCreated), ActorTypePerson)
			if tc.secondCreated != "" {
				f.actor("group.a.tdpl.example", qualificationCommunityDID, ConsentStateOK,
					qualificationTime(t, tc.secondCreated), ActorTypeGroup)
				if tc.secondFollow != "" {
					f.community(qualificationCommunityDID, tc.secondFollow)
				}
			}
			got, err := f.actors.ListOutrightQualifiedLabels(f.ctx, "tdpl.example", []string{"a"},
				qualificationTime(t, "2026-10-03T00:00:00Z"))
			require.NoError(t, err)
			if tc.wantAt == "" {
				assert.Empty(t, got)
				return
			}
			require.Len(t, got, 1, "a must occur once even with two qualifying actors")
			assert.Equal(t, "a", got[0].Label)
			want := qualificationTime(t, tc.wantAt)
			assert.True(t, got[0].QualifiedAt.Equal(want), "qualified_at: got %s, want %s", got[0].QualifiedAt, want)
		})
	}
}
