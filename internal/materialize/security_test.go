package materialize

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rivo/uniseg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/store"
)

// mustObject parses a synthetic AP document (Go map) into an ap.Object — the
// shape the materializer receives from the ingestion layer.
func mustObject(t *testing.T, doc map[string]any) *ap.Object {
	t.Helper()
	body, err := json.Marshal(doc)
	require.NoError(t, err)
	obj, err := ap.ParseObject(body)
	require.NoError(t, err)
	return obj
}

// group builds a minimal synthetic Group (Lemmy community shape).
func group(id, username string, extra map[string]any) map[string]any {
	doc := map[string]any{
		"type":              "Group",
		"id":                id,
		"preferredUsername": username,
		"name":              username,
		"inbox":             id + "/inbox",
		"published":         "2024-01-01T00:00:00.000000Z",
	}
	for k, v := range extra {
		doc[k] = v
	}
	return doc
}

// page builds a minimal synthetic Page (Lemmy post shape).
func page(id, author, groupIRI, title, published string) map[string]any {
	return map[string]any{
		"type":         "Page",
		"id":           id,
		"attributedTo": author,
		"audience":     groupIRI,
		"to":           []any{ap.PublicAudience},
		"name":         title,
		"source":       map[string]any{"content": "body text", "mediaType": "text/markdown"},
		"published":    published,
	}
}

// TestCommentThreadRootedAtNote_SkipsWithoutPanic covers the crash regression:
// a comment whose ancestor chain tops out at a parentless Note (a Mastodon
// status that federated in) must be dropped as a skip, not dereference a nil
// inReplyTo.
func TestCommentThreadRootedAtNote_SkipsWithoutPanic(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// A root Note with no inReplyTo, served upstream. Author and object share
	// an authority throughout, as genuine traffic does, so the skip below can
	// only be the parentless root and not the attribution check.
	rootNote := map[string]any{
		"type":         "Note",
		"id":           "https://lemmy.zip/comment/root",
		"attributedTo": "https://lemmy.zip/u/carol",
		"audience":     groupID,
		"source":       map[string]any{"content": "root", "mediaType": "text/markdown"},
		"published":    "2024-01-02T00:00:00.000000Z",
	}
	h.serveObject("/comment/root", rootNote)

	child := note("https://lemmy.zip/comment/child", "https://lemmy.zip/u/carol",
		"https://lemmy.zip/comment/root", "child", "2024-01-02T01:00:00.000000Z")

	res, err := h.m.MaterializeComment(ctx, mustObject(t, child), groupID)
	require.Nil(t, res)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "parentless-Note root must be a skip, got %v", err)
}

// TestAncestorCrossAuthorityID_Skips covers the forgery fix: an ancestor
// fetched at one instance's IRI that returns a body claiming another
// instance's id is rejected (the id would otherwise become the ap_objects
// mapping key).
func TestAncestorCrossAuthorityID_Skips(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// The parent path is under lemmy.world but the served body claims an
	// evil.example id.
	forged := map[string]any{
		"type":         "Note",
		"id":           "https://evil.example/x",
		"attributedTo": personID,
		"audience":     groupID,
		"source":       map[string]any{"content": "forged", "mediaType": "text/markdown"},
		"published":    "2024-01-02T00:00:00.000000Z",
		"inReplyTo":    groupID,
	}
	h.serveObject("/comment/parent", forged)

	child := note("https://lemmy.world/comment/child", personID,
		"https://lemmy.world/comment/parent", "child", "2024-01-02T01:00:00.000000Z")

	res, err := h.m.MaterializeComment(ctx, mustObject(t, child), groupID)
	require.Nil(t, res)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "cross-authority ancestor id must be a skip, got %v", err)
	assert.Contains(t, err.Error(), "cross-authority")
}

// TestCreateAfterDelete_DoesNotResurrect covers unordered delivery: once an
// object's mapping is tombstoned, a re-delivered Create/Update must not
// resurrect it.
func TestCreateAfterDelete_DoesNotResurrect(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	pageObj := loadFixtureObject(t, "page_lemmy_world.json")
	_, err := h.m.MaterializePost(ctx, pageObj, groupID)
	require.NoError(t, err)

	require.NoError(t, h.m.HandleDelete(ctx, pageObj.ID))
	before := len(h.firehoseEvents())

	res, err := h.m.MaterializePost(ctx, pageObj, groupID)
	require.Nil(t, res)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "re-create after delete must be a skip, got %v", err)

	mapping, err := h.objects.GetByAPID(ctx, pageObj.ID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "mapping must stay tombstoned")
	assert.Equal(t, before, len(h.firehoseEvents()), "no new commit must be emitted")
}

// TestNobridgeOnRefresh_ScrubsExistingContent covers the consent fix: when a
// previously-bridged actor adds #nobridge, a refresh scrubs their existing
// records and suspends bridging (reversibly).
func TestNobridgeOnRefresh_ScrubsExistingContent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const personIRI = "https://lemmy.world/u/scrubme"
	const groupIRI = "https://lemmy.world/c/general"
	const pageIRI = "https://lemmy.world/post/1001"

	// Serve a person whose summary we can flip to #nobridge on refresh.
	summary := ""
	h.mux.HandleFunc("GET /u/scrubme", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
		body, _ := json.Marshal(person(personIRI, "scrubme", map[string]any{"summary": summary}))
		_, _ = w.Write(body)
	})
	h.serveObject("/c/general", group(groupIRI, "general", nil))

	// First pass: bridge + materialize the post (author = scrubme).
	_, err := h.m.MaterializePost(ctx, mustObject(t, page(pageIRI, personIRI, groupIRI, "hello", "2024-02-01T00:00:00.000000Z")), groupIRI)
	require.NoError(t, err)

	authorDID := testDIDFor("scrubme", "lemmy.world")
	mappingsBefore, err := h.objects.ListByActorDID(ctx, authorDID)
	require.NoError(t, err)
	require.NotEmpty(t, mappingsBefore, "author should have live records before opt-out")

	// Author opts out; force a refresh.
	summary = "hobbyist. #nobridge please"
	_, err = h.m.RefreshActor(ctx, &ap.Object{ID: personIRI})
	require.Error(t, err)
	assert.True(t, IsSkip(err), "opted-out actor returns a skip, got %v", err)

	actor, err := h.actors.GetByAPActorID(ctx, personIRI)
	require.NoError(t, err)
	assert.Equal(t, store.ConsentStateNoBridge, actor.ConsentState)

	mappingsAfter, err := h.objects.ListByActorDID(ctx, authorDID)
	require.NoError(t, err)
	assert.Empty(t, mappingsAfter, "opt-out must scrub the author's live records")
}

// TestKnownPersonAsCommunity_Skips covers the type-confusion fix: a post whose
// audience names an actor already bridged as a Person must not create a
// community from that Person.
func TestKnownPersonAsCommunity_Skips(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const personIRI = "https://lemmy.world/u/notacommunity"
	h.serveObject("/u/notacommunity", person(personIRI, "notacommunity", nil))

	// Bridge the person first (as a Person).
	_, err := h.m.EnsureActor(ctx, &ap.Object{ID: personIRI})
	require.NoError(t, err)

	// A post claiming that person's IRI as its community.
	pg := page("https://lemmy.world/post/2002", personIRI, personIRI, "x", "2024-02-02T00:00:00.000000Z")
	res, err := h.m.MaterializePost(ctx, mustObject(t, pg), personIRI)
	require.Nil(t, res)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "person-as-community must be a skip, got %v", err)
}

// TestProfileRefreshCannotRewriteContentMapping: a live ap_objects mapping's
// collection is fixed by the record it maps; a commit for a different
// collection must never take the row over. A bridged actor's origin may serve
// a re-fetched document whose id is another same-host IRI (actorDoc binds the
// id only to the fetch authority), here the id of an already-bridged post. The
// actor has a bridged_actors row but no profile mapping yet (the state a crash
// between mintAndUpsert and the first profile commit leaves), so nothing else
// claims the profile at-uri. The forced refresh must leave the post's mapping
// exactly as it was.
func TestProfileRefreshCannotRewriteContentMapping(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"), groupID)
	require.NoError(t, err)
	before, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	require.Equal(t, authorDID, before.DID)
	require.Equal(t, "social.coves.community.postv2", before.Collection)

	const shapeshifterID = "https://lemmy.world/u/shapeshifter"
	minted, err := h.m.minter.MintActor(ctx, identity.MintRequest{
		ActorType:         store.ActorTypePerson,
		PreferredUsername: "shapeshifter",
		Instance:          "lemmy.world",
	})
	require.NoError(t, err)
	_, err = h.actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:           shapeshifterID,
		ActorType:           store.ActorTypePerson,
		DID:                 minted.DID,
		Handle:              minted.Handle,
		SigningKeyEncrypted: minted.SigningKeyEncrypted,
		ConsentState:        store.ConsentStateOK,
	})
	require.NoError(t, err)

	// The actor's origin now answers with a Person whose id is the post's.
	h.serveObject("/u/shapeshifter", person(pageID, "shapeshifter", nil))

	// Refused or applied elsewhere, the outcome asserted is the post mapping.
	_, _ = h.m.RefreshActor(ctx, &ap.Object{ID: shapeshifterID})

	after, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.Equal(t, "social.coves.community.postv2", after.Collection,
		"a profile commit must not rewrite a post mapping's collection")
	assert.Equal(t, authorDID, after.DID)
	assert.Equal(t, before.RKey, after.RKey)
	assert.Equal(t, "at://"+authorDID+"/social.coves.community.postv2/"+before.RKey, after.ATURI)
	assert.False(t, after.IsDeleted(), "the post must stay live")
	record, _, err := h.manager.GetRecord(ctx, authorDID, "social.coves.community.postv2", before.RKey)
	require.NoError(t, err)
	assert.Contains(t, record["title"], "DRAM price-fixing", "the post record must be unchanged")
}

// TestFirstBridgeCannotMintOverContentID: an unseen actor IRI whose origin
// serves a Person document claiming a same-host content id (actorDoc binds the
// id only to the fetch authority) must not be bridged under that id. Refusing
// at the profile commit is too late: by then a DID has been minted and a
// bridged_actors row keyed on the post's id inserted, so the post's id now
// reads as a bridged actor. Nothing may be minted, live mapping or deleted.
func TestFirstBridgeCannotMintOverContentID(t *testing.T) {
	cases := []struct {
		name        string
		softDeleted bool
	}{
		{name: "live post mapping", softDeleted: false},
		{name: "soft-deleted post mapping", softDeleted: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			h := newHarness(t)
			h.serveLemmyWorldFixtures()
			ctx := context.Background()

			_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"), groupID)
			require.NoError(t, err)
			if testCase.softDeleted {
				require.NoError(t, h.objects.SoftDelete(ctx, pageID))
			}
			before, err := h.objects.GetByAPID(ctx, pageID)
			require.NoError(t, err)
			authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
			require.Equal(t, authorDID, before.DID)
			require.Equal(t, "social.coves.community.postv2", before.Collection)
			require.Equal(t, testCase.softDeleted, before.IsDeleted())

			const aliasID = "https://lemmy.world/u/alias"
			_, err = h.actors.GetByAPActorID(ctx, aliasID)
			require.True(t, errors.IsNotFound(err), "the alias must be unseen before the call, got %v", err)

			// The alias's origin answers with a Person whose id is the post's.
			h.serveObject("/u/alias", person(pageID, "alias", nil))
			mintsBefore := h.minter.mintCount()

			actor, err := h.m.EnsureActor(ctx, &ap.Object{ID: aliasID})
			assert.Error(t, err, "bridging an actor over a content id must be refused")
			assert.Nil(t, actor)
			assert.Equal(t, mintsBefore, h.minter.mintCount(),
				"no identity may be minted for an id that is already content")

			_, err = h.actors.GetByAPActorID(ctx, pageID)
			assert.True(t, errors.IsNotFound(err),
				"no bridged_actors row may be keyed on the post's id, got %v", err)
			_, err = h.actors.GetByAPActorID(ctx, aliasID)
			assert.True(t, errors.IsNotFound(err),
				"no bridged_actors row may be keyed on the alias, got %v", err)
			_, err = h.actors.GetByDID(ctx, testDIDFor("alias", "lemmy.world"))
			assert.True(t, errors.IsNotFound(err),
				"no identity may be minted for the alias, got %v", err)

			after, err := h.objects.GetByAPID(ctx, pageID)
			require.NoError(t, err)
			assert.Equal(t, "social.coves.community.postv2", after.Collection)
			assert.Equal(t, authorDID, after.DID)
			assert.Equal(t, before.RKey, after.RKey)
			assert.Equal(t, "at://"+authorDID+"/social.coves.community.postv2/"+before.RKey, after.ATURI)
			assert.Equal(t, before.CID, after.CID)
			assert.Equal(t, testCase.softDeleted, after.IsDeleted(),
				"the post mapping's deleted state must be unchanged")
		})
	}
}

// bridgedPerson bridges a synthetic same-host Person through EnsureActor and
// returns its bridged_actors row and profile mapping.
func bridgedPerson(t *testing.T, h *harness, path, id, username string) (*store.BridgedActor, *store.APObjectMapping) {
	t.Helper()
	ctx := context.Background()
	h.serveObject(path, person(id, username, map[string]any{"name": username + " display"}))
	row, err := h.m.EnsureActor(ctx, &ap.Object{ID: id})
	require.NoError(t, err)
	require.Equal(t, testDIDFor(username, "lemmy.world"), row.DID)
	profile, err := h.objects.GetByAPID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "social.coves.actor.profile", profile.Collection)
	require.Equal(t, "at://"+row.DID+"/social.coves.actor.profile/self", profile.ATURI)
	return row, profile
}

// TestFirstBridgeCannotMintOverBridgedActorID: an unseen alias IRI whose origin
// serves a Person document claiming an already-bridged actor's same-host id
// (actorDoc binds the id only to the fetch authority) must not mint. The id is
// already a bridged actor's, so the alias can never become a new bridged_actors
// row under it: minting first only orphans a permanent PLC DID when the upsert
// finds the id taken. The bridged actor's row and profile stay as they were.
func TestFirstBridgeCannotMintOverBridgedActorID(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const bobID = "https://lemmy.world/u/bob"
	bobRow, bobProfile := bridgedPerson(t, h, "/u/bob", bobID, "bob")

	const aliasID = "https://lemmy.world/u/alias"
	_, err := h.actors.GetByAPActorID(ctx, aliasID)
	require.True(t, errors.IsNotFound(err), "the alias must be unseen before the call, got %v", err)

	// The alias's origin answers with a Person whose id is bob's.
	h.serveObject("/u/alias", person(bobID, "alias", map[string]any{"name": "Not Bob"}))
	mintsBefore := h.minter.mintCount()

	_, _ = h.m.EnsureActor(ctx, &ap.Object{ID: aliasID})

	assert.Equal(t, mintsBefore, h.minter.mintCount(),
		"no identity may be minted for an id that is already a bridged actor's")
	_, err = h.actors.GetByAPActorID(ctx, aliasID)
	assert.True(t, errors.IsNotFound(err),
		"no bridged_actors row may be keyed on the alias, got %v", err)
	_, err = h.actors.GetByDID(ctx, testDIDFor("alias", "lemmy.world"))
	assert.True(t, errors.IsNotFound(err),
		"no bridged_actors row may carry an identity minted for the alias, got %v", err)

	bobAfter, err := h.actors.GetByAPActorID(ctx, bobID)
	require.NoError(t, err)
	assert.Equal(t, testDIDFor("bob", "lemmy.world"), bobAfter.DID)
	assert.Equal(t, bobRow.Handle, bobAfter.Handle)
	assert.Equal(t, store.ActorTypePerson, bobAfter.ActorType)
	assert.Equal(t, store.ConsentStateOK, bobAfter.ConsentState)

	profileAfter, err := h.objects.GetByAPID(ctx, bobID)
	require.NoError(t, err)
	assert.Equal(t, "social.coves.actor.profile", profileAfter.Collection)
	assert.Equal(t, testDIDFor("bob", "lemmy.world"), profileAfter.DID)
	assert.Equal(t, "at://"+testDIDFor("bob", "lemmy.world")+"/social.coves.actor.profile/self", profileAfter.ATURI)
	assert.Equal(t, bobProfile.CID, profileAfter.CID, "bob's profile record must be unchanged")
	record, _, err := h.manager.GetRecord(ctx, testDIDFor("bob", "lemmy.world"), "social.coves.actor.profile", "self")
	require.NoError(t, err)
	assert.Equal(t, "bob display", record["displayName"])
}

// TestProfileRefreshCannotTakeOverBridgedActorMapping: a bridged actor A whose
// origin, on a forced refresh, serves a Person document claiming bridged actor
// B's same-host id must be refused. A has a bridged_actors row but no profile
// mapping yet (the state a crash between mintAndUpsert and the first profile
// commit leaves), so nothing else claims A's profile at-uri and the profile
// commit for B's id would repoint B's mapping at A's repo. B's mapping must
// stay B's, and nothing may be written under A.
func TestProfileRefreshCannotTakeOverBridgedActorMapping(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const bobID = "https://lemmy.world/u/bob"
	_, bobProfile := bridgedPerson(t, h, "/u/bob", bobID, "bob")

	const shapeshifterID = "https://lemmy.world/u/shapeshifter"
	minted, err := h.m.minter.MintActor(ctx, identity.MintRequest{
		ActorType:         store.ActorTypePerson,
		PreferredUsername: "shapeshifter",
		Instance:          "lemmy.world",
	})
	require.NoError(t, err)
	_, err = h.actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:           shapeshifterID,
		ActorType:           store.ActorTypePerson,
		DID:                 minted.DID,
		Handle:              minted.Handle,
		SigningKeyEncrypted: minted.SigningKeyEncrypted,
		ConsentState:        store.ConsentStateOK,
	})
	require.NoError(t, err)
	shapeshifterDID := testDIDFor("shapeshifter", "lemmy.world")
	require.Equal(t, shapeshifterDID, minted.DID)

	// A's origin now answers with a Person whose id is bob's.
	h.serveObject("/u/shapeshifter", person(bobID, "shapeshifter", map[string]any{"name": "Not Bob"}))

	_, err = h.m.RefreshActor(ctx, &ap.Object{ID: shapeshifterID})
	assert.Error(t, err, "a refresh serving another bridged actor's id must be refused")
	assert.True(t, IsSkip(err), "the refusal must be a skip, got %v", err)

	after, err := h.objects.GetByAPID(ctx, bobID)
	require.NoError(t, err)
	assert.Equal(t, "social.coves.actor.profile", after.Collection)
	assert.Equal(t, testDIDFor("bob", "lemmy.world"), after.DID,
		"bob's profile mapping must keep bob's repo")
	assert.Equal(t, "at://"+testDIDFor("bob", "lemmy.world")+"/social.coves.actor.profile/self", after.ATURI)
	assert.Equal(t, bobProfile.CID, after.CID, "bob's profile mapping must keep its record's cid")
	assert.False(t, after.IsDeleted())
	record, _, err := h.manager.GetRecord(ctx, testDIDFor("bob", "lemmy.world"), "social.coves.actor.profile", "self")
	require.NoError(t, err)
	assert.Equal(t, "bob display", record["displayName"], "bob's profile record must be unchanged")

	_, err = h.objects.GetByAPID(ctx, shapeshifterID)
	assert.True(t, errors.IsNotFound(err), "the shapeshifter must still have no profile mapping, got %v", err)
	_, _, err = h.manager.GetRecord(ctx, shapeshifterDID, "social.coves.actor.profile", "self")
	assert.True(t, errors.IsNotFound(err),
		"no profile record may be written under the shapeshifter from bob's id, got %v", err)
	shapeshifterRow, err := h.actors.GetByAPActorID(ctx, shapeshifterID)
	require.NoError(t, err)
	assert.Equal(t, shapeshifterDID, shapeshifterRow.DID)
}

// hiddenMappingObjects answers GetByAPID for one id as though that id had no
// mapping yet, and delegates everything else to the real store: the read a
// first bridge makes just before another worker commits that id's mapping.
type hiddenMappingObjects struct {
	store.APObjects
	hiddenAPID string
}

func (s hiddenMappingObjects) GetByAPID(ctx context.Context, apID string) (*store.APObjectMapping, error) {
	if apID == s.hiddenAPID {
		return nil, errors.NewNotFoundError("ap_object", apID)
	}
	return s.APObjects.GetByAPID(ctx, apID)
}

// TestFirstBridgeRacingContentMappingKeepsNoActorRow: the pre-mint mapping
// check cannot see a content mapping another worker commits right after it.
// An alias IRI whose origin serves a Person document claiming that content id
// then passes the check; the profile commit is refused when it meets the
// committed mapping, but the bridged_actors row keyed on the content id must
// not outlive that refusal, or the post's id reads as a bridged actor for
// good. The stale read is simulated deterministically by a Materializer whose
// store reports the post unmapped. (The minted DID cannot be un-minted.)
func TestFirstBridgeRacingContentMappingKeepsNoActorRow(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"), groupID)
	require.NoError(t, err)
	before, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	require.Equal(t, authorDID, before.DID)
	require.Equal(t, "social.coves.community.postv2", before.Collection)

	const aliasID = "https://lemmy.world/u/alias"
	h.serveObject("/u/alias", person(pageID, "alias", nil))

	racing := *h.m
	racing.objects = hiddenMappingObjects{APObjects: h.objects, hiddenAPID: pageID}

	actor, err := racing.EnsureActor(ctx, &ap.Object{ID: aliasID})
	assert.Error(t, err, "bridging an actor over a content id must be refused")
	assert.Nil(t, actor)

	_, err = h.actors.GetByAPActorID(ctx, pageID)
	assert.True(t, errors.IsNotFound(err),
		"no bridged_actors row may be keyed on the post's id, got %v", err)
	_, err = h.actors.GetByAPActorID(ctx, aliasID)
	assert.True(t, errors.IsNotFound(err),
		"no bridged_actors row may be keyed on the alias, got %v", err)

	after, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.Equal(t, "social.coves.community.postv2", after.Collection)
	assert.Equal(t, authorDID, after.DID)
	assert.Equal(t, before.RKey, after.RKey)
	assert.Equal(t, "at://"+authorDID+"/social.coves.community.postv2/"+before.RKey, after.ATURI)
	assert.Equal(t, before.CID, after.CID)
	assert.False(t, after.IsDeleted(), "the post must stay live")
}

// staleFirstMappingRead answers the FIRST GetByAPID for one id as though that
// id had no mapping yet, and every later read from the real store: a first
// bridge's pre-mint check that runs just before another worker commits (and
// here deletes) that id's mapping, followed by a profile commit that sees it.
type staleFirstMappingRead struct {
	store.APObjects
	hiddenAPID string
	served     *bool
}

func (s staleFirstMappingRead) GetByAPID(ctx context.Context, apID string) (*store.APObjectMapping, error) {
	if apID == s.hiddenAPID && !*s.served {
		*s.served = true
		return nil, errors.NewNotFoundError("ap_object", apID)
	}
	return s.APObjects.GetByAPID(ctx, apID)
}

// TestFirstBridgeRacingDeletedContentMappingKeepsNoActorRow: the pre-mint
// mapping check cannot see a content mapping another worker commits right after
// it, and that worker can also have soft-deleted it by the time the profile
// commit reads the mapping. The profile commit is refused there, and the
// bridged_actors row keyed on the content id must not outlive that refusal,
// deleted mapping or live. The post's mapping stays exactly as it was, deleted.
func TestFirstBridgeRacingDeletedContentMappingKeepsNoActorRow(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"), groupID)
	require.NoError(t, err)
	require.NoError(t, h.objects.SoftDelete(ctx, pageID))
	before, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	require.Equal(t, authorDID, before.DID)
	require.Equal(t, "social.coves.community.postv2", before.Collection)
	require.True(t, before.IsDeleted())

	const aliasID = "https://lemmy.world/u/alias"
	h.serveObject("/u/alias", person(pageID, "alias", nil))

	racing := *h.m
	racing.objects = staleFirstMappingRead{APObjects: h.objects, hiddenAPID: pageID, served: new(bool)}

	actor, err := racing.EnsureActor(ctx, &ap.Object{ID: aliasID})
	assert.Error(t, err, "bridging an actor over a deleted content id must be refused")
	assert.Nil(t, actor)

	_, err = h.actors.GetByAPActorID(ctx, pageID)
	assert.True(t, errors.IsNotFound(err),
		"no bridged_actors row may be keyed on the deleted post's id, got %v", err)
	_, err = h.actors.GetByAPActorID(ctx, aliasID)
	assert.True(t, errors.IsNotFound(err),
		"no bridged_actors row may be keyed on the alias, got %v", err)

	after, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.Equal(t, "social.coves.community.postv2", after.Collection)
	assert.Equal(t, authorDID, after.DID)
	assert.Equal(t, before.RKey, after.RKey)
	assert.Equal(t, "at://"+authorDID+"/social.coves.community.postv2/"+before.RKey, after.ATURI)
	assert.Equal(t, before.CID, after.CID)
	assert.True(t, after.IsDeleted(), "the post mapping must stay deleted")
}

// strandActorRowOnPost materializes the lemmy.world fixture post, then inserts
// a bridged_actors row keyed on the post's id with an identity minted for
// "alias" and no synced profile: the row a failed compensation strands. The
// caller serves the fixtures. It returns the post's mapping.
func strandActorRowOnPost(t *testing.T, h *harness) *store.APObjectMapping {
	t.Helper()
	ctx := context.Background()
	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"), groupID)
	require.NoError(t, err)
	before, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	require.Equal(t, testDIDFor("LeftLeaningFreedomFighters", "lemmy.world"), before.DID)
	require.Equal(t, "social.coves.community.postv2", before.Collection)

	minted, err := h.m.minter.MintActor(ctx, identity.MintRequest{
		ActorType:         store.ActorTypePerson,
		PreferredUsername: "alias",
		Instance:          "lemmy.world",
	})
	require.NoError(t, err)
	stranded, err := h.actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:           pageID,
		ActorType:           store.ActorTypePerson,
		DID:                 minted.DID,
		Handle:              minted.Handle,
		SigningKeyEncrypted: minted.SigningKeyEncrypted,
		ConsentState:        store.ConsentStateOK,
	})
	require.NoError(t, err)
	require.Equal(t, testDIDFor("alias", "lemmy.world"), stranded.DID)
	require.Nil(t, stranded.ProfileSyncedAt, "the stranded row must be stale so EnsureActor re-checks it")
	return before
}

// assertPostMappingUnchanged asserts the lemmy.world fixture post's mapping is
// still before's, live.
func assertPostMappingUnchanged(t *testing.T, h *harness, before *store.APObjectMapping) {
	t.Helper()
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	after, err := h.objects.GetByAPID(context.Background(), pageID)
	require.NoError(t, err)
	assert.Equal(t, "social.coves.community.postv2", after.Collection)
	assert.Equal(t, authorDID, after.DID)
	assert.Equal(t, before.RKey, after.RKey)
	assert.Equal(t, "at://"+authorDID+"/social.coves.community.postv2/"+before.RKey, after.ATURI)
	assert.Equal(t, before.CID, after.CID)
	assert.False(t, after.IsDeleted(), "the post must stay live")
}

// TestStrandedActorRowOnContentIDHeals: a bridged_actors row keyed on a content
// id (left by a compensation that failed after a refused first bridge) must not
// keep the post's id reading as a bridged actor. Reaching that row through
// EnsureActor or RefreshActor is a skip, and it removes the row; the post's
// mapping stays exactly as it was, whatever document the origin serves at the
// post's id.
func TestStrandedActorRowOnContentIDHeals(t *testing.T) {
	cases := []struct {
		name string
		// servePersonAtPost serves a Person claiming the post's id at the
		// post's path instead of the post itself.
		servePersonAtPost bool
		refresh           bool
	}{
		{name: "EnsureActor, origin serves the post", servePersonAtPost: false, refresh: false},
		{name: "RefreshActor, origin serves the post", servePersonAtPost: false, refresh: true},
		{name: "EnsureActor, origin serves a Person claiming the post's id", servePersonAtPost: true, refresh: false},
		{name: "RefreshActor, origin serves a Person claiming the post's id", servePersonAtPost: true, refresh: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			h.serveFixture("/u/LeftLeaningFreedomFighters", "person_lemmy_world.json")
			h.serveFixture("/c/technology", "group_lemmy_world.json")
			if testCase.servePersonAtPost {
				h.serveObject("/post/49131386", person(pageID, "alias", nil))
			} else {
				h.serveFixture("/post/49131386", "page_lemmy_world.json")
			}

			before := strandActorRowOnPost(t, h)

			var actor *store.BridgedActor
			var err error
			if testCase.refresh {
				actor, err = h.m.RefreshActor(ctx, &ap.Object{ID: pageID})
			} else {
				actor, err = h.m.EnsureActor(ctx, &ap.Object{ID: pageID})
			}
			assert.Nil(t, actor)
			assert.True(t, IsSkip(err), "a content id's stranded actor row must be skipped, got %v", err)

			_, err = h.actors.GetByAPActorID(ctx, pageID)
			assert.True(t, errors.IsNotFound(err),
				"the stranded bridged_actors row on the post's id must be removed, got %v", err)
			_, _, err = h.manager.GetRecord(ctx, testDIDFor("alias", "lemmy.world"), "social.coves.actor.profile", "self")
			assert.True(t, errors.IsNotFound(err),
				"no profile record may be written under the stranded identity, got %v", err)

			assertPostMappingUnchanged(t, h, before)
		})
	}
}

// TestStrandedActorRowOnContentIDHealsInAnyConsentState: a stranded
// bridged_actors row on a content id is healed whatever consent state it has
// reached. A deleted (tombstoned) or nobridge row keyed on the post's id still
// reads the post's id as a bridged actor, so EnsureActor on it is a skip that
// removes the row, and the post's mapping stays exactly as it was.
func TestStrandedActorRowOnContentIDHealsInAnyConsentState(t *testing.T) {
	cases := []struct {
		name    string
		consent store.ConsentState
		// stamped marks the stranded row's profile synced just now, as the
		// nobridge re-check does without committing a profile, so the row
		// reads as fresh.
		stamped bool
	}{
		{name: "deleted", consent: store.ConsentStateDeleted},
		{name: "nobridge", consent: store.ConsentStateNoBridge},
		{name: "nobridge, profile sync stamped", consent: store.ConsentStateNoBridge, stamped: true},
		{name: "deleted, profile sync stamped", consent: store.ConsentStateDeleted, stamped: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			h.serveLemmyWorldFixtures()

			before := strandActorRowOnPost(t, h)
			require.NoError(t, h.actors.SetConsentState(ctx, pageID, testCase.consent))
			stranded, err := h.actors.GetByAPActorID(ctx, pageID)
			require.NoError(t, err)
			require.Equal(t, testCase.consent, stranded.ConsentState)
			if testCase.stamped {
				require.NoError(t, h.actors.MarkProfileSynced(ctx, pageID, time.Now()))
				stranded, err = h.actors.GetByAPActorID(ctx, pageID)
				require.NoError(t, err)
				require.NotNil(t, stranded.ProfileSyncedAt)
			} else {
				require.Nil(t, stranded.ProfileSyncedAt)
			}

			actor, err := h.m.EnsureActor(ctx, &ap.Object{ID: pageID})
			assert.Nil(t, actor)
			assert.True(t, IsSkip(err), "a content id's stranded actor row must be skipped, got %v", err)

			_, err = h.actors.GetByAPActorID(ctx, pageID)
			assert.True(t, errors.IsNotFound(err),
				"the stranded bridged_actors row on the post's id must be removed, got %v", err)
			assertPostMappingUnchanged(t, h, before)
		})
	}
}

// TestAliasNamingStrandedContentIDHeals: an unseen alias IRI whose origin
// serves a Person claiming the post's id, while a stranded bridged_actors row
// sits on that id, is a skip that removes the stranded row. Nothing is minted
// for the alias and no row is keyed on it.
func TestAliasNamingStrandedContentIDHeals(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.serveLemmyWorldFixtures()

	before := strandActorRowOnPost(t, h)

	const aliasID = "https://lemmy.world/u/alias"
	h.serveObject("/u/alias", person(pageID, "alias", nil))
	mintsBefore := h.minter.mintCount()

	actor, err := h.m.EnsureActor(ctx, &ap.Object{ID: aliasID})
	assert.Nil(t, actor)
	assert.True(t, IsSkip(err), "an alias naming a content id must be skipped, got %v", err)

	assert.Equal(t, mintsBefore, h.minter.mintCount(), "no identity may be minted for the alias")
	_, err = h.actors.GetByAPActorID(ctx, pageID)
	assert.True(t, errors.IsNotFound(err),
		"the stranded bridged_actors row on the post's id must be removed, got %v", err)
	_, err = h.actors.GetByAPActorID(ctx, aliasID)
	assert.True(t, errors.IsNotFound(err),
		"no bridged_actors row may be keyed on the alias, got %v", err)
	assertPostMappingUnchanged(t, h, before)
}

// cancelAfterMappingRead cancels processing right after the mapping read for
// one id returns: the delivery is cancelled between the heal's check and its
// removal of the stranded row.
type cancelAfterMappingRead struct {
	store.APObjects
	apID   string
	cancel context.CancelFunc
}

func (s cancelAfterMappingRead) GetByAPID(ctx context.Context, apID string) (*store.APObjectMapping, error) {
	mapping, err := s.APObjects.GetByAPID(ctx, apID)
	if apID == s.apID {
		s.cancel()
	}
	return mapping, err
}

// TestStrandedActorRowHealSurvivesCancellation: the delivery that finds a
// stranded bridged_actors row on a content id can be cancelled after the
// mapping check found it. The removal is detached from that cancellation, so
// the row still goes and the call is still a skip.
func TestStrandedActorRowHealSurvivesCancellation(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()

	before := strandActorRowOnPost(t, h)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelling := *h.m
	cancelling.objects = cancelAfterMappingRead{APObjects: h.objects, apID: pageID, cancel: cancel}

	actor, err := cancelling.EnsureActor(ctx, &ap.Object{ID: pageID})
	assert.Nil(t, actor)
	assert.True(t, IsSkip(err), "a content id's stranded actor row must be skipped, got %v", err)

	_, err = h.actors.GetByAPActorID(context.Background(), pageID)
	assert.True(t, errors.IsNotFound(err),
		"the stranded bridged_actors row must be removed despite the cancelled delivery, got %v", err)
	assertPostMappingUnchanged(t, h, before)
}

// TestPageCannotOverwriteBridgedActorProfile: a Page whose id is an
// already-bridged Person's same-host id must not take that id's mapping as its
// own. Materializing it would write a post record into the person's repo at
// the profile's collection and rkey "self", over the person's profile. The
// call is a skip, nothing is minted, and the person's profile record and
// mapping stay as they were.
func TestPageCannotOverwriteBridgedActorProfile(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	// Bridge the post's author and community first, so nothing the page
	// names needs minting.
	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"), groupID)
	require.NoError(t, err)

	const bobID = "https://lemmy.world/u/bob"
	bobDID := testDIDFor("bob", "lemmy.world")
	bobRow, bobProfile := bridgedPerson(t, h, "/u/bob", bobID, "bob")
	require.Equal(t, bobDID, bobRow.DID)
	recordBefore, _, err := h.manager.GetRecord(ctx, bobDID, "social.coves.actor.profile", "self")
	require.NoError(t, err)
	require.Equal(t, "social.coves.actor.profile", recordBefore["$type"])
	require.Equal(t, "bob display", recordBefore["displayName"])

	mintsBefore := h.minter.mintCount()
	bobPage := mustObject(t, page(bobID, personID, groupID, "Not a profile", "2024-06-01T00:00:00.000000Z"))

	res, err := h.m.MaterializePost(ctx, bobPage, groupID)
	assert.Nil(t, res)
	assert.True(t, IsSkip(err), "a page over a bridged actor's id must be a skip, got %v", err)

	assert.Equal(t, mintsBefore, h.minter.mintCount(), "nothing may be minted for the page")

	profileAfter, err := h.objects.GetByAPID(ctx, bobID)
	require.NoError(t, err)
	assert.Equal(t, "social.coves.actor.profile", profileAfter.Collection)
	assert.Equal(t, bobDID, profileAfter.DID)
	assert.Equal(t, "self", profileAfter.RKey)
	assert.Equal(t, "at://"+bobDID+"/social.coves.actor.profile/self", profileAfter.ATURI)
	assert.Equal(t, bobProfile.CID, profileAfter.CID, "bob's profile mapping must keep its record's cid")
	assert.Equal(t, "Person", profileAfter.APType)
	assert.False(t, profileAfter.IsDeleted())

	record, recordCID, err := h.manager.GetRecord(ctx, bobDID, "social.coves.actor.profile", "self")
	require.NoError(t, err)
	assert.Equal(t, bobProfile.CID, recordCID, "bob's profile record must be unchanged")
	assert.Equal(t, "social.coves.actor.profile", record["$type"])
	assert.Equal(t, "bob display", record["displayName"])
	assert.NotContains(t, record, "title", "no post field may be written into bob's profile")

	bobAfter, err := h.actors.GetByAPActorID(ctx, bobID)
	require.NoError(t, err)
	assert.Equal(t, bobDID, bobAfter.DID)
	assert.Equal(t, store.ConsentStateOK, bobAfter.ConsentState)
}

// TestCommunityAliasCannotMintOverBridgedCommunity: an unseen Group IRI whose
// origin serves a Group document claiming an already-bridged community's
// same-host id must not mint, must not gain a communities row, and must not
// rewrite that community's profile from the alias's document. EnsureCommunity
// passes the fetched document on as the reference, so the alias IRI is gone
// by the time the first bridge compares ids.
func TestCommunityAliasCannotMintOverBridgedCommunity(t *testing.T) {
	cases := []struct {
		name    string
		refresh bool
	}{
		{name: "EnsureCommunity", refresh: false},
		{name: "RefreshCommunity", refresh: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()

			const bravoID = "https://lemmy.world/c/bravo"
			h.serveObject("/c/bravo", group(bravoID, "bravo", map[string]any{"name": "Bravo display"}))
			bravoCommunity, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: bravoID})
			require.NoError(t, err)
			bravoDID := testDIDFor("bravo", "lemmy.world")
			require.Equal(t, bravoDID, bravoCommunity.DID)
			bravoRow, err := h.actors.GetByAPActorID(ctx, bravoID)
			require.NoError(t, err)
			bravoProfile, err := h.objects.GetByAPID(ctx, bravoID)
			require.NoError(t, err)
			require.Equal(t, "social.coves.community.profile", bravoProfile.Collection)

			const aliasID = "https://lemmy.world/c/alias"
			_, err = h.actors.GetByAPActorID(ctx, aliasID)
			require.True(t, errors.IsNotFound(err), "the alias must be unseen before the call, got %v", err)

			// The alias's origin answers with a Group whose id is bravo's.
			h.serveObject("/c/alias", group(bravoID, "alias", map[string]any{"name": "Not Bravo"}))
			mintsBefore := h.minter.mintCount()

			var community *store.Community
			if testCase.refresh {
				community, err = h.m.RefreshCommunity(ctx, &ap.Object{ID: aliasID})
			} else {
				community, err = h.m.EnsureCommunity(ctx, &ap.Object{ID: aliasID})
			}
			assert.Nil(t, community)
			assert.True(t, IsSkip(err), "a community alias over a bridged community must be a skip, got %v", err)

			assert.Equal(t, mintsBefore, h.minter.mintCount(),
				"no identity may be minted for an id that is already a bridged community's")
			_, err = h.actors.GetByAPActorID(ctx, aliasID)
			assert.True(t, errors.IsNotFound(err),
				"no bridged_actors row may be keyed on the alias, got %v", err)
			_, err = h.communities.GetByAPGroupID(ctx, aliasID)
			assert.True(t, errors.IsNotFound(err),
				"no communities row may be keyed on the alias, got %v", err)

			bravoAfter, err := h.actors.GetByAPActorID(ctx, bravoID)
			require.NoError(t, err)
			assert.Equal(t, bravoDID, bravoAfter.DID)
			assert.Equal(t, bravoRow.Handle, bravoAfter.Handle)
			assert.Equal(t, store.ActorTypeGroup, bravoAfter.ActorType)
			assert.Equal(t, store.ConsentStateOK, bravoAfter.ConsentState)

			communityAfter, err := h.communities.GetByAPGroupID(ctx, bravoID)
			require.NoError(t, err)
			assert.Equal(t, bravoDID, communityAfter.DID)
			assert.Equal(t, "bravo", communityAfter.PreferredUsername)

			profileAfter, err := h.objects.GetByAPID(ctx, bravoID)
			require.NoError(t, err)
			assert.Equal(t, "social.coves.community.profile", profileAfter.Collection)
			assert.Equal(t, bravoDID, profileAfter.DID)
			assert.Equal(t, "at://"+bravoDID+"/social.coves.community.profile/self", profileAfter.ATURI)
			assert.Equal(t, bravoProfile.CID, profileAfter.CID, "bravo's profile record must be unchanged")
			record, _, err := h.manager.GetRecord(ctx, bravoDID, "social.coves.community.profile", "self")
			require.NoError(t, err)
			assert.Equal(t, "Bravo display", record["displayName"])
		})
	}
}

// staleFirstActorRead answers the FIRST GetByAPActorID for one id as though
// that id had no bridged_actors row yet, and every later read from the real
// store: the alias check a first bridge makes just before another worker
// inserts that id's row.
type staleFirstActorRead struct {
	store.BridgedActors
	hiddenAPActorID string
	served          *bool
}

func (s staleFirstActorRead) GetByAPActorID(ctx context.Context, apActorID string) (*store.BridgedActor, error) {
	if apActorID == s.hiddenAPActorID && !*s.served {
		*s.served = true
		return nil, errors.NewNotFoundError("bridged_actor", apActorID)
	}
	return s.BridgedActors.GetByAPActorID(ctx, apActorID)
}

// TestAliasLosingBridgeRaceCannotRewriteWinner: an unseen alias IRI whose
// origin serves a Person claiming another actor's same-host id passes the alias
// check when that actor's row lands just after it. The upsert then loses to the
// row and finds a winner under a DIFFERENT id than the alias asked for: that is
// not a concurrent bridge of the same actor, and reusing the winner would
// rewrite its profile from the alias's document. The winner's profile and
// mapping stay as they were. (The alias's DID is minted before the race shows.)
func TestAliasLosingBridgeRaceCannotRewriteWinner(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const bobID = "https://lemmy.world/u/bob"
	bobRow, bobProfile := bridgedPerson(t, h, "/u/bob", bobID, "bob")

	const aliasID = "https://lemmy.world/u/alias"
	h.serveObject("/u/alias", person(bobID, "alias", map[string]any{"name": "Not Bob"}))

	racing := *h.m
	racing.actors = staleFirstActorRead{BridgedActors: h.actors, hiddenAPActorID: bobID, served: new(bool)}

	actor, err := racing.EnsureActor(ctx, &ap.Object{ID: aliasID})
	assert.Nil(t, actor)
	assert.True(t, IsSkip(err), "an alias over another actor's id must be a skip, got %v", err)

	_, err = h.actors.GetByAPActorID(ctx, aliasID)
	assert.True(t, errors.IsNotFound(err),
		"no bridged_actors row may be keyed on the alias, got %v", err)

	bobAfter, err := h.actors.GetByAPActorID(ctx, bobID)
	require.NoError(t, err)
	assert.Equal(t, testDIDFor("bob", "lemmy.world"), bobAfter.DID)
	assert.Equal(t, bobRow.Handle, bobAfter.Handle)
	assert.Equal(t, store.ConsentStateOK, bobAfter.ConsentState)

	profileAfter, err := h.objects.GetByAPID(ctx, bobID)
	require.NoError(t, err)
	assert.Equal(t, "social.coves.actor.profile", profileAfter.Collection)
	assert.Equal(t, testDIDFor("bob", "lemmy.world"), profileAfter.DID)
	assert.Equal(t, "at://"+testDIDFor("bob", "lemmy.world")+"/social.coves.actor.profile/self", profileAfter.ATURI)
	assert.Equal(t, bobProfile.CID, profileAfter.CID, "bob's profile record must be unchanged")
	record, _, err := h.manager.GetRecord(ctx, testDIDFor("bob", "lemmy.world"), "social.coves.actor.profile", "self")
	require.NoError(t, err)
	assert.Equal(t, "bob display", record["displayName"], "bob's profile must not be rewritten from the alias's document")
}

// --- pure-unit tests (no DB) ---

func TestContainsHashtag(t *testing.T) {
	cases := []struct {
		hay, marker string
		want        bool
	}{
		{"i said #nobot", "#nobot", true},
		{"#nobot at start", "#nobot", true},
		{"ends with #nobot", "#nobot", true},
		{"tag #nobot. done", "#nobot", true},
		{"this is #nobotany not opt-out", "#nobot", false},
		{"#nobots plural", "#nobot", false},
		{"opt out #nobridge here", "#nobridge", true},
		{"#nobridges is different", "#nobridge", false},
		{"nothing here", "#nobot", false},
	}
	for _, c := range cases {
		assert.Equalf(t, c.want, containsHashtag(c.hay, c.marker), "containsHashtag(%q,%q)", c.hay, c.marker)
	}
}

func TestTruncateText_ByteCap(t *testing.T) {
	// Family emoji: 1 grapheme, 25 bytes. 100 of them = 100 graphemes but
	// 2500 bytes — inside a 200-grapheme cap yet over a 300-byte cap.
	emoji := "\U0001F468‍\U0001F469‍\U0001F467‍\U0001F466"
	s := strings.Repeat(emoji, 100)
	got := truncateText(s, 200, 300)
	assert.LessOrEqual(t, len(got), 300, "must satisfy the byte cap")
	assert.LessOrEqual(t, uniseg.GraphemeClusterCount(got), 200, "must satisfy the grapheme cap")
	// No split cluster: re-truncating is idempotent.
	assert.Equal(t, got, truncateText(got, 200, 300))
}

func TestIsSafeLinkScheme(t *testing.T) {
	assert.True(t, isSafeLinkScheme("https://example.com/x"))
	assert.True(t, isSafeLinkScheme("http://example.com"))
	assert.True(t, isSafeLinkScheme("  HTTPS://Example.com "))
	assert.False(t, isSafeLinkScheme("javascript:alert(1)"))
	assert.False(t, isSafeLinkScheme("data:text/html;base64,PHNjcmlwdD4="))
	assert.False(t, isSafeLinkScheme("ftp://example.com"))
	assert.False(t, isSafeLinkScheme(""))
}

func TestStripActiveHTML(t *testing.T) {
	assert.Equal(t, "hello world",
		stripActiveHTML("hello <script>steal()</script>world"))
	assert.Equal(t, "keep <b>bold</b>",
		stripActiveHTML("keep <b>bold</b><style>.x{}</style>"))
	// False tag-name match is preserved.
	assert.Equal(t, "text about <scripting> languages",
		stripActiveHTML("text about <scripting> languages"))
	// Unterminated block is dropped to end.
	assert.Equal(t, "before",
		stripActiveHTML("before <script>oops no close"))
	// Case-insensitive.
	assert.Equal(t, "a b",
		stripActiveHTML("a <SCRIPT>x</SCRIPT>b"))
}
