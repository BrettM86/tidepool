package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/outbound"
	"tidepool/internal/store"
)

// M1 — the announced-delete echo.
//
// When a native author deletes their own post, the acceptance engine enqueues a
// PostIntent{delete}, the bridge sends a Delete, and the community announces it
// straight back. Today that echo is dropped BY ACCIDENT: the enqueuer writes no
// community_did on the bridge-origin mapping, so CommunityDIDOf falls through to
// reading the post record out of the AUTHOR's repo — a repo the bridge does not
// host — gets NotFound, and the announced-delete authorization refuses with
// "cannot bind target to a community". Every native post deletion logs a Warn
// that reads like a data-integrity bug.
//
// The moment 17c populates community_did (decision 18's authorization rule
// REQUIRES it), that refusal turns into a PASS: the mapping is live, it is a
// postv2, the echoed Delete carries no `summary`, and deleteIsByAuthor answers
// false — so the echo of an author deleting their OWN post is written up as a
// community-signed social.coves.community.removal with code
// moderator-discretion. A user deletes their own post and the bridge publicly
// asserts, on the firehose, that a moderator removed it.
//
// These tests pin the drop at the CLASSIFIER, under both worlds.
const (
	mdUserOrigin = "https://coves.social"
	mdAuthorDID  = "did:plc:mdauthor7iza6de2dwap2s"
	mdActorID    = mdUserOrigin + "/ap/actor/" + mdAuthorDID
	mdPostRKey   = "3lzmddelete11"
	mdPostATURI  = "at://" + mdAuthorDID + "/social.coves.community.postv2/" + mdPostRKey
	mdPostAPID   = mdUserOrigin + "/ap/object/" + mdAuthorDID +
		"/social.coves.community.postv2/" + mdPostRKey
	mdPostCID = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
)

// deleteEchoWorld is a native post that was federated out and then deleted by
// its author, with the outbound Delete already sent — the exact state the
// community's echo arrives into.
type deleteEchoWorld struct {
	group         *remoteActor
	communityDID  string
	digestRKey    string
	deletePayload map[string]any
}

// setupNativeDeleteEcho federates a native post through the REAL enqueuer, then
// enqueues the author's own delete, and writes the acceptance the engine would
// have written. mappingFields decides how much of the mapping is populated:
// today's enqueuer leaves community_did and author_did empty, and 17c fills
// community_did in — the difference between an accidental drop and a
// destructive one.
func setupNativeDeleteEcho(t *testing.T, h *harness, communityDID, authorDID string) deleteEchoWorld {
	t.Helper()
	ctx := context.Background()
	group := h.subscribeTechnology()
	actualCommunityDID := testDIDFor("technology", "lemmy.world")

	// The native author's persona (the acceptance engine mints it lazily on the
	// first federating interaction).
	_, err := store.NewAPActors(h.db).Create(ctx, store.APActor{
		DID:              mdAuthorDID,
		Kind:             store.ActorTypePerson,
		ActorID:          mdActorID,
		NormalizedOrigin: "coves.social",
		LocalPart:        "mdauthor",
		RSAKeySealed:     []byte{0x01, 0x02, 0x03},
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err, "mint the native author's persona")

	_, err = store.NewOutboundObjects(h.db).Upsert(ctx, store.OutboundObject{
		ATURI:              mdPostATURI,
		APObjectID:         mdPostAPID,
		LastCID:            mdPostCID,
		LastRev:            "3lzmdrev000001",
		CommunityDID:       actualCommunityDID,
		CommunityAPID:      groupID,
		TranslatedSnapshot: mdSnapshot(t),
	})
	require.NoError(t, err, "seed the accepted post's outbound state")

	enqueuer, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         h.db,
		Translator: outbound.NewTranslator(mdUserOrigin),
		Inboxes:    outbound.NewInboxResolver(h.client, time.Minute),
		Actors:     store.NewAPActors(h.db),
		UserOrigin: mdUserOrigin,
	})
	require.NoError(t, err, "build the outbound enqueuer")

	// 1. The post federates out: this is what writes the bridge-origin mapping.
	enqueueAs(t, h.db, enqueuer, mdAuthorDID, consume.PostIntent{
		Op:            "create",
		ATURI:         mdPostATURI,
		ID:            consume.ActivityID(mdUserOrigin, mdPostATURI, "create", 0),
		CommunityAPID: groupID,
		Snapshot:      mdSnapshot(t),
	})
	// 2. The AUTHOR deletes their own post: engine.go enqueues PostIntent{delete}.
	deleteIntent := consume.PostIntent{
		Op:            "delete",
		ATURI:         mdPostATURI,
		ID:            consume.ActivityID(mdUserOrigin, mdPostATURI, "delete", 1),
		CommunityAPID: groupID,
		Snapshot:      mdSnapshot(t),
	}
	enqueueAs(t, h.db, enqueuer, mdAuthorDID, deleteIntent)

	stored, err := store.NewOutboundActivities(h.db).Get(ctx, deleteIntent.ID)
	require.NoError(t, err, "the outbound Delete must be persisted before first delivery")
	require.Equal(t, "Delete", stored.Kind)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(stored.Payload, &payload),
		"the echo body is the delivered payload verbatim")

	// The mapping as the world will have it. PutMapping is keyed on the ap id,
	// so this rewrites the row the enqueuer just wrote.
	mapping, err := h.objects.GetByAPID(ctx, mdPostAPID)
	require.NoError(t, err, "federating the post must have written its bridge-origin mapping")
	require.Equal(t, store.OriginBridge, mapping.Origin)
	require.Equal(t, materialize.CollectionPostV2, mapping.Collection)
	require.Empty(t, mapping.CommunityDID,
		"precondition: today's enqueuer records no community on the mapping — the accident this test exists to outlive")
	mapping.CommunityDID = communityDID
	mapping.AuthorDID = authorDID
	_, err = h.objects.PutMapping(ctx, *mapping)
	require.NoError(t, err, "rewrite the mapping the way this scenario's world has it")

	// The acceptance the engine wrote when the post was admitted. Without it
	// there is nothing for a spurious removal to destroy, and the test would
	// pass for the wrong reason.
	digest := testDigestRKey(mdPostATURI)
	_, err = h.manager.PutRecord(ctx, actualCommunityDID, materialize.CollectionAcceptance, digest,
		map[string]any{
			"$type":     materialize.CollectionAcceptance,
			"subject":   map[string]any{"uri": mdPostATURI, "cid": mdPostCID},
			"createdAt": "2026-08-13T09:00:00.000Z",
		})
	require.NoError(t, err, "seed the acceptance the admission wrote")

	return deleteEchoWorld{
		group:         group,
		communityDID:  actualCommunityDID,
		digestRKey:    digest,
		deletePayload: payload,
	}
}

func mdSnapshot(t *testing.T) []byte {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      mdPostATURI,
		"cid":        mdPostCID,
		"rev":        "3lzmdrev000001",
		"collection": "social.coves.community.postv2",
		"record": map[string]any{
			"$type":     "social.coves.community.postv2",
			"community": testDIDFor("technology", "lemmy.world"),
			"title":     "A post its own author deleted",
			"content":   "the delete goes out, and the community announces it back",
			"createdAt": "2026-08-13T09:00:00.000Z",
		},
		"communityApId": groupID,
	})
	require.NoError(t, err)
	return snap
}

// TestAnnouncedDeleteEchoNeverWritesAModeratorRemoval is M1a/M1b/M1c.
//
// The two worlds differ ONLY in how much of the mapping is filled in. The
// second and third are the world 17c creates, where the accidental drop is
// gone: if the classifier does not catch the echo there, the bridge fabricates
// a moderation record against an author who moderated nobody.
func TestAnnouncedDeleteEchoNeverWritesAModeratorRemoval(t *testing.T) {
	cases := []struct {
		name         string
		communityDID func(actual string) string
		authorDID    string
		why          string
	}{
		{
			name:         "M1a mapping as today's enqueuer writes it",
			communityDID: func(string) string { return "" },
			why: "today the echo dies on an accident — the community binding fails — and " +
				"the drop must become the classifier's decision instead",
		},
		{
			name:         "M1b mapping carrying its community DID, as 17c will leave it",
			communityDID: func(actual string) string { return actual },
			why: "with community_did populated the authorization PASSES, the summary-less " +
				"Delete is not provably the author's, and RemovePost writes a " +
				"community-signed removal against a self-delete",
		},
		{
			name:         "M1b mapping carrying community AND author DIDs",
			communityDID: func(actual string) string { return actual },
			authorDID:    mdAuthorDID,
			why: "author_did does not save it: deleteIsByAuthor resolves the author through " +
				"bridged_actors, and a NATIVE persona has no row there — the answer is " +
				"false however complete the mapping is",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			actualCommunityDID := testDIDFor("technology", "lemmy.world")
			world := setupNativeDeleteEcho(t, h, tc.communityDID(actualCommunityDID), tc.authorDID)

			acceptanceBefore, acceptanceCIDBefore, err := h.manager.GetRecord(ctx,
				world.communityDID, materialize.CollectionAcceptance, world.digestRKey)
			require.NoError(t, err, "precondition: the post is accepted")
			require.NotNil(t, acceptanceBefore)
			commitsBefore := len(communityEvents(t, h, world.communityDID))
			dropsBefore := map[echo.Class]int64{}
			for _, class := range echoClasses {
				dropsBefore[class] = echo.Drops(class)
			}

			// WHEN: the community announces our own Delete back at us.
			announceID := "https://lemmy.world/activities/announce/delete/echo-of-our-own"
			require.Equal(t, http.StatusAccepted,
				h.deliver(world.group, echoAnnounce(announceID, world.deletePayload)))
			h.drain()

			// THEN: no moderation record, ever.
			_, _, err = h.manager.GetRecord(ctx,
				world.communityDID, materialize.CollectionRemoval, world.digestRKey)
			assert.True(t, errors.IsNotFound(err),
				"THE BUG: an author deleting their own post must never produce a "+
					"community-signed removal record — %s (err=%v)", tc.why, err)

			// ...and the acceptance is not disturbed either. A removal deletes it
			// in the same commit, so an intact acceptance is the second half of
			// the same proof.
			_, acceptanceCIDAfter, err := h.manager.GetRecord(ctx,
				world.communityDID, materialize.CollectionAcceptance, world.digestRKey)
			require.NoError(t, err, "the echo must not withdraw the acceptance")
			assert.Equal(t, acceptanceCIDBefore, acceptanceCIDAfter,
				"the acceptance must be byte-identical: the echo changes nothing")
			assert.Equal(t, commitsBefore, len(communityEvents(t, h, world.communityDID)),
				"an echo must produce NO commit in the community repo — every commit here "+
					"is a public claim on the firehose")

			// The post's own state is untouched: no tombstone marker, no
			// soft-deleted mapping (the bridge already applied the author's
			// delete on the way out; the echo is not a second delete).
			mapping, err := h.objects.GetByAPID(ctx, mdPostAPID)
			require.NoError(t, err)
			assert.False(t, mapping.IsDeleted(),
				"the echo must not soft-delete the mapping of an object we still serve")
			tombstoned, err := h.tombstones.ExistsFor(ctx, mdPostAPID, groupID)
			require.NoError(t, err)
			assert.False(t, tombstoned,
				"an echo must lay no tombstone marker: a marker here would suppress this "+
					"id's own later re-creation")

			// M1c: the drop is INTENTIONAL and attributed, not incidental.
			for _, class := range echoClasses {
				want := dropsBefore[class]
				if class == echo.ClassLocalActivity {
					want++
				}
				assert.Equal(t, want, echo.Drops(class),
					"the echoed Delete carries the id of an activity WE sent, so it drops as "+
						"%q — attribution is what distinguishes a decision from an accident",
					echo.ClassLocalActivity)
			}
			assert.NotContains(t, h.logs.String(), "cannot bind target to a community",
				"the community-binding refusal must NOT be what stops this: it is an "+
					"accident of an unpopulated column, it reads like a data-integrity bug "+
					"on every native deletion, and 17c removes it")

			// And the event is decided once: not retried, not poisoned.
			event, err := h.events.GetEvent(ctx, announceID)
			require.NoError(t, err)
			assert.NotNil(t, event.ProcessedAt, "an echo is processed (skipped), not left pending")
			assert.Nil(t, event.FailedAt, "an echo must never poison: %s", event.Error)
			assert.Equal(t, 1, event.Attempts, "decided on the first attempt")
		})
	}
}

// TestGenuineModRemovalSurvivesEchoSuppression is M1d — the false-positive
// guard. Suppressing our own deletes must not become "ignore inbound deletes":
// a real moderator removing a real Lemmy post is the behaviour the whole
// moderation path exists for, and it must be untouched, with no echo counter
// moving.
func TestGenuineModRemovalSurvivesEchoSuppression(t *testing.T) {
	h := newHarness(t)
	post := setupModeratedPost(t, h)
	ctx := context.Background()
	dropsBefore := map[echo.Class]int64{}
	for _, class := range echoClasses {
		dropsBefore[class] = echo.Drops(class)
	}

	reason := "brigading"
	h.announceDeleteWithSummary(post.group,
		"https://lemmy.world/activities/announce/delete/genuine-removal", pageID, &reason)

	removal, _, err := h.manager.GetRecord(ctx,
		post.communityDID, materialize.CollectionRemoval, post.digestRKey)
	require.NoError(t, err,
		"a GENUINE moderator removal of fediverse-origin content must still be honored — "+
			"echo suppression is about ids WE minted, not about inbound deletes in general")
	assert.Equal(t, reason, removal["reason"])
	_, _, err = h.manager.GetRecord(ctx,
		post.communityDID, materialize.CollectionAcceptance, post.digestRKey)
	assert.True(t, errors.IsNotFound(err), "the acceptance is withdrawn by the removal (err=%v)", err)

	for _, class := range echoClasses {
		assert.Equal(t, dropsBefore[class], echo.Drops(class),
			"no echo counter may move for genuine remote moderation: a false positive here "+
				"silently disables moderation for every bridged community")
	}
}

// enqueueAs runs the real enqueuer for one intent inside the transaction the
// consumer's rev gate would be holding, signing as actorDID.
func enqueueAs(t *testing.T, db *sql.DB, enqueuer *outbound.Enqueuer, actorDID string, intent consume.Intent) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, enqueuer.EnqueueActivity(ctx, tx, actorDID, groupID, "", intent),
		"enqueue %s", intent.ActivityID())
	require.NoError(t, tx.Commit())
}
