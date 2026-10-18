package materialize

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/repo"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

func TestEnsureActor_IdentityBeforeFirstProfileCommit(t *testing.T) {
	h := newRealMinterHarness(t)
	h.serveObject("/u/alice", person("https://lemmy.world/u/alice", "alice", nil))
	stored, err := h.m.EnsureActor(context.Background(), &ap.Object{ID: "https://lemmy.world/u/alice"})
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, "alice.lemmy.world.ap.tidepool.test", stored.Handle)

	var identities []*repo.Event
	var firstProfile *repo.Event
	for _, event := range h.firehoseEvents() {
		if event.DID != stored.DID {
			continue
		}
		if event.Kind == repo.EventKindIdentity {
			identities = append(identities, event)
		}
		if event.Kind == repo.EventKindCommit {
			for _, op := range event.Ops {
				if op.Path == CollectionActorProfile+"/"+ProfileRKey && firstProfile == nil {
					firstProfile = event
				}
			}
		}
	}
	require.Len(t, identities, 1, "mint must publish one identity for the actor's DID")
	require.Equal(t, "alice.lemmy.world.ap.tidepool.test", identities[0].Handle)
	require.NotNil(t, firstProfile, "the actor's first profile commit must exist")
	require.Less(t, identities[0].Seq, firstProfile.Seq)
}

func TestEnsureActor_RetryEmitsOnlyStoredIdentity(t *testing.T) {
	h := newRealMinterHarness(t)
	db := testutil.DB(t)
	ctx := context.Background()
	owner, err := h.actors.UpsertActor(ctx, store.BridgedActor{
		APActorID: "https://lemmy.world/u/owner", ActorType: store.ActorTypePerson,
		DID: "did:plc:ewvi7nxzyoun6zhxrhs64oiz", Handle: "owner.lemmy-world.tidepool.test",
		SigningKeyEncrypted: []byte("owner-key"), ConsentState: store.ConsentStateOK,
	})
	require.NoError(t, err)
	minter := &reservingMinter{ActorMinter: h.m.minter, db: db, ownerID: owner.ID}
	h.m.minter = minter
	h.serveObject("/u/alice", person("https://lemmy.world/u/alice", "alice", nil))

	stored, err := h.m.EnsureActor(ctx, &ap.Object{ID: "https://lemmy.world/u/alice"})
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.True(t, minter.reserved, "first mint must lose the handle claim race")
	require.NotEmpty(t, minter.orphanedDID)
	require.NotEqual(t, minter.orphanedDID, stored.DID)
	require.Equal(t, "alice-2.lemmy.world.ap.tidepool.test", stored.Handle)

	var identities []*repo.Event
	for _, event := range h.firehoseEvents() {
		if event.Kind == repo.EventKindIdentity {
			identities = append(identities, event)
		}
	}
	require.Len(t, identities, 1, "discarded mint must not publish an identity")
	require.Equal(t, stored.DID, identities[0].DID)
	require.Equal(t, "alice-2.lemmy.world.ap.tidepool.test", identities[0].Handle)
	require.NotEqual(t, minter.orphanedDID, identities[0].DID)
	require.NotEqual(t, "alice.lemmy.world.ap.tidepool.test", identities[0].Handle)
}

// rejectIdentityEventsTrigger is installed on firehose_events by
// TestEnsureActor_IdentityAppendFailureLeavesNoActorRow to make every #identity
// append fail.
const rejectIdentityEventsTrigger = "test_reject_identity_events"

// TestEnsureActor_IdentityAppendFailureLeavesNoActorRow: the mint's #identity
// is appended in the transaction that inserts the bridged_actors row and its
// handle claim, so an identity append that fails takes both writes with it.
func TestEnsureActor_IdentityAppendFailureLeavesNoActorRow(t *testing.T) {
	h := newHarness(t)
	db := testutil.DB(t)
	ctx := context.Background()
	dropTrigger := func() {
		_, err := db.Exec(`DROP TRIGGER IF EXISTS ` + rejectIdentityEventsTrigger + ` ON firehose_events`)
		require.NoError(t, err)
		_, err = db.Exec(`DROP FUNCTION IF EXISTS ` + rejectIdentityEventsTrigger + `()`)
		require.NoError(t, err)
	}
	dropTrigger()
	t.Cleanup(dropTrigger)
	_, err := db.Exec(`CREATE FUNCTION ` + rejectIdentityEventsTrigger + `() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'identity append rejected by test'; END $$`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TRIGGER ` + rejectIdentityEventsTrigger + `
		BEFORE INSERT ON firehose_events FOR EACH ROW
		WHEN (NEW.kind = 'identity')
		EXECUTE FUNCTION ` + rejectIdentityEventsTrigger + `()`)
	require.NoError(t, err)

	const actorID = "https://lemmy.world/u/alice"
	h.serveObject("/u/alice", person(actorID, "alice", nil))

	stored, err := h.m.EnsureActor(ctx, &ap.Object{ID: actorID})
	require.ErrorContains(t, err, "identity append rejected by test", "a failed identity append must fail the bridge")
	require.Nil(t, stored)

	var rows int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM bridged_actors WHERE ap_actor_id = $1`, actorID).Scan(&rows))
	require.Zero(t, rows, "no bridged_actors row may outlive the failed identity append")
	var claims int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM handle_claims WHERE handle = $1`,
		"alice.lemmy-world.tidepool.test").Scan(&claims))
	require.Zero(t, claims, "no handle claim may outlive the failed identity append")
}
