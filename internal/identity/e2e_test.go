package identity

import (
	"bytes"
	"testing"

	indigorepo "github.com/bluesky-social/indigo/atproto/repo"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// TestMintedIdentityEndToEnd is the task-03 definition of done, verbatim:
// mint a DID on a local PLC directory, create its repo, write a profile
// record, read it back, and verify the commit signature with the minted
// key (the exact key registered as the DID's verification key).
func TestMintedIdentityEndToEnd(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"bridged_actors", "blocks", "repo_state", "firehose_events")
	actors := store.NewBridgedActors(database)
	minter, custodian, _, _ := testMinter(t, actors) // skips without local PLC
	ctx := t.Context()

	// Mint.
	identity, err := minter.MintActor(ctx, MintRequest{
		ActorType:         store.ActorTypeGroup,
		PreferredUsername: "technology",
		Instance:          "lemmy.world",
	})
	require.NoError(t, err)

	// Persist the bridged actor the way tasks 05/06 will.
	_, err = actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:           "https://lemmy.world/c/technology",
		ActorType:           store.ActorTypeGroup,
		DID:                 identity.DID,
		Handle:              identity.Handle,
		SigningKeyEncrypted: identity.SigningKeyEncrypted,
		ConsentState:        store.ConsentStateOK,
	})
	require.NoError(t, err)

	// Create the repo and write the profile record (rkey "self", per the
	// materialization principle: profiles land before any content).
	manager, err := repo.NewManager(database, NewActorKeys(actors, custodian), nil)
	require.NoError(t, err)
	profile := map[string]any{
		"$type":       "social.coves.community.profile",
		"name":        "technology",
		"description": "bridged from lemmy.world by Tidepool",
	}
	res, err := manager.PutRecord(ctx, identity.DID, "social.coves.community.profile", "self", profile)
	require.NoError(t, err)
	require.NotEmpty(t, res.Rev)

	// Read it back.
	got, gotCID, err := manager.GetRecord(ctx, identity.DID, "social.coves.community.profile", "self")
	require.NoError(t, err)
	assert.Equal(t, res.RecordCID, gotCID)
	assert.Equal(t, "technology", got["name"])

	// Export and verify the commit signature with the minted key — parsed
	// from the did:key the PLC operation registered, not from local state.
	carBytes, err := manager.ExportCAR(ctx, identity.DID)
	require.NoError(t, err)
	commit, _, err := indigorepo.LoadRepoFromCAR(ctx, bytes.NewReader(carBytes))
	require.NoError(t, err)
	assert.Equal(t, identity.DID, commit.DID)

	mintedPub, err := atcrypto.ParsePublicDIDKey(identity.DIDKey)
	require.NoError(t, err)
	require.NoError(t, commit.VerifySignature(mintedPub),
		"commit signature must verify with the key minted into the DID document")
}

// TestTombstoneFreezesRepoAtCommitLayer wires a real repo.Manager to
// identity.ActorKeys (the production SigningKeys implementation) and proves
// the consent-revocation freeze end to end: tombstoning an actor blocks new
// writes at the commit layer while keeping their existing records deletable
// (task 05's Delete(Actor) → scrub-records flow). PLC-independent: the actor
// is created directly in the store with a fake DID.
func TestTombstoneFreezesRepoAtCommitLayer(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"bridged_actors", "blocks", "repo_state", "firehose_events")
	actors := store.NewBridgedActors(database)
	custodian := testCustodian(t)
	ctx := t.Context()

	signing, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	const (
		did        = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		apActorID  = "https://lemmy.world/u/alice"
		collection = "social.coves.community.post"
		rkey       = "3mks4zznhkard" // any valid TID-form rkey
	)
	sealed, err := custodian.EncryptActorKey(did, signing)
	require.NoError(t, err)
	_, err = actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:           apActorID,
		ActorType:           store.ActorTypePerson,
		DID:                 did,
		Handle:              "alice.lemmy-world.tidepool.example",
		SigningKeyEncrypted: sealed,
		ConsentState:        store.ConsentStateOK,
	})
	require.NoError(t, err)

	manager, err := repo.NewManager(database, NewActorKeys(actors, custodian), nil)
	require.NoError(t, err)

	record := map[string]any{"$type": collection, "text": "hello"}
	before, err := manager.PutRecord(ctx, did, collection, rkey, record)
	require.NoError(t, err)
	require.False(t, before.NoOp)

	firehoseCount := func() int {
		var n int
		require.NoError(t, database.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM firehose_events WHERE did = $1`, did).Scan(&n))
		return n
	}
	require.Equal(t, 1, firehoseCount())

	// Consent flip: the actor tombstones (terminal).
	require.NoError(t, actors.SetConsentState(ctx, apActorID, store.ConsentStateDeleted))

	// (a) New writes are frozen at the commit layer, surfacing the consent
	// state — not a generic failure.
	_, err = manager.PutRecord(ctx, did, collection, "3mks522bgpxc5",
		map[string]any{"$type": collection, "text": "after tombstone"})
	require.Error(t, err)
	assert.True(t, errors.IsTombstoned(err),
		"post-tombstone writes must fail with IsTombstoned")
	assert.False(t, errors.IsNotFound(err), "tombstoned must not read as missing")

	// (b) The head did not move.
	headCID, headRev, err := manager.Head(ctx, did)
	require.NoError(t, err)
	assert.Equal(t, before.CommitCID, headCID, "blocked write must not move the head")
	assert.Equal(t, before.Rev, headRev, "blocked write must not advance the rev")

	// (c) No firehose event leaked from the blocked write.
	assert.Equal(t, 1, firehoseCount(), "blocked write must not emit a firehose event")

	// (d) Deleting the existing record still works (KeyUseDelete releases
	// the key) and emits a firehose event: scrubbing IS the consent intent.
	deleted, err := manager.DeleteRecord(ctx, did, collection, rkey)
	require.NoError(t, err,
		"tombstoned actor's records must remain deletable (scrub flow)")
	assert.Greater(t, deleted.Rev, before.Rev)
	assert.Greater(t, deleted.Seq, before.Seq, "the delete must emit a firehose event")
	assert.Equal(t, 2, firehoseCount())

	_, _, err = manager.GetRecord(ctx, did, collection, rkey)
	assert.True(t, errors.IsNotFound(err), "the scrubbed record must be gone")
}
