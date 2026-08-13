package consume

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Task 14 cycles F3/F4: the profile cache and the #identity handler.
//
// Neither enqueues anything. Lemmy has no Update{Person} handler — verified
// against 0.19.20 and 1.0 main, it 400s — so an outbound profile activity
// would be a guaranteed-failed delivery. Peers refresh through their own lazy
// ≤24h actor refetch, which means this cache only has to be right when it is
// READ.

func profileFrame(did, rev, operation string, fields string) []byte {
	record := ""
	if operation != "delete" {
		record = fmt.Sprintf(`,"cid":"bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",`+
			`"record":{"$type":"social.coves.actor.profile"%s}`, fields)
	}
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":7100,"kind":"commit","commit":{"rev":%q,"operation":%q,`+
			`"collection":"social.coves.actor.profile","rkey":"self"%s}}`,
		did, rev, operation, record))
}

func profileCache(t *testing.T, database *sql.DB, did string) (displayName, summary, avatarURL string) {
	t.Helper()
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT display_name, summary, avatar_url FROM ap_actors WHERE did = $1`, did).
		Scan(&displayName, &summary, &avatarURL))
	return displayName, summary, avatarURL
}

// ---------------------------------------------------------------------------
// F3 — actor.profile
// ---------------------------------------------------------------------------

func TestProfileHandler_RefreshesTheCache(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	// displayName and description are the lexicon's names; description maps to
	// ap_actors.summary, which is the AP vocabulary for the same thing.
	require.NoError(t, fixture.handle(t, profileFrame(dispatchNativeDID, dispatchRev, "create",
		`,"displayName":"Alice Anderson","description":"posts about boats"`)))

	displayName, summary, _ := profileCache(t, database, dispatchNativeDID)
	assert.Equal(t, "Alice Anderson", displayName)
	assert.Equal(t, "posts about boats", summary,
		"the lexicon's description IS the AP summary")

	assert.Empty(t, fixture.enqueuer.Calls(),
		"NO outbound activity: Lemmy has no Update{Person} handler, so an enqueue here "+
			"would be a guaranteed 400")
}

func TestProfileHandler_UpdateOverwritesAndOmissionsClear(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t, profileFrame(dispatchNativeDID, dispatchRev, "create",
		`,"displayName":"Alice Anderson","description":"posts about boats"`)))

	// A record is a whole document: the user removed their bio.
	require.NoError(t, fixture.handle(t, profileFrame(dispatchNativeDID, dispatchRevHigher, "update",
		`,"displayName":"Alice A."`)))

	displayName, summary, _ := profileCache(t, database, dispatchNativeDID)
	assert.Equal(t, "Alice A.", displayName)
	assert.Empty(t, summary,
		"an omitted field means the user REMOVED it — the record is the whole profile, "+
			"not a patch, so a merge would keep a bio the user deleted")
}

func TestProfileHandler_AvatarBlobIsNotCachedYet(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	// avatar is a BLOB ref in atproto: {$type, ref:{$link:cid}, mimeType, size}.
	require.NoError(t, fixture.handle(t, profileFrame(dispatchNativeDID, dispatchRev, "create",
		`,"displayName":"Alice","avatar":{"$type":"blob",`+
			`"ref":{"$link":"bafkreiabc123"},"mimeType":"image/png","size":1234}`)))

	displayName, _, avatarURL := profileCache(t, database, dispatchNativeDID)
	assert.Equal(t, "Alice", displayName)
	assert.Empty(t, avatarURL,
		"a blob ref is a CID, not a URL. Turning it into one needs the author's PDS "+
			"host plus a getBlob convention this task has not established, and a "+
			"half-derived URL would serve peers a broken avatar. Follow-up — see the "+
			"cycle F report")
}

func TestProfileHandler_DeleteClearsTheCache(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t, profileFrame(dispatchNativeDID, dispatchRev, "create",
		`,"displayName":"Alice Anderson","description":"posts about boats"`)))

	require.NoError(t, fixture.handle(t,
		profileFrame(dispatchNativeDID, dispatchRevHigher, "delete", "")))

	displayName, summary, avatarURL := profileCache(t, database, dispatchNativeDID)
	assert.Empty(t, displayName, "deleting the profile record clears the cache")
	assert.Empty(t, summary)
	assert.Empty(t, avatarURL)

	var localPart string
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT local_part FROM ap_actors WHERE did = $1`, dispatchNativeDID).Scan(&localPart))
	assert.Equal(t, "alice", localPart,
		"the ACTOR survives: a deleted profile record is not a deleted identity, and "+
			"the local part is frozen regardless")
}

func TestProfileHandler_ActorlessDIDIsSkipped(t *testing.T) {
	database := dispatchTestDB(t)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t, profileFrame(dispatchNativeDID, dispatchRev, "create",
		`,"displayName":"Nobody"`)),
		"a profile edit by a DID with no actor is a no-op, not an error")

	assert.Zero(t, countRows(t, database, "ap_actors"),
		"editing a profile is not a FEDERATING interaction, so it must not mint: "+
			"otherwise every Coves user who ever set a display name would get an AP "+
			"identity they never asked for")
	assert.Empty(t, fixture.minter.Handles())
	assert.Empty(t, fixture.resolver.Calls(),
		"and no handle is resolved for an actor that is not going to exist")
}

// ---------------------------------------------------------------------------
// F4 — #identity
// ---------------------------------------------------------------------------

func TestIdentityHandler_UsesTheReResolvedHandleNotTheEventsOwn(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	// The event says one thing; current DID resolution says another. Identity
	// events can be stale or replayed, so the event's handle is a hint about
	// WHICH DID to re-check, never an answer about what its handle is.
	fixture.resolver.handle = "carol.coves.social"
	require.NoError(t, fixture.handle(t, identityFrameFor(dispatchNativeDID, "bob.coves.social")))

	assert.Equal(t, []string{dispatchNativeDID}, fixture.resolver.Calls(),
		"the handle is re-resolved and re-verified rather than taken from the frame")

	displayName, _, _ := profileCache(t, database, dispatchNativeDID)
	assert.Equal(t, "carol.coves.social", displayName,
		"the VERIFIED handle is cached, not the one the event carried")

	var localPart string
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT local_part FROM ap_actors WHERE did = $1`, dispatchNativeDID).Scan(&localPart))
	assert.Equal(t, "alice", localPart,
		"and the local part is untouched: it was frozen at creation, and re-deriving it "+
			"would strand every federated mention of the old name")

	assert.Empty(t, fixture.enqueuer.Calls(),
		"a rename enqueues nothing; peers pick it up on their own refetch")
}

func TestIdentityHandler_ActorlessDIDResolvesNothing(t *testing.T) {
	database := dispatchTestDB(t)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t, identityFrameFor(dispatchNativeDID, "bob.coves.social")),
		"a rename by a DID with no actor is a no-op")

	assert.Empty(t, fixture.resolver.Calls(),
		"the actor check comes FIRST: resolving would spend two network round-trips "+
			"(PLC + well-known) to update a cache that does not exist. Every Coves user "+
			"who renames emits one of these")
	assert.Zero(t, countRows(t, database, "ap_actors"), "and no eager mint")
}

func TestIdentityHandler_ResolverFailureLeavesTheCacheAlone(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t, profileFrame(dispatchNativeDID, dispatchRev, "create",
		`,"displayName":"Alice Anderson"`)))

	fixture.resolver.err = fmt.Errorf("plc directory unreachable")
	err := fixture.handle(t, identityFrameFor(dispatchNativeDID, "bob.coves.social"))

	require.Error(t, err, "an unverifiable rename fails the event so it can be retried")
	assert.NotErrorIs(t, err, ErrPermanentEvent, "a directory outage is transient")

	displayName, _, _ := profileCache(t, database, dispatchNativeDID)
	assert.Equal(t, "Alice Anderson", displayName,
		"the cache keeps the last VERIFIED value rather than being cleared or filled "+
			"with the unverified handle from the frame")
}
