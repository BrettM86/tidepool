package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// TASK 17d — `purged_at` IS THE DIFFERENCE BETWEEN A REQUEST AND A FACT.
//
// (The Upsert/Get/Delete basics live in outbound_test.go with the rest of the
// task 14 tables; THIS file is the purge terminality layer added by migrations
// 028/030.)
//
// A federation preference row normally records something reversible: the user
// said stop, and the user may say go again. purged_at records the one thing in
// this table that is NOT: peers were actually asked to delete this user's
// content, and no peer un-honours a Delete. Every guard in federation_prefs.go
// that reads that column is a single SQL predicate, which means every one of
// them is deletable in a one-line "simplification" that leaves the rest of the
// suite green — and each such deletion resumes federating for somebody who was
// erased. These tests exist so each of those one-liners fails loudly instead.
//
// Following the store discipline, every negative carries its positive control
// beside it: the cheapest way to satisfy a guard test is a method that stopped
// doing its job entirely.

// federationPrefsTestDB returns a migrated connection with federation_prefs
// emptied.
func federationPrefsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "federation_prefs")
	return database
}

// fpThirdDID is a third actor for the three-way ClearRequestedPurge test.
const fpThirdDID = "did:plc:z72i7hdynmk6r22z27h6fpfp"

// fpOptOut is the row the record door writes: enabled=false, destructive
// intent recorded.
func fpOptOut(did string, source FederationPrefSource) FederationPref {
	return FederationPref{DID: did, Enabled: false, DeleteRemote: true, Source: source}
}

// mustPurgedPref seeds an opt-out row and marks it purged, returning the row
// with its stamp — the state every terminality test below starts from.
func mustPurgedPref(t *testing.T, repo FederationPrefs, did string, source FederationPrefSource) *FederationPref {
	t.Helper()
	ctx := context.Background()
	_, err := repo.Upsert(ctx, fpOptOut(did, source))
	require.NoError(t, err, "seed opt-out for %s", did)
	require.NoError(t, repo.MarkPurged(ctx, did), "mark %s purged", did)
	pref, err := repo.Get(ctx, did)
	require.NoError(t, err)
	require.NotNil(t, pref.PurgedAt, "precondition: MarkPurged stamped the row")
	return pref
}

// TestFederationPrefs_DeleteRefusesAPurgedRow pins Delete's `AND purged_at IS
// NULL`. The one-line deletion this kills: dropping that predicate — after
// which an ORDINARY OPT-OUT RECORD DELETE (the most innocuous event in the
// whole pipeline) removes the row, absence means default-on, and the bridge
// resumes publishing for a user whose content every peer was told to erase.
func TestFederationPrefs_DeleteRefusesAPurgedRow(t *testing.T) {
	database := federationPrefsTestDB(t)
	repo := NewFederationPrefs(database)
	ctx := context.Background()

	purged := mustPurgedPref(t, repo, testDID, FederationPrefSourceRecord)

	require.NoError(t, repo.Delete(ctx, testDID),
		"the refusal is SILENT by contract: the caller reads the row back to learn the "+
			"outcome (restoreDefaultFederation does exactly that), so an error here would "+
			"fail — and endlessly replay — an event that was applied as far as it may go")

	survivor, err := repo.Get(ctx, testDID)
	require.NoError(t, err,
		"the purged row SURVIVES an ordinary record delete: absence means default-on, and "+
			"a user whose content peers already deleted has nothing to come back to")
	require.NotNil(t, survivor.PurgedAt, "and it keeps its purge stamp")
	assert.True(t, survivor.PurgedAt.Equal(*purged.PurgedAt),
		"the stamp is untouched — it is the only record that the withdrawal happened")

	// Positive control, beside the negative: the guard is one predicate, not a
	// Delete that stopped deleting (TestFederationPrefs_DeleteRestoresDefaultOn
	// covers the ordinary path in full).
	_, err = repo.Upsert(ctx, fpOptOut(testSecondDID, FederationPrefSourceRecord))
	require.NoError(t, err)
	require.NoError(t, repo.Delete(ctx, testSecondDID))
	_, err = repo.Get(ctx, testSecondDID)
	assert.True(t, errors.IsNotFound(err),
		"an UNPURGED row still deletes: a guard that froze the table would leave every "+
			"re-enabling user permanently opted out, got %v", err)
}

// TestFederationPrefs_ClearRequestedPurgeClearsOnlyUnpurgedAccountRows is the
// three-way over ClearRequestedPurge's predicate `source = 'account' AND
// purged_at IS NULL`. Both terms are load-bearing and each is a one-line
// deletion with the suite otherwise green:
//
//   - drop `source = 'account'` → a confirmed-live verdict deletes a
//     RECORD-sourced row: the terminal tier erases a user's own standing
//     opt-out because their account flickered, and the bridge federates for
//     somebody who asked it to stop and never asked it to start again;
//   - drop `purged_at IS NULL` → a committed purge becomes clearable: the
//     account comes back live, the row goes, and the bridge publishes under an
//     identity every peer was told is gone.
func TestFederationPrefs_ClearRequestedPurgeClearsOnlyUnpurgedAccountRows(t *testing.T) {
	database := federationPrefsTestDB(t)
	repo := NewFederationPrefs(database)
	ctx := context.Background()

	// (1) A RECORD-sourced row: the user's own opt-out, not this tier's.
	_, err := repo.Upsert(ctx, fpOptOut(testDID, FederationPrefSourceRecord))
	require.NoError(t, err)
	// (2) An ACCOUNT-sourced row whose purge COMMITTED.
	purged := mustPurgedPref(t, repo, testSecondDID, FederationPrefSourceAccount)
	// (3) An ACCOUNT-sourced row that is still only a REQUEST.
	_, err = repo.Upsert(ctx, fpOptOut(fpThirdDID, FederationPrefSourceAccount))
	require.NoError(t, err)

	cleared, err := repo.ClearRequestedPurge(ctx, testDID)
	require.NoError(t, err)
	assert.False(t, cleared,
		"a record-sourced row is NOT this tier's to withdraw: enabled=false written off the "+
			"user's own record means THEY asked, and a confirmed-live account is no evidence "+
			"they changed their mind")
	_, err = repo.Get(ctx, testDID)
	assert.NoError(t, err, "the user's own opt-out survives the terminal tier's housekeeping")

	cleared, err = repo.ClearRequestedPurge(ctx, testSecondDID)
	require.NoError(t, err)
	assert.False(t, cleared,
		"a COMMITTED purge is not a request any more: peers were already asked to delete, "+
			"and the account being live again does not un-ask them")
	survivor, err := repo.Get(ctx, testSecondDID)
	require.NoError(t, err, "the purged row survives")
	require.NotNil(t, survivor.PurgedAt)
	assert.True(t, survivor.PurgedAt.Equal(*purged.PurgedAt), "stamp untouched")

	// Positive control: the row the method EXISTS to clear — this tier's own
	// request, nothing irreversible done yet — really is cleared, or the two
	// refusals above are satisfied by a method that clears nothing and a live
	// account stays permanently opted out by a deletion that never happened.
	cleared, err = repo.ClearRequestedPurge(ctx, fpThirdDID)
	require.NoError(t, err)
	assert.True(t, cleared, "an unpurged account-sourced request is withdrawn")
	_, err = repo.Get(ctx, fpThirdDID)
	assert.True(t, errors.IsNotFound(err),
		"and absence restores default-on for the user who came back, got %v", err)
}

// TestFederationPrefs_MarkPurgedFirstTimestampWins pins the COALESCE in
// markPurged. The one-line deletion this kills: `purged_at = now()` instead of
// `COALESCE(purged_at, now())` — an idempotent purge replay (both doors
// document the replay as ordinary) would silently move the only record of WHEN
// the user's content was actually withdrawn.
func TestFederationPrefs_MarkPurgedFirstTimestampWins(t *testing.T) {
	database := federationPrefsTestDB(t)
	repo := NewFederationPrefs(database)
	ctx := context.Background()

	first := mustPurgedPref(t, repo, testDID, FederationPrefSourceRecord)

	// now() has microsecond resolution and the two marks land in different
	// transactions; the sleep makes "the timestamp did not move" a real
	// assertion rather than one the clock usually wins by accident.
	time.Sleep(25 * time.Millisecond)

	// The replay arrives through the OTHER writer — the tx seam the purge
	// itself uses — so both entry points share the guarded statement.
	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, repo.MarkPurgedTx(ctx, tx, testDID),
		"a re-mark is an ordinary replay, not an error")
	require.NoError(t, tx.Commit())

	again, err := repo.Get(ctx, testDID)
	require.NoError(t, err)
	require.NotNil(t, again.PurgedAt)
	assert.True(t, again.PurgedAt.Equal(*first.PurgedAt),
		"the FIRST commit is the one that happened: a retry that re-enqueues an idempotent "+
			"withdrawal must not move the date")
	assert.False(t, again.UpdatedAt.Before(first.UpdatedAt),
		"while updated_at still moves — proof the second call really executed")
}

// TestFederationPrefs_MarkPurgedOnAMissingRowIsNotFound keeps the hole
// visible. The one-line deletion this kills: `affected == 0 → nil` — a purge
// that committed with no preference to mark is a hole in the caller's
// ordering, and NotFound is the only signal the Purger has to Warn about it;
// swallowed here, a later come-back is silently forced off with no operator
// trace anywhere.
func TestFederationPrefs_MarkPurgedOnAMissingRowIsNotFound(t *testing.T) {
	database := federationPrefsTestDB(t)
	repo := NewFederationPrefs(database)
	ctx := context.Background()

	err := repo.MarkPurged(ctx, testDID)
	require.Error(t, err, "marking a preference that does not exist must be reported")
	assert.True(t, errors.IsNotFound(err), "want NotFound, got %v", err)
}

// TestFederationPrefs_UpsertNeverTouchesPurgedAt pins the deliberate ABSENCE
// of purged_at from Upsert's INSERT and DO UPDATE lists. The one-line
// "completion" this kills: adding `purged_at = EXCLUDED.purged_at` (or any
// spelling that lets the statement write the column) — after which a
// re-delivered account event, a probe refresh, or ANY caller constructing a
// model blanks the stamp, and the row silently degrades from "withdrawal
// committed" back to "request", which Delete and ClearRequestedPurge would
// then happily remove.
func TestFederationPrefs_UpsertNeverTouchesPurgedAt(t *testing.T) {
	database := federationPrefsTestDB(t)
	repo := NewFederationPrefs(database)
	ctx := context.Background()

	purged := mustPurgedPref(t, repo, testDID, FederationPrefSourceRecord)

	// A later preference write with every field changed — the model carries
	// PurgedAt nil, exactly as any caller would construct it.
	rewritten, err := repo.Upsert(ctx, FederationPref{
		DID:     testDID,
		Enabled: true,
		Source:  FederationPrefSourceProbe,
	})
	require.NoError(t, err)
	require.NotNil(t, rewritten)

	require.NotNil(t, rewritten.PurgedAt,
		"no preference write can make 'peers were asked to delete' untrue: purged_at is "+
			"absent from the upsert statement entirely, and MarkPurged is its only writer")
	assert.True(t, rewritten.PurgedAt.Equal(*purged.PurgedAt), "the stamp is byte-for-byte the original")

	// Positive control: the upsert really did apply — the guard is one omitted
	// column, not a statement that stopped writing. (That enabled=true CAN land
	// on a purged row is the documented state of finding 6: "purged ⇒ never
	// federate" is enforced structurally on the ap_actors mirror and by
	// call-graph convention here.)
	got, err := repo.Get(ctx, testDID)
	require.NoError(t, err)
	assert.True(t, got.Enabled, "the preference fields themselves moved")
	assert.Equal(t, FederationPrefSourceProbe, got.Source)
	require.NotNil(t, got.PurgedAt, "and the read-back agrees with the returned row")
	assert.True(t, got.PurgedAt.Equal(*purged.PurgedAt))
}
