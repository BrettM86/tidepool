package identity

import (
	"bytes"
	"context"
	"crypto/rsa"
	"database/sql"
	"fmt"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/repo"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Pagination correctness, made observable.
//
// The walk pages through bridged_actors and ap_actors while REWRITING the very
// column it is selecting on. That is the classic shape of a silent data-loss
// bug: an OFFSET window (or any window whose ordering key the walk mutates)
// shifts as rows are updated, and rows slide past the reader unseen. The report
// would still say the table was covered — and the operator, reading a clean
// inventory, would unset BRIDGE_KEK_PREVIOUS and destroy the keys that were
// skipped.
//
// So the claim to pin is not "the code uses keyset pagination"; it is the
// consequence: with more rows than one page holds, EVERY row is moved, each
// exactly once. Both halves matter. A skipped row is a key lost at the moment
// the previous KEK is retired; a double-counted row is an inventory an
// operator cannot reconcile against the table.
//
// BOTH paging tables are covered, because they page on different columns and
// the bug is per-implementation: bridged_actors walks a BIGSERIAL id, ap_actors
// walks its did TEXT primary key. A keyset bug in one is invisible in the
// other, and the ap_actors keys are what let a Coves user sign a single
// outbound request.

// resealPageOverflow is deliberately larger than resealBatchSize and not a
// multiple of it, so the walk must handle a short final page as well as a full
// one — the off-by-one that only shows up at a page boundary.
const resealPageOverflow = 150

// paginationDID is a distinct DID space from the drill's fixtures.
func paginationDID(i int) string { return fmt.Sprintf("did:plc:pagination%010d", i) }

// seedManyBridgedActors inserts n escrowed actors sealed under kek, returning
// each one's plaintext key by DID. The insert is direct SQL rather than
// UpsertActor: this is bulk fixture data, and the store path's per-row
// validation and RETURNING scan would dominate the test's runtime without
// testing anything the store's own tests do not.
func seedManyBridgedActors(t *testing.T, ctx context.Context, database *sql.DB, kek []byte, n int) map[string]*atcrypto.PrivateKeyK256 {
	t.Helper()
	custodian, err := NewCustodian(kek)
	require.NoError(t, err)

	keys := make(map[string]*atcrypto.PrivateKeyK256, n)
	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO bridged_actors (ap_actor_id, actor_type, did, handle, signing_key, consent_state)
		VALUES ($1, 'person', $2, $3, $4, 'ok')`)
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()

	for i := 0; i < n; i++ {
		did := paginationDID(i)
		key, err := atcrypto.GeneratePrivateKeyK256()
		require.NoError(t, err)
		sealed, err := custodian.EncryptActorKey(did, key)
		require.NoError(t, err)
		_, err = stmt.ExecContext(ctx,
			fmt.Sprintf("https://lemmy.world/u/page-%d", i),
			did,
			fmt.Sprintf("page-%d.lemmy-world.tidepool.example", i),
			sealed)
		require.NoError(t, err)
		keys[did] = key
	}
	require.NoError(t, tx.Commit())
	return keys
}

// seedManyAPActors inserts n Coves-side AP actors whose RSA keys are sealed
// under kek. Direct SQL for the same reason as above.
//
// All n rows carry the SAME RSA private key: generating 150 of them would cost
// more than the rest of the package's runtime put together, and it would buy
// nothing here. The AAD binds each ciphertext to its own DID, so the 150 sealed
// blobs are still 150 distinct byte strings, each of which only opens when
// asked for under the DID it was stored beside — which is the property the
// reopen assertions actually lean on.
func seedManyAPActors(t *testing.T, ctx context.Context, database *sql.DB, kek []byte, n int) *rsa.PrivateKey {
	t.Helper()
	custodian, err := NewCustodian(kek)
	require.NoError(t, err)
	key := testRSAKey(t)

	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO ap_actors (
			did, kind, actor_id, normalized_origin, local_part,
			rsa_key_sealed, rsa_key_version, public_key_pem
		) VALUES ($1, 'person', $2, 'coves.social', $3, $4, 1, $5)`)
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()

	for i := 0; i < n; i++ {
		did := paginationDID(i)
		sealed, err := custodian.EncryptActorRSAKey(did, key)
		require.NoError(t, err)
		_, err = stmt.ExecContext(ctx,
			did,
			"https://coves.social/ap/actor/"+did,
			fmt.Sprintf("page-%d", i),
			sealed,
			"-----BEGIN PUBLIC KEY-----\nTEST\n-----END PUBLIC KEY-----\n")
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
	return key
}

// seedRotationKeyForPagination gives the fixture the escrow rotation key a
// database with this many identities in it would really have.
//
// It is not decoration. A populated database with no plc-rotation row is the
// restore-gone-wrong case, and the walk now fails the run over it — correctly,
// since the next boot would mint a replacement and orphan every DID's recovery
// path. A pagination fixture that seeds 150 identities and no rotation key is
// therefore not a healthy database, and the walk is right to say so.
func seedRotationKeyForPagination(t *testing.T, ctx context.Context, database *sql.DB, kek []byte) {
	t.Helper()
	custodian, err := NewCustodian(kek)
	require.NoError(t, err)
	_, err = LoadOrCreateRotationKey(ctx, database, store.NewServiceKeys(database), custodian)
	require.NoError(t, err)
}

// TestReseal_CoversEveryRowAcrossPages walks each paging table with more rows
// than one page holds.
//
// GIVEN 150 sealed rows under KEK A and a page size of 100, WHEN the drill
// runs, THEN all 150 are re-sealed — the counts add up to exactly the number of
// rows in the table, every key opens under the current KEK alone as its
// original self, and a second pass finds nothing left to do.
func TestReseal_CoversEveryRowAcrossPages(t *testing.T) {
	require.Greater(t, resealPageOverflow, resealBatchSize,
		"the fixture must exceed one page or this test never crosses a page boundary and pagination goes untested")
	require.NotZero(t, resealPageOverflow%resealBatchSize,
		"the fixture must NOT be a whole number of pages, so the short final page is exercised too")

	kekA, kekB := previousTestKEK(), currentTestKEK()

	for _, leg := range []struct {
		// table is the postgres table this leg fills and asserts against.
		table string
		// counts pulls this leg's bucket out of the report.
		counts func(*ResealReport) ResealCounts
		// seed fills the table with resealPageOverflow rows under kekA and
		// returns a verify closure that re-opens every one of them under the
		// current KEK alone and checks the key material survived.
		seed func(t *testing.T, ctx context.Context, database *sql.DB) (verify func(t *testing.T, ctx context.Context))
	}{
		{
			// Keyset column: the BIGSERIAL id, while the walk rewrites
			// signing_key.
			table:  "bridged_actors",
			counts: func(r *ResealReport) ResealCounts { return r.BridgedActors },
			seed: func(t *testing.T, ctx context.Context, database *sql.DB) func(*testing.T, context.Context) {
				original := seedManyBridgedActors(t, ctx, database, kekA, resealPageOverflow)
				return func(t *testing.T, ctx context.Context) {
					actorKeys := NewActorKeys(store.NewBridgedActors(database), currentOnly(t))
					for i := 0; i < resealPageOverflow; i++ {
						did := paginationDID(i)
						got, err := actorKeys.SigningKey(ctx, did, repo.KeyUseWrite)
						require.NoError(t, err,
							"row %d of %d must open under the current KEK alone after the walk; a row left under the previous KEK is invisible in a clean report and lost when the old key is retired", i, resealPageOverflow)
						gotK256, ok := got.(*atcrypto.PrivateKeyK256)
						require.True(t, ok)
						require.True(t, bytes.Equal(original[did].Bytes(), gotK256.Bytes()),
							"row %d must still hold its ORIGINAL key material", i)
					}
				}
			},
		},
		{
			// Keyset column: the did TEXT primary key, while the walk rewrites
			// rsa_key_sealed. A different column type and a different ordering
			// than bridged_actors, so its paging is a separate thing to get
			// wrong — and these are the keys behind every HTTP signature a
			// Coves user sends.
			table:  "ap_actors",
			counts: func(r *ResealReport) ResealCounts { return r.APActors },
			seed: func(t *testing.T, ctx context.Context, database *sql.DB) func(*testing.T, context.Context) {
				original := seedManyAPActors(t, ctx, database, kekA, resealPageOverflow)
				return func(t *testing.T, ctx context.Context) {
					custodian := currentOnly(t)
					for i := 0; i < resealPageOverflow; i++ {
						did := paginationDID(i)
						opened, err := custodian.DecryptActorRSAKey(did, storedRSAKeySealed(t, database, did))
						require.NoError(t, err,
							"row %d of %d must open under the current KEK alone after the walk; a Coves user whose RSA key stayed under the previous KEK cannot sign one outbound request once it is retired, and peers reject the failures silently", i, resealPageOverflow)
						require.True(t, opened.Equal(original),
							"row %d must still hold its ORIGINAL key material", i)
					}
				}
			},
		},
	} {
		t.Run(leg.table, func(t *testing.T) {
			database := testutil.DB(t)
			testutil.Truncate(t, database, "bridged_actors", "ap_actors", "service_keys")
			ctx := t.Context()

			verify := leg.seed(t, ctx, database)
			seedRotationKeyForPagination(t, ctx, database, kekA)

			report, err := Reseal(ctx, database, kekB, kekA)
			require.NoError(t, err)

			assert.Equal(t, ResealCounts{Resealed: resealPageOverflow, AlreadyCurrent: 0, Skipped: 0, Failed: 0}, leg.counts(report),
				"every row across every page must be re-sealed exactly once: a row the paging window slid past is a key the operator destroys the moment they unset BRIDGE_KEK_PREVIOUS, and a row counted twice is an inventory that can never be reconciled against the table")

			// The counts must account for the table, not merely for whatever
			// the walk happened to visit. Reading the row count back from
			// postgres is what makes "no row skipped" checkable rather than
			// asserted against itself.
			var rowCount int
			require.NoError(t, database.QueryRowContext(ctx,
				`SELECT count(*) FROM `+leg.table).Scan(&rowCount))
			counts := leg.counts(report)
			assert.Equal(t, rowCount, counts.Resealed+counts.AlreadyCurrent+counts.Skipped+counts.Failed,
				"the four buckets must sum to the number of rows in %s; any other total means the report is describing a different set of rows than the one on disk", leg.table)

			// And the counts are backed by key material. A tally is only worth
			// what the rows behind it can still decrypt.
			verify(t, ctx)

			// Ordering stability: a second pass over the same rows must find
			// nothing left to do. If the walk's window depended on the values
			// it rewrites, the re-run is where the drift shows up as a non-zero
			// re-seal count.
			second, err := Reseal(ctx, database, kekB, kekA)
			require.NoError(t, err)
			assert.Equal(t, ResealCounts{Resealed: 0, AlreadyCurrent: resealPageOverflow, Skipped: 0, Failed: 0}, leg.counts(second),
				"the zero-run gate must read zero across page boundaries too: an operator with 150 rows in %s decides to unset BRIDGE_KEK_PREVIOUS on exactly this number", leg.table)
		})
	}
}
