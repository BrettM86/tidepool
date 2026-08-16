package identity

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/repo"
)

// The optimistic guard on the re-seal walk's write-back.
//
// IMPORTANT, and the reason the concurrent writer here is INJECTED BY THE
// TEST: production has no writer that DELIBERATELY rewrites an existing
// bridged_actors.signing_key, ap_actors.rsa_key_sealed, or the plc-rotation
// key_material, so there is no racer to reproduce by simply running the real
// code. That is weaker than "no writer can reach that column". UpsertActor's
// DO UPDATE carries `signing_key = COALESCE(EXCLUDED.signing_key, ...)`
// (store/bridged_actors.go:50) and materialize's re-mint retry does supply a
// freshly sealed key on the conflicting insert (materialize/actors.go:175), so
// a re-mint that lands mid-walk is a plausible live racer today — a slim path,
// but not an impossible one.
//
// Either way the conclusion is the same, and it is why these tests exist: the
// guard is safe BY CONSTRUCTION, not safe by audit. It must hold for writers
// nobody has enumerated, because the day one is added (key claiming, a
// per-actor rotation, a second operator running the drill from another shell)
// is precisely the day nobody will re-derive whether the walk can clobber.
// These tests inject a writer on demand, so the property is pinned before it
// is load-bearing.
//
// What the guard must give: the walk NEVER writes bytes it did not read. Its
// UPDATE is conditioned on the exact blob it decided about, and a lost guard
// sends it back to re-read and decide again on the bytes that are really
// there. Clobbering is unrecoverable — the overwritten value is key material
// that exists nowhere else.

// hookDriver wraps lib/pq so a test can fire a callback the instant a query
// whose text contains marker returns. That is the only seam that puts a
// concurrent write BETWEEN the walk's SELECT and its guarded UPDATE, and it
// needs nothing from production code: reseal.go has no test hooks in it, and
// must not grow any.
type hookDriver struct {
	marker string
	fire   func()
}

func (d *hookDriver) Open(name string) (driver.Conn, error) {
	conn, err := pq.Driver{}.Open(name)
	if err != nil {
		return nil, err
	}
	return &hookConn{Conn: conn, owner: d}, nil
}

type hookConn struct {
	driver.Conn
	owner *hookDriver
}

func (c *hookConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	rows, err := queryer.QueryContext(ctx, query, args)
	if err == nil && strings.Contains(query, c.owner.marker) {
		c.owner.fire()
	}
	return rows, err
}

// racingDriverSeq numbers registered driver names. sql.Register PANICS on a
// duplicate name and offers no way to unregister, so the name cannot be
// derived from anything that repeats — and t.Name() repeats on the very first
// thing anyone reaches for when a concurrency test looks flaky: `-count=2`.
// A monotonic counter is unique for the life of the process, which is exactly
// the lifetime of the registry it guards.
var racingDriverSeq atomic.Uint64

// racingDB returns a second pool onto the test database whose queries fire
// hook exactly once — on the first query carrying marker.
func racingDB(t *testing.T, marker string, hook func()) *sql.DB {
	t.Helper()
	databaseURL := os.Getenv("TIDEPOOL_TEST_DATABASE_URL")
	require.NotEmpty(t, databaseURL,
		"the racing pool needs the same database the harness migrated")

	var once sync.Once
	// t.Name() stays in the name for readability in a panic or a pq log line;
	// the counter is what makes it unique.
	name := fmt.Sprintf("tidepool-reseal-race-%s-%d", t.Name(), racingDriverSeq.Add(1))
	sql.Register(name, &hookDriver{
		marker: marker,
		fire:   func() { once.Do(hook) },
	})
	database, err := sql.Open(name, databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = database.Close() })
	return database
}

// bridgedActorPageQuery is the fragment identifying the walk's paging SELECT
// over bridged_actors — deliberately not the single-row re-read, which must
// see the injected bytes rather than trigger another injection.
const bridgedActorPageQuery = "SELECT id, did, signing_key"

// injectSigningKey overwrites one actor's sealed key out of band: the
// concurrent writer the codebase does not yet have.
func injectSigningKey(t *testing.T, database *sql.DB, did string, blob []byte) {
	t.Helper()
	_, err := database.Exec(
		`UPDATE bridged_actors SET signing_key = $2 WHERE did = $1`, did, blob)
	require.NoError(t, err)
}

// sealedActorKeyUnder mints a fresh signing key for did and seals it under
// kek, returning both halves.
func sealedActorKeyUnder(t *testing.T, kek []byte, did string) (*atcrypto.PrivateKeyK256, []byte) {
	t.Helper()
	custodian, err := NewCustodian(kek)
	require.NoError(t, err)
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	sealed, err := custodian.EncryptActorKey(did, key)
	require.NoError(t, err)
	return key, sealed
}

// TestReseal_ConcurrentRewriteIsNotClobbered pins the guard against a writer
// injected by the test.
func TestReseal_ConcurrentRewriteIsNotClobbered(t *testing.T) {
	kekA, kekB := previousTestKEK(), currentTestKEK()

	// The plain case: another process finished re-sealing this row before the
	// walk ever looked at it. No race window is needed — the walk simply must
	// not touch a blob that already opens under the current KEK.
	t.Run("already current before the walk started", func(t *testing.T) {
		fixture := seedRotationDrill(t)
		ctx := t.Context()

		otherKey, otherSealed := sealedActorKeyUnder(t, kekB, resealLiveDID)
		injectSigningKey(t, fixture.database, resealLiveDID, otherSealed)

		report, err := Reseal(ctx, fixture.database, kekB, kekA)
		require.NoError(t, err)
		assert.Equal(t, ResealCounts{Resealed: 1, AlreadyCurrent: 1, Skipped: 1, Failed: 0}, report.BridgedActors,
			"a row already sealed under the current KEK must count as AlreadyCurrent, not as work this walk did; counting someone else's write as our own is how a drill reports progress it did not make")

		assert.Equal(t, otherSealed, storedSigningKey(t, fixture.database, resealLiveDID),
			"a blob already under the current KEK must be left byte-identical")
		requireSigningKeyEquals(t, ctx, NewActorKeys(fixture.actors, currentOnly(t)), resealLiveDID, repo.KeyUseWrite, otherKey,
			"the surviving key must be the one the other writer stored, not the one this walk happened to read first")
	})

	// The real race: the write lands between the walk's SELECT and its
	// guarded UPDATE. Without the guard the walk would re-seal the plaintext
	// it read a moment ago and overwrite the newer key material with an older
	// one — silently, since both blobs open under the current KEK afterwards.
	t.Run("re-sealed by another process mid-walk", func(t *testing.T) {
		fixture := seedRotationDrill(t)
		ctx := t.Context()

		winnerKey, winnerSealed := sealedActorKeyUnder(t, kekB, resealLiveDID)
		injected := false
		racing := racingDB(t, bridgedActorPageQuery, func() {
			injectSigningKey(t, fixture.database, resealLiveDID, winnerSealed)
			injected = true
		})

		report, err := Reseal(ctx, racing, kekB, kekA)
		require.True(t, injected,
			"the concurrent write must actually have landed mid-walk, or this test proves nothing about the guard")
		require.NoError(t, err,
			"losing a guard is not a failure: the row ended up under the current KEK, which is all the drill wanted")
		assert.Equal(t, ResealCounts{Resealed: 1, AlreadyCurrent: 1, Skipped: 1, Failed: 0}, report.BridgedActors,
			"a row rewritten under our feet must be re-read and RECLASSIFIED — here as AlreadyCurrent — rather than counted from the stale decision; a count taken before the write is a count of what the walk intended, not of what the database holds")

		assert.Equal(t, winnerSealed, storedSigningKey(t, fixture.database, resealLiveDID),
			"the walk must not overwrite a blob it never read: these bytes are the only copy of that key, and the walk's replacement would silently substitute an older key for a newer one")
		requireSigningKeyEquals(t, ctx, NewActorKeys(fixture.actors, currentOnly(t)), resealLiveDID, repo.KeyUseWrite, winnerKey,
			"the concurrent writer's key must be the one that survives")
	})

	// The same race, but the interloper wrote under the PREVIOUS KEK — so
	// there is still work to do, on bytes this walk has not seen. The re-read
	// must feed a fresh decision: the walk re-seals what is THERE, never the
	// plaintext it opened before the write landed.
	t.Run("rewritten under the previous KEK mid-walk", func(t *testing.T) {
		fixture := seedRotationDrill(t)
		ctx := t.Context()

		interloperKey, interloperSealed := sealedActorKeyUnder(t, kekA, resealLiveDID)
		injected := false
		racing := racingDB(t, bridgedActorPageQuery, func() {
			injectSigningKey(t, fixture.database, resealLiveDID, interloperSealed)
			injected = true
		})

		report, err := Reseal(ctx, racing, kekB, kekA)
		require.NoError(t, err)
		require.True(t, injected,
			"the concurrent write must actually have landed mid-walk, or this test proves nothing about the guard")
		assert.Equal(t, ResealCounts{Resealed: 2, AlreadyCurrent: 0, Skipped: 1, Failed: 0}, report.BridgedActors,
			"a lost guard on a row that still needs moving must be retried, not abandoned: leaving it under the previous KEK while the report says the table is clean is exactly the state that makes unsetting BRIDGE_KEK_PREVIOUS destroy a key")

		requireSigningKeyEquals(t, ctx, NewActorKeys(fixture.actors, currentOnly(t)), resealLiveDID, repo.KeyUseWrite, interloperKey,
			"after the retry the row must hold the INTERLOPER's key sealed under the current KEK; re-sealing the plaintext the walk read before the write would resurrect a key the other writer had already replaced")
		assert.NotEqual(t, interloperSealed, storedSigningKey(t, fixture.database, resealLiveDID),
			"the row must actually have been re-sealed — same key material, new KEK — not merely left as the interloper wrote it")
	})
}
