package identity

import (
	"bytes"
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// countServiceKeys reports how many rows service_keys holds, so a test can
// prove that a failed load created nothing.
func countServiceKeys(t *testing.T, database *sql.DB) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`SELECT count(*) FROM service_keys`).Scan(&n))
	return n
}

// storedRotationMaterial returns the sealed bytes currently at rest.
func storedRotationMaterial(t *testing.T, ctx context.Context, keys store.ServiceKeys) []byte {
	t.Helper()
	stored, err := keys.Get(ctx, RotationKeyName)
	require.NoError(t, err)
	return append([]byte(nil), stored.KeyMaterial...)
}

// TestLoadOrCreateRotationKey_WrongKEKIsTheBootCanary is a CHARACTERIZATION
// test: it passes at birth and exists so the behavior cannot regress quietly.
//
// What it pins is a boot-time property with no test seam of its own.
// cmd/tidepool/main.go builds the custodian at line 173 and calls
// LoadOrCreateRotationKey at line 257 — unconditionally, and some 350 lines
// before ListenAndServe at line 604. So a bridge started under the wrong
// BRIDGE_KEK dies during startup rather than serving traffic with key
// material it cannot read. That ordering is deliberate and there is no
// injectable boot seam to assert it through; this test is the tier where the
// fact is observable, because LoadOrCreateRotationKey returning an error IS
// the mechanism that stops the process.
//
// The canary depends on the create path being unreachable on a provisioned
// database: LoadOrCreateRotationKey only mints a fresh key when Get reports
// NotFound, and store.ServiceKeys.Get produces NotFound from sql.ErrNoRows
// alone. A row that exists but will not decrypt is an error, never an
// invitation to generate a replacement — if it ever became one, the bridge
// would boot happily onto a brand-new rotation key while every bridged DID
// still listed the old one as its rotation authority.
//
// GIVEN a plc-rotation row sealed under KEK A,
// WHEN LoadOrCreateRotationKey runs with a custodian holding only KEK B,
// THEN it fails and changes nothing; and WHEN it runs with
// NewCustodianWithPrevious(B, A) it succeeds and returns the original key —
// the legitimate way past the canary for an operator mid-rotation.
func TestLoadOrCreateRotationKey_WrongKEKIsTheBootCanary(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "service_keys")
	keys := store.NewServiceKeys(database)
	ctx := t.Context()

	kekA := previousTestKEK()
	kekB := currentTestKEK()

	// GIVEN: the bridge has booted once under KEK A and persisted its escrow
	// rotation key sealed under it.
	custodianA, err := NewCustodian(kekA)
	require.NoError(t, err)
	original, err := LoadOrCreateRotationKey(ctx, keys, custodianA)
	require.NoError(t, err)

	sealedUnderA := storedRotationMaterial(t, ctx, keys)
	rowsAfterSeed := countServiceKeys(t, database)
	require.Equal(t, 1, rowsAfterSeed, "the seeded fixture is one plc-rotation row")

	// WHEN: the operator restarts with a different BRIDGE_KEK and nothing
	// else — the fat-fingered rotation, or a config rollback.
	custodianB, err := NewCustodian(kekB)
	require.NoError(t, err)
	_, err = LoadOrCreateRotationKey(ctx, keys, custodianB)

	// THEN: boot fails. This error is what keeps the process from reaching
	// ListenAndServe.
	require.Error(t, err,
		"a rotation key that will not open under the configured BRIDGE_KEK must fail the load; this error is the only thing standing between a wrong-KEK config and a bridge serving traffic it cannot sign for")

	// And it fails without touching anything. The dangerous failure mode is
	// not the error — it is a load that quietly mints a replacement, leaving
	// the bridge holding a rotation key that no bridged DID document names,
	// with the real one still sealed under a key nobody configured.
	assert.Equal(t, rowsAfterSeed, countServiceKeys(t, database),
		"a failed rotation-key load must not create a second service_keys row; minting a replacement would strand every bridged DID under a rotation authority the bridge no longer holds")
	assert.True(t, bytes.Equal(sealedUnderA, storedRotationMaterial(t, ctx, keys)),
		"a failed rotation-key load must leave the sealed bytes exactly as they were, so pointing BRIDGE_KEK back at the right key is a full recovery")

	// WHEN: the same operator declares the old key as the previous one —
	// the rotation done properly.
	rotating, err := NewCustodianWithPrevious(kekB, kekA)
	require.NoError(t, err)
	rescued, err := LoadOrCreateRotationKey(ctx, keys, rotating)

	// THEN: boot proceeds, on the ORIGINAL key. This is the whole point of
	// run 1: the canary is not weakened, it is given a legitimate way past.
	require.NoError(t, err,
		"a custodian carrying the previous KEK must get past the boot canary; without it an operator cannot change BRIDGE_KEK at all without losing the escrow rotation key")
	assert.True(t, bytes.Equal(original.Bytes(), rescued.Bytes()),
		"the rescued rotation key must be the ORIGINAL key, not a fresh one; it is the rotation authority listed in every bridged DID document, and a different key there is an unrecoverable loss of control over those DIDs")

	// The rescue is read-only. Run 1 reads under two keys; nothing re-seals
	// the row under the new KEK yet, and this test says so out loud rather
	// than leaving a later re-seal to arrive unnoticed.
	assert.True(t, bytes.Equal(sealedUnderA, storedRotationMaterial(t, ctx, keys)),
		"reading under the previous KEK must not silently re-seal the row under the current one; until re-sealing exists, BRIDGE_KEK_PREVIOUS remains load-bearing and an operator who drops it early loses the rotation key")
	assert.Equal(t, rowsAfterSeed, countServiceKeys(t, database),
		"the rescue path must not add a row either")
}
