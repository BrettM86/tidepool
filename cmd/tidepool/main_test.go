package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/identity"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// capturedLog collects the records a command logs, because for rotate-kek the
// log IS the product. The command's only other output is an exit code, and the
// operator's decision to retire a KEK is made entirely from these lines — so
// asserting on them is asserting on the deliverable, not on incidental
// chatter.
type capturedLog struct {
	slog.Handler
	records *[]slog.Record
}

func (c capturedLog) Handle(ctx context.Context, record slog.Record) error {
	*c.records = append(*c.records, record.Clone())
	return c.Handler.Handle(ctx, record)
}

// capturingLogger returns a logger and the slice its records land in. The
// wrapped text handler writes to a discard buffer: the point is the structured
// record, not the rendering.
func capturingLogger() (*slog.Logger, *[]slog.Record) {
	records := new([]slog.Record)
	return slog.New(capturedLog{
		Handler: slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}),
		records: records,
	}), records
}

// attr pulls one attribute off a record, reporting whether it was there.
func attr(record slog.Record, key string) (slog.Value, bool) {
	var value slog.Value
	found := false
	record.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			value, found = a.Value, true
			return false
		}
		return true
	})
	return value, found
}

// tableLines indexes the per-table inventory lines by their table attribute.
func tableLines(records []slog.Record) map[string]slog.Record {
	lines := map[string]slog.Record{}
	for _, record := range records {
		if record.Message != "kek re-seal table" {
			continue
		}
		if table, ok := attr(record, "table"); ok {
			lines[table.String()] = record
		}
	}
	return lines
}

func TestDispatchRejectsUnknownCommands(t *testing.T) {
	err := dispatch(testLogger(), []string{"serve"})
	require.Error(t, err)
	// UPDATED for the KEK rotation drill: the usage line is the only place an
	// operator learns that rotate-kek exists at all. A subcommand the binary
	// answers to but never advertises is a subcommand nobody runs — and the
	// one that has to be run is the one that lets BRIDGE_KEK_PREVIOUS be
	// retired, so a rotation left half-finished is the cost of omitting it.
	assert.Contains(t, err.Error(), "usage: tidepool [migrate|rotate-kek]")
}

func TestRunMigrationsRequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	err := runMigrations(testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DATABASE_URL is required")
}

// The rotate-kek subcommand.
//
// It follows the minimal-env principle main.go states for migrate: an
// operational one-shot must not be blockable by configuration it does not
// use. rotate-kek reads exactly three variables — DATABASE_URL, BRIDGE_KEK,
// BRIDGE_KEK_PREVIOUS — and nothing else. That is not cosmetic. The operator
// running this command is mid-rotation, quite possibly from a one-off
// container or a maintenance shell where the bridge's HTTP, PLC, and relay
// settings are absent; a rotation that refuses to start because
// AP_USER_ORIGIN is unset strands every sealed blob under a KEK the operator
// is trying to retire.
//
// The tests below set ONLY those three variables. They inherit whatever
// environment the test process has, which does not carry the bridge's
// production config — so a rotate-kek that reached for config.Load would fail
// naming some unrelated variable and every assertion here would miss. That is
// the pressure: read the three, then act.

// Two distinct well-formed KEKs. Content is irrelevant — these tests never
// reach a cipher — but the LENGTH is not: a rejection for the wrong reason
// would let a missing-variable test pass while the variable was present.
const (
	rotateTestKEK         = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	rotateTestPreviousKEK = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	// rotateTestKEKBase64 is rotateTestKEK's 32 bytes in the other accepted
	// spelling. Both variables take either encoding, so the same key can wear
	// two different strings — which is exactly what a string comparison would
	// wave through.
	rotateTestKEKBase64 = "qrvM3e7/ABEiM0RVZneImaq7zN3u/wARIjNEVWZ3iJk="
	// A syntactically valid URL pointing at a port nothing listens on. The
	// env-var checks must all happen BEFORE the database is dialled: an
	// operator who forgot BRIDGE_KEK_PREVIOUS deserves to be told that, not a
	// connection timeout that sends them to look at postgres.
	rotateTestDatabaseURL = "postgres://tidepool:tidepool@127.0.0.1:1/tidepool?sslmode=disable"
)

func TestDispatchRoutesRotateKEK(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("BRIDGE_KEK", rotateTestKEK)
	t.Setenv("BRIDGE_KEK_PREVIOUS", rotateTestPreviousKEK)

	err := dispatch(testLogger(), []string{"rotate-kek"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DATABASE_URL",
		"`tidepool rotate-kek` must reach the rotation command itself — a subcommand that parses but does nothing is worse than one that does not exist, because the operator's exit code says the rotation succeeded")
}

func TestRunRotateKEKRequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("BRIDGE_KEK", rotateTestKEK)
	t.Setenv("BRIDGE_KEK_PREVIOUS", rotateTestPreviousKEK)

	err := runRotateKEK(testLogger())
	require.Error(t, err,
		"a rotation with nowhere to connect must fail; nothing was re-sealed, and an exit code of 0 is what an operator reads as permission to unset BRIDGE_KEK_PREVIOUS")
	assert.Contains(t, err.Error(), "DATABASE_URL",
		"the error must name the variable the operator has to set, not merely report that something is missing")
}

func TestRunRotateKEKRequiresBridgeKEK(t *testing.T) {
	t.Setenv("DATABASE_URL", rotateTestDatabaseURL)
	t.Setenv("BRIDGE_KEK", "")
	t.Setenv("BRIDGE_KEK_PREVIOUS", rotateTestPreviousKEK)

	err := runRotateKEK(testLogger())
	require.Error(t, err,
		"without BRIDGE_KEK there is no key to re-seal ONTO; proceeding would mean writing blobs under nothing at all")
	msg := err.Error()
	assert.Contains(t, msg, "BRIDGE_KEK",
		"the error must name BRIDGE_KEK")
	assert.NotContains(t, msg, "BRIDGE_KEK_PREVIOUS",
		"only the missing variable may be blamed: BRIDGE_KEK_PREVIOUS was supplied, and an operator sent to fix a variable that is already correct will edit the wrong key mid-rotation")
}

func TestRunRotateKEKRequiresPreviousKEK(t *testing.T) {
	t.Setenv("DATABASE_URL", rotateTestDatabaseURL)
	t.Setenv("BRIDGE_KEK", rotateTestKEK)
	t.Setenv("BRIDGE_KEK_PREVIOUS", "")

	err := runRotateKEK(testLogger())
	require.Error(t, err,
		"rotate-kek without a previous KEK has nothing to move onto the current one; succeeding silently would hand the operator a clean report over an untouched database and invite them to retire a key that is still load-bearing")

	msg := err.Error()
	assert.Contains(t, msg, "BRIDGE_KEK_PREVIOUS",
		"the error must name BRIDGE_KEK_PREVIOUS so the operator sets the one variable that makes the command meaningful")
	// "BRIDGE_KEK_PREVIOUS" contains "BRIDGE_KEK", so cut the full name out
	// before checking that the current key was not also blamed.
	assert.NotContains(t, strings.ReplaceAll(msg, "BRIDGE_KEK_PREVIOUS", ""), "BRIDGE_KEK",
		"the message must not also blame BRIDGE_KEK: it is set and correct, and an operator who 'fixes' it will orphan every blob still sealed under the previous key")
}

// TestRunRotateKEKRefusesEqualKeys pins the guard against the rotation that
// is not one.
//
// One key pasted into both variables — the copy-paste an operator makes when
// they mean to set BRIDGE_KEK_PREVIOUS and reach for the wrong line of their
// secret store — is not a rotation. Left unchecked, the walk would run, find
// every blob already opening under "the current KEK", and report a clean
// zero-run with nothing re-sealed. That report is precisely the signal the
// runbook says clears an operator to unset BRIDGE_KEK_PREVIOUS, so the
// failure mode is not a wasted run: it is a confident retirement of a key
// that everything is still sealed under.
//
// The refusal must also come BEFORE the database is dialled, which is what
// gives this test teeth: without the guard the command reaches db.Open and
// fails against the dead port with an entirely different message.
func TestRunRotateKEKRefusesEqualKeys(t *testing.T) {
	for _, tt := range []struct {
		name     string
		current  string
		previous string
	}{
		// The plain copy-paste.
		{name: "the same string in both variables", current: rotateTestKEK, previous: rotateTestKEK},
		// The same key wearing two spellings. Both variables accept hex or
		// base64, so this pair is byte-identical while comparing unequal as
		// strings — an operator who re-encoded the key they already had gets
		// no warning at all unless the check runs on the DECODED bytes, the
		// same rule config.Load applies at startup.
		{name: "the same key, one hex and one base64", current: rotateTestKEK, previous: rotateTestKEKBase64},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", rotateTestDatabaseURL)
			t.Setenv("BRIDGE_KEK", tt.current)
			t.Setenv("BRIDGE_KEK_PREVIOUS", tt.previous)

			err := runRotateKEK(testLogger())
			require.Error(t, err,
				"re-sealing a key onto itself must be refused: the walk would touch nothing and report the zero-run that tells the operator it is safe to retire BRIDGE_KEK_PREVIOUS — while every blob is still sealed under it")

			msg := strings.ToLower(err.Error())
			assert.Contains(t, msg, "same",
				"the operator must be told the two keys are the SAME key; anything vaguer and they will re-run the command rather than fix the variable")
			assert.Contains(t, msg, "different",
				"and told what is needed instead — two different keys — so the fix is obvious without opening the runbook")

			assert.NotContains(t, err.Error(), "127.0.0.1",
				"the refusal must happen before the database is dialled: a rotation aborted for a bad key pair must never surface as a connection problem, or the operator goes to look at postgres while their KEK variables stay wrong")
		})
	}
}

// The integration leg: rotate-kek against a real database.
//
// Everything above this line exercises the command's argument handling with a
// dead port behind it. That leaves the most consequential line in the file
// untested — the call itself:
//
//	identity.Reseal(ctx, database, current, previous)
//
// Two []byte parameters, adjacent, same type, and no compiler or unit test in
// this package can tell them apart. Swapped, the command would open every blob
// under BRIDGE_KEK_PREVIOUS, find them all "already current", re-seal nothing,
// and print the all-AlreadyCurrent inventory that DEPLOY.md calls the operator's
// clearance to unset BRIDGE_KEK_PREVIOUS — after which nothing opens at all.
// Only a run against real sealed bytes catches it, which is what this is.

// rotateIntegrationDID owns a DID space of its own so this test never collides
// with internal/identity's fixtures in the shared test database.
const rotateIntegrationDID = "did:plc:cmdrotatekek00000000000000"

// rotateKEKBytes decodes the hex KEK constants above into the raw bytes a
// custodian takes, so the test seals under exactly the key the command will be
// handed through the environment.
func rotateKEKBytes(t *testing.T, encoded string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(encoded)
	require.NoError(t, err)
	require.Len(t, raw, identity.KEKSize)
	return raw
}

// rotateIntegrationDB returns the migrated test database, taking the same
// cross-package advisory lock every other postgres-backed package takes — this
// test truncates tables internal/identity's tests also use, so it must not run
// beside them. It skips when TIDEPOOL_TEST_DATABASE_URL is unset, exactly as
// testutil.DB does everywhere else.
func rotateIntegrationDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	database := testutil.DB(t)
	return database, os.Getenv("TIDEPOOL_TEST_DATABASE_URL")
}

// TestRunRotateKEKResealsARealDatabase is the end-to-end pin.
//
// GIVEN a bridged actor and a rotation key sealed under BRIDGE_KEK_PREVIOUS,
// WHEN the operator runs rotate-kek with the three variables set, THEN the
// command exits zero, the actor's key opens under BRIDGE_KEK ALONE as the
// original key, and the log carries the per-table inventory the operator reads
// their decision off — with no failure lines in it.
func TestRunRotateKEKResealsARealDatabase(t *testing.T) {
	database, databaseURL := rotateIntegrationDB(t)
	testutil.Truncate(t, database, "bridged_actors", "ap_actors", "service_keys")
	ctx := t.Context()

	current := rotateKEKBytes(t, rotateTestKEK)
	previous := rotateKEKBytes(t, rotateTestPreviousKEK)
	underPrevious, err := identity.NewCustodian(previous)
	require.NoError(t, err)

	// One escrowed actor, sealed under the key being retired.
	original, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	sealed, err := underPrevious.EncryptActorKey(rotateIntegrationDID, original)
	require.NoError(t, err)
	_, err = database.ExecContext(ctx, `
		INSERT INTO bridged_actors (ap_actor_id, actor_type, did, handle, signing_key, consent_state)
		VALUES ($1, 'person', $2, $3, $4, 'ok')`,
		"https://lemmy.world/u/rotate-integration",
		rotateIntegrationDID,
		"rotate-integration.lemmy-world.tidepool.example",
		sealed)
	require.NoError(t, err)

	// And the escrow rotation key that a database holding identities always
	// has. Without it the walk would (rightly) fail the run as a restore that
	// lost the key, and this test would be measuring that alarm instead of the
	// happy path.
	rotationKey, err := identity.LoadOrCreateRotationKey(ctx, store.NewServiceKeys(database), underPrevious)
	require.NoError(t, err)

	// NEGATIVE CONTROL: under BRIDGE_KEK alone the seeded world is unreadable.
	// Without this the whole test could pass against bytes that were already
	// sealed under the current key, which is precisely the state a swapped
	// argument pair leaves behind.
	underCurrent, err := identity.NewCustodian(current)
	require.NoError(t, err)
	_, err = underCurrent.DecryptActorKey(rotateIntegrationDID, sealed)
	require.Error(t, err,
		"before the rotation the seeded key must NOT open under BRIDGE_KEK alone, or this test is not exercising a rotation at all")

	t.Setenv("DATABASE_URL", databaseURL)
	t.Setenv("BRIDGE_KEK", rotateTestKEK)
	t.Setenv("BRIDGE_KEK_PREVIOUS", rotateTestPreviousKEK)

	logger, records := capturingLogger()
	require.NoError(t, runRotateKEK(logger),
		"a clean rotation over a healthy database must exit zero; an operator who cannot get a zero exit here can never retire BRIDGE_KEK_PREVIOUS")

	// THE ASSERTION THE ARGUMENT ORDER LIVES OR DIES ON: the bytes at rest now
	// open under the CURRENT key, on their own, and are the same key material.
	var stored []byte
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT signing_key FROM bridged_actors WHERE did = $1`, rotateIntegrationDID).Scan(&stored))
	reopened, err := underCurrent.DecryptActorKey(rotateIntegrationDID, stored)
	require.NoError(t, err,
		"after rotate-kek the actor's key must open under BRIDGE_KEK ALONE. If it does not, the command handed Reseal its two keys in the wrong order: every blob was compared against the key being retired, reported AlreadyCurrent, and left exactly where it was — and the operator's next step destroys them")
	assert.True(t, bytes.Equal(original.Bytes(), reopened.Bytes()),
		"the re-sealed key must be the ORIGINAL key material; a different one means the actor's repo can never be signed for again")

	rescued, err := identity.LoadOrCreateRotationKey(ctx, store.NewServiceKeys(database), underCurrent)
	require.NoError(t, err,
		"the escrow rotation key must open under BRIDGE_KEK alone after the rotation, or the bridge cannot boot once BRIDGE_KEK_PREVIOUS is unset")
	assert.True(t, bytes.Equal(rotationKey.Bytes(), rescued.Bytes()),
		"the rotation key must still be the original: it is the authority named in every bridged DID document")

	// The log is the operator's whole view of the run, so it is part of the
	// contract and not an implementation detail.
	lines := tableLines(*records)
	require.Len(t, lines, 3,
		"all three sealed domains must get their own inventory line — bridged_actors, ap_actors, service_keys. A table folded into a total is a table an operator cannot check for the zero-run, and a table missing entirely reads as one that was never walked")
	// All four buckets on every table, and every one of them a NUMBER: an
	// operator filtering their aggregator for failed>0 gets nothing back if
	// the count was rendered as a string.
	for table, want := range map[string][4]int64{
		//                       resealed, already_current, skipped, failed
		"bridged_actors": {1, 0, 0, 0},
		"ap_actors":      {0, 0, 0, 0},
		"service_keys":   {1, 0, 0, 0},
	} {
		require.Contains(t, lines, table)
		for i, key := range []string{"resealed", "already_current", "skipped", "failed"} {
			value, ok := attr(lines[table], key)
			require.True(t, ok,
				"the %s line must carry a %q count: the operator's zero-run gate is read off these four numbers, and a missing one is a bucket they cannot see", table, key)
			require.Equal(t, slog.KindInt64, value.Kind(),
				"%s.%s must be logged as a number, not a rendered string, or an operator's failed>0 filter matches nothing", table, key)
			assert.Equal(t, want[i], value.Int64(),
				"%s.%s must report the work that actually happened; counts that do not match the database are what an operator retires a KEK on", table, key)
		}
	}

	for _, record := range *records {
		assert.NotEqual(t, "kek re-seal failure", record.Message,
			"a healthy rotation must log no failure lines; a spurious one is indistinguishable from a real one and stops the rotation dead")
		assert.NotEqual(t, slog.LevelError, record.Level,
			"nothing may be logged at ERROR on a clean run, got %q", record.Message)
	}
}

// TestRunRotateKEKWarnsWhenTheInventoryIsPartial covers the trap in the log
// above.
//
// logResealReport prints its three table lines unconditionally, because the
// report comes back even on the error path — and that is right for the row
// failure case, where the walk finished every table and the counts are a true
// inventory of a database with some bad rows in it.
//
// But an INFRASTRUCTURE error aborts the walk mid-table. The report handed back
// then is a PREFIX: the counts describe only the rows reached before the
// connection dropped, and the tables after the failure read as clean zeros.
// Those zeros are indistinguishable, line for line, from the ones a healthy
// empty table prints. An operator who reads "ap_actors resealed=0 failed=0",
// sees an error they attribute to a flaky connection, and re-runs — or worse,
// concludes the table had nothing in it — is looking at numbers that were never
// measured.
//
// So when the error is anything OTHER than the row-level aggregate, the command
// must say so at ERROR, next to the numbers it is disowning.
func TestRunRotateKEKWarnsWhenTheInventoryIsPartial(t *testing.T) {
	_, databaseURL := rotateIntegrationDB(t)

	// A connection to the real database whose search_path resolves nothing, so
	// the first table the walk touches raises 42P01 and the walk aborts with an
	// infrastructure error rather than a row verdict. This is a stand-in for
	// the connection that drops or the permission that is revoked mid-walk:
	// what matters is the SHAPE — Reseal returns an error that is not the
	// row-level aggregate, having counted only a prefix.
	separator := "?"
	if strings.Contains(databaseURL, "?") {
		separator = "&"
	}
	t.Setenv("DATABASE_URL", databaseURL+separator+"options=-c%20search_path%3Dpg_temp")
	t.Setenv("BRIDGE_KEK", rotateTestKEK)
	t.Setenv("BRIDGE_KEK_PREVIOUS", rotateTestPreviousKEK)

	logger, records := capturingLogger()
	err := runRotateKEK(logger)
	require.Error(t, err, "a walk that could not read its tables must not exit zero")

	// The counts were still printed — that is the behavior being guarded, not
	// a bug to fix. An operator needs the prefix.
	require.Len(t, tableLines(*records), 3,
		"the inventory is still logged on the abort path; the fix is to label it, not to withhold it")

	var warned bool
	for _, record := range *records {
		if record.Level != slog.LevelError || record.Message == "kek re-seal failure" {
			continue
		}
		if strings.Contains(strings.ToLower(record.Message), "partial") {
			warned = true
		}
	}
	assert.True(t, warned,
		"an aborted walk must log an explicit ERROR saying the inventory above is PARTIAL. Without it the operator's only clue is an error string, while three lines of authoritative-looking zeros sit directly above it — and zeros are exactly what a clean run over an empty table prints")
}

// TestRunRotateKEKDoesNotDisownACompleteInventory is the other half of the
// partial-inventory warning, and the half that keeps it meaningful.
//
// A walk that ran to completion over a database with some unmovable rows also
// returns an error — and its counts are a TRUE inventory: every table paged to
// the end, every bad row named. Labelling that report partial would teach the
// operator to distrust the numbers on exactly the run where the numbers are how
// they find the damaged rows, and would make the warning noise they learn to
// scroll past on the run where it is real.
func TestRunRotateKEKDoesNotDisownACompleteInventory(t *testing.T) {
	database, databaseURL := rotateIntegrationDB(t)
	testutil.Truncate(t, database, "bridged_actors", "ap_actors", "service_keys")
	ctx := t.Context()

	previous := rotateKEKBytes(t, rotateTestPreviousKEK)
	underPrevious, err := identity.NewCustodian(previous)
	require.NoError(t, err)

	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	sealed, err := underPrevious.EncryptActorKey(rotateIntegrationDID, key)
	require.NoError(t, err)
	// Full length, unknown version byte: a row the walk reaches, reads, and
	// cannot open. The walk still finishes every table.
	sealed[0] = 99
	_, err = database.ExecContext(ctx, `
		INSERT INTO bridged_actors (ap_actor_id, actor_type, did, handle, signing_key, consent_state)
		VALUES ($1, 'person', $2, $3, $4, 'ok')`,
		"https://lemmy.world/u/rotate-integration",
		rotateIntegrationDID,
		"rotate-integration.lemmy-world.tidepool.example",
		sealed)
	require.NoError(t, err)
	_, err = identity.LoadOrCreateRotationKey(ctx, store.NewServiceKeys(database), underPrevious)
	require.NoError(t, err)

	t.Setenv("DATABASE_URL", databaseURL)
	t.Setenv("BRIDGE_KEK", rotateTestKEK)
	t.Setenv("BRIDGE_KEK_PREVIOUS", rotateTestPreviousKEK)

	logger, records := capturingLogger()
	require.Error(t, runRotateKEK(logger),
		"a row that could not be moved must still exit nonzero")

	var sawFailureLine, disowned bool
	for _, record := range *records {
		if record.Message == "kek re-seal failure" {
			sawFailureLine = true
			continue
		}
		if record.Level == slog.LevelError && strings.Contains(strings.ToLower(record.Message), "partial") {
			disowned = true
		}
	}
	assert.True(t, sawFailureLine,
		"the unmovable row must get its own ERROR line naming it")
	assert.False(t, disowned,
		"a walk that finished every table must NOT have its inventory called partial: those counts are exactly how the operator sizes the damage, and a warning that fires on the complete case as well as the aborted one distinguishes nothing")
}
