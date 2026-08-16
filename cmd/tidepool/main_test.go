package main

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
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
