package consume

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Second-opinion C6: the rev gate and CursorSchemaVersion are coupled, and the
// coupling must not be a silent trap.
//
// consumer_cursors is keyed (consumer_name, schema_version) precisely so a
// future incompatible handler can bump CursorSchemaVersion and replay the
// entire retained Jetstream store from cursor 0 without stomping production.
// But jetstream_record_revs — the gate — is keyed by record_uri ALONE, with no
// schema notion. So that from-scratch replay hits gate rows the OLD schema
// version left, every record reads as "already applied at an equal-or-higher
// rev", and the new handlers process NOTHING. The replay the versioned cursor
// promised is silently a no-op.
//
// RULING (flagged to the coordinator): keep the gate global — schema-scoping
// it threads a version through every gate call and a migration through the
// table, for a v2 that does not exist. Instead the coupling is made EXPLICIT
// and greppable: bumping CursorSchemaVersion REQUIRES resetting the gate (and
// the DLQ) as an operational step, documented by GateResetRequiredOnSchemaBump.
// The alternative (schema-scoped gate) is the better long-term fix and is
// noted for whenever a real v2 lands.

func TestSchemaVersioning_GateResetIsADocumentedRequirement(t *testing.T) {
	assert.True(t, GateResetRequiredOnSchemaBump,
		"this constant is the machine-readable record of the C6 ruling: because the gate "+
			"is NOT schema-scoped, a CursorSchemaVersion bump that expects a clean replay "+
			"must reset jetstream_record_revs and jetstream_dead_letters, or the replay "+
			"is silently a no-op. Flipping this to false is a claim that the gate became "+
			"schema-aware — which must come with the migration and the threaded version")
}

// TestSchemaVersioning_GateIsNotSchemaScoped is the concrete evidence behind
// the ruling: it demonstrates that a gate row blocks re-application regardless
// of which schema version is 'replaying', so the documented reset really is
// necessary.
func TestSchemaVersioning_GateIsNotSchemaScoped(t *testing.T) {
	database := revGateTestDB(t)
	gate := NewRevGate(database)
	ctx := context.Background()

	// "Schema version 1" applies a record and advances the gate.
	applied := 0
	require.NoError(t, applyGated(ctx, gate, gateConsumer, gateDID,
		gateCommit("create", gateRevMid), func() error { applied++; return nil }))
	require.Equal(t, 1, applied)

	// "Schema version 2" replays the SAME record from cursor 0. The store it
	// would use for cursors is versioned; the gate is not, so the record is
	// seen as already applied and the new handler never runs.
	require.NoError(t, applyGated(ctx, gate, gateConsumer, gateDID,
		gateCommit("create", gateRevMid), func() error { applied++; return nil }))
	assert.Equal(t, 1, applied,
		"the gate blocks the replay across 'schema versions' — proof that a bump "+
			"expecting a fresh replay must reset the gate first, exactly what "+
			"GateResetRequiredOnSchemaBump documents")
}
