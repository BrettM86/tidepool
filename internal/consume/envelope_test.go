package consume

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Second-opinion C1 (SECURITY CRITICAL): envelope validation before storage.
//
// A Jetstream frame is attacker-influenced data — a native user writes the
// record it carries. A commit whose rkey or did contains a NUL byte, or is
// otherwise not a valid identifier, must be rejected as PERMANENT at dispatch,
// BEFORE it reaches a store: postgres TEXT columns reject NUL, so a raw NUL
// that survives to an INSERT turns into a transient write error retried
// forever — and the dead-letter write ALSO fails if the error string echoes
// the NUL — so the whole consumer wedges on one poison frame. Rejecting it up
// front, with a sanitized message, dead-letters it cleanly and the cursor
// moves on.

// nul is a single NUL byte, built at runtime because Go source may not contain
// one. When json.Marshal encodes a string carrying it, the NUL becomes the
// VALID JSON escape "", which json.Unmarshal then decodes back to a real
// NUL byte in the field — so the frame parses and the production identifier
// validation is the thing under test, not the JSON decoder.
var nul = string([]byte{0})

// jsonString encodes s as a JSON string literal (quotes included), escaping any
// byte the wire could carry — a NUL, a control char — the way a real PDS's
// serializer would. Splicing %q instead would emit Go's "\x00", which is not
// valid JSON and fails to parse before dispatch ever runs.
func jsonString(t *testing.T, s string) string {
	t.Helper()
	encoded, err := json.Marshal(s)
	require.NoError(t, err)
	return string(encoded)
}

// malformedCommit builds a commit frame with its identifier fields JSON-encoded
// individually, so a genuine NUL (or any hostile byte) survives serialization
// as  and reaches the handler as a real byte in commit.RKey / event.DID.
func malformedCommit(t *testing.T, did, rev, operation, collection, rkey, cid string) []byte {
	t.Helper()
	record := ""
	if operation != "delete" {
		record = `,"record":{"$type":"social.coves.community.comment",` +
			`"content":"x","createdAt":"2026-08-13T10:00:00.000Z",` +
			`"reply":{"root":{"uri":"` + acceptRootATURI + `","cid":"` + acceptRootCID + `"},` +
			`"parent":{"uri":"` + acceptRootATURI + `","cid":"` + acceptRootCID + `"}}}`
	}
	cidField := ""
	if cid != "" {
		cidField = `,"cid":` + jsonString(t, cid)
	}
	return []byte(`{"did":` + jsonString(t, did) +
		`,"time_us":9800,"kind":"commit","commit":{"rev":` + jsonString(t, rev) +
		`,"operation":` + jsonString(t, operation) +
		`,"collection":` + jsonString(t, collection) +
		`,"rkey":` + jsonString(t, rkey) + cidField + record + `}}`)
}

func TestEnvelope_MalformedCommitsAreRejectedPermanentlyBeforeStorage(t *testing.T) {
	const goodRev = "3lzrev0000001"
	const goodCID = "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4"

	tests := []struct {
		name  string
		frame []byte
		why   string
	}{
		{
			name:  "NUL byte in rkey",
			frame: malformedCommit(t, dispatchNativeDID, goodRev, "create", CollectionComment, "3lz"+nul+"evil", goodCID),
			why:   "a NUL in the rkey reaches the at_uri TEXT column and turns an INSERT into a retry loop",
		},
		{
			name:  "NUL byte in did",
			frame: malformedCommit(t, "did:plc:7iza6"+nul+"c6c6", goodRev, "create", CollectionComment, "3lzcmnt000001", goodCID),
			why:   "the repo DID is used to build the at-uri and to key the hosted filter; a NUL poisons both",
		},
		{
			name:  "rkey is not a valid record key",
			frame: malformedCommit(t, dispatchNativeDID, goodRev, "create", CollectionComment, "has spaces/and slashes", goodCID),
			why:   "a record key with path separators forges an at-uri pointing at a different collection",
		},
		{
			name:  "did is not a syntactically valid DID",
			frame: malformedCommit(t, "not-a-did", goodRev, "create", CollectionComment, "3lzcmnt000002", goodCID),
			why:   "a non-DID repo id can never resolve; retrying it is pointless",
		},
		{
			name:  "empty rev",
			frame: malformedCommit(t, dispatchNativeDID, "", "create", CollectionComment, "3lzcmnt000003", goodCID),
			why:   "a real wire frame always carries rev; an empty one would bypass the gate and replay forever",
		},
		{
			name:  "unknown operation",
			frame: malformedCommit(t, dispatchNativeDID, goodRev, "frobnicate", CollectionComment, "3lzcmnt000004", goodCID),
			why:   "an operation outside create/update/delete is malformed, not a forward-compatible value",
		},
		{
			name:  "create with no CID",
			frame: malformedCommit(t, dispatchNativeDID, goodRev, "create", CollectionComment, "3lzcmnt000005", ""),
			why:   "a create names the CID of what it created; missing means the frame is truncated or forged",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			database := dispatchTestDB(t)
			seedBridgedCommunity(t, database)
			seedThreadRoot(t, database)
			fixture := newDispatchFixture(t, database)

			err := fixture.handle(t, tc.frame)

			require.Error(t, err, tc.why)
			assert.ErrorIs(t, err, ErrPermanentEvent,
				"a malformed envelope can never succeed, so it is dead-lettered exhausted "+
					"rather than retried into a poison loop: %s", tc.why)

			msg := err.Error()
			assert.False(t, strings.ContainsRune(msg, 0),
				"the error message must NOT echo a raw NUL: the connector stores "+
					"err.Error() in the last_error TEXT column, and a NUL there fails the "+
					"dead-letter write, which wedges the whole consumer on this one frame")
			assert.True(t, utf8.ValidString(msg),
				"the message must be printable UTF-8 for the same reason — an operator has "+
					"to be able to read it out of the DLQ")

			assert.Zero(t, countRows(t, database, "jetstream_record_revs"),
				"validation runs BEFORE the gate: no gate row may be claimed for an event "+
					"no handler ran, or it would reject the legitimate record that later "+
					"reuses the URI")
			assert.Zero(t, countRows(t, database, "outbound_objects"),
				"and nothing is written: rejection is the point")
		})
	}
}

func TestEnvelope_ParseableTimeUSMustBePositive(t *testing.T) {
	database := dispatchTestDB(t)
	fixture := newDispatchFixture(t, database)

	frame := []byte(fmt.Sprintf(
		`{"did":%q,"time_us":0,"kind":"commit","commit":{"rev":"3lzrev0000001",`+
			`"operation":"create","collection":%q,"rkey":"3lzcmnt000006",`+
			`"cid":"bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",`+
			`"record":{"$type":"social.coves.community.comment","content":"x",`+
			`"createdAt":"2026-08-13T10:00:00.000Z"}}}`,
		dispatchNativeDID, CollectionComment))

	err := fixture.handle(t, frame)
	require.Error(t, err,
		"time_us is the cursor position; a parseable frame carrying 0 (or negative) "+
			"would let the cursor sit at or before every retained event and replay the "+
			"entire store on the next reconnect")
	assert.ErrorIs(t, err, ErrPermanentEvent)
}
