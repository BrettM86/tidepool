package lexicons

import (
	"testing"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/lexicon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// federationType is the opt-OUT record of decision 11: absence means
// federation is ON for native users, so the record only ever exists to turn
// it down. Coves' settings UI writes it; the bridge reads it.
const federationType = "social.coves.bridge.federation"

// validate runs a record through the same path the materializer uses
// (atdata.UnmarshalJSON then lexicon.ValidateRecord), so a shape that
// validates here validates in production.
func validate(t *testing.T, raw string) error {
	t.Helper()
	catalog, err := Catalog()
	require.NoError(t, err)
	record, err := atdata.UnmarshalJSON([]byte(raw))
	require.NoError(t, err, "fixture must be valid JSON in the atproto data model")
	return lexicon.ValidateRecord(catalog, record, federationType, lexicon.ValidateFlags(0))
}

// TestFederationLexiconLoads: the vendored file must be in the embedded
// catalog at all. A lexicon that ships but is not embedded fails silently —
// nothing validates against it.
func TestFederationLexiconLoads(t *testing.T) {
	catalog, err := Catalog()
	require.NoError(t, err)

	schema, err := catalog.Resolve(federationType)
	require.NoError(t, err, "the embedded catalog must resolve %s", federationType)
	require.NotNil(t, schema)

	def, ok := schema.Def.(lexicon.SchemaRecord)
	require.True(t, ok, "main must be a record def, got %T", schema.Def)
	// One federation preference per repo. The wire form of a fixed key is
	// "literal:self" — indigo accepts only tid/nsid/any/literal:*, and the
	// vendored lexicons spell it that way (community/profile.json, rules.json).
	assert.Equal(t, "literal:self", def.Key)
}

func TestFederationRecordShapes(t *testing.T) {
	t.Run("soft disable", func(t *testing.T) {
		assert.NoError(t, validate(t,
			`{"$type":"`+federationType+`","enabled":false}`))
	})

	t.Run("destructive tier", func(t *testing.T) {
		assert.NoError(t, validate(t,
			`{"$type":"`+federationType+`","enabled":false,"deleteRemote":true}`))
	})

	t.Run("re-enabled", func(t *testing.T) {
		assert.NoError(t, validate(t,
			`{"$type":"`+federationType+`","enabled":true}`))
	})

	t.Run("enabled is required", func(t *testing.T) {
		err := validate(t, `{"$type":"`+federationType+`","deleteRemote":true}`)
		require.Error(t, err,
			"the opt-out must state its intent: a record with no enabled field is ambiguous")
		assert.Contains(t, err.Error(), "enabled")
	})

	t.Run("enabled must be a boolean", func(t *testing.T) {
		assert.Error(t, validate(t, `{"$type":"`+federationType+`","enabled":"false"}`),
			`the string "false" is truthy in most languages; the lexicon must reject it`)
	})

	t.Run("deleteRemote must be a boolean", func(t *testing.T) {
		assert.Error(t, validate(t,
			`{"$type":"`+federationType+`","enabled":false,"deleteRemote":"yes"}`))
	})

	t.Run("unknown fields are tolerated", func(t *testing.T) {
		// Pinning ACTUAL behavior, verified against indigo: record objects
		// are open unless a lexicon closes them, and none of the Coves
		// lexicons do. A future field added upstream must not make today's
		// bridge reject the record.
		assert.NoError(t, validate(t,
			`{"$type":"`+federationType+`","enabled":false,"futureField":"whatever"}`))
	})
}
