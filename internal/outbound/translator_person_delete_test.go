package outbound

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
)

// SECOND-OPINION (chunk 8, top test gap sev 8): `removeData: true` was asserted
// nowhere. This pins the WHOLE Delete{Person} payload the destructive tier
// fans out — the one activity whose meaning lives in its flags rather than its
// content — so that each one-line deletion in personDelete has a named test
// that kills it. Verified against Lemmy 0.19 (translator.go's own comment):
// without removeData, Delete{Person} marks the person deleted and KEEPS their
// content, so the purge would leave every post standing while reporting
// success.
func TestPersonDeletePayloadIsAPurge(t *testing.T) {
	tr := NewTranslator("https://coves.social")
	actorID := "https://coves.social/ap/actor/did:plc:purged"

	out, err := tr.Translate(actorID, consume.PersonDeleteIntent{
		ActorDID: "did:plc:purged",
		ID:       "https://coves.social/ap/activity/purge-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "Delete", out.Kind, "the queue indexes it as a Delete")
	assert.Equal(t, "https://coves.social/ap/activity/purge-1", out.ActivityID)

	var activity map[string]any
	require.NoError(t, json.Unmarshal(out.Payload, &activity))

	assert.Equal(t, "Delete", activity["type"],
		"the destructive tier's wire verb is Delete — anything else is a different protocol "+
			"conversation entirely")

	// --- Self-referential: actor == object == the persona's own AP id.
	assert.Equal(t, actorID, activity["actor"])
	assert.Equal(t, actorID, activity["object"],
		"actor and object must be the SAME id: this is the user withdrawing THEMSELVES, the "+
			"one activity this bridge sends that is about its own sender — a Delete whose "+
			"object is anyone else is a moderation action this actor has no authority for")

	// --- The flag that makes the purge a purge.
	removeData, present := activity["removeData"]
	require.True(t, present,
		"`removeData` must be ON THE WIRE: this is the one-line deletion this test exists to "+
			"kill (translator.go personDelete) — without it Lemmy marks the person deleted and "+
			"KEEPS their content, so the destructive tier would leave every post standing "+
			"while reporting the erasure succeeded")
	assert.Equal(t, true, removeData,
		"and it must be boolean true — a string \"true\" is not the flag Lemmy reads")

	// --- Addressing: to Public, and NOTHING community-scoped.
	assert.Equal(t, []string{ap.PublicAudience}, asStringSet(t, activity["to"], "to"),
		"`to` is exactly [as:Public]: one canonical payload is fanned out over every inbox "+
			"the user ever reached, so the addressing cannot name any one of them")
	_, hasCC := activity["cc"]
	assert.False(t, hasCC,
		"no `cc`: unlike every other activity here, a person withdrawal is for every instance "+
			"that holds the identity, not for one community — a cc would scope-stamp the "+
			"canonical payload with whichever community happened to be rendered first")
	_, hasAudience := activity["audience"]
	assert.False(t, hasAudience, "and no `audience`, for the same reason")

	// --- And NO summary.
	_, hasSummary := activity["summary"]
	assert.False(t, hasSummary,
		"a Delete carrying a `summary` is read by Lemmy as a MODERATOR removal with a reason "+
			"— a user's own withdrawal must not arrive dressed as a moderation action taken "+
			"against them")
}
