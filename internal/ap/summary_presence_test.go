package ap

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Lemmy's Delete convention (PLAN.md decision 18): a Delete activity carrying
// a `summary` is a MODERATOR removal, and one without it is the author
// deleting their own post. Lemmy sends the key with an EMPTY string when the
// moderator gave no reason — so "" and absent are two different activities
// with two different outcomes, and `Summary string` collapses them into one.
// Read as self-delete, a mod removal deletes the author's record instead of
// writing a removal; read as mod removal, an author's own delete fabricates a
// moderation action against them.
//
// WHY A NIL-SAFE PREDICATE RATHER THAN A POINTER FIELD. This package already
// has a present-but-check idiom: *Time carries a Valid flag and answers
// through OK(), which is nil-safe, so callers ask a question instead of
// destructuring a wrapper. Summary is asked the same kind of question, but it
// has one constraint Time does not — two existing readers consume it AS A
// STRING (materialize/actors.go's #nobridge scan and its profile-description
// conversion). Changing the field's type would rewrite both call sites for a
// question neither of them asks. A predicate beside the unchanged string field
// keeps those readers untouched and gives the delete path the distinction it
// needs; Object already has a custom UnmarshalJSON, so recording presence
// needs no new machinery.

// deleteWithSummary is the mod-removal shape: the key is present.
const deleteWithEmptySummary = `{
	"type": "Delete",
	"id": "https://lemmy.world/activities/delete/1",
	"actor": "https://lemmy.world/u/mod",
	"object": "https://lemmy.world/post/1",
	"summary": ""
}`

// deleteWithoutSummary is the self-delete shape: no key at all.
const deleteWithoutSummary = `{
	"type": "Delete",
	"id": "https://lemmy.world/activities/delete/2",
	"actor": "https://lemmy.world/u/author",
	"object": "https://lemmy.world/post/2"
}`

const deleteWithReason = `{
	"type": "Delete",
	"id": "https://lemmy.world/activities/delete/3",
	"actor": "https://lemmy.world/u/mod",
	"object": "https://lemmy.world/post/3",
	"summary": "spam"
}`

// TestSummaryPresentButEmpty (P1): the case the current shape cannot see. A
// moderator removing a post without typing a reason is the ordinary path, not
// an edge case, and it arrives as `"summary": ""`.
func TestSummaryPresentButEmpty(t *testing.T) {
	obj, err := ParseObject([]byte(deleteWithEmptySummary))
	require.NoError(t, err)

	assert.True(t, obj.HasSummary(),
		"a present-but-empty summary is a moderator removal with no stated reason; "+
			"reading it as absent turns every reasonless mod removal into a self-delete")
	assert.Equal(t, "", obj.Summary, "the text is genuinely empty")
}

// TestSummaryAbsent (P2) is the negative control: no key means the author
// deleted their own content, and nothing may fabricate a moderation action.
func TestSummaryAbsent(t *testing.T) {
	obj, err := ParseObject([]byte(deleteWithoutSummary))
	require.NoError(t, err)

	assert.False(t, obj.HasSummary(),
		"an absent summary is a self-delete; treating it as present would fabricate a "+
			"moderation record against an author who moderated nobody")
	assert.Equal(t, "", obj.Summary)
}

// TestSummaryPresentWithText (P3): presence and the string accessor answer
// together. The two existing readers (the #nobridge scan and the profile
// description) consume Summary as a plain string and must keep doing so.
func TestSummaryPresentWithText(t *testing.T) {
	obj, err := ParseObject([]byte(deleteWithReason))
	require.NoError(t, err)

	assert.True(t, obj.HasSummary())
	assert.Equal(t, "spam", obj.Summary,
		"the reason text must stay readable as a string: materialize/actors.go scans it for "+
			"#nobridge and renders it into profile descriptions")

	// The same accessor on an actor document, which is where those readers
	// actually meet it.
	actor, err := ParseObject([]byte(`{"type":"Person","id":"https://lemmy.world/u/x","summary":"<p>I opt out. #nobridge</p>"}`))
	require.NoError(t, err)
	assert.True(t, actor.HasSummary())
	assert.Contains(t, actor.Summary, "#nobridge")
}

// TestSummaryPresenceDoesNotLeakIntoMarshal (P4): whatever records presence
// must not become a wire field. An Object with no summary must marshal without
// a summary key — the bridge's own service actor document goes out through
// this same marshal, and a spurious `"summary": ""` on it would be a claim the
// bridge does not mean to make (and, on a Delete the bridge ever emits, would
// assert a moderator removal).
func TestSummaryPresenceDoesNotLeakIntoMarshal(t *testing.T) {
	raw, err := json.Marshal(&Object{Type: TypeDelete, ID: "https://bridge.example/activities/delete/1"})
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))
	assert.NotContains(t, decoded, "summary",
		"an object with no summary must not marshal one; omitempty on the string field is what "+
			"guarantees it, so presence must be recorded somewhere that does not serialize")

	// A summary that IS set still goes out.
	raw, err = json.Marshal(&Object{Type: TypeDelete, Summary: "spam"})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &decoded))
	assert.Equal(t, "spam", decoded["summary"])

	// The real document path: the service actor's own JSON.
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)
	actor := &ServiceActor{ID: "https://bridge.example/actor", Hostname: "bridge.example", Key: key}
	doc, err := actor.DocumentJSON()
	require.NoError(t, err)

	var actorDoc map[string]any
	require.NoError(t, json.Unmarshal(doc, &actorDoc))
	assert.Contains(t, actorDoc["summary"], "Bridges threadiverse communities",
		"the service actor's own summary must survive the shape change unchanged")
}

// TestSummaryPresentButNull (F6): `"summary": null` is a PRESENT key, and the
// bridge must classify it as one.
//
// The two readings are not symmetric, which is what makes this worth pinning.
// Read as present, an explicit null is treated as a moderator removal with no
// reason — the post stays in the author's repo and the community records a
// removal. Read as absent, it selects the SELF-DELETE path, which destroys the
// author's record. A JSON null is not a statement that no summary key was
// sent; it is a sender saying the field is there and empty, and the shadow
// struct's `*string` nil makes those two indistinguishable unless presence is
// decided on the key rather than on the value.
func TestSummaryPresentButNull(t *testing.T) {
	obj, err := ParseObject([]byte(`{
		"type": "Delete",
		"id": "https://lemmy.world/activities/delete/4",
		"actor": "https://lemmy.world/u/mod",
		"object": "https://lemmy.world/post/4",
		"summary": null
	}`))
	require.NoError(t, err)

	assert.True(t, obj.HasSummary(),
		"an explicit null summary is a PRESENT key; classifying it absent picks the "+
			"self-delete path and destroys the author's record over a sender's null")
	assert.Equal(t, "", obj.Summary, "a null summary carries no text")
}
