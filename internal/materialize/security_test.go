package materialize

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/rivo/uniseg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

// mustObject parses a synthetic AP document (Go map) into an ap.Object — the
// shape the materializer receives from the ingestion layer.
func mustObject(t *testing.T, doc map[string]any) *ap.Object {
	t.Helper()
	body, err := json.Marshal(doc)
	require.NoError(t, err)
	obj, err := ap.ParseObject(body)
	require.NoError(t, err)
	return obj
}

// group builds a minimal synthetic Group (Lemmy community shape).
func group(id, username string, extra map[string]any) map[string]any {
	doc := map[string]any{
		"type":              "Group",
		"id":                id,
		"preferredUsername": username,
		"name":              username,
		"inbox":             id + "/inbox",
		"published":         "2024-01-01T00:00:00.000000Z",
	}
	for k, v := range extra {
		doc[k] = v
	}
	return doc
}

// page builds a minimal synthetic Page (Lemmy post shape).
func page(id, author, groupIRI, title, published string) map[string]any {
	return map[string]any{
		"type":         "Page",
		"id":           id,
		"attributedTo": author,
		"audience":     groupIRI,
		"to":           []any{ap.PublicAudience},
		"name":         title,
		"source":       map[string]any{"content": "body text", "mediaType": "text/markdown"},
		"published":    published,
	}
}

// TestCommentThreadRootedAtNote_SkipsWithoutPanic covers the crash regression:
// a comment whose ancestor chain tops out at a parentless Note (a Mastodon
// status that federated in) must be dropped as a skip, not dereference a nil
// inReplyTo.
func TestCommentThreadRootedAtNote_SkipsWithoutPanic(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// A root Note with no inReplyTo, served upstream.
	rootNote := map[string]any{
		"type":         "Note",
		"id":           "https://lemmy.zip/comment/root",
		"attributedTo": personID,
		"audience":     groupID,
		"source":       map[string]any{"content": "root", "mediaType": "text/markdown"},
		"published":    "2024-01-02T00:00:00.000000Z",
	}
	h.serveObject("/comment/root", rootNote)

	child := note("https://lemmy.zip/comment/child", personID,
		"https://lemmy.zip/comment/root", "child", "2024-01-02T01:00:00.000000Z")

	res, err := h.m.MaterializeComment(ctx, mustObject(t, child))
	require.Nil(t, res)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "parentless-Note root must be a skip, got %v", err)
}

// TestAncestorCrossAuthorityID_Skips covers the forgery fix: an ancestor
// fetched at one instance's IRI that returns a body claiming another
// instance's id is rejected (the id would otherwise become the ap_objects
// mapping key).
func TestAncestorCrossAuthorityID_Skips(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// The parent path is under lemmy.world but the served body claims an
	// evil.example id.
	forged := map[string]any{
		"type":         "Note",
		"id":           "https://evil.example/x",
		"attributedTo": personID,
		"audience":     groupID,
		"source":       map[string]any{"content": "forged", "mediaType": "text/markdown"},
		"published":    "2024-01-02T00:00:00.000000Z",
		"inReplyTo":    groupID,
	}
	h.serveObject("/comment/parent", forged)

	child := note("https://lemmy.world/comment/child", personID,
		"https://lemmy.world/comment/parent", "child", "2024-01-02T01:00:00.000000Z")

	res, err := h.m.MaterializeComment(ctx, mustObject(t, child))
	require.Nil(t, res)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "cross-authority ancestor id must be a skip, got %v", err)
	assert.Contains(t, err.Error(), "cross-authority")
}

// TestCreateAfterDelete_DoesNotResurrect covers unordered delivery: once an
// object's mapping is tombstoned, a re-delivered Create/Update must not
// resurrect it.
func TestCreateAfterDelete_DoesNotResurrect(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	pageObj := loadFixtureObject(t, "page_lemmy_world.json")
	_, err := h.m.MaterializePost(ctx, pageObj)
	require.NoError(t, err)

	require.NoError(t, h.m.HandleDelete(ctx, pageObj.ID))
	before := len(h.firehoseEvents())

	res, err := h.m.MaterializePost(ctx, pageObj)
	require.Nil(t, res)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "re-create after delete must be a skip, got %v", err)

	mapping, err := h.objects.GetByAPID(ctx, pageObj.ID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "mapping must stay tombstoned")
	assert.Equal(t, before, len(h.firehoseEvents()), "no new commit must be emitted")
}

// TestNobridgeOnRefresh_ScrubsExistingContent covers the consent fix: when a
// previously-bridged actor adds #nobridge, a refresh scrubs their existing
// records and suspends bridging (reversibly).
func TestNobridgeOnRefresh_ScrubsExistingContent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const personIRI = "https://lemmy.world/u/scrubme"
	const groupIRI = "https://lemmy.world/c/general"
	const pageIRI = "https://lemmy.world/post/1001"

	// Serve a person whose summary we can flip to #nobridge on refresh.
	summary := ""
	h.mux.HandleFunc("GET /u/scrubme", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
		body, _ := json.Marshal(person(personIRI, "scrubme", map[string]any{"summary": summary}))
		_, _ = w.Write(body)
	})
	h.serveObject("/c/general", group(groupIRI, "general", nil))

	// First pass: bridge + materialize the post (author = scrubme).
	_, err := h.m.MaterializePost(ctx, mustObject(t, page(pageIRI, personIRI, groupIRI, "hello", "2024-02-01T00:00:00.000000Z")))
	require.NoError(t, err)

	authorDID := testDIDFor("scrubme", "lemmy.world")
	mappingsBefore, err := h.objects.ListByActorDID(ctx, authorDID)
	require.NoError(t, err)
	require.NotEmpty(t, mappingsBefore, "author should have live records before opt-out")

	// Author opts out; force a refresh.
	summary = "hobbyist. #nobridge please"
	_, err = h.m.RefreshActor(ctx, &ap.Object{ID: personIRI})
	require.Error(t, err)
	assert.True(t, IsSkip(err), "opted-out actor returns a skip, got %v", err)

	actor, err := h.actors.GetByAPActorID(ctx, personIRI)
	require.NoError(t, err)
	assert.Equal(t, store.ConsentStateNoBridge, actor.ConsentState)

	mappingsAfter, err := h.objects.ListByActorDID(ctx, authorDID)
	require.NoError(t, err)
	assert.Empty(t, mappingsAfter, "opt-out must scrub the author's live records")
}

// TestKnownPersonAsCommunity_Skips covers the type-confusion fix: a post whose
// audience names an actor already bridged as a Person must not create a
// community from that Person.
func TestKnownPersonAsCommunity_Skips(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const personIRI = "https://lemmy.world/u/notacommunity"
	h.serveObject("/u/notacommunity", person(personIRI, "notacommunity", nil))

	// Bridge the person first (as a Person).
	_, err := h.m.EnsureActor(ctx, &ap.Object{ID: personIRI})
	require.NoError(t, err)

	// A post claiming that person's IRI as its community.
	pg := page("https://lemmy.world/post/2002", personIRI, personIRI, "x", "2024-02-02T00:00:00.000000Z")
	res, err := h.m.MaterializePost(ctx, mustObject(t, pg))
	require.Nil(t, res)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "person-as-community must be a skip, got %v", err)
}

// --- pure-unit tests (no DB) ---

func TestContainsHashtag(t *testing.T) {
	cases := []struct {
		hay, marker string
		want        bool
	}{
		{"i said #nobot", "#nobot", true},
		{"#nobot at start", "#nobot", true},
		{"ends with #nobot", "#nobot", true},
		{"tag #nobot. done", "#nobot", true},
		{"this is #nobotany not opt-out", "#nobot", false},
		{"#nobots plural", "#nobot", false},
		{"opt out #nobridge here", "#nobridge", true},
		{"#nobridges is different", "#nobridge", false},
		{"nothing here", "#nobot", false},
	}
	for _, c := range cases {
		assert.Equalf(t, c.want, containsHashtag(c.hay, c.marker), "containsHashtag(%q,%q)", c.hay, c.marker)
	}
}

func TestTruncateText_ByteCap(t *testing.T) {
	// Family emoji: 1 grapheme, 25 bytes. 100 of them = 100 graphemes but
	// 2500 bytes — inside a 200-grapheme cap yet over a 300-byte cap.
	emoji := "\U0001F468‍\U0001F469‍\U0001F467‍\U0001F466"
	s := strings.Repeat(emoji, 100)
	got := truncateText(s, 200, 300)
	assert.LessOrEqual(t, len(got), 300, "must satisfy the byte cap")
	assert.LessOrEqual(t, uniseg.GraphemeClusterCount(got), 200, "must satisfy the grapheme cap")
	// No split cluster: re-truncating is idempotent.
	assert.Equal(t, got, truncateText(got, 200, 300))
}

func TestIsSafeLinkScheme(t *testing.T) {
	assert.True(t, isSafeLinkScheme("https://example.com/x"))
	assert.True(t, isSafeLinkScheme("http://example.com"))
	assert.True(t, isSafeLinkScheme("  HTTPS://Example.com "))
	assert.False(t, isSafeLinkScheme("javascript:alert(1)"))
	assert.False(t, isSafeLinkScheme("data:text/html;base64,PHNjcmlwdD4="))
	assert.False(t, isSafeLinkScheme("ftp://example.com"))
	assert.False(t, isSafeLinkScheme(""))
}

func TestStripActiveHTML(t *testing.T) {
	assert.Equal(t, "hello world",
		stripActiveHTML("hello <script>steal()</script>world"))
	assert.Equal(t, "keep <b>bold</b>",
		stripActiveHTML("keep <b>bold</b><style>.x{}</style>"))
	// False tag-name match is preserved.
	assert.Equal(t, "text about <scripting> languages",
		stripActiveHTML("text about <scripting> languages"))
	// Unterminated block is dropped to end.
	assert.Equal(t, "before",
		stripActiveHTML("before <script>oops no close"))
	// Case-insensitive.
	assert.Equal(t, "a b",
		stripActiveHTML("a <SCRIPT>x</SCRIPT>b"))
}
