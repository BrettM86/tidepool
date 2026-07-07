package ap

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fixture provenance: group_lemmy_world.json, person_lemmy_world.json,
// page_lemmy_world.json, note_lemmy_zip.json, outbox_lemmy_world.json
// (truncated to 2 items), announce_create_page_lemmy_world.json, and
// webfinger_group.json were captured live from lemmy.world / lemmy.zip on
// 2026-07-06 with `curl -H 'Accept: application/activity+json'`.
// announce_create_note.json and announce_like.json are constructed: the
// Announce/Create wrapper shapes are copied verbatim from the live outbox
// items, the inner Note is the live comment, and the inner Like follows
// Lemmy's crates/apub/src/protocol/activities/voting/vote.rs shape.
// delete_page.json follows Lemmy's deletion protocol shape.

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err, "fixture %s must exist", name)
	return data
}

func parseFixture(t *testing.T, name string) *Object {
	t.Helper()
	obj, err := ParseObject(loadFixture(t, name))
	require.NoError(t, err, "fixture %s must parse", name)
	return obj
}

func TestParseGroupFixture(t *testing.T) {
	group := parseFixture(t, "group_lemmy_world.json")

	assert.Equal(t, TypeGroup, group.Type)
	assert.True(t, group.IsActor())
	assert.Equal(t, "https://lemmy.world/c/technology", group.ID)
	assert.Equal(t, "technology", group.PreferredUsername)
	assert.Equal(t, "Technology", group.Name)
	assert.Equal(t, "lemmy.world", group.Host())
	assert.Equal(t, "https://lemmy.world/c/technology/inbox", group.Inbox)
	assert.Equal(t, "https://lemmy.world/c/technology/outbox", group.Outbox)
	assert.Equal(t, "https://lemmy.world/c/technology/followers", group.Followers)
	assert.Equal(t, "https://lemmy.world/c/technology/featured", group.Featured)
	assert.Equal(t, "https://lemmy.world/inbox", group.SharedInboxOrInbox())

	// Lemmy Group attributedTo is the moderators collection IRI.
	assert.Equal(t, "https://lemmy.world/c/technology/moderators", group.AttributedTo.FirstID())

	require.NotNil(t, group.PublicKey)
	assert.Equal(t, "https://lemmy.world/c/technology#main-key", group.PublicKey.ID)
	assert.Equal(t, group.ID, group.PublicKey.Owner)
	_, err := ParsePublicKeyPEM([]byte(group.PublicKey.PublicKeyPem))
	require.NoError(t, err, "publicKeyPem must be a parseable RSA key")

	// Lemmy quirk: Group.language is an ARRAY of language objects.
	require.Len(t, group.Language, 1)
	assert.Equal(t, "en", group.Language[0].Identifier)

	// Lemmy extensions.
	require.NotNil(t, group.PostingRestrictedToMods)
	assert.False(t, *group.PostingRestrictedToMods)
	require.NotNil(t, group.Sensitive)
	assert.False(t, *group.Sensitive)

	// Markdown source alongside rendered HTML.
	require.NotNil(t, group.Source)
	assert.Equal(t, "text/markdown", group.Source.MediaType)
	assert.NotEmpty(t, group.Source.Content)
	assert.NotEmpty(t, group.Summary)

	require.NotNil(t, group.Icon)
	assert.Equal(t, TypeImage, group.Icon.Type)
	assert.NotEmpty(t, group.Icon.URLString())

	require.NotNil(t, group.Published)
	assert.Equal(t, 2023, group.Published.Year())
	require.NotNil(t, group.Updated)
}

func TestParsePersonFixture(t *testing.T) {
	person := parseFixture(t, "person_lemmy_world.json")

	assert.Equal(t, TypePerson, person.Type)
	assert.True(t, person.IsActor())
	assert.Equal(t, "https://lemmy.world/u/LeftLeaningFreedomFighters", person.ID)
	assert.Equal(t, "LeftLeaningFreedomFighters", person.PreferredUsername)
	assert.Equal(t, "Surprised Neelix", person.Name)
	require.NotNil(t, person.PublicKey)
	assert.Equal(t, person.ID+"#main-key", person.PublicKey.ID)
	assert.Equal(t, "https://lemmy.world/inbox", person.SharedInboxOrInbox())
	require.NotNil(t, person.Icon)
	assert.NotEmpty(t, person.Icon.URLString())
}

func TestParsePageFixture(t *testing.T) {
	page := parseFixture(t, "page_lemmy_world.json")

	assert.Equal(t, TypePage, page.Type)
	assert.Equal(t, "https://lemmy.world/post/49131386", page.ID)
	assert.Equal(t, "https://lemmy.world/u/LeftLeaningFreedomFighters", page.AttributedTo.FirstID())
	assert.NotEmpty(t, page.Name)
	assert.True(t, page.IsPublic(), "post addressed to as:Public must be public")
	assert.True(t, page.To.Contains("https://lemmy.world/c/technology"))
	assert.True(t, page.Audience.Contains("https://lemmy.world/c/technology"),
		"audience (bare string on the wire) must normalize to a list")

	// Link post: the external URL rides in attachment.
	require.Len(t, page.Attach, 1)
	assert.Equal(t, TypeLink, page.Attach[0].Type)
	assert.Contains(t, page.Attach[0].Href, "tomshardware.com")

	// Image embed.
	require.NotNil(t, page.Image)
	assert.Equal(t, TypeImage, page.Image.Type)
	assert.Contains(t, page.Image.URLString(), "pictrs")

	// Single language object (vs the Group's array form).
	require.Len(t, page.Language, 1)
	assert.Equal(t, "en", page.Language[0].Identifier)

	require.NotNil(t, page.Sensitive)
	assert.False(t, *page.Sensitive)

	require.Len(t, page.Tag, 1)
	assert.Equal(t, TypeHashtag, page.Tag[0].Type)
	assert.Equal(t, "#technology", page.Tag[0].Name)

	require.NotNil(t, page.Published)
	assert.Equal(t,
		time.Date(2026, 7, 7, 3, 27, 37, 28201000, time.UTC),
		page.Published.Time)
}

func TestParseNoteFixture(t *testing.T) {
	note := parseFixture(t, "note_lemmy_zip.json")

	assert.Equal(t, TypeNote, note.Type)
	assert.Equal(t, "https://lemmy.zip/comment/27485395", note.ID)
	assert.Equal(t, "https://lemmy.zip/u/tixooo", note.AttributedTo.FirstID())

	// The parent pointer — the whole reason comments need strongRef
	// resolution.
	require.NotNil(t, note.InReplyTo)
	assert.Equal(t, "https://sh.itjust.works/comment/26248018", note.InReplyTo.ID)

	assert.Contains(t, note.Content, "human history")
	require.NotNil(t, note.Source)
	assert.Equal(t, "text/markdown", note.Source.MediaType)
	assert.Contains(t, note.Source.Content, "human history")

	require.Len(t, note.Tag, 1)
	assert.Equal(t, TypeMention, note.Tag[0].Type)

	// distinguished is a Lemmy extension on comments.
	require.NotNil(t, note.Distinguished)
	assert.False(t, *note.Distinguished)

	// Empty attachment array must parse to empty, not fail.
	assert.Empty(t, note.Attach)
}

func TestParseAnnounceCreatePageFixture(t *testing.T) {
	announce := parseFixture(t, "announce_create_page_lemmy_world.json")

	assert.Equal(t, TypeAnnounce, announce.Type)
	assert.Equal(t, "https://lemmy.world/c/technology", announce.Actor.ID)
	assert.True(t, announce.IsPublic())
	assert.True(t, announce.Cc.Contains("https://lemmy.world/c/technology/followers"))

	create := announce.Object
	require.NotNil(t, create)
	assert.Equal(t, TypeCreate, create.Type)
	assert.Equal(t, "https://lemmy.world/u/LeftLeaningFreedomFighters", create.Actor.ID)

	page := create.Object
	require.NotNil(t, page)
	assert.Equal(t, TypePage, page.Type)
	assert.Equal(t, "https://lemmy.world/post/49131386", page.ID)
	assert.NotEmpty(t, page.Name)
}

func TestParseAnnounceCreateNoteFixture(t *testing.T) {
	announce := parseFixture(t, "announce_create_note.json")

	require.NotNil(t, announce.Object)
	assert.Equal(t, TypeCreate, announce.Object.Type)
	note := announce.Object.Object
	require.NotNil(t, note)
	assert.Equal(t, TypeNote, note.Type)
	require.NotNil(t, note.InReplyTo)
	assert.Equal(t, "https://sh.itjust.works/comment/26248018", note.InReplyTo.ID)
}

func TestParseAnnounceLikeFixture(t *testing.T) {
	announce := parseFixture(t, "announce_like.json")

	assert.Equal(t, TypeAnnounce, announce.Type)
	like := announce.Object
	require.NotNil(t, like)
	assert.Equal(t, TypeLike, like.Type)
	assert.Equal(t, "https://lemmy.zip/u/tixooo", like.Actor.ID)
	// The liked object is a bare IRI string on the wire.
	require.NotNil(t, like.Object)
	assert.Equal(t, "https://lemmy.world/post/49131386", like.Object.ID)
	assert.Empty(t, like.Object.Type)
}

func TestParseDeleteFixture(t *testing.T) {
	deleteActivity := parseFixture(t, "delete_page.json")

	assert.Equal(t, TypeDelete, deleteActivity.Type)
	require.NotNil(t, deleteActivity.Object)
	assert.Equal(t, "https://lemmy.world/post/49131386", deleteActivity.Object.ID)
}

func TestParseOutboxFixture(t *testing.T) {
	outbox := parseFixture(t, "outbox_lemmy_world.json")

	assert.Equal(t, TypeOrderedCollection, outbox.Type)
	assert.True(t, outbox.IsCollection())
	assert.Equal(t, 50, outbox.TotalItems)
	// Truncated at capture time; every item is a full Announce activity.
	require.Len(t, outbox.OrderedItems, 2)
	for i := range outbox.OrderedItems {
		item := &outbox.OrderedItems[i]
		assert.Equal(t, TypeAnnounce, item.Type)
		require.NotNil(t, item.Object)
		assert.Equal(t, TypeCreate, item.Object.Type)
	}
}

func TestParseTombstone(t *testing.T) {
	obj, err := ParseObject([]byte(`{
		"type": "Tombstone",
		"id": "https://lemmy.world/post/1",
		"formerType": "Page",
		"deleted": "2026-01-02T03:04:05.000000Z"
	}`))
	require.NoError(t, err)
	assert.True(t, obj.IsTombstone())
	assert.Equal(t, TypePage, obj.FormerType)
	require.NotNil(t, obj.Deleted)
	assert.Equal(t, 2026, obj.Deleted.Year())
}

// TestTolerantFields exercises the string-or-object-or-array wire variants
// that differ across fediverse implementations (granary's as2 quirk list).
func TestTolerantFields(t *testing.T) {
	obj, err := ParseObject([]byte(`{
		"id": "https://example.com/note/1",
		"type": "Note",
		"to": "https://www.w3.org/ns/activitystreams#Public",
		"cc": [{"id": "https://example.com/u/bob"}, "https://example.com/u/carol"],
		"attributedTo": [{"type": "Person", "id": "https://example.com/u/alice"}],
		"icon": "https://example.com/icon.png",
		"url": {"type": "Link", "href": "https://example.com/note/1.html"},
		"tag": {"type": "Hashtag", "href": "https://example.com/tag/x", "name": "#x"},
		"language": {"identifier": "de", "name": "Deutsch"},
		"object": "https://example.com/note/0"
	}`))
	require.NoError(t, err)

	assert.True(t, obj.IsPublic(), "bare-string to must be understood")
	assert.Equal(t, Audience{"https://example.com/u/bob", "https://example.com/u/carol"}, obj.Cc)
	assert.Equal(t, "https://example.com/u/alice", obj.AttributedTo.FirstID())
	assert.Equal(t, TypePerson, obj.AttributedTo.First().Type)
	require.NotNil(t, obj.Icon)
	assert.Equal(t, "https://example.com/icon.png", obj.Icon.ID, "bare-string icon decodes as IRI-only ref")
	assert.Equal(t, "https://example.com/note/1.html", obj.URLString())
	require.Len(t, obj.Tag, 1)
	assert.Equal(t, "#x", obj.Tag[0].Name)
	require.Len(t, obj.Language, 1)
	assert.Equal(t, "de", obj.Language[0].Identifier)
	require.NotNil(t, obj.Object)
	assert.Equal(t, "https://example.com/note/0", obj.Object.ID)
}

func TestUnknownFieldsIgnored(t *testing.T) {
	obj, err := ParseObject([]byte(`{
		"id": "https://example.com/note/1",
		"type": "Note",
		"content": "hi",
		"somePieFedExtension": {"nested": ["values", 1, true]},
		"litepub:capabilities": {"acceptsChatMessages": false}
	}`))
	require.NoError(t, err, "unknown fields must never be fatal")
	assert.Equal(t, "hi", obj.Content)
}

func TestTolerantTimestamps(t *testing.T) {
	cases := map[string]bool{ // value → expect parsed (non-zero)
		`"2026-07-07T03:27:37.028201Z"`:   true,
		`"2026-07-07T03:27:37Z"`:          true,
		`"2026-07-07T03:27:37+02:00"`:     true,
		`"2026-07-07T03:27:37.028201"`:    true, // zone-less, seen in the wild
		`"Mon, 06 Jul 2026 10:00:00 GMT"`: true, // legacy RFC1123
		`"not a date"`:                    false,
		`12345`:                           false, // wrong JSON type
		`{"unexpected": "object"}`:        false,
	}
	for raw, wantParsed := range cases {
		var parsed Time
		err := json.Unmarshal([]byte(raw), &parsed)
		require.NoError(t, err, "timestamp %s must not error", raw)
		assert.Equal(t, wantParsed, !parsed.IsZero(), "timestamp %s parsed-ness", raw)
	}

	// And a bad published date must not sink the whole object.
	obj, err := ParseObject([]byte(`{"id": "x", "type": "Note", "published": "yesterday-ish"}`))
	require.NoError(t, err)
	assert.True(t, obj.Published.IsZero())
	// A present-but-unparseable timestamp is a non-nil *Time, but OK() reports
	// it as unusable so task-05 rkey/TID derivation isn't fooled (finding 6).
	require.NotNil(t, obj.Published, "the field was present, so *Time is non-nil")
	assert.False(t, obj.Published.OK(), "a malformed published must be detectable as invalid")
	assert.False(t, obj.Published.Valid)

	// And it must NOT round-trip as a fabricated year-0001 date: it marshals to
	// null, not "0001-01-01T...".
	out, err := json.Marshal(obj)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"published":null`,
		"a malformed published re-serializes as null, never a plausible timestamp")
	assert.NotContains(t, string(out), "0001-01-01")

	// A real published parses, OK()s true, and round-trips.
	good, err := ParseObject([]byte(`{"id": "x", "type": "Note", "published": "2026-07-07T03:27:37.028201Z"}`))
	require.NoError(t, err)
	assert.True(t, good.Published.OK())
	goodOut, err := json.Marshal(good)
	require.NoError(t, err)
	assert.Contains(t, string(goodOut), "2026-07-07T03:27:37")
}

// TestTolerantSingleValueArrays covers finding 10: a server that wraps a
// logically single-valued field (actor/object/target) in a one-element array
// must degrade to the first element, not sink the whole object.
func TestTolerantSingleValueArrays(t *testing.T) {
	obj, err := ParseObject([]byte(`{
		"id": "https://example.com/activities/1",
		"type": "Create",
		"actor": ["https://example.com/u/alice"],
		"object": [{"id": "https://example.com/note/1", "type": "Note", "content": "hi"}],
		"target": ["https://example.com/c/tech", "https://example.com/c/ignored"]
	}`))
	require.NoError(t, err, "an array-wrapped single-value field must not be fatal")

	require.NotNil(t, obj.Actor)
	assert.Equal(t, "https://example.com/u/alice", obj.Actor.ID)
	require.NotNil(t, obj.Object)
	assert.Equal(t, "https://example.com/note/1", obj.Object.ID)
	assert.Equal(t, "hi", obj.Object.Content)
	require.NotNil(t, obj.Target)
	assert.Equal(t, "https://example.com/c/tech", obj.Target.ID,
		"extra array elements are dropped; the first wins")

	// An empty array degrades to a zero (nil-id) object rather than failing.
	empty, err := ParseObject([]byte(`{"id": "x", "type": "Create", "object": []}`))
	require.NoError(t, err)
	require.NotNil(t, empty.Object)
	assert.Empty(t, empty.Object.ID)
}

func TestRefMarshalRoundTrip(t *testing.T) {
	// A bare-IRI ref re-marshals as a bare string; an inline object stays an
	// object.
	obj, err := ParseObject([]byte(`{"id": "a", "type": "Like", "object": "https://example.com/post/1"}`))
	require.NoError(t, err)
	out, err := json.Marshal(obj)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"object":"https://example.com/post/1"`)

	obj, err = ParseObject([]byte(`{"id": "a", "type": "Create", "object": {"id": "b", "type": "Note", "content": "hi"}}`))
	require.NoError(t, err)
	out, err = json.Marshal(obj)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"content":"hi"`)
}

// TestFixturesRoundTrip proves every committed fixture survives
// parse → marshal → parse with the fields we read intact.
func TestFixturesRoundTrip(t *testing.T) {
	fixtures := []string{
		"group_lemmy_world.json",
		"person_lemmy_world.json",
		"page_lemmy_world.json",
		"note_lemmy_zip.json",
		"announce_create_page_lemmy_world.json",
		"announce_create_note.json",
		"announce_like.json",
		"delete_page.json",
		"outbox_lemmy_world.json",
	}
	for _, name := range fixtures {
		first := parseFixture(t, name)
		encoded, err := json.Marshal(first)
		require.NoError(t, err, "%s must re-marshal", name)
		second, err := ParseObject(encoded)
		require.NoError(t, err, "%s must re-parse", name)
		assert.Equal(t, first.ID, second.ID, name)
		assert.Equal(t, first.Type, second.Type, name)
		assert.Equal(t, len(first.OrderedItems), len(second.OrderedItems), name)
		if first.Object != nil {
			require.NotNil(t, second.Object, name)
			assert.Equal(t, first.Object.ID, second.Object.ID, name)
		}
	}
}

func TestParseObjectRejectsNonObjects(t *testing.T) {
	for _, raw := range []string{``, `"just-a-string"`, `[1,2,3]`, `null`} {
		_, err := ParseObject([]byte(raw))
		assert.Error(t, err, "payload %q must be rejected", raw)
	}
}
