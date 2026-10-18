package materialize

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/lexicon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/lexicons"
)

// parseActorDoc builds an AP actor document as JSON and parses it through
// ap.ParseObject, so `url` goes through the real string/object/array
// decoding. url is a raw JSON fragment; "" leaves the key out.
func parseActorDoc(t *testing.T, actorType, id, preferredUsername, url string) *ap.Object {
	t.Helper()
	doc := map[string]any{
		"@context":          "https://www.w3.org/ns/activitystreams",
		"type":              actorType,
		"id":                id,
		"preferredUsername": preferredUsername,
		"published":         "2026-01-02T03:04:05Z",
	}
	if url != "" {
		doc["url"] = json.RawMessage(url)
	}
	raw, err := json.Marshal(doc)
	require.NoError(t, err)
	obj, err := ap.ParseObject(raw)
	require.NoError(t, err)
	return obj
}

// requireValidRecord validates a built record against the vendored Coves
// lexicons the way the materializer does (JSON round-trip first).
func requireValidRecord(t *testing.T, record map[string]any, collection string) {
	t.Helper()
	catalog, err := lexicons.Catalog()
	require.NoError(t, err)
	data, err := jsonRoundTrip(record)
	require.NoError(t, err)
	require.NoError(t, lexicon.ValidateRecord(catalog, data, collection, lexicon.ValidateFlags(0)))
}

// pickFields returns the subset of record keys named in keys that are
// present, so a test can assert presence, absence and value in one compare.
func pickFields(t *testing.T, record map[string]any, keys ...string) map[string]string {
	t.Helper()
	got := map[string]string{}
	for _, key := range keys {
		value, ok := record[key]
		if !ok {
			continue
		}
		text, isString := value.(string)
		require.True(t, isString, "%s must be a string, got %T", key, value)
		got[key] = text
	}
	return got
}

func originFieldsOf(t *testing.T, record map[string]any) map[string]string {
	return pickFields(t, record, "origin", "originUsername", "originUrl")
}

// TestActorProfileOriginAddress: the actor profile carries the home-instance
// address — origin from the actor id's host (same rule as community origin),
// originUsername as preferredUsername byte-for-byte — and originUrl.
func TestActorProfileOriginAddress(t *testing.T) {
	cases := []struct {
		name     string
		id       string
		username string
		want     map[string]string
	}{
		{
			name: "lemmy user", id: "https://lemmy.world/u/Alice_B", username: "Alice_B",
			want: map[string]string{"origin": "lemmy.world", "originUsername": "Alice_B", "originUrl": "https://lemmy.world/u/Alice_B"},
		},
		{
			name: "mixed-case host lowercased", id: "https://LEMMY.World/u/Alice_B", username: "Alice_B",
			want: map[string]string{"origin": "lemmy.world", "originUsername": "Alice_B", "originUrl": "https://LEMMY.World/u/Alice_B"},
		},
		{
			name: "explicit port omits origin", id: "https://lemmy.world:8443/u/Alice_B", username: "Alice_B",
			want: map[string]string{"originUsername": "Alice_B", "originUrl": "https://lemmy.world:8443/u/Alice_B"},
		},
		{
			name: "IP literal omits origin", id: "https://192.0.2.7/u/Alice_B", username: "Alice_B",
			want: map[string]string{"originUsername": "Alice_B", "originUrl": "https://192.0.2.7/u/Alice_B"},
		},
		{
			name: "decomposed unicode preserved", id: "https://lemmy.world/u/amelie", username: "Amélie",
			want: map[string]string{"origin": "lemmy.world", "originUsername": "Amélie", "originUrl": "https://lemmy.world/u/amelie"},
		},
	}
	m := &Materializer{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := parseActorDoc(t, "Person", tc.id, tc.username, "")
			record := m.buildActorProfile(doc, time.Time{}, nil, nil)
			assert.Equal(t, tc.want, originFieldsOf(t, record))
			requireValidRecord(t, record, CollectionActorProfile)
		})
	}
}

// TestActorProfileOriginUsernameByteLimit: the lexicon caps originUsername
// at 256 UTF-8 bytes. A longer username is never truncated: origin and
// originUsername are both dropped, while originUrl stays.
func TestActorProfileOriginUsernameByteLimit(t *testing.T) {
	const id = "https://lemmy.world/u/long"
	cases := []struct {
		name     string
		username string
		want     map[string]string
	}{
		{
			name: "256 ASCII bytes", username: strings.Repeat("a", 256),
			want: map[string]string{"origin": "lemmy.world", "originUsername": strings.Repeat("a", 256), "originUrl": id},
		},
		{
			name: "257 ASCII bytes", username: strings.Repeat("a", 257),
			want: map[string]string{"originUrl": id},
		},
		{
			name: "128 two-byte runes (256 bytes)", username: strings.Repeat("é", 128),
			want: map[string]string{"origin": "lemmy.world", "originUsername": strings.Repeat("é", 128), "originUrl": id},
		},
		{
			name: "129 two-byte runes (258 bytes)", username: strings.Repeat("é", 129),
			want: map[string]string{"originUrl": id},
		},
	}
	m := &Materializer{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := parseActorDoc(t, "Person", id, tc.username, "")
			record := m.buildActorProfile(doc, time.Time{}, nil, nil)
			assert.Equal(t, tc.want, originFieldsOf(t, record))
			requireValidRecord(t, record, CollectionActorProfile)
		})
	}
}

// TestActorProfileOriginURL: originUrl is the actor's single same-host
// https `url` when it has one, otherwise the actor id, and absent when the
// id is not an absolute URL. A candidate the lexicon's `uri` format would
// refuse, or that is not http(s), is skipped rather than written.
func TestActorProfileOriginURL(t *testing.T) {
	const mastodonID = "https://example.social/users/alice"
	cases := []struct {
		name string
		id   string
		url  string // raw JSON; "" = absent
		want map[string]string
	}{
		{
			name: "same-host https url", id: mastodonID, url: `"https://example.social/@alice"`,
			want: map[string]string{"origin": "example.social", "originUsername": "alice", "originUrl": "https://example.social/@alice"},
		},
		{
			name: "url host differs only in case", id: mastodonID, url: `"https://Example.Social/@alice"`,
			want: map[string]string{"origin": "example.social", "originUsername": "alice", "originUrl": "https://Example.Social/@alice"},
		},
		{
			name: "url on another host", id: mastodonID, url: `"https://elsewhere.example/@alice"`,
			want: map[string]string{"origin": "example.social", "originUsername": "alice", "originUrl": mastodonID},
		},
		{
			name: "http url", id: mastodonID, url: `"http://example.social/@alice"`,
			want: map[string]string{"origin": "example.social", "originUsername": "alice", "originUrl": mastodonID},
		},
		{
			name: "same hostname different port", id: mastodonID, url: `"https://example.social:8443/@alice"`,
			want: map[string]string{"origin": "example.social", "originUsername": "alice", "originUrl": mastodonID},
		},
		{
			name: "relative url", id: mastodonID, url: `"/@alice"`,
			want: map[string]string{"origin": "example.social", "originUsername": "alice", "originUrl": mastodonID},
		},
		{
			name: "multi-valued url", id: mastodonID,
			url:  `["https://example.social/@alice", {"type": "Link", "href": "https://example.social/@alice/profile"}]`,
			want: map[string]string{"origin": "example.social", "originUsername": "alice", "originUrl": mastodonID},
		},
		{
			name: "url absent", id: mastodonID, url: "",
			want: map[string]string{"origin": "example.social", "originUsername": "alice", "originUrl": mastodonID},
		},
		{
			name: "id not an absolute URL", id: "alice", url: `"https://example.social/@alice"`,
			want: map[string]string{"originUsername": "alice"},
		},
		{
			name: "url with uppercase scheme", id: mastodonID, url: `"HTTPS://example.social/@alice"`,
			want: map[string]string{"origin": "example.social", "originUsername": "alice", "originUrl": mastodonID},
		},
		{
			name: "url with a space in the path", id: mastodonID, url: `"https://example.social/@alice smith"`,
			want: map[string]string{"origin": "example.social", "originUsername": "alice", "originUrl": mastodonID},
		},
		{
			name: "url with a non-ASCII character", id: mastodonID, url: `"https://example.social/@alicé"`,
			want: map[string]string{"origin": "example.social", "originUsername": "alice", "originUrl": mastodonID},
		},
		{
			name: "url over 8192 bytes", id: mastodonID, url: `"https://example.social/@` + strings.Repeat("a", 8192) + `"`,
			want: map[string]string{"origin": "example.social", "originUsername": "alice", "originUrl": mastodonID},
		},
		{
			name: "id with uppercase scheme and no url", id: "HTTPS://example.social/users/alice", url: "",
			want: map[string]string{"origin": "example.social", "originUsername": "alice"},
		},
		{
			name: "id with a non-ASCII character and no url", id: "https://example.social/users/alicé", url: "",
			want: map[string]string{"origin": "example.social", "originUsername": "alice"},
		},
		{
			name: "id not http or https", id: "ftp://example.social/users/alice", url: "",
			want: map[string]string{"origin": "example.social", "originUsername": "alice"},
		},
	}
	m := &Materializer{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := parseActorDoc(t, "Person", tc.id, "alice", tc.url)
			record := m.buildActorProfile(doc, time.Time{}, nil, nil)
			assert.Equal(t, tc.want, originFieldsOf(t, record))
			requireValidRecord(t, record, CollectionActorProfile)
		})
	}
}

// TestCommunityProfileOriginURL: the community profile gains originUrl under
// the same rule as the actor profile, keeping origin and name.
func TestCommunityProfileOriginURL(t *testing.T) {
	const groupID = "https://lemmy.world/c/technology"
	cases := []struct {
		name string
		url  string // raw JSON; "" = absent
		want map[string]string
	}{
		{
			name: "same-host https url", url: `"https://lemmy.world/c/technology"`,
			want: map[string]string{"origin": "lemmy.world", "name": "technology", "originUrl": "https://lemmy.world/c/technology"},
		},
		{
			name: "same-host url distinct from id", url: `"https://LEMMY.World/c/technology"`,
			want: map[string]string{"origin": "lemmy.world", "name": "technology", "originUrl": "https://LEMMY.World/c/technology"},
		},
		{
			name: "url on another host", url: `"https://mirror.example/c/technology"`,
			want: map[string]string{"origin": "lemmy.world", "name": "technology", "originUrl": groupID},
		},
	}
	m := &Materializer{serviceDID: "did:web:tidepool.test"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := parseActorDoc(t, "Group", groupID, "technology", tc.url)
			record := m.buildCommunityProfile(doc, time.Time{}, nil, nil)
			assert.Equal(t, tc.want, pickFields(t, record, "origin", "name", "originUrl"))
			requireValidRecord(t, record, CollectionCommunityProfile)
		})
	}
}
