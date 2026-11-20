package ap

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseObject_ElementBudget: a wide-but-shallow payload (hundreds of
// thousands of tiny elements) is rejected before decoding, cheaply; commas
// inside JSON strings are not elements. Not parallel: it measures
// process-wide allocation.
func TestParseObject_ElementBudget(t *testing.T) {
	cases := []struct {
		name        string
		payload     string
		wantErr     bool
		wantErrText string
		wantContent string
		wantItems   int
	}{
		{
			name: "1 MiB array of empty objects is rejected cheaply",
			payload: `{"type":"OrderedCollectionPage","orderedItems":[` +
				strings.Repeat(`{},`, 349000) + `{}]}`,
			wantErr:     true,
			wantErrText: "JSON elements",
		},
		{
			name: "1 MiB brace-free array of empty strings is rejected cheaply",
			payload: `{"type":"OrderedCollectionPage","orderedItems":[` +
				strings.Repeat(`"",`, 349000) + `""]}`,
			wantErr:     true,
			wantErrText: "JSON elements",
		},
		{
			name: "commas inside a string do not count",
			payload: `{"type":"Note","id":"https://x.example/notes/1","content":"` +
				strings.Repeat("a,", 100000) + `"}`,
			wantContent: strings.Repeat("a,", 100000),
		},
		{
			// '{' + '[' + (k-1) commas = k+1 elements, so k = cap-1 hits the cap exactly.
			name:      "exactly MaxObjectElements elements is accepted",
			payload:   emptyStringOutbox(MaxObjectElements - 1),
			wantItems: MaxObjectElements - 1,
		},
		{
			name:        "MaxObjectElements+1 elements is rejected",
			payload:     emptyStringOutbox(MaxObjectElements),
			wantErr:     true,
			wantErrText: fmt.Sprintf("maximum of %d JSON elements", MaxObjectElements),
		},
		{
			name:      "50-item Lemmy outbox page is accepted",
			payload:   lemmyOutboxPage(50),
			wantItems: 50,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(tc.payload)

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			obj, err := ParseObject(data)
			runtime.ReadMemStats(&after)

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrText)
				assert.True(t, obj == nil, "rejected payload must return a nil object")
				allocated := after.TotalAlloc - before.TotalAlloc
				assert.Less(t, allocated, uint64(8<<20), "bytes allocated during ParseObject")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, obj)
			assert.Equal(t, tc.wantContent, obj.Content)
			assert.Len(t, obj.OrderedItems, tc.wantItems)
		})
	}
}

// TestMaxObjectElements pins the cap: the 50-item Lemmy outbox page case
// above uses about 2,600 elements, so the cap must stay well above it.
func TestMaxObjectElements(t *testing.T) {
	assert.Equal(t, 30000, MaxObjectElements)
}

// emptyStringOutbox builds {"orderedItems":["",...]} with k empty strings,
// which holds exactly k+1 elements: the outer '{', the '[' and k-1 commas.
func emptyStringOutbox(k int) string {
	return `{"orderedItems":[` + strings.TrimSuffix(strings.Repeat(`"",`, k), ",") + `]}`
}

// lemmyOutboxPage builds a Lemmy community outbox page of n Announce(Create(Page))
// activities shaped like testdata/outbox_lemmy_world.json.
func lemmyOutboxPage(n int) string {
	items := make([]string, n)
	for i := range items {
		items[i] = fmt.Sprintf(`{
  "actor": "https://lemmy.world/c/technology",
  "to": ["https://www.w3.org/ns/activitystreams#Public"],
  "object": {
    "id": "https://lemmy.world/activities/create/%[1]d",
    "actor": "https://lemmy.world/u/author%[1]d",
    "to": ["https://www.w3.org/ns/activitystreams#Public"],
    "object": {
      "type": "Page",
      "id": "https://lemmy.world/post/%[1]d",
      "attributedTo": "https://lemmy.world/u/author%[1]d",
      "to": ["https://lemmy.world/c/technology", "https://www.w3.org/ns/activitystreams#Public"],
      "name": "Post title %[1]d, with a comma",
      "cc": [],
      "content": "<p>Body of post %[1]d, which has commas, inside the string.</p>",
      "mediaType": "text/html",
      "source": {"content": "Body of post %[1]d", "mediaType": "text/markdown"},
      "attachment": [{"href": "https://example.com/article/%[1]d", "mediaType": "text/html; charset=utf-8", "type": "Link"}],
      "image": {"type": "Image", "url": "https://lemmy.world/pictrs/image/%[1]d.jpeg"},
      "sensitive": false,
      "published": "2026-07-07T03:27:37.028201Z",
      "updated": "2026-07-07T04:00:00.000000Z",
      "language": {"identifier": "en", "name": "English"},
      "audience": "https://lemmy.world/c/technology",
      "tag": [{"href": "https://lemmy.world/post/%[1]d", "name": "#technology", "type": "Hashtag"}]
    },
    "cc": ["https://lemmy.world/c/technology"],
    "type": "Create",
    "audience": "https://lemmy.world/c/technology"
  },
  "cc": ["https://lemmy.world/c/technology/followers"],
  "type": "Announce",
  "id": "https://lemmy.world/activities/announce/create/%[1]d"
}`, i)
	}
	return `{
  "@context": ["https://join-lemmy.org/context.json", "https://www.w3.org/ns/activitystreams"],
  "type": "OrderedCollection",
  "id": "https://lemmy.world/c/technology/outbox",
  "totalItems": ` + fmt.Sprint(n) + `,
  "orderedItems": [` + strings.Join(items, ",") + `]
}`
}
