package outbound

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTranslator_TakedownDeleteAndUndo (B1): the takedown Delete of a native
// post (Page) and comment (Note) carries no summary, is from the persona, names
// the object's /ap/object URL and addresses the community; its Undo embeds
// exactly that Delete. A second episode gets fresh activity ids.
func TestTranslator_TakedownDeleteAndUndo(t *testing.T) {
	const (
		actorID   = "https://coves.social/ap/actor/did:plc:7iza6de2dwap2sbkpav7c6c6"
		community = "https://lemmy.world/c/technology"
	)
	cases := []struct {
		name      string
		atURI     string
		objectURL string
	}{
		{
			name:      "post (Page)",
			atURI:     "at://did:plc:7iza6de2dwap2sbkpav7c6c6/social.coves.community.postv2/3lztkdnpost01",
			objectURL: "https://coves.social/ap/object/did:plc:7iza6de2dwap2sbkpav7c6c6/social.coves.community.postv2/3lztkdnpost01",
		},
		{
			name:      "comment (Note)",
			atURI:     "at://did:plc:7iza6de2dwap2sbkpav7c6c6/social.coves.community.comment/3lztkdncmnt01",
			objectURL: "https://coves.social/ap/object/did:plc:7iza6de2dwap2sbkpav7c6c6/social.coves.community.comment/3lztkdncmnt01",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := NewTranslator("https://coves.social", "")
			seen := map[string]bool{}
			for _, episode := range []int{1, 2} {
				del, err := tr.TakedownDelete(actorID, tc.atURI, community, episode)
				require.NoError(t, err)
				require.NotNil(t, del, "episode %d: TakedownDelete renders an activity", episode)
				assert.Equal(t, "Delete", del.Kind)
				deleteID := activityIDOf(t, del.Payload)
				assert.Equal(t, deleteID, del.ActivityID, "the row id is the payload id")
				assert.True(t, strings.HasPrefix(deleteID, "https://coves.social/ap/activity/"),
					"the Delete id is minted under the user origin, got %q", deleteID)
				assert.JSONEq(t, fmt.Sprintf(`{
					"@context": "https://www.w3.org/ns/activitystreams",
					"id": %q,
					"type": "Delete",
					"actor": %q,
					"to": ["https://www.w3.org/ns/activitystreams#Public"],
					"cc": [%q],
					"audience": %q,
					"object": %q
				}`, deleteID, actorID, community, community, tc.objectURL), string(del.Payload),
					"episode %d: the takedown Delete is a bare self-delete with no summary", episode)

				undo, err := tr.TakedownUndo(actorID, community, del.Payload, episode)
				require.NoError(t, err)
				require.NotNil(t, undo, "episode %d: TakedownUndo renders an activity", episode)
				assert.Equal(t, "Undo", undo.Kind)
				undoID := activityIDOf(t, undo.Payload)
				assert.Equal(t, undoID, undo.ActivityID, "the row id is the payload id")
				assert.True(t, strings.HasPrefix(undoID, "https://coves.social/ap/activity/"),
					"the Undo id is minted under the user origin, got %q", undoID)
				assert.JSONEq(t, fmt.Sprintf(`{
					"@context": "https://www.w3.org/ns/activitystreams",
					"id": %q,
					"type": "Undo",
					"actor": %q,
					"to": ["https://www.w3.org/ns/activitystreams#Public"],
					"cc": [%q],
					"audience": %q,
					"object": %s
				}`, undoID, actorID, community, community, del.Payload), string(undo.Payload),
					"episode %d: the Undo embeds exactly the Delete it withdraws", episode)

				for _, id := range []string{deleteID, undoID} {
					assert.False(t, seen[id], "activity id %q is reused across kinds or episodes", id)
					seen[id] = true
				}
			}
		})
	}
}

// activityIDOf reads the top-level id of an activity payload.
func activityIDOf(t *testing.T, payload []byte) string {
	t.Helper()
	var activity struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(payload, &activity))
	return activity.ID
}
