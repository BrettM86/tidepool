package outbound

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
)

// Second-opinion (important): the Page/Note addressing split fence. A Note
// carries the community in `cc` — it must NEVER appear in `to` (only Public). A
// Note with the community in `to` is the Page shape, which changes how Lemmy
// routes it. This pins the fence from the Note side (the Page side is pinned in
// translator_page_test.go).

func TestTranslator_NoteToExcludesCommunity(t *testing.T) {
	tr := NewTranslator("https://coves.social")
	community := "https://lemmy.world/c/tech"
	actorID := "https://coves.social/ap/actor/did:plc:x"
	atURI := "at://did:plc:x/social.coves.community.comment/rk"

	snap, err := json.Marshal(map[string]any{
		"atUri":         atURI,
		"record":        map[string]any{"content": "a reply", "createdAt": "2026-08-12T10:00:00.000Z"},
		"parentAtUri":   "at://did:plc:p/social.coves.community.postv2/rp",
		"parentApId":    "https://lemmy.world/post/1",
		"communityApId": community,
	})
	require.NoError(t, err)

	out, err := tr.Translate(actorID, consume.CommentIntent{
		Op:            "create",
		ATURI:         atURI,
		ID:            "https://coves.social/ap/activity/deadbeef",
		CommunityAPID: community,
		ParentAPID:    "https://lemmy.world/post/1",
		Snapshot:      snap,
	})
	require.NoError(t, err)

	var activity map[string]any
	require.NoError(t, json.Unmarshal(out.Payload, &activity))
	note := mustMap(t, activity["object"], "object")

	to := asStringSet(t, note["to"], "Note.to")
	assert.NotContains(t, to, community,
		"a Note must NOT carry the community in `to` — that is the Page shape (Page/Note split)")
	assert.Contains(t, to, ap.PublicAudience, "a Note's `to` is Public only")

	cc := asStringSet(t, note["cc"], "Note.cc")
	assert.Contains(t, cc, community, "a Note carries the community in `cc`")
}
