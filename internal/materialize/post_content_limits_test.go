package materialize

import (
	"context"
	"strings"
	"testing"

	"github.com/rivo/uniseg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// The body limits a bridged record is truncated to follow its lexicon:
// community.postv2 allows 50000 graphemes / 500000 bytes, the deprecated
// community.post keeps 10000 / 100000. A comment body is truncated well under
// the comment lexicon's 10000 graphemes.

const limitsFamilyEmoji = "\U0001F468\u200d\U0001F469\u200d\U0001F467\u200d\U0001F466" // one grapheme, 25 bytes

func limitsPage(t *testing.T, body string) map[string]any {
	t.Helper()
	return map[string]any{
		"type":         "Page",
		"id":           "https://lemmy.example/post/1",
		"attributedTo": "https://lemmy.example/u/alice",
		"name":         "A title",
		"published":    "2026-10-01T12:00:00Z",
		"source":       map[string]any{"content": body, "mediaType": "text/markdown"},
	}
}

func postV2Content(t *testing.T, body string) string {
	t.Helper()
	page := mustObject(t, limitsPage(t, body))
	author := &store.BridgedActor{APActorID: "https://lemmy.example/u/alice"}
	record := (&Materializer{}).buildPostV2Record(context.Background(), page, "did:plc:community", author, nil, "did:plc:alice")
	content, _ := record["content"].(string)
	return content
}

func TestBuildPostV2Record_BodyWithinThePostLimitIsKeptWhole(t *testing.T) {
	for name, body := range map[string]string{
		"20000 graphemes":             strings.Repeat("a", 20000),
		"50000 graphemes":             strings.Repeat("a", 50000),
		"300000 bytes in 12000 emoji": strings.Repeat(limitsFamilyEmoji, 12000),
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, body, postV2Content(t, body))
		})
	}
}

func TestBuildPostV2Record_BodyOverThePostLimitIsTruncated(t *testing.T) {
	for name, body := range map[string]string{
		"50001 graphemes":             strings.Repeat("a", 50001),
		"500025 bytes in 20001 emoji": strings.Repeat(limitsFamilyEmoji, 20001),
	} {
		t.Run(name, func(t *testing.T) {
			content := postV2Content(t, body)
			require.NotEmpty(t, content)
			assert.NotEqual(t, body, content)
			assert.LessOrEqual(t, uniseg.GraphemeClusterCount(content), 50000)
			assert.LessOrEqual(t, len(content), 500000)
		})
	}
}

func TestBuildPostRecord_LegacyBodyKeepsTheDeprecatedPostLimit(t *testing.T) {
	page := mustObject(t, limitsPage(t, strings.Repeat("a", 20000)))
	record := (&Materializer{}).buildPostRecord(context.Background(), page, "did:plc:community", "did:plc:alice", "did:plc:alice")
	content, _ := record["content"].(string)
	require.NotEmpty(t, content)
	assert.LessOrEqual(t, uniseg.GraphemeClusterCount(content), 10000)
	assert.LessOrEqual(t, len(content), 100000)
}

func TestDraftComment_BodyOverTheCommentLimitIsTruncated(t *testing.T) {
	note := mustObject(t, map[string]any{
		"type":         "Note",
		"id":           "https://lemmy.example/comment/1",
		"attributedTo": "https://lemmy.example/u/alice",
		"inReplyTo":    "https://lemmy.example/post/1",
		"published":    "2026-10-01T12:00:00Z",
		"source":       map[string]any{"content": strings.Repeat("a", 10001), "mediaType": "text/markdown"},
	})
	draft, err := draftComment(note)
	require.NoError(t, err)
	require.NotEmpty(t, draft.body)
	assert.LessOrEqual(t, uniseg.GraphemeClusterCount(draft.body), 3000)
	assert.LessOrEqual(t, len(draft.body), 30000)
}
