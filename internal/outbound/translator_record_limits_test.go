package outbound

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/apobject"
	"tidepool/internal/consume"
	"tidepool/internal/errors"
)

// The lexicon limits (social.coves.community.comment / postv2 and
// social.coves.richtext.facet) are enforced on every native render path. A
// comment never passes the postv2 lexicon validation the acceptance engine runs,
// so without this an arbitrarily large facet list reaches the renderer. An
// over-limit record is REJECTED rather than rendered with facets dropped: a
// dropped spoiler facet would publish the text it hides.

// familyEmoji is one grapheme of 25 bytes (a ZWJ family emoji), so content
// built from it crosses a byte cap while staying under the grapheme cap.
const familyEmoji = "\U0001F468\u200D\U0001F469\u200D\U0001F467\u200D\U0001F466"

const limitsCommentATURI = "at://did:plc:comment/social.coves.community.comment/rk"

func boldFacets(count int) []any {
	facets := make([]any, 0, count)
	for index := 0; index < count; index++ {
		facets = append(facets, map[string]any{
			"index":    map[string]any{"byteStart": 0, "byteEnd": 1},
			"features": []any{map[string]any{"$type": "social.coves.richtext.facet#bold"}},
		})
	}
	return facets
}

func facetWithFeatures(count int) []any {
	features := make([]any, 0, count)
	for index := 0; index < count; index++ {
		features = append(features, map[string]any{"$type": "social.coves.richtext.facet#bold"})
	}
	return []any{map[string]any{
		"index":    map[string]any{"byteStart": 0, "byteEnd": 1},
		"features": features,
	}}
}

func limitsCommentSnapshot(t *testing.T, record map[string]any) []byte {
	t.Helper()
	snapshot, err := json.Marshal(map[string]any{
		"atUri":         limitsCommentATURI,
		"collection":    "social.coves.community.comment",
		"record":        record,
		"parentAtUri":   pagePostATURI,
		"parentApId":    pagePostAPID,
		"communityApId": pageCommunityAPI,
	})
	require.NoError(t, err)
	return snapshot
}

// overLimitRecords builds records one step past the lexicon limits, given the
// record type's content limits: postv2 allows 50000 graphemes / 500000 bytes,
// comment 10000 / 100000. Facet limits are the same for both.
func overLimitRecords(maximumGraphemes, maximumBytes int) []struct {
	name   string
	field  string
	record map[string]any
} {
	return []struct {
		name   string
		field  string
		record map[string]any
	}{
		{
			name:   "201 facets",
			field:  "facets",
			record: map[string]any{"content": "a", "facets": boldFacets(201)},
		},
		{
			name:   "a facet with 21 features",
			field:  "facets.features",
			record: map[string]any{"content": "a", "facets": facetWithFeatures(21)},
		},
		{
			name:   fmt.Sprintf("content over %d bytes", maximumBytes),
			field:  "content",
			record: map[string]any{"content": strings.Repeat(familyEmoji, maximumBytes/len(familyEmoji)+1)},
		},
		{
			name:   fmt.Sprintf("content over %d graphemes", maximumGraphemes),
			field:  "content",
			record: map[string]any{"content": strings.Repeat("a", maximumGraphemes+1)},
		},
	}
}

func TestTranslator_CommentOverLexiconLimitsIsRejected(t *testing.T) {
	translator := NewTranslator(pageUserOrigin)
	for _, test := range overLimitRecords(10000, 100000) {
		for _, operation := range []string{"create", "update"} {
			t.Run(test.name+" on "+operation, func(t *testing.T) {
				translated, err := translator.Translate(pageActorID, consume.CommentIntent{
					Op: operation, ATURI: limitsCommentATURI,
					ID:            "https://coves.social/ap/activity/comment-" + operation,
					CommunityAPID: pageCommunityAPI, ParentAPID: pagePostAPID,
					Snapshot: limitsCommentSnapshot(t, test.record),
				})
				require.Error(t, err)
				assert.Nil(t, translated, "nothing is federated for an over-limit comment")
				assert.True(t, errors.IsValidation(err), "want a ValidationError, got %v", err)
				var validation errors.ValidationError
				require.ErrorAs(t, err, &validation)
				assert.Equal(t, "record."+test.field, validation.Field)
			})
		}
	}
}

func TestTranslator_PostOverLexiconLimitsIsRejected(t *testing.T) {
	translator := NewTranslator(pageUserOrigin)
	for _, test := range overLimitRecords(50000, 500000) {
		t.Run(test.name, func(t *testing.T) {
			record := map[string]any{"title": "A title"}
			for key, value := range test.record {
				record[key] = value
			}
			translated, err := translator.Translate(pageActorID, consume.PostIntent{
				Op: "create", ATURI: pagePostATURI, ID: "https://coves.social/ap/activity/post-create",
				CommunityAPID: pageCommunityAPI, Snapshot: postSnapshot(t, record),
			})
			require.Error(t, err)
			assert.Nil(t, translated)
			assert.True(t, errors.IsValidation(err), "want a ValidationError, got %v", err)
		})
	}
}

func TestTranslator_CommentAtLexiconLimitsIsRendered(t *testing.T) {
	translator := NewTranslator(pageUserOrigin)
	tests := []struct {
		name   string
		record map[string]any
	}{
		{name: "200 facets", record: map[string]any{"content": "a", "facets": boldFacets(200)}},
		{name: "a facet with 20 features", record: map[string]any{"content": "a", "facets": facetWithFeatures(20)}},
		{name: "10000 graphemes", record: map[string]any{"content": strings.Repeat("a", 10000)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			translated, err := translator.Translate(pageActorID, consume.CommentIntent{
				Op: "create", ATURI: limitsCommentATURI,
				ID:            "https://coves.social/ap/activity/comment-create",
				CommunityAPID: pageCommunityAPI, ParentAPID: pagePostAPID,
				Snapshot: limitsCommentSnapshot(t, test.record),
			})
			require.NoError(t, err)
			assert.Equal(t, "Create", translated.Kind)
		})
	}
}

func TestRenderObject_CommentOverLexiconLimitsIsNotServed(t *testing.T) {
	for _, test := range overLimitRecords(10000, 100000) {
		t.Run(test.name, func(t *testing.T) {
			served, err := apobject.RenderObject(pageUserOrigin, limitsCommentSnapshot(t, test.record))
			require.Error(t, err)
			assert.Nil(t, served, "an over-limit snapshot is not served, with or without its facets")
			assert.True(t, errors.IsValidation(err), "want a ValidationError, got %v", err)
		})
	}
}

// A post body between the comment limit and the postv2 limit (50000 graphemes /
// 500000 bytes) federates and is served whole: the post limit is not the
// comment limit.
func TestTranslator_PostBetweenCommentAndPostLimitsIsRendered(t *testing.T) {
	translator := NewTranslator(pageUserOrigin)
	bodies := map[string]string{
		"20000 graphemes":             strings.Repeat("a", 20000),
		"50000 graphemes":             strings.Repeat("a", 50000),
		"300000 bytes in 12000 emoji": strings.Repeat(familyEmoji, 12000),
	}
	for name, body := range bodies {
		for _, operation := range []string{"create", "update"} {
			t.Run(name+" on "+operation, func(t *testing.T) {
				translated, err := translator.Translate(pageActorID, consume.PostIntent{
					Op: operation, ATURI: pagePostATURI, ID: "https://coves.social/ap/activity/post-" + operation,
					CommunityAPID: pageCommunityAPI,
					Snapshot:      postSnapshot(t, map[string]any{"title": "A title", "content": body}),
				})
				require.NoError(t, err)
				var activity struct {
					Object struct {
						Source struct {
							Content string `json:"content"`
						} `json:"source"`
					} `json:"object"`
				}
				require.NoError(t, json.Unmarshal(translated.Payload, &activity))
				assert.Equal(t, body, activity.Object.Source.Content, "the body federates untruncated")
			})
		}
		t.Run(name+" served", func(t *testing.T) {
			served, err := apobject.RenderObject(pageUserOrigin,
				postSnapshot(t, map[string]any{"title": "A title", "content": body}))
			require.NoError(t, err)
			source, _ := served["source"].(map[string]any)
			assert.Equal(t, body, source["content"], "the body is served untruncated")
		})
	}
}

func TestRenderObject_PostOverLexiconLimitsIsNotServed(t *testing.T) {
	for _, test := range overLimitRecords(50000, 500000) {
		t.Run(test.name, func(t *testing.T) {
			record := map[string]any{"title": "A title"}
			for key, value := range test.record {
				record[key] = value
			}
			served, err := apobject.RenderObject(pageUserOrigin, postSnapshot(t, record))
			require.Error(t, err)
			assert.Nil(t, served)
			assert.True(t, errors.IsValidation(err), "want a ValidationError, got %v", err)
		})
	}
}

// The enforced limits are read back out of the vendored lexicon JSON, so a
// lexicon sync that changes them fails here instead of drifting silently.
func TestLexiconRecordLimits_MatchTheVendoredLexicons(t *testing.T) {
	type property struct {
		MaxLength    int `json:"maxLength"`
		MaxGraphemes int `json:"maxGraphemes"`
	}
	type document struct {
		Defs struct {
			Main struct {
				Record struct {
					Properties map[string]property `json:"properties"`
				} `json:"record"`
				Properties map[string]property `json:"properties"`
			} `json:"main"`
		} `json:"defs"`
	}
	read := func(path string) document {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		var parsed document
		require.NoError(t, json.Unmarshal(raw, &parsed))
		return parsed
	}

	const commentPath = "../../lexicons/social/coves/community/comment.json"
	comment := read(commentPath).Defs.Main.Record.Properties
	assert.Equal(t, comment["facets"].MaxLength, apobject.MaximumFacets, commentPath)
	assert.Equal(t, comment["content"].MaxLength, apobject.MaximumCommentContentBytes, commentPath)
	assert.Equal(t, comment["content"].MaxGraphemes, apobject.MaximumCommentContentGraphemes, commentPath)

	const postPath = "../../lexicons/social/coves/community/postv2.json"
	post := read(postPath).Defs.Main.Record.Properties
	assert.Equal(t, post["facets"].MaxLength, apobject.MaximumFacets, postPath)
	assert.Equal(t, post["content"].MaxLength, apobject.MaximumPostContentBytes, postPath)
	assert.Equal(t, post["content"].MaxGraphemes, apobject.MaximumPostContentGraphemes, postPath)
	facet := read("../../lexicons/social/coves/richtext/facet.json").Defs.Main.Properties
	assert.Equal(t, facet["features"].MaxLength, apobject.MaximumFeaturesPerFacet)
}
