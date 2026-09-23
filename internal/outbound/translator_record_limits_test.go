package outbound

import (
	"encoding/json"
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

func overLimitRecords() []struct {
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
			name:  "content over 100000 bytes",
			field: "content",
			// 4001 graphemes (under the grapheme cap) of 25 bytes each.
			record: map[string]any{"content": strings.Repeat("\U0001F468\u200D\U0001F469\u200D\U0001F467\u200D\U0001F466", 4001)},
		},
		{
			name:   "content over 10000 graphemes",
			field:  "content",
			record: map[string]any{"content": strings.Repeat("a", 10001)},
		},
	}
}

func TestTranslator_CommentOverLexiconLimitsIsRejected(t *testing.T) {
	translator := NewTranslator(pageUserOrigin)
	for _, test := range overLimitRecords() {
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
	for _, test := range overLimitRecords() {
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
	for _, test := range overLimitRecords() {
		t.Run(test.name, func(t *testing.T) {
			served, err := apobject.RenderObject(pageUserOrigin, limitsCommentSnapshot(t, test.record))
			require.Error(t, err)
			assert.Nil(t, served, "an over-limit snapshot is not served, with or without its facets")
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

	for _, path := range []string{
		"../../lexicons/social/coves/community/comment.json",
		"../../lexicons/social/coves/community/postv2.json",
	} {
		properties := read(path).Defs.Main.Record.Properties
		assert.Equal(t, properties["facets"].MaxLength, apobject.MaximumFacets, path)
		assert.Equal(t, properties["content"].MaxLength, apobject.MaximumContentBytes, path)
		assert.Equal(t, properties["content"].MaxGraphemes, apobject.MaximumContentGraphemes, path)
	}
	facet := read("../../lexicons/social/coves/richtext/facet.json").Defs.Main.Properties
	assert.Equal(t, facet["features"].MaxLength, apobject.MaximumFeaturesPerFacet)
}
