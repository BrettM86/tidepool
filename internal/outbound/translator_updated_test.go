package outbound

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
)

// Edit timestamps (task 04): an Update{Page}/Update{Note} object carries
// `updated` = the rev TID's time when strictly later than record.createdAt,
// else createdAt + 1 ms; an unparsable rev falls back to the snapshot's timeUs
// under the same floor. A Create carries no `updated` key. `published` stays
// record.createdAt either way.
//
// TID literals below were computed outside the repo (clock id 7) and decoded
// back with indigo's syntax.ParseTID. Each is commented with the instant it
// encodes.
const (
	updatedCreatedAt = "2026-11-01T10:00:00Z"

	tidAt100500   = "3mzakoskms22b" // 2026-11-01T10:05:00.000000Z
	tidAt100500p1 = "3mzakosnoh22b" // 2026-11-01T10:05:00.100000Z
	tidAt100500p2 = "3mzakosqq422b" // 2026-11-01T10:05:00.200000Z
	tidAt095900   = "3mzake3acc22b" // 2026-11-01T09:59:00.000000Z
	tidAt100000   = "3mzakfuhe222b" // 2026-11-01T10:00:00.000000Z

	timeUsAt111111   int64 = 1793531471000000 // 2026-11-01T11:11:11Z (conflicts with every valid rev; rev must win)
	timeUsAt100600p5 int64 = 1793527560500000 // 2026-11-01T10:06:00.5Z
	timeUsAt090000   int64 = 1793523600000000 // 2026-11-01T09:00:00Z
)

const updatedCommentATURI = "at://did:plc:comment/social.coves.community.comment/rkupdated"

func updatedPostIntent(t *testing.T, op, rev string, timeUs int64) consume.Intent {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      pagePostATURI,
		"cid":        pagePostCID,
		"rev":        rev,
		"timeUs":     timeUs,
		"collection": "social.coves.community.postv2",
		"record": map[string]any{
			"$type":     "social.coves.community.postv2",
			"title":     pageTitle,
			"content":   pageBody,
			"createdAt": updatedCreatedAt,
		},
		"communityApId": pageCommunityAPI,
	})
	require.NoError(t, err)
	return consume.PostIntent{
		Op: op, ATURI: pagePostATURI, ID: "https://coves.social/ap/activity/post-" + op,
		CommunityAPID: pageCommunityAPI, Snapshot: snap,
	}
}

func updatedCommentIntent(t *testing.T, op, rev string, timeUs int64) consume.Intent {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      updatedCommentATURI,
		"rev":        rev,
		"timeUs":     timeUs,
		"collection": "social.coves.community.comment",
		"record": map[string]any{
			"content":   "a reply",
			"createdAt": updatedCreatedAt,
		},
		"parentAtUri":   pagePostATURI,
		"parentApId":    pagePostAPID,
		"communityApId": pageCommunityAPI,
	})
	require.NoError(t, err)
	return consume.CommentIntent{
		Op: op, ATURI: updatedCommentATURI, ID: "https://coves.social/ap/activity/comment-" + op,
		CommunityAPID: pageCommunityAPI, ParentAPID: pagePostAPID, Snapshot: snap,
	}
}

func TestTranslator_UpdatedTimestamp(t *testing.T) {
	tests := []struct {
		name        string
		intent      func(t *testing.T) consume.Intent
		wantKind    string
		wantType    string
		wantUpdated string // "" means the object must have no "updated" key
	}{
		// U1 post
		{
			name:     "post create has no updated",
			intent:   func(t *testing.T) consume.Intent { return updatedPostIntent(t, "create", tidAt100500, timeUsAt111111) },
			wantKind: "Create", wantType: "Page",
		},
		{
			name:     "post update takes rev time",
			intent:   func(t *testing.T) consume.Intent { return updatedPostIntent(t, "update", tidAt100500, timeUsAt111111) },
			wantKind: "Update", wantType: "Page", wantUpdated: "2026-11-01T10:05:00Z",
		},
		// U2 comment
		{
			name: "comment create has no updated",
			intent: func(t *testing.T) consume.Intent {
				return updatedCommentIntent(t, "create", tidAt100500, timeUsAt111111)
			},
			wantKind: "Create", wantType: "Note",
		},
		{
			name: "comment update takes rev time",
			intent: func(t *testing.T) consume.Intent {
				return updatedCommentIntent(t, "update", tidAt100500, timeUsAt111111)
			},
			wantKind: "Update", wantType: "Note", wantUpdated: "2026-11-01T10:05:00Z",
		},
		// U3 sub-second
		{
			name: "post update sub-second rev .1",
			intent: func(t *testing.T) consume.Intent {
				return updatedPostIntent(t, "update", tidAt100500p1, timeUsAt111111)
			},
			wantKind: "Update", wantType: "Page", wantUpdated: "2026-11-01T10:05:00.1Z",
		},
		{
			name: "comment update sub-second rev .2",
			intent: func(t *testing.T) consume.Intent {
				return updatedCommentIntent(t, "update", tidAt100500p2, timeUsAt111111)
			},
			wantKind: "Update", wantType: "Note", wantUpdated: "2026-11-01T10:05:00.2Z",
		},
		// U4 floor
		{
			name:     "post update rev before createdAt floors to published plus 1ms",
			intent:   func(t *testing.T) consume.Intent { return updatedPostIntent(t, "update", tidAt095900, timeUsAt111111) },
			wantKind: "Update", wantType: "Page", wantUpdated: "2026-11-01T10:00:00.001Z",
		},
		{
			name:     "post update rev equal to createdAt floors to published plus 1ms",
			intent:   func(t *testing.T) consume.Intent { return updatedPostIntent(t, "update", tidAt100000, timeUsAt111111) },
			wantKind: "Update", wantType: "Page", wantUpdated: "2026-11-01T10:00:00.001Z",
		},
		// U5 fallback to timeUs
		{
			name: "post update unparsable rev uses timeUs",
			intent: func(t *testing.T) consume.Intent {
				return updatedPostIntent(t, "update", "not-a-tid", timeUsAt100600p5)
			},
			wantKind: "Update", wantType: "Page", wantUpdated: "2026-11-01T10:06:00.5Z",
		},
		{
			name: "comment update unparsable rev with timeUs before createdAt floors",
			intent: func(t *testing.T) consume.Intent {
				return updatedCommentIntent(t, "update", "not-a-tid", timeUsAt090000)
			},
			wantKind: "Update", wantType: "Note", wantUpdated: "2026-11-01T10:00:00.001Z",
		},
	}

	translator := NewTranslator(pageUserOrigin, "")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			translated, err := translator.Translate(pageActorID, test.intent(t))
			require.NoError(t, err)
			assert.Equal(t, test.wantKind, translated.Kind)

			var activity map[string]any
			require.NoError(t, json.Unmarshal(translated.Payload, &activity))
			assert.Equal(t, test.wantKind, activity["type"])
			object := mustMap(t, activity["object"], "activity.object")
			assert.Equal(t, test.wantType, object["type"])
			assert.Equal(t, updatedCreatedAt, object["published"], "published stays record.createdAt")

			if test.wantUpdated == "" {
				assert.NotContains(t, object, "updated", "a Create object carries no updated key")
				return
			}
			assert.Equal(t, test.wantUpdated, object["updated"])
		})
	}
}
