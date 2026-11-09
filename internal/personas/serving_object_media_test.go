package personas

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
	"tidepool/internal/outbound"
	"tidepool/internal/store"
)

// A re-fetched object must be the object that was delivered: a postv2 whose
// gallery image is placed inline by an #image facet serves, under the same media
// base, the same Page the Translator put in the Create — image URL included.

const (
	serveMediaBase      = "https://img.example"
	servePostCollection = "social.coves.community.postv2"
	servePostRKey       = "3lzservepost01"
	servePostATURI      = "at://" + serveObjectDID + "/" + servePostCollection + "/" + servePostRKey
	servePostImageCID   = "bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy"
)

func servedImagePostSnapshot(t *testing.T) []byte {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      servePostATURI,
		"cid":        "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte",
		"rev":        "3lzpostrev0001",
		"collection": servePostCollection,
		"record": map[string]any{
			"$type":     "social.coves.community.postv2",
			"community": serveCommunityDID,
			"title":     "A photo post",
			"content":   "see pic now",
			"facets": []any{map[string]any{
				"index": map[string]any{"byteStart": 4, "byteEnd": 7},
				"features": []any{map[string]any{
					"$type": "social.coves.richtext.facet#image", "index": 0,
				}},
			}},
			"embed": map[string]any{
				"$type": "social.coves.embed.images",
				"images": []any{map[string]any{
					"image": map[string]any{
						"$type":    "blob",
						"ref":      map[string]any{"$link": servePostImageCID},
						"mimeType": "image/png",
						"size":     1234,
					},
					"alt": "a photo",
				}},
			},
			"createdAt": "2026-07-07T03:27:37.028Z",
		},
		"communityApId": serveCommunityAPID,
	})
	require.NoError(t, err)
	return snap
}

func TestServing_ImagePostObjectMatchesDeliveredPage(t *testing.T) {
	conn := servingObjectTestDB(t)
	svc := newServingServiceWith(t, conn, func(o *Options) { o.MediaBaseURL = serveMediaBase })
	snapshot := servedImagePostSnapshot(t)
	objectAPID := userOrigin + "/ap/object/" + serveObjectDID + "/" + servePostCollection + "/" + servePostRKey
	_, err := store.NewOutboundObjects(conn).Upsert(context.Background(), store.OutboundObject{
		ATURI:              servePostATURI,
		APObjectID:         objectAPID,
		CommunityDID:       serveCommunityDID,
		CommunityAPID:      serveCommunityAPID,
		TranslatedSnapshot: snapshot,
	})
	require.NoError(t, err)

	// The delivered Create{Page}, translated from the same snapshot under the
	// same media base.
	translated, err := outbound.NewTranslator(userOrigin, serveMediaBase).Translate(
		userOrigin+"/ap/actor/"+serveObjectDID,
		consume.PostIntent{
			Op:            "create",
			ATURI:         servePostATURI,
			ID:            consume.ActivityID(userOrigin, servePostATURI, "create", 0),
			CommunityAPID: serveCommunityAPID,
			CommunityDID:  serveCommunityDID,
			Snapshot:      snapshot,
		})
	require.NoError(t, err)
	var activity map[string]any
	require.NoError(t, json.Unmarshal(translated.Payload, &activity))
	deliveredPage, ok := activity["object"].(map[string]any)
	require.True(t, ok, "the Create carries its Page object")

	path := "/ap/object/" + serveObjectDID + "/" + servePostCollection + "/" + servePostRKey
	rec := serveOnHost(svc, userHost, http.MethodGet, path,
		http.Header{"Accept": []string{ap.ContentTypeActivityJSON}})
	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	var served map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &served))

	assert.Equal(t, "https://www.w3.org/ns/activitystreams", served["@context"],
		"the standalone object carries its own context")
	delete(served, "@context")
	assert.Equal(t, deliveredPage, served,
		"the re-fetched object equals the delivered Page for the same snapshot and media base")

	source, ok := served["source"].(map[string]any)
	require.True(t, ok, "the served Page carries a markdown source")
	assert.Contains(t, source["content"],
		"![a photo](https://img.example/img/content_full/plain/did:plc:7iza6de2dwap2sbkpav7c6c6/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy)",
		"the served object places the gallery image at the facet under the configured media base")
}
