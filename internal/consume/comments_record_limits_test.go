package consume

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// A native comment is never lexicon-validated upstream of this bridge (the
// acceptance engine validates postv2 only), so the comment path enforces the
// comment lexicon's content and facet limits itself. An over-limit comment is a
// PERMANENT rejection taken before any state is written: retrying cannot shrink
// the record, and letting it reach the translator would turn a bad record into
// an error the connector retries as if it were transient.

// commentFrameWithFacets builds a comment commit whose record carries facets.
func commentFrameWithFacets(t *testing.T, rev, rkey, operation string, facets []any) []byte {
	t.Helper()
	frame, err := json.Marshal(map[string]any{
		"did":     dispatchNativeDID,
		"time_us": 9100,
		"kind":    "commit",
		"commit": map[string]any{
			"rev":        rev,
			"operation":  operation,
			"collection": CollectionComment,
			"rkey":       rkey,
			"cid":        "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",
			"record": map[string]any{
				"$type": CollectionComment,
				"reply": map[string]any{
					"root":   map[string]any{"uri": acceptRootATURI, "cid": acceptRootCID},
					"parent": map[string]any{"uri": acceptRootATURI, "cid": acceptRootCID},
				},
				"content":   "a spoiler",
				"facets":    facets,
				"createdAt": "2026-08-13T10:00:00.000Z",
			},
		},
	})
	require.NoError(t, err)
	return frame
}

func spoilerFacets(count int) []any {
	facets := make([]any, 0, count)
	for index := 0; index < count; index++ {
		facets = append(facets, map[string]any{
			"index":    map[string]any{"byteStart": 2, "byteEnd": 9},
			"features": []any{map[string]any{"$type": "social.coves.richtext.facet#spoiler"}},
		})
	}
	return facets
}

func TestCommentCreate_OverTheFacetLimitDeadLettersPermanently(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)
	outboundRowsBefore := countRows(t, database, "outbound_objects")

	err := fixture.handle(t, commentFrameWithFacets(t, dispatchRev, "3lzcmntlim001", "create", spoilerFacets(201)))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPermanentEvent)
	assert.Contains(t, err.Error(), "facets")
	assert.Equal(t, outboundRowsBefore, countRows(t, database, "outbound_objects"),
		"no outbound state is written for a rejected comment")
	assert.Empty(t, fixture.enqueuer.Calls())
}

func TestCommentUpdate_OverTheFacetLimitKeepsTheLastAcceptedSnapshot(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)

	const rkey = "3lzcmntlim002"
	atURI := commentATURIFor(dispatchNativeDID, rkey)
	require.NoError(t, fixture.handle(t, commentFrameWithFacets(t, dispatchRev, rkey, "create", spoilerFacets(1))))
	created, err := store.NewOutboundObjects(database).GetByATURI(context.Background(), atURI)
	require.NoError(t, err)
	require.NotNil(t, created)

	err = fixture.handle(t, commentFrameWithFacets(t, dispatchRevHigher, rkey, "update", spoilerFacets(201)))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPermanentEvent)
	stored, err := store.NewOutboundObjects(database).GetByATURI(context.Background(), atURI)
	require.NoError(t, err)
	assert.Equal(t, created.LastActivitySeq, stored.LastActivitySeq)
	assert.JSONEq(t, string(created.TranslatedSnapshot), string(stored.TranslatedSnapshot))
	assert.Len(t, fixture.enqueuer.Calls(), 1, "only the create was enqueued")
}

func TestCommentCreate_AtTheFacetLimitFederates(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t, commentFrameWithFacets(t, dispatchRev, "3lzcmntlim003", "create", spoilerFacets(200))))
	assert.Len(t, fixture.enqueuer.Calls(), 1)
}
