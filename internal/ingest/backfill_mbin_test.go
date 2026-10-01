package ingest

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/materialize"
)

// Mbin link post fixture ids, rehomed into the harness's subscribed
// lemmy.world/c/technology community.
const (
	mbinPageID    = "https://fedia.io/m/technology@lemmy.world/t/4194502"
	mbinAuthorID  = "https://fedia.io/u/troed"
	mbinSourceURL = "https://video.troed.se/w/5FqDa2jVqCcQWxetJEivnw"
)

// serveMbinLinkPage serves the Mbin link Page as raw JSON — never
// round-tripped through ap.Object, which would normalize away the wire shape
// under test — rehomed from the fixture's startrek.website community into the
// harness's subscribed technology community, plus its Mbin author.
func serveMbinLinkPage(t *testing.T, h *harness) {
	t.Helper()
	page := loadFixture(t, "page_mbin_fedia_io.json")
	require.Equal(t, mbinSourceURL, page["source"],
		"precondition: the fixture carries Mbin's bare-URL-string source")
	page["id"] = mbinPageID
	page["url"] = mbinPageID
	page["audience"] = groupID
	page["to"] = []any{groupID, ap.PublicAudience}
	page["cc"] = []any{mbinAuthorID + "/followers"}
	h.serveObject(urlPath(t, mbinPageID), page)
	h.serveObject("/u/troed", person(mbinAuthorID, "troed", nil))
}

// serveMbinOutbox replaces the technology outbox with a single Lemmy-style
// Announce{Create} whose Page is referenced BY IRI, so the backfill has to
// fetch the Mbin Page from its origin.
func serveMbinOutbox(h *harness) {
	h.serveObject("/c/technology/outbox", map[string]any{
		"type":       "OrderedCollection",
		"id":         "https://lemmy.world/c/technology/outbox",
		"totalItems": 1,
		"orderedItems": []any{
			map[string]any{
				"type":  "Announce",
				"id":    "https://lemmy.world/activities/announce/create/mbin-4194502",
				"actor": groupID,
				"to":    []any{ap.PublicAudience},
				"cc":    []any{groupID + "/followers"},
				"object": map[string]any{
					"type":     "Create",
					"id":       "https://fedia.io/f/object/7c1b2f4e-3d5a-4b8e-9f10-2a6c8d4e5b71",
					"actor":    mbinAuthorID,
					"to":       []any{groupID, ap.PublicAudience},
					"cc":       []any{mbinAuthorID + "/followers"},
					"audience": groupID,
					"object":   mbinPageID,
				},
			},
		},
	})
}

// TestBackfillMaterializesMbinLinkPageWithStringSource: Mbin (fedia.io)
// serves a link post as a Page whose `source` is the bare external URL string
// rather than Lemmy's {content, mediaType} markdown object. A backfill that
// meets such a Page in a subscribed community's outbox must materialize it —
// not fail the item on the parse — and the post body must come from the
// Page's HTML `content`: the URL string is never markdown source.
//
// The Page is hand-built from Mbin's EntryPageFactory output shape (no live
// fetch): internal/ap/testdata/page_mbin_fedia_io.json.
func TestBackfillMaterializesMbinLinkPageWithStringSource(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveMbinLinkPage(t, h)
	serveMbinOutbox(h)
	// The run's error only counts failed items; the per-item cause is logged,
	// so the log is captured to name it when this fails.
	logs := &syncBuffer{}
	b, err := NewBackfill(BackfillOptions{
		Fetcher:      h.client,
		Materializer: h.mat,
		Communities:  h.communities,
		Tombstones:   h.tombstones,
		Echo:         h.classifier,
		MaxPosts:     10,
		Logger:       slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	require.NoError(t, err)
	ctx := context.Background()

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	err = b.Run(ctx, community, true)
	require.NoError(t, err,
		"an Mbin Page with a string source must not fail the backfill item; backfill log:\n%s", logs.String())

	mapping, err := h.objects.GetByAPID(ctx, mbinPageID)
	require.NoError(t, err, "the Mbin link post must be materialized")
	assert.Equal(t, materialize.CollectionPostV2, mapping.Collection)

	record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	content, _ := record["content"].(string)
	assert.Equal(t, "Mbin link post body text", content,
		"the body is the plaintext of the Page's HTML content")
	assert.NotContains(t, content, mbinSourceURL,
		"the string source is a URL, never the post's markdown body")
}
