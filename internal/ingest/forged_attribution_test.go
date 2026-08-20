package ingest

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
)

// TestBareCreateCannotAttributeToAnotherInstance: bare delivery is signed by
// whoever sent it, and nothing about that signature vouches for the content's
// `attributedTo`. Since the postv2 flip a materialized post lands in the
// AUTHOR's repo and is signed by that repo's key, so an accepted forgery here
// is a signed post the victim never wrote. The delivering instance may only
// ever attribute content to its own users — which is exactly what Lemmy's
// verify_domains_match already guarantees of genuine traffic.
func TestBareCreateCannotAttributeToAnotherInstance(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	// The victim's actor document is fetchable, so the refusal below is the
	// attribution check and not a failed mint.
	h.serveLemmyWorldContent()
	ctx := context.Background()

	const forgedID = "https://evil.example/post/1"
	eve := h.newRemoteActor("https://evil.example/u/eve", person("https://evil.example/u/eve", "eve", nil))

	require.Equal(t, http.StatusAccepted, h.deliver(eve, map[string]any{
		"id":    "https://evil.example/activities/create/1",
		"type":  "Create",
		"actor": eve.id,
		"object": map[string]any{
			"type":         "Page",
			"id":           forgedID,
			"attributedTo": personID, // a user on lemmy.world, not on evil.example
			"to":           []any{ap.PublicAudience},
			"audience":     groupID,
			"name":         "a post the victim never wrote",
			"source":       map[string]any{"content": "body", "mediaType": "text/markdown"},
			"published":    "2026-07-08T17:00:00.000000Z",
		},
	}))
	h.drain()

	event, err := h.events.GetEvent(ctx, "https://evil.example/activities/create/1")
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "the drop is a processed skip, never a retry")

	_, err = h.objects.GetByAPID(ctx, forgedID)
	assert.True(t, errors.IsNotFound(err), "forged content must not be materialized")
	_, err = h.actors.GetByAPActorID(ctx, personID)
	assert.True(t, errors.IsNotFound(err), "naming a victim must not bridge them")
}
