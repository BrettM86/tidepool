package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
)

// The Coves user origin (task 13) serves its own /ap/inbox, but it must not
// grow a second delivery pipeline: it dispatches to the handler exported
// here. These tests pin that the exported handler IS the mounted one — same
// verification, same refusal taxonomy, same queueing — because a divergence
// would mean two inboxes with two security postures.

// TestInboxHandler_MatchesMountedRoute: an unsigned delivery must be refused
// identically through both paths.
func TestInboxHandler_MatchesMountedRoute(t *testing.T) {
	h := newHarness(t)

	body := []byte(`{"id":"https://lemmy.world/activities/like/unsigned","type":"Like",` +
		`"actor":"https://lemmy.world/u/nobody","object":"` + pageID + `"}`)
	newRequest := func(target string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+target, bytes.NewReader(body))
		req.Header.Set("Content-Type", ap.ContentTypeActivityJSON)
		return req
	}

	mounted := httptest.NewRecorder()
	h.router.ServeHTTP(mounted, newRequest("/inbox"))

	exported := httptest.NewRecorder()
	h.inbox.InboxHandler().ServeHTTP(exported, newRequest("/ap/inbox"))

	assert.Equal(t, http.StatusUnauthorized, mounted.Code,
		"an unsigned delivery is a definitive refusal (4xx), not a retryable one")
	assert.Equal(t, mounted.Code, exported.Code,
		"the exported handler must refuse exactly as the mounted route does")
	assert.Equal(t, mounted.Body.String(), exported.Body.String(),
		"same refusal, same body: one implementation, two mount points")
}

// TestInboxHandler_AcceptsSignedDelivery walks the whole pipeline through
// the exported handler — verification, dedupe, enqueue — so the test cannot
// pass on an error path alone.
func TestInboxHandler_AcceptsSignedDelivery(t *testing.T) {
	h := newHarness(t)
	alice := h.newRemoteActor("https://lemmy.world/u/exported",
		person("https://lemmy.world/u/exported", "exported", nil))

	const activityID = "https://lemmy.world/activities/like/exported"
	body, err := json.Marshal(likeActivity(activityID, alice.id))
	require.NoError(t, err)

	// Addressed to the USER origin's inbox path: the handler must not care
	// which mount point it was reached through.
	req := httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+"/ap/inbox", bytes.NewReader(body))
	req.Header.Set("Content-Type", ap.ContentTypeActivityJSON)
	require.NoError(t, alice.signer().SignRequest(req, body))

	rec := httptest.NewRecorder()
	h.inbox.InboxHandler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code, "body=%s", rec.Body.String())

	_, err = h.events.GetEvent(context.Background(), activityID)
	assert.NoError(t, err, "an accepted delivery must be recorded for dedupe and queueing")
}
