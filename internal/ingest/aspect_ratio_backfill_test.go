package ingest

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAspectRatioBackfillRoute: POST /admin/media/backfill-aspect-ratios
// requires dry_run and answers the materializer's report. The back-fill
// itself is pinned in the materialize package.
func TestAspectRatioBackfillRoute(t *testing.T) {
	const path = "/admin/media/backfill-aspect-ratios"
	h := newHarness(t)

	for _, body := range []map[string]any{nil, {}} {
		response := h.adminRequest(http.MethodPost, path, body)
		assert.Equal(t, http.StatusBadRequest, response.Code, "dry_run is required (body %v): %s", body, response.Body.String())
	}

	for _, dryRun := range []bool{true, false} {
		response := h.adminRequest(http.MethodPost, path, map[string]any{"dry_run": dryRun})
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var report map[string]any
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &report), response.Body.String())
		assert.Equal(t, map[string]any{
			"dry_run": dryRun, "updated": float64(0), "images_set": float64(0), "already_set": float64(0),
			"unreadable_images": float64(0), "without_image_embed": float64(0), "gone": float64(0), "failed": float64(0),
		}, report)
	}
}
