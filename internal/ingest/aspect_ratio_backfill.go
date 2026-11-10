package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"tidepool/internal/materialize"
)

// AspectRatioBackfiller is the materializer surface behind POST
// /admin/media/backfill-aspect-ratios. *materialize.Materializer implements
// it; the Admin reads it off the Materializer it already holds.
type AspectRatioBackfiller interface {
	BackfillImageAspectRatios(ctx context.Context, dryRun bool) (*materialize.AspectRatioBackfillReport, error)
}

// handleBackfillAspectRatios serves POST /admin/media/backfill-aspect-ratios
// {"dry_run": bool}: the one-time aspectRatio back-fill of bridged post images
// materialized before the bridge recorded ratios. dry_run is required, as for
// the other back-fills, and it extends the write deadline as they do.
func (a *Admin) handleBackfillAspectRatios(w http.ResponseWriter, r *http.Request) {
	backfiller, ok := a.mat.(AspectRatioBackfiller)
	if !ok {
		http.Error(w, "aspect ratio back-fill is not configured", http.StatusNotImplemented)
		return
	}
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(banReplayWriteTimeout)); err != nil {
		a.logger.WarnContext(r.Context(), "extend aspect ratio back-fill write deadline", "error", err)
	}
	var req struct {
		DryRun *bool `json:"dry_run"`
	}
	if err := decodeJSONBody(r, &req); err != nil || req.DryRun == nil {
		http.Error(w, `body must be {"dry_run":true} or {"dry_run":false}`, http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	report, err := backfiller.BackfillImageAspectRatios(ctx, *req.DryRun)
	if errors.Is(err, context.Canceled) {
		// The client went away; the run is idempotent and is simply run again.
		a.logger.WarnContext(ctx, "aspect ratio back-fill cut short by the client", "dry_run", *req.DryRun)
		return
	}
	if err != nil {
		a.logger.ErrorContext(ctx, "aspect ratio back-fill failed", "dry_run", *req.DryRun, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	a.logger.InfoContext(ctx, "aspect ratio back-fill finished",
		"dry_run", report.DryRun, "updated", report.Updated, "images_set", report.ImagesSet,
		"already_set", report.AlreadySet, "unreadable_images", report.UnreadableImages,
		"without_image_embed", report.WithoutImageEmbed, "gone", report.Gone, "failed", report.Failed)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(report); err != nil {
		a.logger.WarnContext(ctx, "write aspect ratio back-fill report", "dry_run", report.DryRun, "error", err)
	}
}
