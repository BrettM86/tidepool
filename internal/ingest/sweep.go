// Origin-verified delete sweep: the cleanup path for deletes the bridge
// missed (a dropped activity, an outage, the pre-fix cross-authority
// authorization bug). Unlike handleDelete there is no delivered activity to
// authorize, so the origin's own word IS the authorization: an object is
// swept only when its origin serves a tombstone (HTTP 410 or an ActivityPub
// Tombstone body — how Lemmy serves deleted content), fetched with the
// redirect authority pinned to the object's own origin so an open redirect
// cannot hand that decision to someone else. A plain 404 is NOT treated as
// deleted: ap.Client also folds 401/403 into it, so 404 means "the origin
// will not show it to us", not "it is gone". A transient outage (5xx, a
// network failure) is not evidence either — it reports OutcomeError and the
// mapping is left alone. FOLLOWUPS.md:39 records the same 410-not-404 rule
// at the bridge's other trust-the-origin site, the inbox's actor
// self-delete confirmation.

package ingest

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
)

// SweepOutcome is the verdict for one swept ap_id: exactly one per id, and
// only OutcomeDeleted mutates anything.
type SweepOutcome string

// Sweep outcomes.
//
// OutcomeActorSkipped covers actor profiles: the Delete(Actor) scrub is
// terminal and deliberate, never swept. OutcomeUnavailable covers the
// ambiguous 404 — ap.Client maps 401/403 there too (secure-mode instances
// hide objects from unauthorized fetchers), so it means "not visible to us",
// which is not grounds for deleting a bridged record.
const (
	OutcomeDeleted        SweepOutcome = "deleted"         // origin served a tombstone; delete applied
	OutcomeAlreadyDeleted SweepOutcome = "already-deleted" // mapping was already soft-deleted
	OutcomeUnknown        SweepOutcome = "unknown"         // no mapping for this ap_id
	OutcomeStillLive      SweepOutcome = "still-live"      // origin still serves the object
	OutcomeUnavailable    SweepOutcome = "unavailable"     // origin 404/401/403s — ambiguous, not applied
	OutcomeActorSkipped   SweepOutcome = "actor-skipped"   // target is an actor profile
	OutcomeError          SweepOutcome = "error"           // lookup, fetch, or delete failed
)

const (
	// maxSweepBatch caps the ap_ids one request may sweep. Every id costs a
	// serial signed fetch of a remote origin, so a large batch holds one
	// request open for minutes; chunking keeps each pass short enough to
	// complete, observe, and re-run.
	maxSweepBatch = 200
	// sweepMutationTimeout bounds the detached tombstone+delete pair for one
	// id, so a stuck DB cannot pin a goroutine after the request is gone.
	sweepMutationTimeout = 30 * time.Second
)

// mutated reports whether the outcome changed durable state; only the
// applied delete does.
func (o SweepOutcome) mutated() bool { return o == OutcomeDeleted }

// failed reports whether the id could not be verified at all (as opposed to
// being verified and deliberately left alone).
func (o SweepOutcome) failed() bool { return o == OutcomeError }

// DeleteSweeper is the slice of the ingest Handler the admin sweep endpoint
// drives — the seam that keeps the admin API from depending on the whole
// dispatcher, like RepoReemitter for re-emission.
type DeleteSweeper interface {
	SweepDeleted(ctx context.Context, apID string) DeleteSweepResult
}

// DeleteSweepResult reports one swept ap_id. (Unrelated to reconcile.go's
// SweepResult, which reports one follow-list reconciliation pass.)
type DeleteSweepResult struct {
	APID    string       `json:"ap_id"`
	Outcome SweepOutcome `json:"outcome"`
	Error   string       `json:"error,omitempty"`
}

// SweepDeleted looks one ap_id's mapping up and, only when that mapping is
// live and is not an actor profile, re-verifies the object against its
// origin and applies the delete iff the origin serves a tombstone. Unknown,
// already-deleted, and actor ids are reported without any fetch. Idempotent;
// every non-tombstone answer is a no-op with a diagnostic outcome.
func (h *Handler) SweepDeleted(ctx context.Context, apID string) DeleteSweepResult {
	res := DeleteSweepResult{APID: apID}
	fail := func(err error) DeleteSweepResult {
		// Logged here, not just returned: an operator scripting a batch reads
		// the response, but the failure must also be greppable in the bridge's
		// own log next to the deletes it did apply.
		h.logger.Error("sweep: ap_id failed", "ap_id", apID, "error", err)
		res.Outcome = OutcomeError
		res.Error = err.Error()
		return res
	}

	mapping, err := h.objects.GetByAPID(ctx, apID)
	if errors.IsNotFound(err) {
		res.Outcome = OutcomeUnknown
		return res
	}
	if err != nil {
		return fail(fmt.Errorf("look up mapping: %w", err))
	}
	if mapping.Collection == materialize.CollectionActorProfile ||
		mapping.Collection == materialize.CollectionCommunityProfile {
		res.Outcome = OutcomeActorSkipped
		return res
	}
	if mapping.IsDeleted() {
		res.Outcome = OutcomeAlreadyDeleted
		return res
	}

	// Pinned to the object's own authority: this fetch's answer is what
	// authorizes the delete, so a redirect off the origin (an open-redirect
	// bug, a compromised origin) must fail it rather than let an attacker
	// host serve the 410.
	_, err = h.fetcher.FetchObjectSameAuthority(ctx, apID)
	switch {
	case err == nil:
		res.Outcome = OutcomeStillLive
	case errors.IsTombstoned(err):
		// Detached from the request: a client disconnect between the two
		// mutations would otherwise leave the marker recorded and the record
		// still live — a half-applied delete nothing retries. (The queue
		// detaches its bookkeeping for the same reason; see queue.go.) Marker
		// first, as in handleDelete, so a crash between them cannot let a
		// re-delivered Create resurrect the object.
		mutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sweepMutationTimeout)
		defer cancel()
		// Global scope (""): the 410 came from the object's OWN origin, so
		// this marker carries origin authority, not any community's.
		if err := h.tombstones.Record(mutCtx, apID, ""); err != nil {
			return fail(fmt.Errorf("record tombstone: %w", err))
		}
		// Content-only entry: the actor cases were already decided above
		// (OutcomeActorSkipped), so the materializer must not re-derive the
		// branch from a fresh bridged_actors read and turn a swept post into an
		// actor scrub.
		if err := h.mat.HandleDeleteRecord(mutCtx, apID); err != nil {
			return fail(fmt.Errorf("apply delete: %w", err))
		}
		h.logger.Info("sweep: applied origin-verified delete", "ap_id", apID)
		res.Outcome = OutcomeDeleted
	case errors.IsNotFound(err):
		res.Outcome = OutcomeUnavailable
	default:
		return fail(fmt.Errorf("fetch origin: %w", err))
	}
	return res
}

// handleSweepDeleted serves POST /admin/objects/sweep-deleted:
// {"ap_ids":["https://...", ...]}, at most maxSweepBatch per request. Each
// id is re-verified against its origin and deleted only if the origin serves
// a tombstone; the response reports one outcome per id plus how many ids
// were requested and whether the pass was truncated (the client hung up
// mid-batch) — a short result list otherwise looks like a complete pass.
// Always 200 once the batch starts, including partial success: per-id
// outcomes, not the status, carry the verdict. Safe to re-run: applied
// deletes report already-deleted on the next pass.
func (a *Admin) handleSweepDeleted(w http.ResponseWriter, r *http.Request) {
	if a.sweeper == nil {
		http.Error(w, "delete sweep not configured", http.StatusNotImplemented)
		return
	}
	var req struct {
		APIDs []string `json:"ap_ids"`
	}
	if err := decodeJSONBody(r, &req); err != nil || len(req.APIDs) == 0 {
		http.Error(w, `body must be {"ap_ids":["https://..."]}`, http.StatusBadRequest)
		return
	}
	if len(req.APIDs) > maxSweepBatch {
		http.Error(w, fmt.Sprintf("at most %d ap_ids per request; chunk the list", maxSweepBatch),
			http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	results := make([]DeleteSweepResult, 0, len(req.APIDs))
	deleted, failed := 0, 0
	for _, apID := range req.APIDs {
		if ctx.Err() != nil {
			break
		}
		res := a.sweeper.SweepDeleted(ctx, apID)
		switch {
		case res.Outcome.mutated():
			deleted++
		case res.Outcome.failed():
			failed++
		}
		results = append(results, res)
	}

	truncated := len(results) < len(req.APIDs)
	summary := []any{"requested", len(req.APIDs), "swept", len(results),
		"deleted", deleted, "failed", failed, "truncated", truncated}
	if failed > 0 || truncated {
		a.logger.Warn("delete sweep finished incomplete", summary...)
	} else {
		a.logger.Info("delete sweep complete", summary...)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"requested": len(req.APIDs),
		"swept":     len(results),
		"deleted":   deleted,
		"failed":    failed,
		"truncated": truncated,
		"result":    results,
	})
}
