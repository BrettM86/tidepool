package ingest

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// actorPurgeWriteTimeout bounds POST /admin/actors/purge's response. A real
// run enqueues a Delete per inbox and an Undo per standing vote before it
// answers; the server's default 30s WriteTimeout would drop the report of a
// large purge after it was already committed.
const actorPurgeWriteTimeout = 10 * time.Minute

// actorPurgeSummary is POST /admin/actors/purge's answer: the actor's state
// (after the purge on a real run) and what the purge reaches.
type actorPurgeSummary struct {
	DID            string `json:"did"`
	ActorID        string `json:"actorId"`
	Kind           string `json:"kind"`
	Enabled        bool   `json:"enabled"`
	DeliveryPaused bool   `json:"deliveryPaused"`
	Tombstoned     bool   `json:"tombstoned"`
	Inboxes        int    `json:"inboxes"`
	StandingVotes  int    `json:"standingVotes"`
	DryRun         bool   `json:"dryRun"`
}

// handleActorPurge is the operator's destructive tier for a native person
// actor: it sends Delete{Person, removeData:true} to every inbox the actor's
// content reached, undoes its standing votes, and tombstones the actor
// (TERMINAL). A spammer never writes the deleteRemote record or deletes their
// account, and Tidepool, as the actor's home instance, is the only server
// that can send that Delete. dryRun is required so a forgotten flag never
// purges; a community (kind group) is never purgeable here.
//
// A real run first pauses and disables the actor, before it reads the purge
// targets: content admitted while the purge prepares must not be delivered
// after the Delete's targets were snapshotted, and the outbound worker cancels
// a disabled or paused actor's non-retraction deliveries (retractions still go
// out). The counts it answers are that pre-purge snapshot. It extends this
// response's write deadline to actorPurgeWriteTimeout.
func (a *Admin) handleActorPurge(w http.ResponseWriter, r *http.Request) {
	if a.purger == nil || a.apActors == nil || a.deliveries == nil || a.votes == nil {
		http.Error(w, "actor purge is not configured", http.StatusNotImplemented)
		return
	}
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(actorPurgeWriteTimeout)); err != nil {
		a.logger.WarnContext(r.Context(), "extend actor purge write deadline", "error", err)
	}
	var req struct {
		DID    string `json:"did"`
		DryRun *bool  `json:"dryRun"`
	}
	if err := decodeJSONBody(r, &req); err != nil || req.DID == "" || req.DryRun == nil {
		http.Error(w, `body must be {"did":"did:...","dryRun":true} or {"did":"did:...","dryRun":false}`, http.StatusBadRequest)
		return
	}
	if _, err := syntax.ParseDID(req.DID); err != nil {
		http.Error(w, "did is not a valid DID", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	actor, err := a.apActors.GetByDID(ctx, req.DID)
	if errors.IsNotFound(err) {
		http.Error(w, "no native actor for that did", http.StatusNotFound)
		return
	}
	if err != nil {
		a.logger.Error("actor purge: look up actor", "did", req.DID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if actor.Kind != store.ActorTypePerson {
		http.Error(w, "only person actors can be purged", http.StatusUnprocessableEntity)
		return
	}
	dryRun := *req.DryRun
	if !dryRun {
		if err := a.apActors.SetPaused(ctx, req.DID, true); err != nil {
			a.logger.Error("actor purge: pause actor", "did", req.DID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if err := a.apActors.SetEnabled(ctx, req.DID, false); err != nil {
			a.logger.Error("actor purge: disable actor", "did", req.DID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	}
	inboxes, err := a.deliveries.DistinctInboxesForActor(ctx, req.DID)
	if err != nil {
		a.logger.Error("actor purge: list inboxes", "did", req.DID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	votes, err := a.votes.ListStandingForActor(ctx, req.DID)
	if err != nil {
		a.logger.Error("actor purge: list standing votes", "did", req.DID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !dryRun {
		if err := a.purger.DeleteRemoteContent(ctx, req.DID); err != nil {
			a.logger.Error("actor purge failed", "did", req.DID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		actor, err = a.apActors.GetByDID(ctx, req.DID)
		if err != nil {
			a.logger.Error("actor purge: re-read actor", "did", req.DID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	}
	a.logger.Info("actor purge",
		"did", req.DID, "dryRun", dryRun, "inboxes", len(inboxes), "standingVotes", len(votes))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(actorPurgeSummary{
		DID:            actor.DID,
		ActorID:        actor.ActorID,
		Kind:           string(actor.Kind),
		Enabled:        actor.Enabled,
		DeliveryPaused: actor.DeliveryPaused,
		Tombstoned:     actor.IsTombstoned(),
		Inboxes:        len(inboxes),
		StandingVotes:  len(votes),
		DryRun:         dryRun,
	}); err != nil {
		a.logger.Warn("write actor purge summary", "did", req.DID, "error", err)
	}
}
