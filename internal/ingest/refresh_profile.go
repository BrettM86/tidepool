package ingest

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// POST /admin/communities/refresh-profile — force a community.profile
// re-materialization, bypassing the profile TTL.
//
// Why this exists: buildCommunityProfile grows fields over time (the
// self-asserted `origin` host is the first), and nothing in the steady state
// ever pushes a NEW field into a community that already has a profile
// record. The profile refreshes only when something touches the community
// inside ensureCommunity AND the 24h profileFresh TTL has lapsed — so an
// active community picks a new field up within a day, but a dormant one
// (no announces, nobody posting) keeps its old record forever. The other
// two maintenance paths do not help: the follow-list reconciler skips every
// community that is already subscribed, and /admin/reemit re-puts the
// stored record bytes verbatim — it is for firehose replay, not for
// changing what the record says. This endpoint is the missing backfill:
// it calls RefreshCommunity, the same force path Update{Group} takes.
//
// Two shapes:
//
//	{"community":"!name@instance"}  one community, SYNCHRONOUS, 200 with the
//	                                community's bridge state (404 if it was
//	                                never bridged, 502 if its instance is
//	                                unreachable, 422 if it refuses bridging).
//	{"all":true}                    every bridged community, ASYNCHRONOUS,
//	                                202 with the count. The walk re-fetches
//	                                one Group actor per community from its
//	                                home instance and commits one repo write
//	                                each, so it is deliberately serialized
//	                                (one at a time, a pause between them —
//	                                refreshProfileWalkPause) and only one
//	                                walk may run at a time (409 while one
//	                                is in flight). Progress and per-community
//	                                failures go to the log; a failure never
//	                                stops the walk.
//
// `all` is an explicit opt-in, never the default of an empty body: a fleet
// walk on a malformed request is exactly the surprise the outbound redrive
// endpoint refuses for the same reason.

// refreshProfileWalkPause is the gap between consecutive community
// refreshes in the all-communities walk. Each refresh is a signed GET to a
// remote instance; a few hundred communities spread over a few minutes is
// invisible to their operators, the same few hundred in one burst is a
// scrape.
const refreshProfileWalkPause = 500 * time.Millisecond

// refreshProfileRequest is the body of POST /admin/communities/refresh-profile.
type refreshProfileRequest struct {
	Community string `json:"community"`
	All       bool   `json:"all"`
}

// refreshProfileWalk serializes the all-communities walk: TryLock refuses a
// second walk while one is running instead of doubling the fetch rate.
var refreshProfileWalk sync.Mutex

func (a *Admin) handleRefreshProfile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req refreshProfileRequest
	if err := decodeJSONBody(r, &req); err != nil {
		http.Error(w, `body must be {"community":"!name@instance"} or {"all":true}`, http.StatusBadRequest)
		return
	}
	req.Community = strings.TrimSpace(req.Community)
	switch {
	case req.All && req.Community != "":
		http.Error(w, `"all" and "community" are mutually exclusive`, http.StatusBadRequest)
		return
	case req.All:
		a.startRefreshProfileWalk(ctx, w)
		return
	case req.Community == "":
		http.Error(w, `body must be {"community":"!name@instance"} or {"all":true}`, http.StatusBadRequest)
		return
	}

	groupIRI, err := a.resolveCommunity(ctx, req.Community)
	if err != nil {
		a.writeResolveError(w, req.Community, err)
		return
	}
	// Only communities we already bridged: RefreshCommunity on an unknown
	// Group would MINT a new one, and minting is the subscribe endpoint's
	// job (it also sends the Follow; a refresh must never create a
	// community that nothing follows).
	if _, err := a.communities.GetByAPGroupID(ctx, groupIRI); err != nil {
		if errors.IsNotFound(err) {
			http.Error(w, "community is not bridged", http.StatusNotFound)
			return
		}
		a.logger.Error("refresh-profile: look up community", "community", groupIRI, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	community, refreshErr := a.refreshCommunityProfile(ctx, groupIRI)
	if refreshErr != nil {
		http.Error(w, refreshErr.public, refreshErr.status)
		return
	}
	writeJSON(w, http.StatusOK, communityJSON(community))
}

// refreshCommunityProfile is the single-community core shared by the
// synchronous handler and the walk. The error mapping mirrors subscribe's:
// a consent refusal (skip) is the community's decision, not our failure.
func (a *Admin) refreshCommunityProfile(ctx context.Context, groupIRI string) (*store.Community, *adminError) {
	community, err := a.mat.RefreshCommunity(ctx, &ap.Object{ID: groupIRI})
	if err != nil {
		if materialize.IsSkip(err) {
			a.logger.Warn("refresh-profile refused", "community", groupIRI, "reason", err.Error())
			return nil, &adminError{status: http.StatusUnprocessableEntity,
				public: "community cannot be bridged: " + err.Error(), err: err}
		}
		a.logger.Error("refresh-profile: materialize community", "community", groupIRI, "error", err)
		return nil, &adminError{status: http.StatusBadGateway,
			public: "failed to refresh community profile", err: err}
	}
	a.logger.Info("community profile refreshed", "community", groupIRI, "did", community.DID)
	return community, nil
}

// startRefreshProfileWalk lists every bridged community up front (so the
// response can carry the count) and refreshes them in the background. The
// list comes from the request context; the walk itself runs under a
// context detached from the request — a 202 has already been written by
// the time the first refresh happens, and an operator closing curl must
// not abort a walk that was accepted.
func (a *Admin) startRefreshProfileWalk(ctx context.Context, w http.ResponseWriter) {
	if !refreshProfileWalk.TryLock() {
		http.Error(w, "a refresh-profile walk is already running", http.StatusConflict)
		return
	}
	// Every follow state, accepted first: a community we unfollowed still
	// has a repo the appview serves, and its profile deserves the new
	// fields as much as an active one. Dead instances surface as per-row
	// failures in the log, not as a reason to skip the state.
	var communities []*store.Community
	for _, state := range []store.FollowState{store.FollowStateAccepted, store.FollowStatePending, store.FollowStateNone} {
		rows, err := a.communities.ListByFollowState(ctx, state)
		if err != nil {
			refreshProfileWalk.Unlock()
			a.logger.Error("refresh-profile: list communities", "state", state, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		communities = append(communities, rows...)
	}

	go func() {
		defer refreshProfileWalk.Unlock()
		a.runRefreshProfileWalk(context.WithoutCancel(ctx), communities)
	}()
	writeJSON(w, http.StatusAccepted, map[string]any{"communities": len(communities)})
}

// runRefreshProfileWalk refreshes each community in turn, pausing between
// them, and logs a summary at the end. Failures are counted and logged per
// community (refreshCommunityProfile already logged the detail) — the walk
// exists to converge the fleet, and one unreachable instance must not
// leave the rest stale.
func (a *Admin) runRefreshProfileWalk(ctx context.Context, communities []*store.Community) {
	a.logger.Info("refresh-profile walk started", "communities", len(communities))
	failed := 0
	for i, c := range communities {
		if i > 0 {
			time.Sleep(refreshProfileWalkPause)
		}
		if _, err := a.refreshCommunityProfile(ctx, c.APGroupID); err != nil {
			failed++
		}
		if (i+1)%25 == 0 {
			a.logger.Info("refresh-profile walk progress",
				"done", i+1, "total", len(communities), "failed", failed)
		}
	}
	a.logger.Info("refresh-profile walk complete",
		"communities", len(communities), "failed", failed)
}
