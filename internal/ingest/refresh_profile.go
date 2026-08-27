package ingest

import (
	"context"
	"net/http"
	"runtime/debug"
	"strings"
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
// inside ensureCommunity AND the profile TTL (PROFILE_REFRESH_TTL, default
// 24h) has lapsed — so an
// active community picks a new field up within a day, but a dormant one
// (no announces, nobody posting) keeps its old record forever. The other
// two maintenance paths do not help: the follow-list reconciler skips every
// community that is already subscribed, and /admin/reemit re-emits the
// stored value unchanged — it is for firehose replay, not for changing
// what the record says. This endpoint is the missing backfill:
// it calls RefreshCommunity, the same force path Update{Group} takes.
//
// Two shapes:
//
//	{"community":"!name@instance"}  one community, SYNCHRONOUS, 200 with the
//	                                community's bridge state (404 if it was
//	                                never bridged or the handle does not
//	                                resolve, 502 if resolution or the refresh
//	                                fetch fails, 422 if it refuses bridging).
//	{"all":true}                    every bridged community, ASYNCHRONOUS,
//	                                202 with the count. The walk re-fetches
//	                                one Group actor per community from its
//	                                home instance and commits a repo write
//	                                when the record changed, so it is
//	                                deliberately serialized (one at a time, a
//	                                pause between them —
//	                                refreshProfileWalkPause) and only one
//	                                walk may run at a time (409 while one
//	                                is in flight). Progress and per-community
//	                                failures go to the log; a failure never
//	                                stops the walk. Only process shutdown
//	                                does: the walk runs under the Admin's
//	                                base context (the run context in
//	                                production), stops at the next community
//	                                once that is cancelled, and Wait drains
//	                                it — the same lifecycle as an async
//	                                backfill.
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
// list comes from the request context; the walk itself runs under the
// Admin's base context, not the request's — an operator closing curl must
// not abort a walk that was accepted, but process shutdown must (see
// Wait). Detaching from the request also matters for a subtler reason: chi
// returns the request's route context to a pool when the handler returns,
// so nothing that outlives the handler may hold r.Context().
func (a *Admin) startRefreshProfileWalk(ctx context.Context, w http.ResponseWriter) {
	if !a.walkMu.TryLock() {
		http.Error(w, "a refresh-profile walk is already running", http.StatusConflict)
		return
	}
	// Every follow state, accepted first: a community we unfollowed still
	// has a repo the appview serves, and its profile deserves the new
	// fields as much as an active one. Dead instances surface as per-row
	// failures in the log, not as a reason to skip the state. The three
	// queries are not one snapshot — a community whose follow state flips
	// between them can be listed twice — so dedupe by Group id.
	var communities []*store.Community
	seen := map[string]bool{}
	for _, state := range []store.FollowState{store.FollowStateAccepted, store.FollowStatePending, store.FollowStateNone} {
		rows, err := a.communities.ListByFollowState(ctx, state)
		if err != nil {
			a.walkMu.Unlock()
			a.logger.Error("refresh-profile: list communities", "state", state, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		for _, c := range rows {
			if !seen[c.APGroupID] {
				seen[c.APGroupID] = true
				communities = append(communities, c)
			}
		}
	}

	// Answer before the first fetch so the 202 is never queued behind a
	// slow instance.
	writeJSON(w, http.StatusAccepted, map[string]any{"communities": len(communities)})
	a.walks.Add(1)
	go func() {
		defer a.walks.Done()
		defer a.walkMu.Unlock()
		a.runRefreshProfileWalk(a.baseCtx, communities)
	}()
}

// Wait blocks until every background refresh-profile walk has finished.
// Shutdown cancels the base context first, so a walk stops at its next
// community; Wait then lets it finish the in-flight refresh and log its
// summary instead of being killed mid-commit.
func (a *Admin) Wait() { a.walks.Wait() }

// runRefreshProfileWalk refreshes each community in turn, pausing between
// them, and logs a summary at the end. Consent refusals (422) are counted
// as skipped — the community's decision, permanent, and expected to recur
// on every walk — separately from failures (unreachable instance, store
// error), so a fleet with a few opted-out communities does not report a
// baseline failure count the operator learns to ignore. Each outcome is
// logged per community by refreshCommunityProfile; the walk exists to
// converge the fleet, and one unreachable instance must not leave the rest
// stale. A panic in one refresh (the walk feeds attacker-controlled
// documents from every instance the bridge ever touched into the
// materializer, unattended) is logged and ends the walk without taking the
// process down: chi's Recoverer covers handler goroutines, not this one.
func (a *Admin) runRefreshProfileWalk(ctx context.Context, communities []*store.Community) {
	a.logger.Info("refresh-profile walk started", "communities", len(communities))
	var done, failed, skipped int
	defer func() {
		if r := recover(); r != nil {
			a.logger.Error("refresh-profile walk panicked",
				"done", done, "total", len(communities), "panic", r, "stack", string(debug.Stack()))
		}
		a.logger.Info("refresh-profile walk finished",
			"communities", len(communities), "done", done, "failed", failed, "skipped", skipped,
			"aborted", ctx.Err() != nil)
	}()
	for i, c := range communities {
		if i > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(refreshProfileWalkPause):
			}
		}
		if ctx.Err() != nil {
			return
		}
		start := time.Now()
		if _, err := a.refreshCommunityProfile(ctx, c.APGroupID); err != nil {
			if err.status == http.StatusUnprocessableEntity {
				skipped++
			} else {
				failed++
			}
			a.logger.Warn("refresh-profile walk: community not refreshed",
				"community", c.APGroupID, "status", err.status, "elapsed", time.Since(start))
		}
		done = i + 1
		if done%25 == 0 {
			a.logger.Info("refresh-profile walk progress",
				"done", done, "total", len(communities), "failed", failed, "skipped", skipped)
		}
	}
}
