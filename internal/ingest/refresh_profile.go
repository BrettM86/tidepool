package ingest

import (
	"context"
	"net/http"
	"runtime/debug"
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

// communityProfileWalk is the all-communities walk's log vocabulary; its
// plural also keys the 202 response's count.
var communityProfileWalk = profileWalk{
	logName: "refresh-profile walk",
	item:    "community",
	items:   "communities",
}

// startRefreshProfileWalk lists every bridged community and hands them to
// startProfileWalk, gated by walkMu.
func (a *Admin) startRefreshProfileWalk(ctx context.Context, w http.ResponseWriter) {
	a.startProfileWalk(ctx, w, &a.walkMu, communityProfileWalk, a.listWalkCommunities,
		func(ctx context.Context, groupIRI string) error {
			if _, err := a.refreshCommunityProfile(ctx, groupIRI); err != nil {
				return err
			}
			return nil
		})
}

// listWalkCommunities returns the Group id of every bridged community, in
// every follow state, accepted first: a community we unfollowed still has a
// repo the appview serves, and its profile deserves the new fields as much
// as an active one. Dead instances surface as per-row failures in the log,
// not as a reason to skip the state. The three queries are not one snapshot
// — a community whose follow state flips between them can be listed twice —
// so dedupe by Group id.
func (a *Admin) listWalkCommunities(ctx context.Context) ([]string, error) {
	var groupIRIs []string
	seen := map[string]bool{}
	for _, state := range []store.FollowState{store.FollowStateAccepted, store.FollowStatePending, store.FollowStateNone} {
		rows, err := a.communities.ListByFollowState(ctx, state)
		if err != nil {
			a.logger.Error("refresh-profile: list communities", "state", state, "error", err)
			return nil, err
		}
		for _, c := range rows {
			if !seen[c.APGroupID] {
				seen[c.APGroupID] = true
				groupIRIs = append(groupIRIs, c.APGroupID)
			}
		}
	}
	return groupIRIs, nil
}

// profileWalk names one kind of background refresh-profile walk for its
// log lines and its 202 response.
type profileWalk struct {
	logName string // log message prefix, "refresh-profile walk"
	item    string // log key for one entry, "community"
	items   string // log and response key for the count, "communities"
}

// startProfileWalk lists the walk's ids up front (so the response can carry
// the count) and refreshes them in the background, one walk per gate at a
// time (409 while one runs). The list comes from the request context; the
// walk itself runs under the Admin's base context, not the request's — an
// operator closing curl must not abort a walk that was accepted, but
// process shutdown must (see Wait). Detaching from the request also matters
// for a subtler reason: chi returns the request's route context to a pool
// when the handler returns, so nothing that outlives the handler may hold
// r.Context(). list logs its own failure; refresh logs each outcome.
func (a *Admin) startProfileWalk(ctx context.Context, w http.ResponseWriter, gate *sync.Mutex, walk profileWalk,
	list func(context.Context) ([]string, error), refresh func(context.Context, string) error) {
	if !gate.TryLock() {
		http.Error(w, "a "+walk.logName+" is already running", http.StatusConflict)
		return
	}
	ids, err := list(ctx)
	if err != nil {
		gate.Unlock()
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Counted before the answer so a Wait issued after the 202 always
	// covers this walk.
	a.walks.Add(1)
	// Answer before the first fetch so the 202 is never queued behind a
	// slow instance.
	writeJSON(w, http.StatusAccepted, map[string]any{walk.items: len(ids)})
	go func() {
		defer a.walks.Done()
		defer gate.Unlock()
		a.runProfileWalk(a.baseCtx, walk, ids, refresh)
	}()
}

// Wait blocks until every background refresh-profile walk has finished.
// Shutdown cancels the base context first, so a walk stops at its next
// entry; Wait then lets it finish the in-flight refresh and log its
// summary instead of being killed mid-commit.
func (a *Admin) Wait() { a.walks.Wait() }

// runProfileWalk refreshes each id in turn, pausing between them, and logs a
// summary at the end. Skips (materialize.IsSkip: opt-outs, upstream
// tombstones or 404s, documents that cannot be bridged) are counted
// separately from failures (unreachable instance, store error), so a fleet
// with a few opted-out or gone actors does not report a baseline failure
// count the operator learns to ignore. The walk exists to converge the
// fleet, and one unreachable instance must not leave the rest stale. A panic in one
// refresh (the walk feeds attacker-controlled documents from every instance
// the bridge ever touched into the materializer, unattended) is logged and
// ends the walk without taking the process down: chi's Recoverer covers
// handler goroutines, not this one.
func (a *Admin) runProfileWalk(ctx context.Context, walk profileWalk, ids []string, refresh func(context.Context, string) error) {
	a.logger.Info(walk.logName+" started", walk.items, len(ids))
	var done, failed, skipped int
	defer func() {
		if r := recover(); r != nil {
			a.logger.Error(walk.logName+" panicked",
				"done", done, "total", len(ids), "panic", r, "stack", string(debug.Stack()))
		}
		a.logger.Info(walk.logName+" finished",
			walk.items, len(ids), "done", done, "failed", failed, "skipped", skipped,
			"aborted", ctx.Err() != nil)
	}()
	for i, id := range ids {
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
		if err := refresh(ctx, id); err != nil {
			outcome := "failed"
			if materialize.IsSkip(err) {
				skipped++
				outcome = "skipped"
			} else {
				failed++
			}
			a.logger.Warn(walk.logName+": "+walk.item+" not refreshed",
				walk.item, id, "outcome", outcome, "elapsed", time.Since(start))
		}
		done = i + 1
		if done%25 == 0 {
			a.logger.Info(walk.logName+" progress",
				"done", done, "total", len(ids), "failed", failed, "skipped", skipped)
		}
	}
}
