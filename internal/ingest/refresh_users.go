package ingest

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"

	"tidepool/internal/ap"
	"tidepool/internal/materialize"
)

// POST /admin/users/refresh-profile {"all":true} — force an actor.profile
// re-materialization for every bridged person, bypassing the profile TTL.
//
// The user counterpart of the all-communities walk in refresh_profile.go,
// for the same reason: buildActorProfile grows fields (origin,
// originUsername, originUrl) and a dormant user's profile never refreshes on
// its own. Each user goes through RefreshActor, the force path
// Update{Person} takes, so consent is re-checked for every user: one still
// carrying #nobridge stays without a profile record (counted as skipped),
// one who removed it is re-bridged, one who added it since is scrubbed.
// Tombstoned users are not listed, so their documents are never fetched.
// The walk is asynchronous (202 with the count), serialized with the same
// pause as the community walk, gated separately from it (409 only while
// another user walk runs), and stops at its next user once the Admin's base
// context is cancelled; Wait drains it.
//
// There is no single-user shape: `{"all":true}` is the only accepted body,
// an explicit opt-in, never the default of an empty body. The body is
// decoded strictly (unknown keys and trailing values are a 400), so an
// operator guessing at a single-user shape such as {"all":true,"user":…}
// is refused instead of walking the whole fleet.

// PersonActorLister lists the bridged persons the user walk refreshes.
// store.BridgedActors satisfies it.
type PersonActorLister interface {
	ListLivePersonAPActorIDs(ctx context.Context) ([]string, error)
}

// userProfileWalk is the user walk's log vocabulary; its plural also keys
// the 202 response's count.
var userProfileWalk = profileWalk{
	logName: "user refresh-profile walk",
	item:    "actor",
	items:   "users",
}

// refreshUsersRequest is the body of POST /admin/users/refresh-profile.
type refreshUsersRequest struct {
	All bool `json:"all"`
}

func (a *Admin) handleRefreshUsers(w http.ResponseWriter, r *http.Request) {
	var req refreshUsersRequest
	if err := decodeStrictJSONBody(r, &req); err != nil || !req.All {
		http.Error(w, `body must be {"all":true}`, http.StatusBadRequest)
		return
	}
	a.startProfileWalk(r.Context(), w, &a.userWalkMu, userProfileWalk, a.listWalkUsers, a.refreshUserProfile)
}

// decodeStrictJSONBody decodes exactly one JSON value into dst, with the
// same size limit as decodeJSONBody, refusing unknown fields and anything
// after the first value.
func decodeStrictJSONBody(r *http.Request, dst any) error {
	defer func() { _, _ = io.Copy(io.Discard, r.Body) }()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<16))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("decode request body: %w", err)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !stderrors.Is(err, io.EOF) {
		if err == nil {
			return stderrors.New("decode request body: trailing data after the JSON value")
		}
		return fmt.Errorf("decode request body: trailing data: %w", err)
	}
	return nil
}

func (a *Admin) listWalkUsers(ctx context.Context) ([]string, error) {
	ids, err := a.actors.ListLivePersonAPActorIDs(ctx)
	if err != nil {
		a.logger.Error("user refresh-profile: list users", "error", err)
		return nil, err
	}
	return ids, nil
}

// refreshUserProfile re-materializes one user's profile and logs the
// outcome; a skip (materialize.IsSkip: an opt-out, an upstream tombstone or
// 404, a document that cannot be bridged) is logged as such, not as our
// failure.
func (a *Admin) refreshUserProfile(ctx context.Context, apActorID string) error {
	actor, err := a.mat.RefreshActor(ctx, &ap.Object{ID: apActorID})
	if err != nil {
		if materialize.IsSkip(err) {
			a.logger.Warn("user refresh-profile skipped", "actor", apActorID, "reason", err.Error())
			return err
		}
		a.logger.Error("user refresh-profile: materialize actor", "actor", apActorID, "error", err)
		return err
	}
	a.logger.Info("user profile refreshed", "actor", apActorID, "did", actor.DID)
	return nil
}
