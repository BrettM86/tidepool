package handlemigration

import (
	"context"
	stderrors "errors"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// failUsernameSaveFor fails SetPreferredUsernameByDID for one DID and
// delegates every other call to the real store.
type failUsernameSaveFor struct {
	store.BridgedActors
	did   string
	calls []string
}

func (a *failUsernameSaveFor) SetPreferredUsernameByDID(ctx context.Context, did, preferredUsername string) error {
	a.calls = append(a.calls, did)
	if did == a.did {
		return stderrors.New("injected preferred username save failure")
	}
	return a.BridgedActors.SetPreferredUsernameByDID(ctx, did, preferredUsername)
}

func TestRunner_UsernameSaveFailureKeepsOldHandleAndContinues(t *testing.T) {
	const (
		firstDID  = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		secondDID = "did:plc:44ybard66vv44zksje25o7dz"
		firstOld  = "alice.my-lemmy-org.tidepool.test"
		secondOld = "bob.my-lemmy-org.tidepool.test"
		secondNew = "bob.my-lemmy.org.ap.tidepool.test"
		actorPath = "/u/alice"
	)
	h := newRenameHarness(t)
	first := h.insertOldActor(t, firstDID, "https://my-lemmy.org"+actorPath, firstOld, "")
	h.insertOldActor(t, secondDID, "https://my-lemmy.org/u/bob", secondOld, "bob")
	h.actorDocs.handlers[actorPath] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/activity+json")
		_, _ = w.Write([]byte(`{"type":"Person","id":"https://my-lemmy.org/u/alice","preferredUsername":"alice"}`))
	}
	failing := &failUsernameSaveFor{BridgedActors: h.actors, did: firstDID}

	require.NoError(t, h.runnerWithActors(t, failing).Run(t.Context(), []string{firstDID, secondDID}))

	require.Equal(t, []string{firstDID}, failing.calls, "only the actor without a stored username saves one")
	actorRequests, actorHookErrors := h.actorDocs.snapshot()
	require.Empty(t, actorHookErrors)
	require.Equal(t, []actorRequest{{Method: http.MethodGet, Path: actorPath}}, actorRequests)

	row := h.row(t, firstDID)
	require.Equal(t, firstOld, row.Handle)
	require.Empty(t, row.PendingHandle)
	require.Nil(t, row.HandleMigratedAt)
	var preferred string
	require.NoError(t, h.db.QueryRowContext(t.Context(),
		`SELECT COALESCE(preferred_username, '') FROM bridged_actors WHERE did = $1`, firstDID).Scan(&preferred))
	require.Empty(t, preferred)

	rows, err := h.db.QueryContext(t.Context(), `SELECT handle FROM handle_claims WHERE actor_id = $1`, first.ID)
	require.NoError(t, err)
	var claims []string
	for rows.Next() {
		var handle string
		require.NoError(t, rows.Scan(&handle))
		claims = append(claims, handle)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, []string{firstOld}, claims, "the old handle must be the only claim")

	plcRequests, plcHookErrors := h.plc.snapshot()
	require.Empty(t, plcHookErrors)
	for _, request := range plcRequests {
		require.False(t, strings.Contains(request.Path, firstDID), "unexpected PLC request for failed actor: %s", request.Path)
	}

	// The runner only logs a failed save; it records no handle migration
	// error for the actor.
	code, message := migrationFailure(t, h, firstDID)
	require.False(t, code.Valid)
	require.False(t, message.Valid)

	assertFinalizedRename(t, h, secondDID, secondOld, secondNew)
}
