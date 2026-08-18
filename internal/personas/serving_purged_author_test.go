package personas

import (
	"context"
	"database/sql"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Task 17d follow-up: the erasure promise covers the CONTENT surfaces, not just
// the actor document. After an irreversible Delete{Person, removeData:true} the
// actor doc and outbox answer 410 — but /ap/object/{did}/... and
// /ap/activity/{hash} are stable, previously-published, discoverable ids, and a
// peer (or anyone) that re-fetches them must not receive the purged user's full
// content forever. Both handlers therefore gate on the AUTHOR actor's tombstone,
// not only on the object's own: a tombstoned author's objects and activities are
// 410 Gone. The rows stay in the database — the 17e sweep reads tables, not
// HTTP — so serving is withdrawn without destroying the reconciliation record.

// seedServedActivity inserts one canonical activity for the serving DID and
// returns its /ap/activity/{hash} path.
func seedServedActivity(t *testing.T, conn *sql.DB) string {
	t.Helper()
	activityID := userOrigin + activityPathPrefix + serveActivityHash
	payload := []byte(`{"@context":"https://www.w3.org/ns/activitystreams",` +
		`"id":"` + activityID + `","type":"Create","actor":"` + serveObjectAPID() + `",` +
		`"object":{"type":"Note","content":"content the purge must withdraw"}}`)
	inserted, err := store.NewOutboundActivities(conn).Insert(context.Background(), store.OutboundActivity{
		ActivityID: activityID,
		ActorDID:   serveObjectDID,
		Kind:       "Create",
		Payload:    payload,
	})
	require.NoError(t, err)
	require.True(t, inserted)
	return activityPathPrefix + serveActivityHash
}

// seedServingAuthor mints the ap_actors row for the DID the served object and
// activity belong to — the row the destructive tier tombstones.
func seedServingAuthor(t *testing.T, svc *Service) {
	t.Helper()
	_, err := svc.CreateActorForDID(context.Background(), serveObjectDID, "servedauthor.example.com")
	require.NoError(t, err, "mint the author actor for the serving DID")
}

func TestServing_PurgedAuthorsObjectIs410(t *testing.T) {
	conn := servingObjectTestDB(t)
	svc := newServingService(t, conn)
	seedServingAuthor(t, svc)
	seedServedObject(t, conn)

	require.NoError(t, store.NewAPActors(conn).Tombstone(context.Background(), serveObjectDID),
		"withdraw the author identity, as the destructive tier does")

	path := "/ap/object/" + serveObjectDID + "/" + serveObjectCollection + "/" + serveObjectRKey
	rec := serveOnHost(svc, userHost, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusGone, rec.Code,
		"a purged author's object must answer 410: the object URL is a stable, "+
			"previously-published id, and a 200 here serves the erased user's full "+
			"content to anyone, forever — the bridge itself breaking the erasure promise; body=%s",
		rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "served from snapshot",
		"and the withdrawn content itself must not leak in the response body")
}

func TestServing_PurgedAuthorsActivityIs410(t *testing.T) {
	conn := servingObjectTestDB(t)
	svc := newServingService(t, conn)
	seedServingAuthor(t, svc)
	path := seedServedActivity(t, conn)

	require.NoError(t, store.NewAPActors(conn).Tombstone(context.Background(), serveObjectDID),
		"withdraw the author identity, as the destructive tier does")

	rec := serveOnHost(svc, userHost, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusGone, rec.Code,
		"a purged author's activity must answer 410: peers re-verify and re-fetch "+
			"activities by id (Lemmy un-deletes a person on refetch), so a 200 here can "+
			"resurrect exactly what the withdrawal just asked them to drop; body=%s",
		rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "content the purge must withdraw",
		"and the withdrawn content itself must not leak in the response body")
}

// TestServing_LiveAuthorsContentStillServes is the positive control: gating on
// the author must not withdraw anything for an author who was never tombstoned.
func TestServing_LiveAuthorsContentStillServes(t *testing.T) {
	conn := servingObjectTestDB(t)
	svc := newServingService(t, conn)
	seedServingAuthor(t, svc)
	seedServedObject(t, conn)
	activityPath := seedServedActivity(t, conn)

	objectPath := "/ap/object/" + serveObjectDID + "/" + serveObjectCollection + "/" + serveObjectRKey
	rec := serveOnHost(svc, userHost, http.MethodGet, objectPath, nil)
	assert.Equal(t, http.StatusOK, rec.Code,
		"a live author's object keeps serving — the gate is the author's tombstone, "+
			"nothing else; body=%s", rec.Body.String())

	rec = serveOnHost(svc, userHost, http.MethodGet, activityPath, nil)
	assert.Equal(t, http.StatusOK, rec.Code,
		"a live author's activity keeps serving; body=%s", rec.Body.String())
}
