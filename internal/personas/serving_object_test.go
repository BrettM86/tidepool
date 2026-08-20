package personas

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/identity"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Task 15 cycle D: the user-origin object/activity serving surface. Lemmy
// re-fetches a native record by id, so coves.social must serve:
//
//   - GET /ap/object/{did}/{collection}/{rkey} → the AP object rendered from the
//     outbound_objects SNAPSHOT (survives PDS outages); a tombstoned row → 410;
//   - GET /ap/activity/{hash} → the CANONICAL immutable payload from
//     outbound_activities, byte-for-byte (an edited object must NOT change an
//     already-served activity — activities are immutable, objects are current);
//   - unknown object/activity → 404.

const (
	serveObjectDID        = "did:plc:7iza6de2dwap2sbkpav7c6c6"
	serveObjectCollection = "social.coves.community.comment"
	serveObjectRKey       = "3lzserveobj001"
	serveObjectATURI      = "at://" + serveObjectDID + "/" + serveObjectCollection + "/" + serveObjectRKey
	serveCommunityDID     = "did:plc:44ybard66vv44zksje25o7dz"
	serveCommunityAPID    = "https://lemmy.world/c/technology"
	serveActivityHash     = "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0"
)

func servingObjectTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "ap_actors", "outbound_objects", "outbound_activities")
	return database
}

func newServingService(t *testing.T, conn *sql.DB) *Service {
	t.Helper()
	custodian, err := identity.NewCustodian([]byte("0123456789abcdef0123456789abcdef"))
	require.NoError(t, err)
	svc, err := New(Options{DB: conn, Custodian: custodian, UserOrigin: userOrigin})
	require.NoError(t, err)
	return svc
}

func serveObjectAPID() string {
	return userOrigin + "/ap/object/" + serveObjectDID + "/" + serveObjectCollection + "/" + serveObjectRKey
}

func seedServedObject(t *testing.T, conn *sql.DB) {
	t.Helper()
	_, err := store.NewOutboundObjects(conn).Upsert(context.Background(), store.OutboundObject{
		ATURI:         serveObjectATURI,
		APObjectID:    serveObjectAPID(),
		CommunityDID:  serveCommunityDID,
		CommunityAPID: serveCommunityAPID,
		TranslatedSnapshot: []byte(`{
			"atUri": "` + serveObjectATURI + `",
			"collection": "` + serveObjectCollection + `",
			"record": {"$type":"social.coves.community.comment","content":"served from snapshot",
				"reply":{"root":{"uri":"at://x/y/z"},"parent":{"uri":"at://x/y/z"}}},
			"parentApId": "https://lemmy.world/post/1",
			"communityApId": "` + serveCommunityAPID + `"
		}`),
	})
	require.NoError(t, err, "seed outbound_objects snapshot")
}

func TestServing_ObjectRendersFromSnapshot(t *testing.T) {
	conn := servingObjectTestDB(t)
	svc := newServingService(t, conn)
	seedServedObject(t, conn)

	path := "/ap/object/" + serveObjectDID + "/" + serveObjectCollection + "/" + serveObjectRKey
	rec := serveOnHost(svc, userHost, http.MethodGet, path,
		http.Header{"Accept": []string{ap.ContentTypeActivityJSON}})

	require.Equal(t, http.StatusOK, rec.Code,
		"GET %s (Host %s) must render the native object from its snapshot; body=%s",
		path, userHost, rec.Body.String())
	assert.Contains(t, rec.Header().Get("Content-Type"), "activity+json",
		"the object is served as application/activity+json")

	var doc map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc),
		"the served object must be JSON, got %s", rec.Body.String())
	assert.Equal(t, serveObjectAPID(), doc["id"],
		"the served object's id is the coves.social object URL")
	assert.NotEmpty(t, doc["type"], "the rendered object carries an AP type (Note/Page)")
}

func TestServing_TombstonedObjectIs410(t *testing.T) {
	conn := servingObjectTestDB(t)
	svc := newServingService(t, conn)
	seedServedObject(t, conn)

	_, err := store.NewOutboundObjects(conn).Tombstone(context.Background(), serveObjectATURI)
	require.NoError(t, err, "tombstone the served object")

	path := "/ap/object/" + serveObjectDID + "/" + serveObjectCollection + "/" + serveObjectRKey
	rec := serveOnHost(svc, userHost, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusGone, rec.Code,
		"a tombstoned outbound_objects row must serve 410 Gone, not the stale body")
}

func TestServing_UnknownObjectIs404(t *testing.T) {
	conn := servingObjectTestDB(t)
	svc := newServingService(t, conn)

	path := "/ap/object/" + serveObjectDID + "/" + serveObjectCollection + "/nonexistent"
	rec := serveOnHost(svc, userHost, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code,
		"an object we hold no state for must 404, not 200 with an empty body")
}

func TestServing_ActivityServesCanonicalPayloadVerbatim(t *testing.T) {
	conn := servingObjectTestDB(t)
	svc := newServingService(t, conn)

	activityID := userOrigin + "/ap/activity/" + serveActivityHash
	payload := []byte(`{"@context":"https://www.w3.org/ns/activitystreams",` +
		`"id":"` + activityID + `","type":"Create","actor":"` + serveObjectAPID() + `",` +
		`"object":{"type":"Note","content":"original, pre-edit"}}`)
	inserted, err := store.NewOutboundActivities(conn).Insert(context.Background(), store.OutboundActivity{
		ActivityID: activityID,
		ActorDID:   serveObjectDID,
		Kind:       "Create",
		Payload:    payload,
	})
	require.NoError(t, err)
	require.True(t, inserted)

	path := "/ap/activity/" + serveActivityHash
	rec := serveOnHost(svc, userHost, http.MethodGet, path,
		http.Header{"Accept": []string{ap.ContentTypeActivityJSON}})

	require.Equal(t, http.StatusOK, rec.Code,
		"GET %s must serve the canonical activity payload; body=%s", path, rec.Body.String())
	assert.Contains(t, rec.Header().Get("Content-Type"), "activity+json")
	assert.JSONEq(t, string(payload), rec.Body.String(),
		"the activity payload is served verbatim — activities are immutable, so an edited object "+
			"must NOT change an already-served activity")
}

func TestServing_UnknownActivityIs404(t *testing.T) {
	conn := servingObjectTestDB(t)
	svc := newServingService(t, conn)

	rec := serveOnHost(svc, userHost, http.MethodGet, "/ap/activity/deadbeef", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code,
		"an activity id we never minted must 404")
}
