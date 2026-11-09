package accept

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/personas"
)

// Task 04: every outbound edit carries `updated`, and the object served at
// GET /ap/object/... carries the same value — Lemmy 0.19.20 drops an
// Update{Page} whose object has no `updated` later than the copy it holds.
// A Create (first federation, or re-federation after a Delete) carries none.
//
// The TIDs are hand-computed literals (clock id 7), decoded independently:
//
//	3mzakfuhe222b = 2026-11-01T10:00:00Z
//	3mzakoskms22b = 2026-11-01T10:05:00Z
//	3mzakseyqc22b = 2026-11-01T10:07:00Z
//	3mzaku67s222b = 2026-11-01T10:08:00Z
const (
	euCreatedAt = "2026-11-01T10:00:00Z"

	euRevCreate  = "3mzakfuhe222b"
	euRevEdit    = "3mzakoskms22b"
	euRevRemove  = "3mzakseyqc22b"
	euRevRestore = "3mzaku67s222b"
	// Not a TID, and it sorts after every TID above, so the rev gate admits it.
	euRevNotATID = "zz-not-a-tid"

	euCreateTimeUS = int64(1_793_527_200_000_000) // 2026-11-01T10:00:00Z
	euEditTimeUS   = int64(1_793_527_560_500_000) // 2026-11-01T10:06:00.5Z
)

type euStep struct {
	op, rev, cid string
	timeUS       int64
	titleless    bool
	// The one activity this step must enqueue, and its object's `updated`
	// ("" = the key must be absent). The served object must match.
	wantKind    string
	wantUpdated string
	// A redelivery of an already-applied event, handed to the engine directly:
	// the rev gate drops an identical replay, so the engine sees one only when
	// the gate claim was lost after the acceptance committed. It enqueues
	// nothing new, and the served object must still carry no updated.
	redelivered bool
}

func TestEditFederatesUpdatedAndServesTheSameValue(t *testing.T) {
	rows := []struct {
		name  string
		steps []euStep
	}{
		{
			name: "a: create then edit; the edit's rev TID is the updated time",
			steps: []euStep{
				{op: "create", rev: euRevCreate, cid: acPostCID, timeUS: euCreateTimeUS, wantKind: "Create"},
				{op: "update", rev: euRevEdit, cid: acPostCID2, timeUS: euEditTimeUS,
					wantKind: "Update", wantUpdated: "2026-11-01T10:05:00Z"},
			},
		},
		{
			name: "b: first seen as an update, never federated, federates as a Create",
			steps: []euStep{
				{op: "update", rev: euRevEdit, cid: acPostCID2, timeUS: euEditTimeUS, wantKind: "Create"},
			},
		},
		{
			name: "c: create, edit, removal, corrective edit re-federates as a Create",
			steps: []euStep{
				{op: "create", rev: euRevCreate, cid: acPostCID, timeUS: euCreateTimeUS, wantKind: "Create"},
				{op: "update", rev: euRevEdit, cid: acPostCID2, timeUS: euEditTimeUS,
					wantKind: "Update", wantUpdated: "2026-11-01T10:05:00Z"},
				{op: "update", rev: euRevRemove, cid: acPostCID, timeUS: euEditTimeUS + 1, titleless: true,
					wantKind: "Delete"},
				{op: "update", rev: euRevRestore, cid: acPostCID2, timeUS: euEditTimeUS + 2, wantKind: "Create"},
			},
		},
		{
			name: "d: an edit whose rev is not a TID takes the commit time",
			steps: []euStep{
				{op: "create", rev: euRevCreate, cid: acPostCID, timeUS: euCreateTimeUS, wantKind: "Create"},
				{op: "update", rev: euRevNotATID, cid: acPostCID2, timeUS: euEditTimeUS,
					wantKind: "Update", wantUpdated: "2026-11-01T10:06:00.5Z"},
			},
		},
		{
			name: "e: a redelivered first create enqueues nothing and serves no updated",
			steps: []euStep{
				{op: "create", rev: euRevCreate, cid: acPostCID, timeUS: euCreateTimeUS, wantKind: "Create"},
				{op: "create", rev: euRevCreate, cid: acPostCID, timeUS: euCreateTimeUS, redelivered: true},
			},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			conn := acceptanceDB(t)
			ctx := context.Background()
			seedBridgedCommunity(t, conn)
			repos := newRepos(t, conn)
			enq := realEnqueuer(t, conn)
			engine := engineWith(t, conn, repos, enq)
			dispatcher := wireDispatcher(t, conn, engine, enq)
			served := newMinter(t, conn)
			seen := map[string]bool{}

			for i, step := range row.steps {
				record := pv2Record(func(r map[string]any) {
					r["createdAt"] = euCreatedAt
					if step.titleless {
						delete(r, "title")
					}
				})
				event := postEvent(step.op, acPostRKey, step.rev, step.cid, step.timeUS, record)
				if step.redelivered {
					event.Commit.TimeUS = event.TimeUS
					require.NoError(t, engine.AdmitPost(ctx, event.DID, event.Commit),
						"step %d (redelivered %s %s)", i, step.op, step.rev)
					euNoNewActivity(t, conn, seen)
					assert.NotContains(t, euServedObject(t, served), "updated",
						"step %d: the object served after a redelivered Create carries no updated", i)
					continue
				}
				require.NoError(t, dispatcher.HandleEvent(ctx, event), "step %d (%s %s)", i, step.op, step.rev)

				kind, object := euNewActivity(t, conn, seen)
				require.Equal(t, step.wantKind, kind, "step %d enqueues one %s", i, step.wantKind)

				if step.wantKind == "Delete" {
					continue
				}
				doc := euServedObject(t, served)
				if step.wantUpdated == "" {
					assert.NotContains(t, object, "updated",
						"step %d: a %s carries no updated", i, step.wantKind)
					assert.NotContains(t, doc, "updated",
						"step %d: the object served after a %s carries no updated", i, step.wantKind)
					continue
				}
				assert.Equal(t, step.wantUpdated, object["updated"],
					"step %d: the delivered Update{Page}'s updated", i)
				assert.Equal(t, step.wantUpdated, doc["updated"],
					"step %d: the served object's updated equals the delivered one", i)
				assert.Equal(t, "2026-11-01T10:00:00Z", object["published"],
					"step %d: published stays the record's createdAt", i)
			}
		})
	}
}

// euNewActivity returns the kind and inner object of the single activity
// enqueued since the last call, marking it seen.
func euNewActivity(t *testing.T, conn *sql.DB, seen map[string]bool) (string, map[string]any) {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(),
		`SELECT activity_id, kind, payload FROM outbound_activities`)
	require.NoError(t, err)
	defer rows.Close()
	var kind string
	var object map[string]any
	fresh := 0
	for rows.Next() {
		var id, k string
		var payload []byte
		require.NoError(t, rows.Scan(&id, &k, &payload))
		if seen[id] {
			continue
		}
		seen[id] = true
		fresh++
		var activity map[string]any
		require.NoError(t, json.Unmarshal(payload, &activity))
		kind = k
		object, _ = activity["object"].(map[string]any)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 1, fresh, "each step enqueues exactly one activity")
	return kind, object
}

// euNoNewActivity asserts nothing was enqueued since the last call.
func euNoNewActivity(t *testing.T, conn *sql.DB, seen map[string]bool) {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(),
		`SELECT activity_id, kind FROM outbound_activities`)
	require.NoError(t, err)
	defer rows.Close()
	var fresh []string
	for rows.Next() {
		var id, kind string
		require.NoError(t, rows.Scan(&id, &kind))
		if !seen[id] {
			seen[id] = true
			fresh = append(fresh, kind+" "+id)
		}
	}
	require.NoError(t, rows.Err())
	require.Empty(t, fresh, "a redelivery enqueues no new activity")
}

// euServedObject fetches the post the way Lemmy re-fetches it.
func euServedObject(t *testing.T, svc *personas.Service) map[string]any {
	t.Helper()
	path := "/ap/object/" + acAuthorDID + "/social.coves.community.postv2/" + acPostRKey
	req := httptest.NewRequest(http.MethodGet, acUserOrigin+path, nil)
	req.Host = "coves.social"
	rec := httptest.NewRecorder()
	svc.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "GET %s: %s", path, rec.Body.String())
	var doc map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc))
	return doc
}
