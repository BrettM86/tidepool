package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/identity"
	"tidepool/internal/personas"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// noHandleLookups satisfies the dispatcher's required resolver. The commenter
// is minted up front, so a lookup here means the test world is wrong.
type noHandleLookups struct{}

func (noHandleLookups) ResolveDIDHandle(context.Context, string) (string, error) {
	return "", errors.New("no handle lookups in this test: the commenter is already minted")
}

// Task 04, comment wiring: an edit federated from a Jetstream frame carries
// `updated`, taken from the frame's time_us when the rev is not a TID, and the
// served object carries the same value. Driven through the REAL dispatcher and
// the REAL enqueuer, because the dispatcher is what copies time_us onto the
// commit and the comment snapshot is written there.
func TestCommentEditFromJetstreamFederatesAndServesUpdated(t *testing.T) {
	conn := outboundAcceptanceDB(t)
	testutil.Truncate(t, conn,
		"ap_objects", "federation_prefs", "jetstream_record_revs", "jetstream_dead_letters",
		"object_moderation", "community_bans", "outbound_votes")
	ctx := context.Background()

	custodian, err := identity.NewCustodian(outKEK)
	require.NoError(t, err)
	svc, err := personas.New(personas.Options{DB: conn, Custodian: custodian, UserOrigin: outUserOrigin})
	require.NoError(t, err)
	_, err = svc.CreateActorForDID(ctx, outCommenterDID, outCommenterHandle)
	require.NoError(t, err)
	seedAcceptedParent(t, conn)

	enqueuer, err := NewEnqueuer(EnqueuerOptions{
		DB: conn, Translator: NewTranslator(outUserOrigin, ""), Inboxes: staticInbox{inbox: outSharedInbox},
		Actors: store.NewAPActors(conn), UserOrigin: outUserOrigin,
	})
	require.NoError(t, err)
	dispatcher, err := consume.NewDispatcher(consume.Options{
		DB: conn, Actors: svc, Enqueuer: enqueuer, Resolver: noHandleLookups{}, UserOrigin: outUserOrigin,
	})
	require.NoError(t, err)

	commentEvent := func(op, rev, cid, content string, timeUS int64) *consume.JetstreamEvent {
		return &consume.JetstreamEvent{
			DID: outCommenterDID, TimeUS: timeUS, Kind: "commit",
			Commit: &consume.CommitEvent{
				Rev: rev, Operation: op, Collection: "social.coves.community.comment",
				RKey: outCommentRKey, CID: cid,
				Record: map[string]any{
					"$type": "social.coves.community.comment",
					"reply": map[string]any{
						"root":   map[string]any{"uri": outRootATURI, "cid": outRootCID},
						"parent": map[string]any{"uri": outRootATURI, "cid": outRootCID},
					},
					"content":   content,
					"createdAt": "2026-11-01T10:00:00Z",
				},
			},
		}
	}
	served := func() map[string]any {
		t.Helper()
		path := "/ap/object/" + outCommenterDID + "/social.coves.community.comment/" + outCommentRKey
		req := httptest.NewRequest(http.MethodGet, outUserOrigin+path, nil)
		req.Host = outCovesHost
		rec := httptest.NewRecorder()
		svc.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "GET %s: %s", path, rec.Body.String())
		var doc map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc))
		return doc
	}
	storedObject := func(kind string) map[string]any {
		t.Helper()
		var payload []byte
		require.NoError(t, conn.QueryRowContext(ctx,
			`SELECT payload FROM outbound_activities WHERE kind = $1`, kind).Scan(&payload),
			"exactly one %s{Note} must be enqueued", kind)
		var activity map[string]any
		require.NoError(t, json.Unmarshal(payload, &activity))
		return asMap(t, activity["object"], kind+".object")
	}

	// 3mzakfuhe222b = 2026-11-01T10:00:00Z; time_us 1793527200000000 is the same instant.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		commentEvent("create", "3mzakfuhe222b", outCommentCID, "first reply", 1_793_527_200_000_000)))
	assert.NotContains(t, storedObject("Create"), "updated", "a Create{Note} carries no updated")
	assert.NotContains(t, served(), "updated", "before any edit the served Note carries no updated")

	// The edit's rev is not a TID (and sorts after the create's), so updated is
	// the frame's time_us: 1793527560500000 = 2026-11-01T10:06:00.5Z.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		commentEvent("update", "zz-not-a-tid", outRootCID, "edited reply", 1_793_527_560_500_000)))
	update := storedObject("Update")
	assert.Equal(t, "2026-11-01T10:06:00.5Z", update["updated"],
		"the Update{Note} carries the frame's time_us as updated")
	assert.Equal(t, "2026-11-01T10:00:00Z", update["published"], "published stays the record's createdAt")
	assert.Equal(t, "2026-11-01T10:06:00.5Z", served()["updated"],
		"the served Note carries the same updated as the delivered Update{Note}")
}
