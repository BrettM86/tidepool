package personas

import (
	"context"
	"database/sql"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// TestServing_RemovedNativeObjectIs410Tombstone (B5): while a native object's
// takedown source set is non-empty, GET /ap/object/... answers 410 with an AS2
// Tombstone whose id is the object URL.
func TestServing_RemovedNativeObjectIs410Tombstone(t *testing.T) {
	conn := servingObjectTestDB(t)
	truncatePlannedTakedownTables(t, conn)
	svc := newServingService(t, conn)
	seedServedObject(t, conn)
	path := "/ap/object/" + serveObjectDID + "/" + serveObjectCollection + "/" + serveObjectRKey

	rec := serveOnHost(svc, userHost, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec.Code, "precondition: the object is served before the takedown")

	ctx := context.Background()
	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = store.NewOutboundTakedowns(conn).AddSourceTx(ctx, tx, serveObjectATURI,
		store.TakedownSource{Kind: store.TakedownSourceRecordLabel, Key: serveObjectATURI})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	rec = serveOnHost(svc, userHost, http.MethodGet, path, nil)
	require.Equal(t, http.StatusGone, rec.Code, "a removed native object answers 410; body=%s", rec.Body.String())
	doc := decodeJSON(t, rec)
	assert.Equal(t, "Tombstone", doc["type"])
	assert.Equal(t, "https://coves.social/ap/object/did:plc:7iza6de2dwap2sbkpav7c6c6/social.coves.community.comment/3lzserveobj001",
		doc["id"], "the Tombstone's id is the object URL")

	tx, err = conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = store.NewOutboundTakedowns(conn).RemoveSourceTx(ctx, tx, serveObjectATURI,
		store.TakedownSource{Kind: store.TakedownSourceRecordLabel, Key: serveObjectATURI})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	rec = serveOnHost(svc, userHost, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code, "a restored object is served again; body=%s", rec.Body.String())
}

// truncatePlannedTakedownTables truncates the planned takedown tables
// (migration 055) when they exist; a missing one is left for the test to fail on.
func truncatePlannedTakedownTables(t *testing.T, conn *sql.DB) {
	t.Helper()
	for _, table := range []string{"outbound_takedown_sources", "outbound_takedowns"} {
		var exists bool
		require.NoError(t, conn.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists))
		if exists {
			testutil.Truncate(t, conn, table)
		}
	}
}
