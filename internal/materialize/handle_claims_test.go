package materialize

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/identity"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// reservingMinter lets a different actor claim the handle after the real
// minter checked availability but before mintAndUpsert registers its actor.
type reservingMinter struct {
	ActorMinter
	db          *sql.DB
	ownerID     int64
	reserved    bool
	orphanedDID string
}

func (m *reservingMinter) MintActor(ctx context.Context, request identity.MintRequest) (*identity.Identity, error) {
	minted, err := m.ActorMinter.MintActor(ctx, request)
	if err != nil || m.reserved {
		return minted, err
	}
	_, err = m.db.ExecContext(ctx, `INSERT INTO handle_claims (handle, actor_id) VALUES ($1, $2)`, minted.Handle, m.ownerID)
	if err != nil {
		return nil, err
	}
	m.reserved = true
	m.orphanedDID = minted.DID
	return minted, nil
}

func claimedBy(t *testing.T, db *sql.DB, handle string) int64 {
	t.Helper()
	var actorID int64
	require.NoError(t, db.QueryRow(`SELECT actor_id FROM handle_claims WHERE handle = $1`, handle).Scan(&actorID))
	return actorID
}

// newRealMinterHarness uses the shared fixture/DB stack with an actual
// identity.Minter and a local PLC endpoint for genesis operations.
func newRealMinterHarness(t *testing.T) *harness {
	t.Helper()
	h, _ := newRealMinterHarnessCountingPLCRequests(t)
	return h
}

// newRealMinterHarnessCountingPLCRequests is newRealMinterHarness plus a count
// of every request the local PLC endpoint received.
func newRealMinterHarnessCountingPLCRequests(t *testing.T) (*harness, *atomic.Int64) {
	t.Helper()
	h := newHarness(t)
	var plcRequests atomic.Int64
	plc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		plcRequests.Add(1)
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/did:plc:") {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(plc.Close)
	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	custodian, err := identity.NewCustodian(testKEK)
	require.NoError(t, err)
	minter, err := identity.NewMinter(identity.MinterOptions{
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		PLCDirectoryURL: plc.URL,
		HTTPClient:      ap.NewGuardedHTTPClient(true, 10*time.Second),
		Actors:          h.actors, RotationKey: rotationKey, Custodian: custodian,
	})
	require.NoError(t, err)
	h.m.minter = minter
	return h, &plcRequests
}

func TestHandleClaims_MaterializeRetriesReservedHandle(t *testing.T) {
	for _, tc := range []struct {
		name        string
		reserveLate bool
	}{
		{name: "reserved before mint"},
		{name: "reserved after mint before upsert", reserveLate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRealMinterHarness(t)
			db := testutil.DB(t)
			ctx := context.Background()
			owner, err := h.actors.UpsertActor(ctx, store.BridgedActor{
				APActorID: "https://lemmy.world/u/owner", ActorType: store.ActorTypePerson,
				DID: "did:plc:ewvi7nxzyoun6zhxrhs64oiz", Handle: "owner.lemmy-world.tidepool.test",
				SigningKeyEncrypted: []byte("owner-key"), ConsentState: store.ConsentStateOK,
			})
			require.NoError(t, err)

			if !tc.reserveLate {
				_, err = db.ExecContext(ctx, `INSERT INTO handle_claims (handle, actor_id) VALUES ($1, $2)`, "alice.lemmy.world.ap.tidepool.test", owner.ID)
				require.NoError(t, err)
			}

			if tc.reserveLate {
				h.m.minter = &reservingMinter{ActorMinter: h.m.minter, db: db, ownerID: owner.ID}
			}

			bridged, inserted, err := h.m.mintAndUpsert(ctx, &ap.Object{
				ID: "https://lemmy.world/u/alice", PreferredUsername: "alice",
			}, store.ActorTypePerson, "lemmy.world")
			require.NoError(t, err)
			require.True(t, inserted)
			require.Equal(t, "alice-2.lemmy.world.ap.tidepool.test", bridged.Handle)
			stored, err := h.actors.GetByAPActorID(ctx, "https://lemmy.world/u/alice")
			require.NoError(t, err)
			require.Equal(t, "alice-2.lemmy.world.ap.tidepool.test", stored.Handle)
			require.Equal(t, stored.ID, claimedBy(t, db, "alice-2.lemmy.world.ap.tidepool.test"))
			require.Equal(t, owner.ID, claimedBy(t, db, "alice.lemmy.world.ap.tidepool.test"))
		})
	}
}
