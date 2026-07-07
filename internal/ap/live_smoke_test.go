package ap

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLiveSmoke is the manual smoke test from the task-02 definition of
// done: signed GET, WebFinger, and collection paging against a live Lemmy
// instance. It talks to the real lemmy.world, so it is opt-in only —
// CI and normal runs use the recorded fixtures. Run it with:
//
//	TIDEPOOL_LIVE_SMOKE=1 go test ./internal/ap/ -run TestLiveSmoke -v
func TestLiveSmoke(t *testing.T) {
	if testing.Short() || !liveSmokeEnabled() {
		t.Skip("TIDEPOOL_LIVE_SMOKE not set; skipping network smoke test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The signing key is throwaway: lemmy.world does not run authorized
	// fetch, so the signature is not verified server-side — the smoke proves
	// the signed request shape is accepted end to end.
	key := testRSAKey(t)
	client := NewClient(ClientOptions{
		UserAgent: "tidepool/0.1 (+https://tidepool.invalid; task-02 smoke)",
		Signer:    NewSigner("https://tidepool.invalid/actor#main-key", key),
	})

	// WebFinger: community handle → Group actor URL.
	actorURL, err := client.ResolveHandle(ctx, "!technology@lemmy.world")
	require.NoError(t, err)
	assert.Equal(t, "https://lemmy.world/c/technology", actorURL)

	// Signed GET: Group actor.
	group, err := client.FetchActor(ctx, actorURL)
	require.NoError(t, err)
	assert.Equal(t, TypeGroup, group.Type)
	require.NotNil(t, group.PublicKey)

	handle, err := ActorHandle(group)
	require.NoError(t, err)
	assert.Equal(t, "!technology@lemmy.world", handle)

	// Collection paging over the live outbox (single inline page on Lemmy).
	items := 0
	err = client.FetchCollection(ctx, group.Outbox, func(item *Object) error {
		items++
		assert.Equal(t, TypeAnnounce, item.Type)
		return nil
	})
	require.NoError(t, err)
	assert.Greater(t, items, 0, "live outbox should contain announces")
	t.Logf("live smoke OK: actor=%s outbox items=%d", group.ID, items)
}

func liveSmokeEnabled() bool {
	return os.Getenv("TIDEPOOL_LIVE_SMOKE") != ""
}
