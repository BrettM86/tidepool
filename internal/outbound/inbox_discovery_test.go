package outbound

import (
	"context"
	"database/sql"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

// Task 15 cycle H: inbox discovery + rotation. The target inbox is the
// community Group's endpoints.sharedInbox, TTL-cached. A 401/404/410 on delivery
// triggers ONE cache-bypassing re-resolution before poisoning — an endpoint
// rotation must not become a poison; a still-bad inbox after the re-resolve
// does poison.

// rotatingResolver serves a stored inbox normally and a DIFFERENT one on the
// cache-bypassing fresh path (the rotation).
type rotatingResolver struct {
	normal     string
	fresh      string
	freshCalls int
}

func (r *rotatingResolver) ResolveInbox(context.Context, string) (string, error) {
	return r.normal, nil
}

func (r *rotatingResolver) ResolveInboxFresh(context.Context, string) (string, error) {
	r.freshCalls++
	return r.fresh, nil
}

func deliveryState(t *testing.T, conn *sql.DB, activityID string) store.DeliveryState {
	t.Helper()
	var s string
	require.NoError(t, conn.QueryRowContext(context.Background(),
		`SELECT state FROM outbound_deliveries WHERE activity_id = $1`, activityID).Scan(&s))
	return store.DeliveryState(s)
}

func TestInboxRotation_ReresolveThenDeliver(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))

	rotated := "https://lemmy.world/c/tech/inbox-v2"
	resolver := &rotatingResolver{normal: wInbox, fresh: rotated}

	// The stored inbox 404s; the rotated inbox accepts.
	sender := &fakeSender{respond: func(_ int, inbox string) error {
		if inbox == rotated {
			return nil
		}
		return httpErr(http.StatusNotFound, "")
	}}

	w := newWorker(t, conn, sender, func(o *WorkerOptions) { o.Inboxes = resolver })
	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)

	assert.Equal(t, 1, resolver.freshCalls,
		"a 404 triggers exactly ONE cache-bypassing re-resolution (endpoint rotation, not poison)")
	assert.Equal(t, store.DeliveryStateDelivered, deliveryState(t, conn, id),
		"after re-resolving to the rotated inbox, the delivery succeeds")
}

func TestInboxRotation_StillBadAfterReresolvePoisons(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))

	resolver := &rotatingResolver{normal: wInbox, fresh: "https://lemmy.world/c/tech/inbox-v2"}
	// Every inbox 404s — the endpoint is genuinely gone, not rotated.
	sender := senderReturning(httpErr(http.StatusNotFound, ""))

	w := newWorker(t, conn, sender, func(o *WorkerOptions) { o.Inboxes = resolver })
	_, err := w.DeliverNext(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, resolver.freshCalls, "the re-resolve is attempted exactly once")
	assert.Equal(t, store.DeliveryStatePoisoned, deliveryState(t, conn, id),
		"a still-404 inbox after the single re-resolve poisons (the endpoint is really gone)")
}

// ---------------------------------------------------------------------------
// The TTL-cached resolver (NewInboxResolver): a cache hit avoids re-fetching
// the Group doc; the fresh path bypasses it.
// ---------------------------------------------------------------------------

type countingFetcher struct {
	calls int
	doc   *ap.Object
}

func (f *countingFetcher) FetchActor(context.Context, string) (*ap.Object, error) {
	f.calls++
	return f.doc, nil
}

func TestInboxResolver_CachesAndBypasses(t *testing.T) {
	shared := "https://lemmy.world/c/tech/inbox"
	fetcher := &countingFetcher{doc: &ap.Object{
		ID:        wCommunityAPID,
		Type:      "Group",
		Inbox:     shared,
		Endpoints: &ap.Endpoints{SharedInbox: shared},
	}}
	resolver := NewInboxResolver(fetcher, time.Minute)
	ctx := context.Background()

	inbox, err := resolver.ResolveInbox(ctx, wCommunityAPID)
	require.NoError(t, err)
	assert.Equal(t, shared, inbox, "the resolver returns endpoints.sharedInbox")

	_, err = resolver.ResolveInbox(ctx, wCommunityAPID)
	require.NoError(t, err)
	assert.Equal(t, 1, fetcher.calls, "a second resolve within the TTL is served from cache (no re-fetch)")

	fresh, ok := resolver.(FreshInboxResolver)
	require.True(t, ok, "the cached resolver must expose the cache-bypassing FreshInboxResolver")
	_, err = fresh.ResolveInboxFresh(ctx, wCommunityAPID)
	require.NoError(t, err)
	assert.Equal(t, 2, fetcher.calls, "the fresh path bypasses the cache and re-fetches the Group doc")
}
