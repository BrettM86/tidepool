package consume

import (
	"context"
	"database/sql"

	"tidepool/internal/store"
)

// Fault-injection doubles for the second-opinion hardening round. Each wraps a
// REAL store and fails exactly one method, so a test can prove a handler
// propagates a storage failure (and rolls its own writes back) instead of
// swallowing it into a default value or committing state under an unadvanced
// gate.

// failingEnqueuer fails every EnqueueActivity after recording it. It models
// task 15's queue being briefly unreachable AFTER the consumer has written its
// outbound state — the exact window in which a non-atomic handler leaves a
// committed row behind an unadvanced gate.
type failingEnqueuer struct {
	err   error
	calls int
}

func (e *failingEnqueuer) EnqueueActivity(_ context.Context, _ *sql.Tx, _, _, _ string, _ Intent) error {
	e.calls++
	return e.err
}

// failingOutboundObjects delegates to a real store but fails GetByATURI when
// failGet is set. recordedDepth reads through GetByATURI, so this is how the
// "depth swallow" is forced into the open.
type failingOutboundObjects struct {
	store.OutboundObjects
	err     error
	failGet bool
}

func (o *failingOutboundObjects) GetByATURI(ctx context.Context, atURI string) (*store.OutboundObject, error) {
	if o.failGet {
		return nil, o.err
	}
	return o.OutboundObjects.GetByATURI(ctx, atURI)
}

// failingCommunities delegates to a real store but fails GetByDID. The vote
// delete path resolves the target community through it (communityAPID), where
// a swallowed DB error silently addresses the Undo to nobody.
type failingCommunities struct {
	store.Communities
	err error
}

func (c *failingCommunities) GetByDID(_ context.Context, _ string) (*store.Community, error) {
	return nil, c.err
}
