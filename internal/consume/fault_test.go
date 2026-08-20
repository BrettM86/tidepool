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

// failingOutboundDeliveries delegates to a real store but fails the opt-out's
// cancellation. That call is the LAST thing the soft opt-out does on the
// rev-gate transaction before it commits, so failing it is the realistic shape
// of "the preference was written, then the unit failed" — the window in which a
// preference written on its own connection would survive an unadvanced gate.
type failingOutboundDeliveries struct {
	store.OutboundDeliveries
	err error
}

func (d *failingOutboundDeliveries) CancelOutwardForActorTx(_ context.Context, _ *sql.Tx, _ string) (int64, error) {
	return 0, d.err
}

// prefProbingDeleter stands in for the destructive seam and answers the one
// question that seam's ORDERING is about: by the time peers are asked to delete
// this user's content, is the user's intent already durable? It reads
// federation_prefs on its OWN connection, exactly as outbound.Purger does.
type prefProbingDeleter struct {
	db            *sql.DB
	called        bool
	sawPreference bool
}

func (d *prefProbingDeleter) DeleteRemoteContent(ctx context.Context, did string) error {
	d.called = true
	var count int
	if err := d.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM federation_prefs WHERE did = $1`, did).Scan(&count); err != nil {
		return err
	}
	d.sawPreference = count > 0
	return nil
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
